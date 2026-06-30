#!/usr/bin/env bash
set -euo pipefail

# End-to-end deploy for the single-app billing-agent bundle.
#   ./scripts/deploy.sh [profile] [target]
# Defaults: profile=andreas_workspace, target=dev
#
# Steps:
#   1. Initial deploy (creates the Lakebase project; the app fails until the DB ID is set)
#   2. Read the generated Lakebase database ID and write it into databricks.yml
#   3. Redeploy (creates the app)
#   4. Start the app (creates its service principal)
#   5. Grant the app SP a Postgres role + schema permissions on Lakebase
#
# The agent creates its LangGraph checkpoint tables on the first chat through the UI;
# because the app SP owns those tables it retains full access — no cross-grant needed.

PROFILE="${1:-andreas_workspace}"
TARGET="${2:-dev}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

log() { echo ""; echo ">>> $*"; }

cd "$ROOT_DIR"

log "Getting workspace user..."
CURRENT_USER=$(databricks current-user me --profile "$PROFILE" --output json)
USERNAME=$(echo "$CURRENT_USER" | jq -r '.userName')
DISPLAY_NAME=$(echo "$CURRENT_USER" | jq -r '.displayName')

# For human users userName contains '@' — derive domain_friendly_name from the local part.
# For service principals userName is a UUID — DABs uses displayName instead.
if echo "$USERNAME" | grep -q '@'; then
  DOMAIN_FRIENDLY=$(echo "$USERNAME" | sed 's/@.*//' | tr '.' '-')
  DOMAIN_UNDERSCORE=$(echo "$USERNAME" | sed 's/@.*//' | tr '.' '_')
else
  DOMAIN_FRIENDLY=$(echo "$DISPLAY_NAME" | tr '[:upper:]' '[:lower:]' | tr ' .' '-' | tr -s '-')
  DOMAIN_UNDERSCORE=$(echo "$DISPLAY_NAME" | tr '[:upper:]' '[:lower:]' | tr ' .' '_' | tr -s '_')
fi

PROJECT_ID="billing-db-${TARGET}-${DOMAIN_FRIENDLY}"
echo "User: $USERNAME  |  Display: $DISPLAY_NAME  |  Target: $TARGET  |  Project ID: $PROJECT_ID"

# Pre-flight: clean up stale resources left over from a previous `bundle destroy`.
# bundle destroy moves the MLflow experiment to the Workspace Trash; the Terraform provider
# then refuses to recreate an experiment whose workspace node is in Trash.
log "Pre-flight — cleaning up stale resources..."

EXP_TRASH_PATH="/Users/${USERNAME}/Trash/[${TARGET} ${DOMAIN_UNDERSCORE}] billing-agent-exp"
databricks workspace delete "${EXP_TRASH_PATH}" --profile "$PROFILE" --recursive 2>/dev/null \
  && echo "  Removed stale experiment from workspace trash" \
  || echo "  Nothing in workspace trash to clean up"

STATE_FILE="${ROOT_DIR}/.databricks/bundle/${TARGET}/terraform/terraform.tfstate"
python3 - "$STATE_FILE" <<'EOF'
import json, os, sys

state_file = sys.argv[1]
if not os.path.exists(state_file):
    print("  No Terraform state found, skipping state cleanup")
    sys.exit(0)

with open(state_file) as f:
    state = json.load(f)

before = len(state.get("resources", []))
state["resources"] = [r for r in state.get("resources", [])
                      if not (r.get("type") == "databricks_mlflow_experiment"
                              and r.get("name") == "agent_experiment")]
removed = before - len(state.get("resources", []))
if removed:
    state["serial"] = state.get("serial", 0) + 1
    with open(state_file, "w") as f:
        json.dump(state, f, indent=2)
    print("  Removed experiment from Terraform state (will be recreated fresh)")
else:
    print("  No stale experiment in Terraform state")
EOF

log "Step 1 — Initial deploy (creates Lakebase project; app may fail until DB ID is set)..."
# Pass a placeholder DB ID so the stale default in databricks.yml doesn't fail Terraform
# before the postgres project even exists. The app failing here is expected; fixed in step 2.
databricks bundle deploy -t "$TARGET" --profile "$PROFILE" \
  --var="lakebase_database_id=placeholder" 2>&1 | tee /tmp/billing-step1-deploy.log || true

log "Step 2 — Getting Lakebase database ID..."
if ! databricks postgres list-databases "projects/${PROJECT_ID}/branches/production" \
    --profile "$PROFILE" --output json 2>/dev/null | jq -e '.[0]' > /dev/null 2>&1; then
  echo ""
  echo "ERROR: Lakebase project '${PROJECT_ID}' was not created in Step 1."
  echo "       Likely causes: the principal lacks permission to create Lakebase projects,"
  echo "       or the deploy failed for another reason. Step 1 output:"
  cat /tmp/billing-step1-deploy.log
  exit 1
fi
DB_ID=$(databricks postgres list-databases \
  "projects/${PROJECT_ID}/branches/production" \
  --profile "$PROFILE" --output json | jq -r '.[0].name | split("/") | last')
if [ -z "$DB_ID" ] || [ "$DB_ID" = "null" ]; then
  echo "ERROR: Could not get database ID — the Lakebase project may not have been created."
  exit 1
fi
echo "Database ID: $DB_ID"
# Write the DB ID into the lakebase_database_id default (handles any existing value)
awk -v id="$DB_ID" '
  /lakebase_database_id:/ { in_block=1 }
  in_block && /default:/ { sub(/default: .*/, "default: \"" id "\""); in_block=0 }
  { print }
' databricks.yml > databricks.yml.tmp && mv databricks.yml.tmp databricks.yml

log "Step 3 — Redeploying with database ID (creates the app)..."
databricks bundle deploy -t "$TARGET" --profile "$PROFILE"

log "Step 4 — Starting the app..."
databricks bundle run billing_agent -t "$TARGET" --profile "$PROFILE"

log "Step 5 — Granting Lakebase permissions to the app service principal..."
APP_SP=$(databricks apps get billing-agent \
  --profile "$PROFILE" --output json | jq -r '.service_principal_client_id')
echo "App SP: $APP_SP"
uv run python scripts/lakebase-role-setup.py \
  --profile "$PROFILE" --project-id "$PROJECT_ID" --sp-client-id "$APP_SP"

APP_URL=$(databricks apps get billing-agent --profile "$PROFILE" --output json | jq -r '.url')
echo ""
echo "============================================"
echo "Deployment complete!"
echo "Billing agent (chat UI): $APP_URL"
echo "============================================"
