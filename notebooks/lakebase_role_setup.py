# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase Role Setup
# MAGIC
# MAGIC Creates a Postgres role for the app's service principal and grants schema-level permissions
# MAGIC on the Lakebase autoscaling instance. Run this **after** deploying the app with `databricks bundle deploy`.
# MAGIC
# MAGIC Table creation is handled by the app itself on startup, so the SP owns all tables.

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.81.0" "psycopg[binary]>=3.0" --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Update these values for your deployment:
# MAGIC - **APP_NAME**: your Databricks App name
# MAGIC - **PROJECT_ID**: Lakebase project ID (from `databricks postgres list-projects`)

# COMMAND ----------

APP_NAME = "billing-agent"
PROJECT_ID = "billing-db-dev-manffred-calvosanchez"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Get the app's service principal client ID

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

app = w.apps.get(APP_NAME)
sp = app.service_principal_client_id
print(f"App: {APP_NAME}")
print(f"Service Principal Client ID: {sp}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Postgres role for the service principal

# COMMAND ----------

from databricks.sdk.service.postgres import (
    Role, RoleRoleSpec, RoleMembershipRole,
    RoleIdentityType, RoleAuthMethod,
)

branch = f"projects/{PROJECT_ID}/branches/production"

try:
    w.postgres.create_role(
        parent=branch,
        role_id="app-sp",
        role=Role(
            spec=RoleRoleSpec(
                postgres_role=sp,
                identity_type=RoleIdentityType.SERVICE_PRINCIPAL,
                auth_method=RoleAuthMethod.LAKEBASE_OAUTH_V1,
                membership_roles=[RoleMembershipRole.DATABRICKS_SUPERUSER],
            )
        ),
    )
    print("Role created successfully")
except Exception as e:
    if "already exists" in str(e).lower():
        print("Role already exists, skipping")
    else:
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Get Lakebase endpoint host

# COMMAND ----------

endpoint_name = f"{branch}/endpoints/primary"
endpoint = w.postgres.get_endpoint(name=endpoint_name)
host = endpoint.status.hosts.host
print(f"PGHOST: {host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Connect and grant schema-level permissions

# COMMAND ----------

import psycopg

cred = w.postgres.generate_database_credential(endpoint=endpoint_name)
user = w.current_user.me()

conn = psycopg.connect(
    host=host,
    dbname="databricks_postgres",
    user=user.user_name,
    password=cred.token,
    sslmode="require",
)
conn.autocommit = True
cur = conn.cursor()

grants = [
    (f'GRANT CONNECT ON DATABASE databricks_postgres TO "{sp}"', "CONNECT on database"),
    (f'GRANT CREATE ON DATABASE databricks_postgres TO "{sp}"', "CREATE on database"),
    ("CREATE SCHEMA IF NOT EXISTS public", "Ensure public schema exists"),
    (f'GRANT USAGE ON SCHEMA public TO "{sp}"', "USAGE on public"),
    (f'GRANT CREATE ON SCHEMA public TO "{sp}"', "CREATE on public"),
    (f'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "{sp}"', "ALL on existing tables"),
    (f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO "{sp}"', "Default privileges on future tables"),
]

for sql, desc in grants:
    try:
        cur.execute(sql)
        print(f"OK: {desc}")
    except Exception as e:
        print(f"WARN: {desc} -> {e}")

conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 50)
print("Done! Schema-level permissions granted.")
print("The app will create its own tables on startup.")
print("=" * 50)
print()
print(f"PGHOST:               {host}")
print(f"PGUSER:               {sp}")
print(f"PGDATABASE:           databricks_postgres")
print(f"PGPORT:               5432")
print(f"PGSSLMODE:            require")
print(f"LAKEBASE_ENDPOINT:    {endpoint_name}")
