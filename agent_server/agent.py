import asyncio
import logging
import os
from typing import Any, AsyncGenerator, Optional, Sequence, TypedDict

import litellm
import mlflow
from databricks.sdk import WorkspaceClient
from databricks_langchain import (
    AsyncCheckpointSaver,
    ChatDatabricks,
    DatabricksMCPServer,
    DatabricksMultiServerMCPClient,
)
from fastapi import HTTPException
from langchain.agents import create_agent
from langchain_core.messages import AnyMessage
from langgraph.graph.message import add_messages
from mlflow.genai.agent_server import invoke, stream
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
    to_chat_completions_input,
)
from typing_extensions import Annotated

from agent_server.tools import get_customer_key, get_invoice_info, list_invoices
from agent_server.utils import (
    _get_lakebase_access_error_message,
    _get_or_create_thread_id,
    get_databricks_host_from_env,
    process_agent_astream_events,
)

logger = logging.getLogger(__name__)
mlflow.langchain.autolog()
logging.getLogger("mlflow.utils.autologging_utils").setLevel(logging.ERROR)
litellm.suppress_debug_info = True
sp_workspace_client = WorkspaceClient()

# checkpointer.setup() runs the LangGraph checkpoint-table migrations. It is not safe to call
# concurrently (the UI fires several requests in parallel on the first turn, which race on
# inserting checkpoint_migrations rows). Run it once per process under a lock, and treat an
# "already migrated" UniqueViolation as success (covers multiple replicas/workers).
_checkpointer_setup_done = False
_checkpointer_setup_lock = asyncio.Lock()


async def _ensure_checkpointer_setup(checkpointer: Any) -> None:
    global _checkpointer_setup_done
    if _checkpointer_setup_done:
        return
    async with _checkpointer_setup_lock:
        if _checkpointer_setup_done:
            return
        try:
            await checkpointer.setup()
        except Exception as e:
            # psycopg raises UniqueViolation when the migrations already exist; that's benign.
            if e.__class__.__name__ != "UniqueViolation":
                raise
            logger.info("Checkpoint migrations already applied; skipping setup.")
        _checkpointer_setup_done = True


############################################
# Configuration
############################################
LLM_ENDPOINT_NAME = "databricks-claude-sonnet-4-5"
SYSTEM_PROMPT = """You are a call center assistant that helps representatives resolve customer complaints \
related to their monthly invoices. You only have access to invoices from the last 3 months.

Follow this workflow:
1. The representative will provide a customer identifier: either a phone number or an identification number.
2. Use the get_customer_key tool to look up the customer key based on the identifier provided. \
   This stores the customer key in the session so all subsequent tools use it automatically.
3. Once the customer is identified, use list_invoices to show the available invoices. \
   You do NOT need to pass the customer key — it is read from the session automatically.
4. Use get_invoice_info to retrieve detailed information about a specific invoice when needed. \
   Again, the customer key is automatic — just pass the invoice key.

IMPORTANT - Customer identification is mandatory:
- If the representative asks about an invoice or any customer-related information WITHOUT first providing \
  a phone number or identification number, you MUST ask them to provide one before proceeding. \
  Do NOT attempt to call list_invoices or get_invoice_info without first having identified the customer \
  through get_customer_key.
- Example response: "Before I can look up invoice information, I need the customer's identification number \
  or phone number. Could you provide one of those?"

Customer switch:
- If the representative provides a NEW phone number or identification number during the conversation, \
  call get_customer_key again to switch to the new customer. This updates the session automatically.

Guidelines:
- Always start by identifying the customer before looking up invoices.
- Be concise and focused on resolving the complaint.
- If a customer or invoice is not found, let the representative know clearly.
"""

LAKEBASE_ENDPOINT = os.getenv("LAKEBASE_ENDPOINT") or None
LAKEBASE_INSTANCE_NAME = os.getenv("LAKEBASE_INSTANCE_NAME") or None

if not LAKEBASE_ENDPOINT and not LAKEBASE_INSTANCE_NAME:
    raise ValueError(
        "Lakebase configuration is required but not set. "
        "Please set one of the following in your environment:\n"
        "  Option 1 (autoscaling): LAKEBASE_ENDPOINT (auto-injected via value_from postgres resource)\n"
        "  Option 2 (provisioned): LAKEBASE_INSTANCE_NAME=<your-instance-name>\n"
    )


class StatefulAgentState(TypedDict, total=False):
    messages: Annotated[Sequence[AnyMessage], add_messages]
    customer_key: Optional[str]
    custom_inputs: dict[str, Any]
    custom_outputs: dict[str, Any]


def init_mcp_client(workspace_client: WorkspaceClient) -> DatabricksMultiServerMCPClient:
    host_name = get_databricks_host_from_env()
    return DatabricksMultiServerMCPClient(
        [
            DatabricksMCPServer(
                name="system-ai",
                url=f"{host_name}/api/2.0/mcp/functions/system/ai",
                workspace_client=workspace_client,
            ),
        ]
    )


async def init_agent(
    workspace_client: Optional[WorkspaceClient] = None,
    checkpointer: Optional[Any] = None,
):
    tools = [get_customer_key, list_invoices, get_invoice_info]
    # To use MCP server tools instead, uncomment the below lines:
    # mcp_client = init_mcp_client(workspace_client or sp_workspace_client)
    # try:
    #     tools.extend(await mcp_client.get_tools())
    # except Exception:
    #     logger.warning("Failed to fetch MCP tools. Continuing without MCP tools.", exc_info=True)

    model = ChatDatabricks(endpoint=LLM_ENDPOINT_NAME)

    return create_agent(
        model=model,
        tools=tools,
        system_prompt=SYSTEM_PROMPT,
        checkpointer=checkpointer,
        state_schema=StatefulAgentState,
    )


@invoke()
async def invoke_handler(request: ResponsesAgentRequest) -> ResponsesAgentResponse:
    thread_id = _get_or_create_thread_id(request)
    request.custom_inputs = dict(request.custom_inputs or {})
    request.custom_inputs["thread_id"] = thread_id

    outputs = [
        event.item
        async for event in stream_handler(request)
        if event.type == "response.output_item.done"
    ]

    return ResponsesAgentResponse(output=outputs, custom_outputs={"thread_id": thread_id})


@stream()
async def stream_handler(
    request: ResponsesAgentRequest,
) -> AsyncGenerator[ResponsesAgentStreamEvent, None]:
    thread_id = _get_or_create_thread_id(request)
    mlflow.update_current_trace(metadata={"mlflow.trace.session": thread_id})

    config = {"configurable": {"thread_id": thread_id}}
    # When a thread_id is provided, the checkpointer already has conversation history.
    # Only send the last user message to avoid duplicating messages.
    ci = dict(request.custom_inputs or {})
    has_existing_thread = ci.get("thread_id") or (
        request.context and getattr(request.context, "conversation_id", None)
    )
    if has_existing_thread and len(request.input) > 1:
        messages = to_chat_completions_input([request.input[-1].model_dump()])
    else:
        messages = to_chat_completions_input([i.model_dump() for i in request.input])
    input_state: dict[str, Any] = {
        "messages": messages,
        "custom_inputs": ci,
    }

    try:
        async with AsyncCheckpointSaver(
            autoscaling_endpoint=LAKEBASE_ENDPOINT,
            instance_name=LAKEBASE_INSTANCE_NAME,
        ) as checkpointer:
            await _ensure_checkpointer_setup(checkpointer)
            # By default, uses service principal credentials.
            # For on-behalf-of user authentication, pass get_user_workspace_client() to init_agent.
            agent = await init_agent(checkpointer=checkpointer)

            async for event in process_agent_astream_events(
                agent.astream(
                    input_state,
                    config,
                    stream_mode=["updates", "messages"],
                )
            ):
                yield event
    except Exception as e:
        error_msg = str(e).lower()
        # Check for Lakebase access/connection errors
        if any(keyword in error_msg for keyword in ["lakebase", "pg_hba", "postgres", "database instance"]):
            logger.error(f"Lakebase access error: {e}")
            lakebase_desc = LAKEBASE_INSTANCE_NAME or LAKEBASE_ENDPOINT
            raise HTTPException(
                status_code=503, detail=_get_lakebase_access_error_message(lakebase_desc)
            ) from e
        raise
