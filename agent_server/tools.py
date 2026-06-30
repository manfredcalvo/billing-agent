import json
import logging
import os
from typing import Annotated, Optional

import psycopg
from databricks.sdk import WorkspaceClient
from langchain_core.messages import ToolMessage
from langchain_core.tools import tool
from langgraph.prebuilt import InjectedState
from langgraph.types import Command
from psycopg_pool import ConnectionPool

logger = logging.getLogger(__name__)

# Set to True to use mock data instead of the database
USE_MOCK_DATA = os.getenv("USE_MOCK_DATA", "true").lower() == "true"

# Mock data for demo/testing
# Customers: identifier (ID or phone) -> customer_key
MOCK_CUSTOMERS = {
    # Customer 1: Maria Lopez
    "12345678": "CK-900",
    "+51999888777": "CK-900",
    # Customer 2: Carlos Gutierrez
    "87654321": "CK-901",
    "+51998765432": "CK-901",
    # Customer 3: Ana Torres
    "11223344": "CK-902",
    "+51987654321": "CK-902",
}

MOCK_INVOICES = {
    "CK-900": [
        {"invoice_key": "001", "invoice_date": "2026-02-15", "total_amount": 150.00, "status": "paid", "currency": "PEN"},
        {"invoice_key": "002", "invoice_date": "2026-03-15", "total_amount": 175.50, "status": "paid", "currency": "PEN"},
        {"invoice_key": "003", "invoice_date": "2026-04-15", "total_amount": 200.25, "status": "pending", "currency": "PEN"},
    ],
    "CK-901": [
        {"invoice_key": "101", "invoice_date": "2026-02-10", "total_amount": 89.90, "status": "paid", "currency": "PEN"},
        {"invoice_key": "102", "invoice_date": "2026-03-10", "total_amount": 89.90, "status": "paid", "currency": "PEN"},
        {"invoice_key": "103", "invoice_date": "2026-04-10", "total_amount": 134.90, "status": "overdue", "currency": "PEN"},
    ],
    "CK-902": [
        {"invoice_key": "201", "invoice_date": "2026-02-20", "total_amount": 250.00, "status": "paid", "currency": "PEN"},
        {"invoice_key": "202", "invoice_date": "2026-03-20", "total_amount": 250.00, "status": "paid", "currency": "PEN"},
        {"invoice_key": "203", "invoice_date": "2026-04-20", "total_amount": 312.50, "status": "pending", "currency": "PEN"},
    ],
}

MOCK_INVOICE_DETAILS = {
    # Customer 1: Maria Lopez (CK-900) — Plan Basico
    "CK-900": {
        "001": {
            "invoice_key": "001",
            "invoice_date": "2026-02-15",
            "due_date": "2026-03-01",
            "total_amount": 150.00,
            "currency": "PEN",
            "status": "paid",
            "payment_date": "2026-02-28",
            "line_items": [
                {"description": "Plan Basico - Febrero 2026", "amount": 120.00},
                {"description": "Cargo por datos adicionales", "amount": 20.00},
                {"description": "Impuestos", "amount": 10.00},
            ],
        },
        "002": {
            "invoice_key": "002",
            "invoice_date": "2026-03-15",
            "due_date": "2026-04-01",
            "total_amount": 175.50,
            "currency": "PEN",
            "status": "paid",
            "payment_date": "2026-03-30",
            "line_items": [
                {"description": "Plan Basico - Marzo 2026", "amount": 120.00},
                {"description": "Cargo por datos adicionales", "amount": 35.00},
                {"description": "Cargo por roaming internacional", "amount": 10.50},
                {"description": "Impuestos", "amount": 10.00},
            ],
        },
        "003": {
            "invoice_key": "003",
            "invoice_date": "2026-04-15",
            "due_date": "2026-05-01",
            "total_amount": 200.25,
            "currency": "PEN",
            "status": "pending",
            "payment_date": None,
            "line_items": [
                {"description": "Plan Basico - Abril 2026", "amount": 120.00},
                {"description": "Cargo por datos adicionales", "amount": 45.00},
                {"description": "Cargo por llamadas internacionales", "amount": 22.25},
                {"description": "Impuestos", "amount": 13.00},
            ],
        },
    },
    # Customer 2: Carlos Gutierrez (CK-901) — Plan Economico
    "CK-901": {
        "101": {
            "invoice_key": "101",
            "invoice_date": "2026-02-10",
            "due_date": "2026-02-25",
            "total_amount": 89.90,
            "currency": "PEN",
            "status": "paid",
            "payment_date": "2026-02-24",
            "line_items": [
                {"description": "Plan Economico - Febrero 2026", "amount": 69.90},
                {"description": "Cargo por SMS premium", "amount": 12.00},
                {"description": "Impuestos", "amount": 8.00},
            ],
        },
        "102": {
            "invoice_key": "102",
            "invoice_date": "2026-03-10",
            "due_date": "2026-03-25",
            "total_amount": 89.90,
            "currency": "PEN",
            "status": "paid",
            "payment_date": "2026-03-20",
            "line_items": [
                {"description": "Plan Economico - Marzo 2026", "amount": 69.90},
                {"description": "Cargo por SMS premium", "amount": 12.00},
                {"description": "Impuestos", "amount": 8.00},
            ],
        },
        "103": {
            "invoice_key": "103",
            "invoice_date": "2026-04-10",
            "due_date": "2026-04-25",
            "total_amount": 134.90,
            "currency": "PEN",
            "status": "overdue",
            "payment_date": None,
            "line_items": [
                {"description": "Plan Economico - Abril 2026", "amount": 69.90},
                {"description": "Cargo por datos adicionales", "amount": 25.00},
                {"description": "Cargo por llamadas a fijos", "amount": 18.00},
                {"description": "Recargo por mora", "amount": 12.00},
                {"description": "Impuestos", "amount": 10.00},
            ],
        },
    },
    # Customer 3: Ana Torres (CK-902) — Plan Premium
    "CK-902": {
        "201": {
            "invoice_key": "201",
            "invoice_date": "2026-02-20",
            "due_date": "2026-03-05",
            "total_amount": 250.00,
            "currency": "PEN",
            "status": "paid",
            "payment_date": "2026-03-01",
            "line_items": [
                {"description": "Plan Premium - Febrero 2026", "amount": 199.90},
                {"description": "Seguro de equipo", "amount": 25.00},
                {"description": "Impuestos", "amount": 25.10},
            ],
        },
        "202": {
            "invoice_key": "202",
            "invoice_date": "2026-03-20",
            "due_date": "2026-04-05",
            "total_amount": 250.00,
            "currency": "PEN",
            "status": "paid",
            "payment_date": "2026-04-03",
            "line_items": [
                {"description": "Plan Premium - Marzo 2026", "amount": 199.90},
                {"description": "Seguro de equipo", "amount": 25.00},
                {"description": "Impuestos", "amount": 25.10},
            ],
        },
        "203": {
            "invoice_key": "203",
            "invoice_date": "2026-04-20",
            "due_date": "2026-05-05",
            "total_amount": 312.50,
            "currency": "PEN",
            "status": "pending",
            "payment_date": None,
            "line_items": [
                {"description": "Plan Premium - Abril 2026", "amount": 199.90},
                {"description": "Seguro de equipo", "amount": 25.00},
                {"description": "Cargo por roaming internacional", "amount": 45.00},
                {"description": "Compra de contenido digital", "amount": 15.50},
                {"description": "Impuestos", "amount": 27.10},
            ],
        },
    },
}


_w = WorkspaceClient()


class _OAuthConnection(psycopg.Connection):
    """Generates a fresh OAuth token on every new connection the pool creates."""
    @classmethod
    def connect(cls, conninfo="", **kwargs):
        endpoint_name = os.environ["LAKEBASE_ENDPOINT"]
        credential = _w.postgres.generate_database_credential(endpoint=endpoint_name)
        kwargs["password"] = credential.token
        return super().connect(conninfo, **kwargs)


def _make_pool() -> "ConnectionPool | None":
    """Build the connection pool at startup. Returns None if Lakebase is not configured."""
    endpoint_name = os.getenv("LAKEBASE_ENDPOINT")
    if not endpoint_name:
        return None
    try:
        endpoint = _w.postgres.get_endpoint(name=endpoint_name)
        host = endpoint.status.hosts.host
        user = _w.current_user.me().user_name
        return ConnectionPool(
            conninfo=f"dbname=databricks_postgres user={user} host={host} port=5432 sslmode=require",
            connection_class=_OAuthConnection,
            min_size=1,
            max_size=10,
            max_lifetime=2700,  # 45 min — recycle connections before 1-hour OAuth token expires
            open=True,
        )
    except Exception:
        logger.warning("Lakebase not available; tools will use mock data.", exc_info=True)
        return None


_pool: "ConnectionPool | None" = _make_pool()


def _make_command(customer_key: Optional[str], content: str, state: dict) -> Command:
    """Build a Command that updates customer_key and includes the required ToolMessage."""
    # Extract tool_call_id from the last AI message's tool calls
    tool_call_id = ""
    for msg in reversed(state.get("messages", [])):
        if hasattr(msg, "tool_calls") and msg.tool_calls:
            for tc in msg.tool_calls:
                if tc.get("name") == "get_customer_key":
                    tool_call_id = tc.get("id", "")
                    break
            if tool_call_id:
                break

    return Command(
        update={
            "customer_key": customer_key,
            "messages": [ToolMessage(content=content, tool_call_id=tool_call_id)],
        },
    )


@tool
def get_customer_key(identifier: str, identifier_type: str, state: Annotated[dict, InjectedState]) -> Command:
    """Look up a customer key by phone number or identification number.
    This stores the customer key in the session for use by other tools.

    Args:
        identifier: The customer's phone number or identification number.
        identifier_type: The type of identifier, either "phone_number" or "identification".
    """
    if identifier_type not in ("phone_number", "identification"):
        return _make_command(None, json.dumps({"error": "identifier_type must be 'phone_number' or 'identification'"}), state)

    if USE_MOCK_DATA:
        customer_key = MOCK_CUSTOMERS.get(identifier)
        if customer_key:
            return _make_command(customer_key, json.dumps({"customer_key": customer_key}), state)
        return _make_command(None, json.dumps({"error": f"No customer found with {identifier_type}: {identifier}"}), state)

    try:
        with _pool.connection() as conn:
            with conn.cursor() as cur:
                # TODO: fill in actual SQL query
                query = "SELECT 1"  # TODO: replace with actual query
                cur.execute(query, (identifier,))
                row = cur.fetchone()

        if row:
            customer_key = str(row[0])
            return _make_command(customer_key, json.dumps({"customer_key": customer_key}), state)
        else:
            return _make_command(None, json.dumps({"error": f"No customer found with {identifier_type}: {identifier}"}), state)
    except Exception as e:
        logger.error(f"Error looking up customer: {e}")
        return _make_command(None, json.dumps({"error": f"Failed to look up customer: {str(e)}"}), state)


@tool
def list_invoices(state: Annotated[dict, InjectedState]) -> str:
    """List all invoices from the last 3 months for the current customer.
    The customer key is read automatically from the session.
    """
    customer_key = state.get("customer_key")
    if not customer_key:
        return json.dumps({"error": "No customer identified yet. Please use get_customer_key first."})

    if USE_MOCK_DATA:
        invoices = MOCK_INVOICES.get(customer_key)
        if invoices:
            return json.dumps({"customer_key": customer_key, "invoices": invoices})
        return json.dumps({"error": f"No invoices found for customer '{customer_key}' in the last 3 months"})

    try:
        with _pool.connection() as conn:
            with conn.cursor() as cur:
                # TODO: fill in actual SQL query
                query = "SELECT 1"  # TODO: replace with actual query
                cur.execute(query, (customer_key,))
                rows = cur.fetchall()

        if rows:
            invoices = [str(row) for row in rows]
            return json.dumps({"customer_key": customer_key, "invoices": invoices})
        else:
            return json.dumps({"error": f"No invoices found for customer '{customer_key}' in the last 3 months"})
    except Exception as e:
        logger.error(f"Error listing invoices: {e}")
        return json.dumps({"error": f"Failed to list invoices: {str(e)}"})


@tool
def get_invoice_info(invoice_key: str, state: Annotated[dict, InjectedState]) -> str:
    """Retrieve detailed invoice information for the current customer.
    The customer key is read automatically from the session.

    Args:
        invoice_key: The invoice key to look up.
    """
    customer_key = state.get("customer_key")
    if not customer_key:
        return json.dumps({"error": "No customer identified yet. Please use get_customer_key first."})

    if USE_MOCK_DATA:
        customer_invoices = MOCK_INVOICE_DETAILS.get(customer_key, {})
        invoice = customer_invoices.get(invoice_key)
        if invoice:
            return json.dumps({"invoice": invoice})
        return json.dumps({"error": f"No invoice found with key '{invoice_key}' for customer '{customer_key}'"})

    try:
        with _pool.connection() as conn:
            with conn.cursor() as cur:
                # TODO: fill in actual SQL query
                query = "SELECT 1"  # TODO: replace with actual query
                cur.execute(query, (customer_key, invoice_key))
                row = cur.fetchone()

        if row:
            return json.dumps({"invoice": str(row)})
        else:
            return json.dumps({"error": f"No invoice found with key '{invoice_key}' for customer '{customer_key}'"})
    except Exception as e:
        logger.error(f"Error looking up invoice: {e}")
        return json.dumps({"error": f"Failed to look up invoice: {str(e)}"})
