# Databricks notebook source
# MAGIC %md
# MAGIC # Agent with Short-Term Memory - Demo
# MAGIC
# MAGIC This notebook demonstrates how to query the deployed LangGraph agent app with short-term memory.
# MAGIC The agent uses Lakebase (PostgreSQL) to persist conversation state across requests using a `thread_id`.

# COMMAND ----------

# MAGIC %pip install databricks-openai --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Querying Databricks Apps requires OAuth authentication. We use OAuth M2M with a service principal
# MAGIC to authenticate, the same pattern as running `databricks auth login` locally.
# MAGIC
# MAGIC **Prerequisites:** Create a service principal with a secret and store the credentials in a Databricks secret scope.
# MAGIC ```
# MAGIC databricks secrets put-secret <scope> sp-client-id --string-value <client-id>
# MAGIC databricks secrets put-secret <scope> sp-client-secret --string-value <client-secret>
# MAGIC ```

# COMMAND ----------

import os

APP_NAME = "billing-agent"  # Update with your app name
SECRET_SCOPE = "billing-agent"  # Update with your secret scope

# Set OAuth M2M env vars so WorkspaceClient picks them up automatically
os.environ["DATABRICKS_HOST"] = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
os.environ["DATABRICKS_CLIENT_ID"] = dbutils.secrets.get(scope=SECRET_SCOPE, key="sp-client-id")
os.environ["DATABRICKS_CLIENT_SECRET"] = dbutils.secrets.get(scope=SECRET_SCOPE, key="sp-client-secret")

from databricks.sdk import WorkspaceClient
from databricks_openai import DatabricksOpenAI

w = WorkspaceClient()
client = DatabricksOpenAI(workspace_client=w)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Start a new conversation
# MAGIC
# MAGIC The first request creates a new thread. The agent returns a `thread_id` that we use to continue the conversation.

# COMMAND ----------

response = client.responses.create(
    model=f"apps/{APP_NAME}",
    input=[{"role": "user", "content": "Hi, a customer is calling about their bill. Their identification number is 12345678. Can you pull up their account?"}],
)

thread_id = response.custom_outputs["thread_id"]
print(f"Thread ID: {thread_id}")
print(f"Agent: {response.output[0].content[0].text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Continue the conversation (short-term memory)
# MAGIC
# MAGIC Using the same `thread_id`, the agent remembers the customer it already identified, so you don't need to repeat the ID.

# COMMAND ----------

response = client.responses.create(
    model=f"apps/{APP_NAME}",
    input=[{"role": "user", "content": "Can you list their invoices from the last 3 months?"}],
    extra_body={"custom_inputs": {"thread_id": thread_id}},
)

print(f"Agent: {response.output[0].content[0].text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Multi-turn conversation
# MAGIC
# MAGIC The agent maintains full context across multiple turns within the same thread.

# COMMAND ----------

follow_up_questions = [
    "What are the line-item details of the pending invoice?",
    "Can you summarize this customer's invoice situation so far?",
]

for question in follow_up_questions:
    response = client.responses.create(
        model=f"apps/{APP_NAME}",
        input=[{"role": "user", "content": question}],
        extra_body={"custom_inputs": {"thread_id": thread_id}},
    )
    print(f"User: {question}")
    print(f"Agent: {response.output[0].content[0].text}")
    print("-" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. New thread = no memory
# MAGIC
# MAGIC Starting a new conversation (no `thread_id`) creates a fresh session with no prior context.

# COMMAND ----------

response = client.responses.create(
    model=f"apps/{APP_NAME}",
    input=[{"role": "user", "content": "Which customer's invoices were we just reviewing?"}],
)

new_thread_id = response.custom_outputs["thread_id"]
print(f"New Thread ID: {new_thread_id}")
print(f"Agent: {response.output[0].content[0].text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Streaming response
# MAGIC
# MAGIC The agent also supports streaming for real-time output.

# COMMAND ----------

stream = client.responses.create(
    model=f"apps/{APP_NAME}",
    input=[{"role": "user", "content": "In 3 short bullet points, what information do you need from me to look up a customer's invoices?"}],
    stream=True,
)

for event in stream:
    if hasattr(event, "delta") and event.delta:
        print(event.delta, end="", flush=True)

print()
