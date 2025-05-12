import logging
from enum import Enum
from dapr.ext.workflow import DaprWorkflowContext
from dapr_agents import Agent, ToolCallAgent, AgentTool
from dapr_agents.tool.mcp import MCPClient
from dapr_agents.workflow import WorkflowApp, workflow, task
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from get_schema import get_table_schema_as_dict
import asyncio

logging.basicConfig(level=logging.INFO)
load_dotenv()

async def get_db_agent_with_tools() -> ToolCallAgent:
    """Fetches all tools from the MCP server."""
    client = MCPClient()
    await client.connect_sse(
        server_name="local",  # Unique name you assign to this server
        url="http://0.0.0.0:8100/sse",  # MCP SSE endpoint
        headers=None,  # Optional HTTP headers if needed
    )

    tools = client.get_all_tools()
    await client.close()

    return Agent(
        name="SQL",
        role="Database Expert",
        instructions=[
            "You are an assistant designed to translate human readable text to postgresql queries. "
            "Your primary goal is to provide accurate SQL queries based on the user request. "
            "If something is unclear or you need more context, ask thoughtful clarifying questions."
            "Use tools to run the queries and return the results.",
        ],
        tools=tools,
    )

db_agent = asyncio.run(get_db_agent_with_tools())

# Define query types for the router
class QueryType(str, Enum):
    DB = "database"
    NON_DB = "non-database"


# Define models for routing
class RoutingDecision(BaseModel):
    query_type: QueryType = Field(..., description="The type of user query")
    explanation: str = Field(..., description="Explanation of why this routing was chosen")


def create_prompt_for_llm(user_question):
    prompt = "Here is the schema for the tables in the database:\n\n"

    schema_data = get_table_schema_as_dict()
    # Add schema information to the prompt
    for table, columns in schema_data.items():
        prompt += f"Table {table}:\n"
        for col in columns:
            prompt += f"  - {col['column_name']} ({col['data_type']}), Nullable: {col['is_nullable']}, Default: {col['column_default']}\n"

    # Add the user's question for context
    prompt += f"\nUser's question: {user_question}\n"
    prompt += "Generate the postgres SQL query to answer the user's question. Return only the query string and nothing else."

    return prompt


# Define Workflow logic
@workflow(name="query_assistant_workflow")
def query_assistant_workflow(ctx: DaprWorkflowContext, input_params: dict):
    """Defines a Dapr workflow that routes a query to specialized handlers."""

    # Extract the user query
    user_query = input_params.get("query")
    logging.info(f"Received query: {user_query}")

    # Route the query to the appropriate handler using an LLM
    routing_result = yield ctx.call_activity(
        route_query,
        input={"query": user_query}
    )

    query_type = routing_result.get("query_type")
    logging.info(f"Query classified as: {query_type}")

    # Route to the appropriate specialized handler based on the classification
    if query_type == QueryType.DB:
        prepared_query = create_prompt_for_llm(user_query)
        generated_query = yield ctx.call_activity(
            prepare_database_query,
            input={"query": prepared_query}
        )
        response = yield ctx.call_activity(
            handle_database_query,
            input={"query": generated_query}
        )
        logging.info(f"DB QUERY RESPONSE:")
        logging.info(response)
    else:
        response = yield ctx.call_activity(
            handle_non_database_query,
            input={"query": user_query}
        )
        logging.info(f"NON_DB QUERY RESPONSE:")
        logging.info(response)

    return response


@task(
    description="Classify this query into one of these categories: database (for questions about tables, schema, indexes, partitioning, normalization or similar things), non-database (for questions about hotels, rentals, or places to stay), or transportation (for questions about anything not related to database). Query: {query}")
def route_query(query: str) -> RoutingDecision:
    # This will be implemented as an LLM call by the framework
    pass


@task(agent=db_agent,
    description="{query}")
def prepare_database_query(query: str) -> str:
    # This will be implemented as an LLM call by the framework
    pass


@task(agent=db_agent,
      description="Execute the following sql query and always return a table format unless instructed otherwise. If the user asks a question regarding the data, return the result and formalize an answer based on inspecting the data: {query}")
def handle_database_query(query: str) -> str:
    # This will be implemented as an LLM call by the framework
    pass


@task(description="Answer this question : {query}")
def handle_non_database_query(query: str) -> str:
    # This will be implemented as an LLM call by the framework
    pass


def run(query):

    # Example queries for different types
    # queries = [
    #     "Show me the schema of the users table",
    #     "What are the must-see attractions in Dubai for a 3-day trip?",
    # ]

    # Process each query to demonstrate routing
    # for query in queries:
    # logging.info(f"\nQuery {i}: {query}")
    work_flow_app = WorkflowApp()
    result = work_flow_app.run_and_monitor_workflow_sync(
        query_assistant_workflow,
        input={"query": query}
    )

    # logging.info(f"RESPONSE TO QUERY {i}:")
    # logging.info(result)

    if result:
        logging.info(f"\n=== RESPONSE ===\n{result}\n")
    else:
        logging.info("\n=== ERROR: No response received ===\n")


if __name__ == "__main__":
    run("What are the must-see attractions in Dubai for a 3-day trip?") #This will be handled by the non-db agent
    run("Can you identify the problematic area in our product that led to users churning?") #This will be handled by the db agent
    run("Show me the users who are not customers anymore") #This will be handled by the db agent
