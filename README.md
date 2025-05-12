# A conversational agent over a Postgres database using MCP

This quickstart demonstrates how to build a fully functional, enterprise-ready agent that allows users to ask their database any question in natural text and get both the results and a highly structured analysis of complex questions. This quickstart also shows the usage of MCP in Dapr Agents to connect to the database and provides a fully functional ChatGPT-like chat interface using Chainlit.

In this demo, multi-agent pattern of router is used to route the queries to the appropriate agent. 
The agent uses the MCP server to connect to a Postgres database.
Based on the type of query (database or non-database), the router task will decide which agent to route the query to between the LLMAgent or DBAgent.
LLMAgent is used to answer general questions where else DBAgent is used to perform the database operations using the MCP server
and on the provided Postgres database.

## Key Benefits

- **Conversational Knowledge Base**: Users can talk to their database in natural language, ask complex questions and perform advanced analysis over data
- **Conversational Memory**: The agent maintains context across interactions in the user's [database of choice](https://docs.dapr.io/reference/components-reference/supported-state-stores/)
- **Boilerplate-Free DB Layer**: MCP allows the Dapr Agent to connect to the database without requiring users to write Postgres-specific code

## Prerequisites

- Python 3.10 (recommended)
- pip package manager
- OpenAI API key (for the OpenAI example)
- [Dapr CLI installed](https://docs.dapr.io/getting-started/install-dapr-cli/)

## Environment Setup

```bash
# Create a virtual environment
python3.10 -m venv .venv

# Activate the virtual environment 
# On Windows:
.venv\Scripts\activate
# On macOS/Linux:
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Initialize Dapr
dapr init
```

## LLM Configuration

For this example, we'll be using the OpenAI client that is used by default.

Create a `.env` file in the project root:

```env
OPENAI_API_KEY=your_api_key_here
```

Replace `your_api_key_here` with your actual OpenAI API key.

## Postgres Configuration

### Connect to an existing database

Create an `.env` file in the root directory of this quickstart and insert your database configuration

### Create a new sample database

First, install Postgres on your machine.
Create the following directory and copy the sql files there:

```bash
mkdir docker-entrypoint-initdb.d
cp schema.sql users.sql ./docker-entrypoint-initdb.d
```

Run the database container:

```bash
docker run --rm --name dapr_db \
  -e POSTGRES_PASSWORD=mypassword \
  -e POSTGRES_USER=admin \
  -e POSTGRES_DB=dapr_db \
  -p 5454:5432 \
  -v $(pwd)/docker-entrypoint-initdb.d:/docker-entrypoint-initdb.d \
  -d postgres
```

Next, create the users table and seed data:

```bash
psql -h localhost -U admin -d dapr_db -f schema.sql
psql -h localhost -U admin -d dapr_db -f users.sql
```

#### Create .env file

Finally, create an `.env` file in the root directory of this quickstart and insert your database configuration:

```bash
DB_HOST=localhost
DB_PORT=5454
DB_NAME=dapr_db
DB_USER=admin
DB_PASSWORD=mypassword
```

## MCP Configuration

To get the Dapr Agent to connect to our Postgres database, we'll use a Postgres MCP server.
Change the settings below based on your Postgres configuration:

*Note: If you're running Postgres in a Docker container, change `<HOST>` to `localhost`.*

```bash
docker run -d -p 8100:8000 \
  -e DATABASE_URI=postgresql://admin:mypassword@localhost:5454/dapr_db \
  crystaldba/postgres-mcp --access-mode=unrestricted --transport=sse
```

## Examples

### Load data to Postgres and create a knowledge base chat interface

Run the agent:

```bash
dapr run --app-id multi-agent-app -f dapr.yaml
```

Observer the logs in the terminal. You should see the agent starting up and connecting to the Postgres database.
Some queries might be executed to load the data from the database based on the type of query.
### Ask Questions

Now you can start talking to your data. If using the sample database, ask questions like `Show me all churned users from the past month`, `Can you identify the problematic area in our product that led to users churning?`, `Show me the users who are not customers anymore`.

#### Testing the agent's memory

If you exit the app and restart it, the agent will remember all the previous conversation. When you insall Dapr using `dapr init`, Redis is installed by default and this is where the conversation memory is saved. To change it, edit the `./components/conversationmemory.yaml` file.

## Summary

**How It Works:**
1. The MCP server is running and connects to our Postgres database
2. Dapr starts, loading the conversation history storage configs from the `components` folder. The agent connects to the MCP server.
3. Chainlit loads and starts the agent UI in your browser.
4. Users can now talk to their database in natural language and have the agent analyze the data.
5. The conversation history is automatically managed by Dapr and saved in the state store configured in `./components/conversationmemory.yaml`.
