# PostgreSQL MCP Server

An AWS Labs Model Context Protocol (MCP) server (StreamableHTTP) for PostgreSQL databases.

## Natural Language to PostgreSQL SQL Query

Converting human-readable questions and commands into structured PostgreSQL-compatible SQL queries and executing them against the configured PostgreSQL database.

## Overview

This MCP server provides tools for interacting with PostgreSQL databases, including:

1. Running SQL queries
2. Analyzing database structure
3. Analyzing query performance
4. Recommending indexes
5. Executing read-only queries
6. Analyzing table fragmentation
7. Analyzing vacuum statistics
8. Identifying slow-running queries
9. Viewing PostgreSQL configuration settings

## Connection Options

The server supports multiple connection methods:

### 1. AWS RDS Data API (preferred)

- `secret_arn`: ARN of the secret in AWS Secrets Manager containing credentials
- `resource_arn`: ARN of the RDS cluster or instance
- `database`: Database name to connect to
- `region_name`: AWS region where the resources are located (default: us-west-2)

Note: You can directly pass the AWS Secret name when you call MCP server and MCP server is able to pull the credentials to connect RDS Data API. If your secret does not contain resource_arn and secret_arn, it will use the other attributes to connect the database via PostgreSQL connector. If both are not working you can pass manually all the credentials to connect to the PostgreSQL connector.

### 2. AWS Secrets Manager with PostgreSQL connector

- `secret_name`: Name of the secret in AWS Secrets Manager containing database credentials
- `region_name`: AWS region where the secret is stored (default: us-west-2)

### 3. Direct PostgreSQL connection

- `host`: Database host
- `port`: Database port (default: 5432)
- `database`: Database name
- `user`: Database username
- `password`: Database password

## Prerequisites

1. Install Python using `uv python install 3.10`
2. PostgreSQL database with username and password stored in AWS Secrets Manager
3. Enable RDS Data API for your RDS PostgreSQL instance/cluster, see [instructions here](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/data-api.html)
4. This MCP server can be run locally on the same host as your LLM client or remotely (ECS, EKS etc.)
5. Docker runtime 
6. Set up AWS credentials with access to AWS services
    - You need an AWS account with appropriate permissions
    - Configure AWS credentials with `aws configure` or environment variables

## Required PostgreSQL Extensions

For full functionality, the following PostgreSQL extensions should be enabled:

- `pg_stat_statements`: Required for the `get_slow_queries` tool
  ```sql
  CREATE EXTENSION pg_stat_statements;
  ```
  
  For RDS instances, you need to:
  1. Create a parameter group with `shared_preload_libraries = 'pg_stat_statements'`
  2. Associate the parameter group with your RDS instance
  3. Restart the instance
  4. Run `CREATE EXTENSION pg_stat_statements;`

## Running the Server

### Locally with Python

```bash
cd postgres-mcp-server
python3.10 -m venv .venv
source .venv/bin/activate
python3.10 -m pip install -r requirements.txt
python3.10 -m awslabs.postgresql_mcp_server.main
```

### Using Docker

```bash
cd postgres-mcp-server
docker build -t postgres-mcp-server .
docker run -p 8000:8000 \
  -e AWS_ACCESS_KEY_ID=<your aws access key> \
  -e AWS_SECRET_ACCESS_KEY=<your aws secret access key> \
  -e AWS_SESSION_TOKEN=<your aws session token> \
  postgres-mcp-server
```

## Here are some ways you can work with MCP across AWS, and we'll be adding support to more products including Amazon Q Developer CLI soon: (e.g. for Amazon Q Developer CLI MCP, `~/.aws/amazonq/mcp.json`):

```json
{
  "mcpServers": {
    "postgresql-stream": {
      "command": "npx",
      "args": ["mcp-remote", "http://0.0.0.0:8000/mcp", "--allow-http"],
      "disabled": false,
      "autoApprove": []
    }
  }
}
```

## Security

All operations are performed in read-only mode for security reasons. The server includes:

1. SQL injection protection
2. Validation of read-only queries
3. Detection of mutating SQL keywords

## Tools

### connect_database

Connect to a PostgreSQL database and store the connection in the session.

```
connect_database(
    secret_name: str = None, 
    region_name: str = "us-west-2",
    secret_arn: str = None, 
    resource_arn: str = None, 
    database: str = None,
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    readonly: bool = True
) -> str
```

### disconnect_database

Disconnect from the PostgreSQL database and remove the connection from the session.

```
disconnect_database() -> str
```

### run_query

Run a SQL query against a PostgreSQL database.

```
run_query(sql: str) -> list[dict]
```

### get_table_schema

Fetch table schema from the PostgreSQL database.

```
get_table_schema(table_name: str, database_name: str) -> list[dict]
```

### analyze_database_structure

Analyze the database structure and provide insights on schema design, indexes, and potential optimizations.

```
analyze_database_structure(
    secret_name: str = None, 
    region_name: str = "us-west-2",
    secret_arn: str = None, 
    resource_arn: str = None, 
    database: str = None,
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    debug: bool = False
) -> str
```

### analyze_query

Analyze a SQL query and provide optimization recommendations.

```
analyze_query(
    query: str,
    secret_name: str = None, 
    region_name: str = "us-west-2",
    secret_arn: str = None, 
    resource_arn: str = None, 
    database: str = None,
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    debug: bool = False
) -> str
```

### recommend_indexes

Recommend indexes for a given SQL query.

```
recommend_indexes(
    query: str,
    secret_name: str = None, 
    region_name: str = "us-west-2",
    secret_arn: str = None, 
    resource_arn: str = None, 
    database: str = None,
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    debug: bool = False
) -> str
```

### execute_read_only_query

Execute a read-only SQL query and return the results.

```
execute_read_only_query(
    query: str,
    secret_name: str = None, 
    region_name: str = "us-west-2",
    secret_arn: str = None, 
    resource_arn: str = None, 
    database: str = None,
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    max_rows: int = 100,
    debug: bool = False
) -> str
```

### analyze_table_fragmentation

Analyze table fragmentation and provide optimization recommendations.

```
analyze_table_fragmentation(
    secret_name: str = None, 
    region_name: str = "us-west-2",
    secret_arn: str = None, 
    resource_arn: str = None, 
    database: str = None,
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    threshold: float = 10.0,
    debug: bool = False
) -> str
```

### analyze_vacuum_stats

Analyze vacuum statistics and provide recommendations for vacuum settings.

```
analyze_vacuum_stats(
    secret_name: str = None, 
    region_name: str = "us-west-2",
    secret_arn: str = None, 
    resource_arn: str = None, 
    database: str = None,
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    debug: bool = False
) -> str
```

### identify_slow_queries

Identify slow-running queries in the database.

```
identify_slow_queries(
    secret_name: str = None, 
    region_name: str = "us-west-2",
    secret_arn: str = None, 
    resource_arn: str = None, 
    database: str = None,
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    min_execution_time: float = 100.0,
    limit: int = 20,
    debug: bool = False
) -> str
```

### show_postgresql_settings

Show PostgreSQL configuration settings with optional filtering.

```
show_postgresql_settings(
    pattern: str = None,
    secret_name: str = None, 
    region_name: str = "us-west-2",
    secret_arn: str = None, 
    resource_arn: str = None, 
    database: str = None,
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    debug: bool = False
) -> str
```

### health_check

Check if the server is running and responsive.

```
health_check() -> Dict[str, Any]
```

## Dependencies

- boto3
- botocore
- loguru
- mcp[cli]
- pydantic
- psycopg2-binary
- starlette
- uvicorn

## Development and Testing

### Setting Up Development Environment

```bash
cd postgres-mcp-server
python3.10 -m venv .venv
source .venv/bin/activate
python3.10 -m pip install -r requirements.txt -e .
```

### Running Tests

Run all tests:
```bash
python -m pytest tests/
```

Run tests with coverage:
```bash
python -m pytest tests/ --cov=awslabs.postgresql_mcp_server --cov-report=term --cov-report=html
```

Run specific test files:
```bash
python -m pytest tests/test_server.py
python -m pytest tests/test_tools.py
```

### Integration Tests

To run integration tests, set the following environment variables:
```bash
export POSTGRES_MCP_TEST_RESOURCE_ARN="your-resource-arn"
export POSTGRES_MCP_TEST_SECRET_ARN="your-secret-arn"
export POSTGRES_MCP_TEST_DATABASE="your-database"
export POSTGRES_MCP_TEST_REGION="your-region"
python -m pytest tests/test_integration.py -v
```
