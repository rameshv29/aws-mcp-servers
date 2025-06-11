# PostgreSQL MCP Server

A Model Context Protocol (MCP) server for PostgreSQL database analysis and optimization.

## Features

- Connect to PostgreSQL databases using direct connection or AWS RDS Data API
- Analyze database structure and provide optimization recommendations
- Analyze query performance and suggest improvements
- Recommend indexes for better query performance
- Execute read-only queries and return results
- Show PostgreSQL configuration settings
- Identify slow-running queries
- Analyze table fragmentation
- Analyze vacuum statistics and provide recommendations

## Installation

```bash
pip install awslabs.postgresql-mcp-server
```

## Usage

### Running the server

```bash
python -m awslabs.postgresql_mcp_server.server \
  --resource_arn <RDS_RESOURCE_ARN> \
  --secret_arn <SECRET_ARN> \
  --database <DATABASE_NAME> \
  --region <AWS_REGION> \
  --readonly true
```

### Connection Options

The server supports three connection methods:

1. **AWS RDS Data API** (preferred):
   - `secret_arn`: ARN of the secret in AWS Secrets Manager containing credentials
   - `resource_arn`: ARN of the RDS cluster or instance
   - `database`: Database name to connect to
   - `region_name`: AWS region where the resources are located (default: us-west-2)

2. **AWS Secrets Manager with PostgreSQL connector**:
   - `secret_name`: Name of the secret in AWS Secrets Manager containing database credentials
   - `region_name`: AWS region where the secret is stored (default: us-west-2)

3. **Direct PostgreSQL connection**:
   - `host`: Database host
   - `port`: Database port (default: 5432)
   - `database`: Database name
   - `user`: Database username
   - `password`: Database password

## Available Tools

- `analyze_database_structure`: Analyze the database structure and provide insights
- `analyze_query`: Analyze a SQL query and provide optimization recommendations
- `recommend_indexes`: Recommend indexes for a given SQL query
- `execute_read_only_query`: Execute a read-only SQL query and return the results
- `show_postgresql_settings`: Show PostgreSQL configuration settings with optional filtering
- `get_slow_queries`: Identify slow-running queries in the database
- `analyze_table_fragmentation`: Analyze table fragmentation and provide optimization recommendations
- `analyze_vacuum_stats`: Analyze vacuum statistics and provide recommendations for vacuum settings
- `health_check`: Check if the server is running and responsive

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

## Security

All operations are performed in read-only mode for security reasons. No database modifications will be made.

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.
