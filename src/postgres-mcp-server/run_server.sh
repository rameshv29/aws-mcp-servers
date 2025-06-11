#!/bin/bash

# Run the PostgreSQL MCP server with proper initialization
cd /Users/reachrk/Downloads/postgresql-mcp-server
python -m awslabs.postgresql_mcp_server.main --host 0.0.0.0 --port 8000 --session-timeout 1800 --request-timeout 300
