# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Main module for the PostgreSQL MCP Server with HTTP endpoints."""

import argparse
import os
import sys
import time
from starlette.responses import Response
from mcp.server.fastmcp import FastMCP

# Add the current directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from awslabs.postgresql_mcp_server.config import configure_logging, server_lifespan, session_handler
from awslabs.postgresql_mcp_server.tools import register_all_tools

# Configure logging
logger = configure_logging()

# Initialize MCP server
mcp = FastMCP(
    "PostgreSQL MCP Server", 
    instructions="""
    This MCP server helps you interact with PostgreSQL databases using either direct PostgreSQL connection or AWS RDS Data API by:
    - Running SQL queries
    - Analyzing database structure
    - Analyzing query performance
    - Recommending indexes
    - Executing read-only queries
    
    IMPORTANT: This is a READ-ONLY tool. All operations are performed in read-only mode
    for security reasons. No database modifications will be made.
    
    Connection options:
    
    1. AWS RDS Data API (preferred):
       - secret_arn: ARN of the secret in AWS Secrets Manager containing credentials
       - resource_arn: ARN of the RDS cluster or instance
       - database: Database name to connect to
       - region_name: AWS region where the resources are located (default: us-west-2)
    
    2. AWS Secrets Manager with PostgreSQL connector:
       - secret_name: Name of the secret in AWS Secrets Manager containing database credentials
       - region_name: AWS region where the secret is stored (default: us-west-2)
    
    3. Direct PostgreSQL connection:
       - host: Database host
       - port: Database port (default: 5432)
       - database: Database name
       - user: Database username
       - password: Database password
    
    The server will try to connect in the following order:
    1. RDS Data API if secret_arn and resource_arn are provided
    2. PostgreSQL connector using credentials from AWS Secrets Manager if secret_name is provided
    3. Direct PostgreSQL connection if host, database, user, and password are provided
    """,
    stateless_http=True, 
    json_response=False,
    lifespan=server_lifespan
)

# Add a health check route directly to the MCP server
@mcp.custom_route("/health", methods=["GET"])
async def health_check(request):
    """
    Simple health check endpoint for ALB Target Group.
    Always returns 200 OK to indicate the service is running.
    """
    return Response(
        content="healthy",
        status_code=200,
        media_type="text/plain"
    )

# Add a session status endpoint
@mcp.custom_route("/sessions", methods=["GET"])
async def session_status(request):
    """
    Show active sessions for debugging purposes
    """
    active_sessions = len(session_handler.sessions)
    session_ids = list(session_handler.sessions.keys())
    
    content = f"Active sessions: {active_sessions}\n"
    content += f"Session IDs: {', '.join(session_ids)}\n"
    
    return Response(
        content=content,
        status_code=200,
        media_type="text/plain"
    )

# Register all tools with the MCP server
register_all_tools(mcp)

def main():
    """Main entry point for the HTTP server."""
    parser = argparse.ArgumentParser(description='PostgreSQL MCP Server')
    parser.add_argument('--port', type=int, default=8000, help='Port to run the server on')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to bind the server to')
    parser.add_argument('--session-timeout', type=int, default=1800,
                        help='Session timeout in seconds (default: 1800)')
    parser.add_argument('--request-timeout', type=int, default=300,
                        help='Request timeout in seconds (default: 300)')
    
    args = parser.parse_args()
    
    # Configure the MCP server settings
    mcp.settings.port = args.port
    mcp.settings.host = args.host
    
    # Update session handler settings
    session_handler.session_timeout = args.session_timeout
    
    # Configure server to handle multiple concurrent connections
    # Set a high value for max concurrent requests
    os.environ["MCP_MAX_CONCURRENT_REQUESTS"] = "100"  # Allow many concurrent requests
    os.environ["MCP_REQUEST_TIMEOUT_SECONDS"] = str(args.request_timeout)
    
    logger.info(f"Starting PostgreSQL MCP Server on {args.host}:{args.port}")
    logger.info(f"Health check endpoint available at http://{args.host}:{args.port}/health")
    logger.info(f"Session status endpoint available at http://{args.host}:{args.port}/sessions")
    logger.info(f"Session timeout: {args.session_timeout} seconds")
    logger.info(f"Request timeout: {args.request_timeout} seconds")
    
    try:
        mcp.run(transport='streamable-http')
    except Exception as e:
        logger.error(f"Server error: {str(e)}")
        # If the server crashes, try to restart it
        time.sleep(5)  # Wait 5 seconds before restarting
        logger.info("Attempting to restart server...")
        mcp.run(transport='streamable-http')

if __name__ == "__main__":
    main()
