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

"""Connection manager for PostgreSQL MCP Server."""

import logging
from typing import Dict, Any, Optional, Tuple

from mcp.server.fastmcp import Context

from awslabs.postgres_mcp_server.db.connector import UniversalConnector
from awslabs.postgres_mcp_server.session_handler import session_handler

logger = logging.getLogger("postgresql-mcp-server")

async def get_or_create_connection(
    ctx: Context,
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
) -> Tuple[UniversalConnector, bool]:
    """
    Get an existing database connection from the session or create a new one.
    
    Args:
        ctx: The MCP context
        secret_name: Name of the secret in AWS Secrets Manager containing database credentials
        region_name: AWS region where the secret is stored (default: us-west-2)
        secret_arn: ARN of the secret in AWS Secrets Manager containing credentials (for RDS Data API)
        resource_arn: ARN of the RDS cluster or instance (for RDS Data API)
        database: Database name to connect to
        host: Database host (for direct connection)
        port: Database port (for direct connection)
        user: Database username (for direct connection)
        password: Database password (for direct connection)
        readonly: Whether to enforce read-only mode (default: True)
        
    Returns:
        Tuple of (connector, is_new_connection)
    """
    session_id = ctx.session_id
    
    # Check if we already have a connection in the session
    existing_connector = session_handler.get_connection(session_id)
    if existing_connector:
        logger.info(f"Using existing database connection from session: {session_id}")
        return existing_connector, False
    
    # Get connection parameters from the session if they exist
    connection_params = session_handler.get_connection_params(session_id)
    
    # If we have connection parameters in the session, use those
    if connection_params and not any([secret_name, secret_arn, resource_arn, database, host]):
        secret_name = connection_params.get("secret_name")
        region_name = connection_params.get("region_name", "us-west-2")
        secret_arn = connection_params.get("secret_arn")
        resource_arn = connection_params.get("resource_arn")
        database = connection_params.get("database")
        host = connection_params.get("host")
        port = connection_params.get("port")
        user = connection_params.get("user")
        password = connection_params.get("password")
        readonly = connection_params.get("readonly", True)
    
    # Create a new connection if we don't have one
    if not any([secret_name, secret_arn, resource_arn, database, host]):
        await ctx.error("No database connection parameters provided. Please provide connection parameters.")
        return None, False
    
    # Initialize connector with the provided parameters
    connector = UniversalConnector(
        secret_name=secret_name,
        region_name=region_name,
        secret_arn=secret_arn,
        resource_arn=resource_arn,
        database=database,
        host=host,
        port=port,
        user=user,
        password=password,
        readonly=readonly
    )
    
    # Try to connect
    if not connector.connect():
        error_msg = "Failed to connect to database. Please check your credentials and connection parameters."
        await ctx.error(error_msg)
        return None, False
    
    # Store the connection parameters and connector in the session
    connection_params = {
        "secret_name": secret_name,
        "region_name": region_name,
        "secret_arn": secret_arn,
        "resource_arn": resource_arn,
        "database": database,
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "readonly": readonly
    }
    session_handler.set_connection(session_id, connector, connection_params)
    
    logger.info(f"Created new database connection for session: {session_id}")
    return connector, True

async def initialize_connection(
    ctx: Context,
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
) -> bool:
    """
    Initialize a database connection and store it in the session.
    
    Args:
        ctx: The MCP context
        secret_name: Name of the secret in AWS Secrets Manager containing database credentials
        region_name: AWS region where the secret is stored (default: us-west-2)
        secret_arn: ARN of the secret in AWS Secrets Manager containing credentials (for RDS Data API)
        resource_arn: ARN of the RDS cluster or instance (for RDS Data API)
        database: Database name to connect to
        host: Database host (for direct connection)
        port: Database port (for direct connection)
        user: Database username (for direct connection)
        password: Database password (for direct connection)
        readonly: Whether to enforce read-only mode (default: True)
        
    Returns:
        True if connection was successful, False otherwise
    """
    connector, is_new = await get_or_create_connection(
        ctx, 
        secret_name, 
        region_name, 
        secret_arn, 
        resource_arn, 
        database, 
        host, 
        port, 
        user, 
        password,
        readonly
    )
    
    return connector is not None

async def close_connection(ctx: Context) -> bool:
    """
    Close the database connection for the current session.
    
    Args:
        ctx: The MCP context
        
    Returns:
        True if connection was closed, False if no connection existed
    """
    session_id = ctx.session_id
    
    # Check if we have a connection in the session
    existing_connector = session_handler.get_connection(session_id)
    if existing_connector:
        session_handler.close_connection(session_id)
        return True
    
    return False
