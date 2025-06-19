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

"""Enhanced PostgreSQL MCP Server implementation with connection pooling and comprehensive analysis tools."""

import argparse
import asyncio
import json
import os
import sys
from typing import Annotated, Any, Dict, List, Optional, Union

from loguru import logger
from mcp.server.fastmcp import Context, FastMCP
from pydantic import Field

# Import connection management
from .connection.enhanced_singleton import DBConnectionSingleton
from .connection.pool_manager import connection_pool_manager
from .connection.connection_factory import ConnectionFactory

# Import analysis tools
from .analysis.structure import analyze_database_structure
from .analysis.performance import analyze_query_performance
from .analysis.indexes import recommend_indexes
from .analysis.fragmentation import analyze_table_fragmentation
from .analysis.vacuum import analyze_vacuum_stats
from .analysis.slow_queries import identify_slow_queries
from .analysis.settings import show_postgresql_settings

# Import utilities
from .mutable_sql_detector import check_sql_injection_risk, detect_mutating_keywords


# Error message constants
CLIENT_ERROR_KEY = 'run_query ClientError code'
UNEXPECTED_ERROR_KEY = 'run_query unexpected error'
WRITE_QUERY_PROHIBITED_KEY = 'Your MCP tool only allows readonly query. If you want to write, change the MCP configuration per README.md'
QUERY_INJECTION_RISK_KEY = 'Your query contains risky injection patterns'


def extract_cell(cell: dict):
    """Extracts the scalar or array value from a single cell."""
    if cell.get('isNull'):
        return None
    for key in (
        'stringValue',
        'longValue',
        'doubleValue',
        'booleanValue',
        'blobValue',
        'arrayValue',
    ):
        if key in cell:
            return cell[key]
    return None


def parse_execute_response(response: dict) -> list[dict]:
    """Convert RDS Data API execute_statement response to list of rows."""
    columns = [col['name'] for col in response.get('columnMetadata', [])]
    records = []

    for row in response.get('records', []):
        row_data = {col: extract_cell(cell) for col, cell in zip(columns, row)}
        records.append(row_data)

    return records


# Initialize FastMCP server
mcp = FastMCP(
    'Enhanced PostgreSQL MCP Server with connection pooling and comprehensive analysis tools'
)


async def get_connection_from_params(
    secret_arn: Optional[str] = None,
    region_name: str = "us-west-2",
    resource_arn: Optional[str] = None,
    database: Optional[str] = None,
    hostname: Optional[str] = None,
    port: Optional[int] = None,
    readonly: bool = True
) -> Union[Any, None]:
    """
    Get a database connection using provided parameters or environment variables.
    
    Args:
        secret_arn: ARN of the secret containing credentials
        region_name: AWS region name
        resource_arn: ARN of the RDS cluster or instance
        database: Database name
        hostname: Database hostname
        port: Database port
        readonly: Whether connection is read-only
        
    Returns:
        Database connection instance or None
    """
    try:
        # Use provided parameters or fall back to environment variables
        config = ConnectionFactory.get_connection_config()
        
        final_secret_arn = secret_arn or config.get('secret_arn')
        final_resource_arn = resource_arn or config.get('resource_arn')
        final_database = database or config.get('database')
        final_hostname = hostname or config.get('hostname')
        final_port = port or config.get('port', 5432)
        final_region = region_name or config.get('region_name', 'us-west-2')
        final_readonly = readonly if readonly is not None else config.get('readonly', True)
        
        if not final_secret_arn:
            logger.error("No secret_arn provided in parameters or environment variables")
            return None
        
        # Get connection from pool
        connection = await connection_pool_manager.get_connection(
            secret_arn=final_secret_arn,
            region_name=final_region,
            resource_arn=final_resource_arn,
            database=final_database,
            hostname=final_hostname,
            port=final_port,
            readonly=final_readonly
        )
        
        return connection
        
    except Exception as e:
        logger.error(f"Failed to get connection: {str(e)}")
        return None


@mcp.tool(name='run_query', description='Run a SQL query against the PostgreSQL database')
async def run_query(
    sql: Annotated[str, Field(description='The SQL query to run')],
    ctx: Context,
    query_parameters: Annotated[
        Optional[List[Dict[str, Any]]], Field(description='Parameters for the SQL query')
    ] = None,
) -> list[dict]:
    """
    Run a SQL query against the PostgreSQL database.

    Args:
        sql: The SQL statement to run
        ctx: MCP context for logging and state management
        query_parameters: Parameters for the SQL query

    Returns:
        List of dictionary that contains query response rows
    """
    logger.info(f'run_query: SQL:{sql}')
    
    try:
        # Get connection
        connection = await get_connection_from_params()
        if not connection:
            await ctx.error("No database connection available. Please configure the database connection.")
            return [{'error': 'No database connection available'}]
        
        # Check for read-only restrictions
        if connection.readonly:
            matches = detect_mutating_keywords(sql)
            if matches:
                logger.info(f'Query rejected - readonly mode, detected keywords: {matches}')
                await ctx.error(WRITE_QUERY_PROHIBITED_KEY)
                return [{'error': WRITE_QUERY_PROHIBITED_KEY}]
        
        # Check for SQL injection risks
        issues = check_sql_injection_risk(sql)
        if issues:
            logger.info(f'Query rejected - injection risk: {issues}')
            await ctx.error(str({'message': 'Query contains suspicious patterns', 'details': issues}))
            return [{'error': QUERY_INJECTION_RISK_KEY}]
        
        # Execute query
        result = await connection.execute_query(sql, query_parameters)
        
        # Return connection to pool
        await connection_pool_manager.return_connection(connection)
        
        logger.success('Query executed successfully')
        return parse_execute_response(result)
        
    except Exception as e:
        logger.exception("Query execution failed")
        await ctx.error(str({'message': f'{type(e).__name__}: {str(e)}'}))
        return [{'error': UNEXPECTED_ERROR_KEY}]


@mcp.tool(name='get_table_schema', description='Fetch table schema information from PostgreSQL')
async def get_table_schema(
    table_name: Annotated[str, Field(description='Name of the table')],
    ctx: Context
) -> list[dict]:
    """
    Get a table's schema information.

    Args:
        table_name: Name of the table
        ctx: MCP context for logging and state management

    Returns:
        List of dictionary containing table schema information
    """
    logger.info(f'get_table_schema: {table_name}')

    sql = """
        SELECT
            a.attname AS column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
            col_description(a.attrelid, a.attnum) AS column_comment,
            NOT a.attnotnull AS is_nullable,
            pg_get_expr(d.adbin, d.adrelid) AS column_default
        FROM
            pg_attribute a
        LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum
        WHERE
            a.attrelid = :table_name::regclass
            AND a.attnum > 0
            AND NOT a.attisdropped
        ORDER BY a.attnum
    """

    params = [{'name': 'table_name', 'value': {'stringValue': table_name}}]
    return await run_query(sql=sql, ctx=ctx, query_parameters=params)


@mcp.tool(name='connect_database', description='Connect to a PostgreSQL database')
async def connect_database(
    ctx: Context,
    secret_arn: Annotated[Optional[str], Field(description='ARN of the secret in AWS Secrets Manager')] = None,
    region_name: Annotated[str, Field(description='AWS region where the secret is stored')] = "us-west-2",
    resource_arn: Annotated[Optional[str], Field(description='ARN of the RDS cluster or instance')] = None,
    database: Annotated[Optional[str], Field(description='Database name to connect to')] = None,
    hostname: Annotated[Optional[str], Field(description='Database hostname')] = None,
    port: Annotated[Optional[int], Field(description='Database port')] = None,
    readonly: Annotated[bool, Field(description='Whether to enforce read-only mode')] = True
) -> str:
    """
    Connect to a PostgreSQL database.
    
    Args:
        ctx: MCP context
        secret_arn: ARN of the secret in AWS Secrets Manager
        region_name: AWS region where the secret is stored
        resource_arn: ARN of the RDS cluster or instance
        database: Database name to connect to
        hostname: Database hostname
        port: Database port
        readonly: Whether to enforce read-only mode
        
    Returns:
        Success or error message
    """
    try:
        connection = await get_connection_from_params(
            secret_arn=secret_arn,
            region_name=region_name,
            resource_arn=resource_arn,
            database=database,
            hostname=hostname,
            port=port,
            readonly=readonly
        )
        
        if connection:
            # Test the connection
            test_result = await connection.execute_query("SELECT 1")
            await connection_pool_manager.return_connection(connection)
            
            if test_result:
                return "Successfully connected to the PostgreSQL database"
            else:
                return "Connection established but test query failed"
        else:
            return "Failed to establish database connection"
            
    except Exception as e:
        logger.error(f"Connection failed: {str(e)}")
        return f"Failed to connect to database: {str(e)}"


@mcp.tool(name='disconnect_database', description='Disconnect from the PostgreSQL database')
async def disconnect_database(ctx: Context) -> str:
    """
    Disconnect from the PostgreSQL database.
    
    Args:
        ctx: MCP context
        
    Returns:
        Success message
    """
    try:
        # Close all connections in the pool
        await connection_pool_manager.close_all_connections()
        return "Successfully disconnected from the PostgreSQL database"
    except Exception as e:
        logger.warning(f"Error during disconnect: {str(e)}")
        return "Disconnected (with warnings - check logs)"


@mcp.tool(name='health_check', description='Check if the server is running and responsive')
async def health_check(ctx: Context) -> Dict[str, Any]:
    """
    Check if the server is running and responsive.
    
    Args:
        ctx: MCP context
        
    Returns:
        Health check status information
    """
    try:
        # Get pool statistics
        pool_stats = connection_pool_manager.get_pool_stats()
        
        # Test database connectivity if possible
        connection_test = False
        try:
            connection = await get_connection_from_params()
            if connection:
                await connection.execute_query("SELECT 1")
                await connection_pool_manager.return_connection(connection)
                connection_test = True
        except Exception as e:
            logger.warning(f"Health check database test failed: {str(e)}")
        
        return {
            "status": "healthy",
            "timestamp": logger.info("Health check completed"),
            "database_connection": connection_test,
            "connection_pools": pool_stats,
            "server_version": "enhanced-v1.0"
        }
        
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": logger.info("Health check failed")
        }


# Analysis Tools

@mcp.tool(name='analyze_database_structure', description='Analyze database structure and provide optimization insights')
async def analyze_database_structure_tool(
    ctx: Context,
    secret_arn: Annotated[Optional[str], Field(description='ARN of the secret in AWS Secrets Manager')] = None,
    region_name: Annotated[str, Field(description='AWS region')] = "us-west-2",
    resource_arn: Annotated[Optional[str], Field(description='ARN of the RDS cluster or instance')] = None,
    database: Annotated[Optional[str], Field(description='Database name')] = None,
    hostname: Annotated[Optional[str], Field(description='Database hostname')] = None,
    port: Annotated[Optional[int], Field(description='Database port')] = None,
    debug: Annotated[bool, Field(description='Include debug information')] = False
) -> str:
    """Analyze database structure and provide optimization insights."""
    try:
        connection = await get_connection_from_params(
            secret_arn=secret_arn, region_name=region_name, resource_arn=resource_arn,
            database=database, hostname=hostname, port=port, readonly=True
        )
        
        if not connection:
            return json.dumps({"error": "Failed to establish database connection"})
        
        result = await analyze_database_structure(connection)
        await connection_pool_manager.return_connection(connection)
        
        return json.dumps(result, indent=2 if debug else None)
        
    except Exception as e:
        logger.error(f"Database structure analysis failed: {str(e)}")
        return json.dumps({"error": str(e)})


@mcp.tool(name='analyze_query_performance', description='Analyze query performance and provide optimization recommendations')
async def analyze_query_performance_tool(
    ctx: Context,
    query: Annotated[str, Field(description='SQL query to analyze')],
    secret_arn: Annotated[Optional[str], Field(description='ARN of the secret in AWS Secrets Manager')] = None,
    region_name: Annotated[str, Field(description='AWS region')] = "us-west-2",
    resource_arn: Annotated[Optional[str], Field(description='ARN of the RDS cluster or instance')] = None,
    database: Annotated[Optional[str], Field(description='Database name')] = None,
    hostname: Annotated[Optional[str], Field(description='Database hostname')] = None,
    port: Annotated[Optional[int], Field(description='Database port')] = None,
    debug: Annotated[bool, Field(description='Include debug information')] = False
) -> str:
    """Analyze query performance and provide optimization recommendations."""
    try:
        connection = await get_connection_from_params(
            secret_arn=secret_arn, region_name=region_name, resource_arn=resource_arn,
            database=database, hostname=hostname, port=port, readonly=True
        )
        
        if not connection:
            return json.dumps({"error": "Failed to establish database connection"})
        
        result = await analyze_query_performance(connection, query)
        await connection_pool_manager.return_connection(connection)
        
        return json.dumps(result, indent=2 if debug else None)
        
    except Exception as e:
        logger.error(f"Query performance analysis failed: {str(e)}")
        return json.dumps({"error": str(e)})


@mcp.tool(name='recommend_indexes', description='Recommend indexes for a given SQL query')
async def recommend_indexes_tool(
    ctx: Context,
    query: Annotated[str, Field(description='SQL query to analyze for index recommendations')],
    secret_arn: Annotated[Optional[str], Field(description='ARN of the secret in AWS Secrets Manager')] = None,
    region_name: Annotated[str, Field(description='AWS region')] = "us-west-2",
    resource_arn: Annotated[Optional[str], Field(description='ARN of the RDS cluster or instance')] = None,
    database: Annotated[Optional[str], Field(description='Database name')] = None,
    hostname: Annotated[Optional[str], Field(description='Database hostname')] = None,
    port: Annotated[Optional[int], Field(description='Database port')] = None,
    debug: Annotated[bool, Field(description='Include debug information')] = False
) -> str:
    """Recommend indexes for a given SQL query."""
    try:
        connection = await get_connection_from_params(
            secret_arn=secret_arn, region_name=region_name, resource_arn=resource_arn,
            database=database, hostname=hostname, port=port, readonly=True
        )
        
        if not connection:
            return json.dumps({"error": "Failed to establish database connection"})
        
        result = await recommend_indexes(connection, query)
        await connection_pool_manager.return_connection(connection)
        
        return json.dumps(result, indent=2 if debug else None)
        
    except Exception as e:
        logger.error(f"Index recommendation failed: {str(e)}")
        return json.dumps({"error": str(e)})


@mcp.tool(name='analyze_table_fragmentation', description='Analyze table fragmentation and provide optimization recommendations')
async def analyze_table_fragmentation_tool(
    ctx: Context,
    threshold: Annotated[float, Field(description='Bloat percentage threshold for recommendations')] = 10.0,
    secret_arn: Annotated[Optional[str], Field(description='ARN of the secret in AWS Secrets Manager')] = None,
    region_name: Annotated[str, Field(description='AWS region')] = "us-west-2",
    resource_arn: Annotated[Optional[str], Field(description='ARN of the RDS cluster or instance')] = None,
    database: Annotated[Optional[str], Field(description='Database name')] = None,
    hostname: Annotated[Optional[str], Field(description='Database hostname')] = None,
    port: Annotated[Optional[int], Field(description='Database port')] = None,
    debug: Annotated[bool, Field(description='Include debug information')] = False
) -> str:
    """Analyze table fragmentation and provide optimization recommendations."""
    try:
        connection = await get_connection_from_params(
            secret_arn=secret_arn, region_name=region_name, resource_arn=resource_arn,
            database=database, hostname=hostname, port=port, readonly=True
        )
        
        if not connection:
            return json.dumps({"error": "Failed to establish database connection"})
        
        result = await analyze_table_fragmentation(connection, threshold)
        await connection_pool_manager.return_connection(connection)
        
        return json.dumps(result, indent=2 if debug else None)
        
    except Exception as e:
        logger.error(f"Table fragmentation analysis failed: {str(e)}")
        return json.dumps({"error": str(e)})


@mcp.tool(name='analyze_vacuum_stats', description='Analyze vacuum statistics and provide recommendations')
async def analyze_vacuum_stats_tool(
    ctx: Context,
    secret_arn: Annotated[Optional[str], Field(description='ARN of the secret in AWS Secrets Manager')] = None,
    region_name: Annotated[str, Field(description='AWS region')] = "us-west-2",
    resource_arn: Annotated[Optional[str], Field(description='ARN of the RDS cluster or instance')] = None,
    database: Annotated[Optional[str], Field(description='Database name')] = None,
    hostname: Annotated[Optional[str], Field(description='Database hostname')] = None,
    port: Annotated[Optional[int], Field(description='Database port')] = None,
    debug: Annotated[bool, Field(description='Include debug information')] = False
) -> str:
    """Analyze vacuum statistics and provide recommendations for vacuum settings."""
    try:
        connection = await get_connection_from_params(
            secret_arn=secret_arn, region_name=region_name, resource_arn=resource_arn,
            database=database, hostname=hostname, port=port, readonly=True
        )
        
        if not connection:
            return json.dumps({"error": "Failed to establish database connection"})
        
        result = await analyze_vacuum_stats(connection)
        await connection_pool_manager.return_connection(connection)
        
        return json.dumps(result, indent=2 if debug else None)
        
    except Exception as e:
        logger.error(f"Vacuum statistics analysis failed: {str(e)}")
        return json.dumps({"error": str(e)})


@mcp.tool(name='identify_slow_queries', description='Identify slow-running queries in the database')
async def identify_slow_queries_tool(
    ctx: Context,
    min_execution_time: Annotated[float, Field(description='Minimum execution time in milliseconds')] = 100.0,
    limit: Annotated[int, Field(description='Maximum number of queries to return')] = 20,
    secret_arn: Annotated[Optional[str], Field(description='ARN of the secret in AWS Secrets Manager')] = None,
    region_name: Annotated[str, Field(description='AWS region')] = "us-west-2",
    resource_arn: Annotated[Optional[str], Field(description='ARN of the RDS cluster or instance')] = None,
    database: Annotated[Optional[str], Field(description='Database name')] = None,
    hostname: Annotated[Optional[str], Field(description='Database hostname')] = None,
    port: Annotated[Optional[int], Field(description='Database port')] = None,
    debug: Annotated[bool, Field(description='Include debug information')] = False
) -> str:
    """Identify slow-running queries in the database."""
    try:
        connection = await get_connection_from_params(
            secret_arn=secret_arn, region_name=region_name, resource_arn=resource_arn,
            database=database, hostname=hostname, port=port, readonly=True
        )
        
        if not connection:
            return json.dumps({"error": "Failed to establish database connection"})
        
        result = await identify_slow_queries(connection, min_execution_time, limit)
        await connection_pool_manager.return_connection(connection)
        
        return json.dumps(result, indent=2 if debug else None)
        
    except Exception as e:
        logger.error(f"Slow query identification failed: {str(e)}")
        return json.dumps({"error": str(e)})


@mcp.tool(name='show_postgresql_settings', description='Show PostgreSQL configuration settings with optional filtering')
async def show_postgresql_settings_tool(
    ctx: Context,
    pattern: Annotated[Optional[str], Field(description='Pattern to filter settings (SQL LIKE pattern)')] = None,
    secret_arn: Annotated[Optional[str], Field(description='ARN of the secret in AWS Secrets Manager')] = None,
    region_name: Annotated[str, Field(description='AWS region')] = "us-west-2",
    resource_arn: Annotated[Optional[str], Field(description='ARN of the RDS cluster or instance')] = None,
    database: Annotated[Optional[str], Field(description='Database name')] = None,
    hostname: Annotated[Optional[str], Field(description='Database hostname')] = None,
    port: Annotated[Optional[int], Field(description='Database port')] = None,
    debug: Annotated[bool, Field(description='Include debug information')] = False
) -> str:
    """Show PostgreSQL configuration settings with optional filtering."""
    try:
        connection = await get_connection_from_params(
            secret_arn=secret_arn, region_name=region_name, resource_arn=resource_arn,
            database=database, hostname=hostname, port=port, readonly=True
        )
        
        if not connection:
            return json.dumps({"error": "Failed to establish database connection"})
        
        result = await show_postgresql_settings(connection, pattern)
        await connection_pool_manager.return_connection(connection)
        
        return json.dumps(result, indent=2 if debug else None)
        
    except Exception as e:
        logger.error(f"PostgreSQL settings analysis failed: {str(e)}")
        return json.dumps({"error": str(e)})


def main():
    """Main entry point for the enhanced MCP server application."""
    parser = argparse.ArgumentParser(
        description='Enhanced PostgreSQL MCP Server with connection pooling and comprehensive analysis tools'
    )
    
    # Connection parameters (optional - can use environment variables)
    parser.add_argument('--secret-arn', help='ARN of the Secrets Manager secret for database credentials')
    parser.add_argument('--resource-arn', help='ARN of the RDS cluster or instance')
    parser.add_argument('--database', help='Database name')
    parser.add_argument('--hostname', help='Database hostname (for direct connections)')
    parser.add_argument('--port', type=int, default=5432, help='Database port (default: 5432)')
    parser.add_argument('--region', default='us-west-2', help='AWS region (default: us-west-2)')
    parser.add_argument('--readonly', action='store_true', help='Enforce read-only mode')
    
    # Server configuration
    parser.add_argument('--transport', choices=['stdio', 'http'], default='stdio', 
                       help='Transport protocol (default: stdio for Q Chat)')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to (for HTTP transport)')
    parser.add_argument('--port-server', type=int, default=8000, help='Port to bind to (for HTTP transport)')
    
    args = parser.parse_args()
    
    # Set environment variables from command line arguments if provided
    if args.secret_arn:
        os.environ['POSTGRES_SECRET_ARN'] = args.secret_arn
    if args.resource_arn:
        os.environ['POSTGRES_RESOURCE_ARN'] = args.resource_arn
    if args.database:
        os.environ['POSTGRES_DATABASE'] = args.database
    if args.hostname:
        os.environ['POSTGRES_HOSTNAME'] = args.hostname
    if args.port:
        os.environ['POSTGRES_PORT'] = str(args.port)
    if args.region:
        os.environ['POSTGRES_REGION'] = args.region
    if args.readonly:
        os.environ['POSTGRES_READONLY'] = 'true'
    
    logger.info(f"Starting Enhanced PostgreSQL MCP Server")
    logger.info(f"Transport: {args.transport}")
    
    # Test database connectivity if configuration is available
    config = ConnectionFactory.get_connection_config()
    if config.get('secret_arn') and (config.get('resource_arn') or config.get('hostname')):
        logger.info("Testing database connectivity...")
        try:
            async def test_connection():
                connection = await get_connection_from_params()
                if connection:
                    await connection.execute_query("SELECT 1")
                    await connection_pool_manager.return_connection(connection)
                    logger.success("Database connectivity test passed")
                    return True
                else:
                    logger.warning("Database connectivity test failed - no connection")
                    return False
            
            # Run the test
            test_result = asyncio.run(test_connection())
            if not test_result:
                logger.warning("Database connectivity test failed - server will start but database operations may fail")
        except Exception as e:
            logger.warning(f"Database connectivity test failed: {str(e)} - server will start but database operations may fail")
    else:
        logger.info("No database configuration provided - tools will require connection parameters")
    
    # Start the server
    if args.transport == 'stdio':
        logger.info("Starting server with stdio transport for Q Chat integration")
        mcp.run(transport="stdio")
    else:
        logger.info(f"Starting server with HTTP transport on {args.host}:{args.port_server}")
        mcp.run(transport="http", host=args.host, port=args.port_server)


if __name__ == "__main__":
    main()
