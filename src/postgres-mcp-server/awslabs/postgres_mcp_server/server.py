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

"""awslabs postgresql MCP Server implementation."""

import argparse
import asyncio
import boto3
import json
import os
import sys
import time
import traceback
import datetime
from typing import Annotated, Any, Dict, List, Optional
from starlette.responses import Response
from mcp.server.fastmcp import Context, FastMCP
from pydantic import Field
from botocore.exceptions import BotoCoreError, ClientError
from loguru import logger

# Add the current directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from mutable_sql_detector import (
    check_sql_injection_risk,
    detect_mutating_keywords,
)
from config import configure_logging, server_lifespan, session_handler
from db.connector import UniversalConnector
from connection_manager import get_or_create_connection, initialize_connection, close_connection
from analysis.structure import (
    get_database_structure, 
    organize_db_structure_by_table,
    analyze_database_structure_for_response
)
from analysis.query import (
    extract_tables_from_query, 
    get_table_statistics, 
    get_schema_information, 
    get_index_information,
    format_query_analysis_response
)
from analysis.patterns import (
    detect_query_patterns, 
    detect_query_anti_patterns, 
    validate_read_only_query
)
from analysis.indexes import (
    extract_potential_indexes,
    get_table_structure_for_index,
    check_existing_indexes,
    format_index_recommendations_response
)

# Configure logging
logger = configure_logging()

# Error message keys
client_error_code_key = 'run_query ClientError code'
unexpected_error_key = 'run_query unexpected error'
write_query_prohibited_key = 'Your MCP tool only allows readonly query. If you want to write, change the MCP configuration per README.md'
query_injection_risk_key = 'Your query contains risky injection patterns'


class DummyCtx:
    """A dummy context class for error handling in MCP tools."""

    async def error(self, message):
        """Raise a runtime error with the given message.

        Args:
            message: The error message to include in the runtime error
        """
        # Do nothing
        pass


class DBConnection:
    """Class that wraps DB connection client by RDS API."""

    def __init__(self, cluster_arn, secret_arn, database, region, readonly, is_test=False):
        """Initialize a new DB connection.

        Args:
            cluster_arn: The ARN of the RDS cluster
            secret_arn: The ARN of the secret containing credentials
            database: The name of the database to connect to
            region: The AWS region where the RDS instance is located
            readonly: Whether the connection should be read-only
            is_test: Whether this is a test connection
        """
        self.cluster_arn = cluster_arn
        self.secret_arn = secret_arn
        self.database = database
        self.readonly = readonly
        if not is_test:
            self.data_client = boto3.client('rds-data', region_name=region)

    @property
    def readonly_query(self):
        """Get whether this connection is read-only.

        Returns:
            bool: True if the connection is read-only, False otherwise
        """
        return self.readonly


class DBConnectionSingleton:
    """Manages a single DBConnection instance across the application.

    This singleton ensures that only one DBConnection is created and reused.
    """

    _instance = None

    def __init__(self, resource_arn, secret_arn, database, region, readonly, is_test=False):
        """Initialize a new DB connection singleton.

        Args:
            resource_arn: The ARN of the RDS resource
            secret_arn: The ARN of the secret containing credentials
            database: The name of the database to connect to
            region: The AWS region where the RDS instance is located
            readonly: Whether the connection should be read-only
            is_test: Whether this is a test connection
        """
        if not all([resource_arn, secret_arn, database, region]):
            raise ValueError(
                'Missing required connection parameters. '
                'Please provide resource_arn, secret_arn, database, and region.'
            )
        self._db_connection = DBConnection(
            resource_arn, secret_arn, database, region, readonly, is_test
        )

    @classmethod
    def initialize(cls, resource_arn, secret_arn, database, region, readonly, is_test=False):
        """Initialize the singleton instance if it doesn't exist.

        Args:
            resource_arn: The ARN of the RDS resource
            secret_arn: The ARN of the secret containing credentials
            database: The name of the database to connect to
            region: The AWS region where the RDS instance is located
            readonly: Whether the connection should be read-only
            is_test: Whether this is a test connection
        """
        if cls._instance is None:
            cls._instance = cls(resource_arn, secret_arn, database, region, readonly, is_test)

    @classmethod
    def get(cls):
        """Get the singleton instance.

        Returns:
            DBConnectionSingleton: The singleton instance

        Raises:
            RuntimeError: If the singleton has not been initialized
        """
        if cls._instance is None:
            raise RuntimeError('DBConnectionSingleton is not initialized.')
        return cls._instance

    @property
    def db_connection(self):
        """Get the database connection.

        Returns:
            DBConnection: The database connection instance
        """
        return self._db_connection


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


def format_bytes(bytes_value):
    """Format bytes to human-readable format"""
    if bytes_value is None:
        return "Unknown"
    
    bytes_value = float(bytes_value)
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024
    
    return f"{bytes_value:.2f} PB"


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


@mcp.tool(name='run_query', description='Run a SQL query against a PostgreSQL database')
async def run_query(
    sql: Annotated[str, Field(description='The SQL query to run')],
    ctx: Context,
    db_connection=None,
    query_parameters: Annotated[
        Optional[List[Dict[str, Any]]], Field(description='Parameters for the SQL query')
    ] = None,
) -> list[dict]:  # type: ignore
    """Run a SQL query against a PostgreSQL database.

    Args:
        sql: The sql statement to run
        ctx: MCP context for logging and state management
        db_connection: DB connection object passed by unit test. It should be None if if called by MCP server.
        query_parameters: Parameters for the SQL query

    Returns:
        List of dictionary that contains query response rows
    """
    global client_error_code_key
    global unexpected_error_key
    global write_query_prohibited_key

    if db_connection is None:
        db_connection = DBConnectionSingleton.get().db_connection

    if db_connection.readonly_query:
        matches = detect_mutating_keywords(sql)
        if (bool)(matches):
            logger.info(
                f'query is rejected because current setting only allows readonly query. detected keywords: {matches}, SQL query: {sql}'
            )

            await ctx.error(write_query_prohibited_key)
            return [{'error': write_query_prohibited_key}]

    issues = check_sql_injection_risk(sql)
    if issues:
        logger.info(
            f'query is rejected because it contains risky SQL pattern, SQL query: {sql}, reasons: {issues}'
        )
        await ctx.error(
            str({'message': 'Query parameter contains suspicious pattern', 'details': issues})
        )
        return [{'error': query_injection_risk_key}]

    try:
        logger.info(f'run_query: readonly:{db_connection.readonly_query}, SQL:{sql}')

        execute_params = {
            'resourceArn': db_connection.cluster_arn,
            'secretArn': db_connection.secret_arn,
            'database': db_connection.database,
            'sql': sql,
            'includeResultMetadata': True,
        }

        if query_parameters:
            execute_params['parameters'] = query_parameters

        response = await asyncio.to_thread(
            db_connection.data_client.execute_statement, **execute_params
        )

        logger.success('run_query successfully executed query:{}', sql)
        return parse_execute_response(response)
    except ClientError as e:
        logger.exception(client_error_code_key)
        await ctx.error(
            str({'code': e.response['Error']['Code'], 'message': e.response['Error']['Message']})
        )
        return [{'error': client_error_code_key}]
    except Exception as e:
        logger.exception(unexpected_error_key)
        error_details = f'{type(e).__name__}: {str(e)}'
        await ctx.error(str({'message': error_details}))
        return [{'error': unexpected_error_key}]


@mcp.tool(
    name='get_table_schema',
    description='Fetch table schema from the PostgreSQL database',
)
async def get_table_schema(
    table_name: Annotated[str, Field(description='name of the table')],
    database_name: Annotated[str, Field(description='name of the database')],
    ctx: Context,
) -> list[dict]:
    """Get a table's schema information given the table name.

    Args:
        table_name: name of the table
        database_name: name of the database
        ctx: MCP context for logging and state management

    Returns:
        List of dictionary that contains query response rows
    """
    logger.info(f'get_table_schema: {table_name}')

    sql = """
        SELECT
            column_name,
            data_type,
            is_nullable,
            column_default,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM
            information_schema.columns
        WHERE
            table_schema = :database_name
            AND table_name = :table_name
        ORDER BY
            ordinal_position
    """
    params = [
        {'name': 'table_name', 'value': {'stringValue': table_name}},
        {'name': 'database_name', 'value': {'stringValue': database_name}},
    ]

    return await run_query(sql=sql, ctx=ctx, query_parameters=params)


def register_all_tools(mcp_instance: FastMCP):
    """Register all tools with the MCP server"""
    
    @mcp_instance.tool()
    async def health_check(ctx: Context = None) -> Dict[str, Any]:
        """
        Check if the server is running and responsive.
        
        Returns:
            A message indicating the server is healthy
        """
        return {
            "status": "healthy",
            "timestamp": datetime.datetime.now().isoformat()
        }
    
    @mcp_instance.tool()
    async def connect_database(
        secret_name: str = None, 
        region_name: str = "us-west-2",
        secret_arn: str = None, 
        resource_arn: str = None, 
        database: str = None,
        host: str = None,
        port: int = None,
        user: str = None,
        password: str = None,
        readonly: bool = True,
        ctx: Context = None
    ) -> str:
        """
        Connect to a PostgreSQL database and store the connection in the session.
        
        Args:
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
            A message indicating whether the connection was successful
        """
        success = await initialize_connection(
            ctx,
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
        
        if success:
            connection_type = "RDS Data API" if secret_arn and resource_arn else "direct PostgreSQL"
            db_name = database or "unknown"
            return f"Successfully connected to {db_name} database using {connection_type} connection. The connection will be reused for subsequent operations."
        else:
            return "Failed to connect to the database. Please check your connection parameters and try again."
    
    @mcp_instance.tool()
    async def disconnect_database(ctx: Context = None) -> str:
        """
        Disconnect from the PostgreSQL database and remove the connection from the session.
        
        Returns:
            A message indicating whether the disconnection was successful
        """
        success = await close_connection(ctx)
        
        if success:
            return "Successfully disconnected from the database."
        else:
            return "No active database connection to disconnect."
    
    @mcp_instance.tool()
    async def analyze_database_structure(
        secret_name: str = None, 
        region_name: str = "us-west-2",
        secret_arn: str = None, 
        resource_arn: str = None, 
        database: str = None,
        host: str = None,
        port: int = None,
        user: str = None,
        password: str = None,
        debug: bool = False,
        ctx: Context = None
    ) -> str:
        """
        Analyze the database structure and provide insights on schema design, indexes, and potential optimizations.
        
        Args:
            secret_name: Name of the secret in AWS Secrets Manager containing database credentials (optional if already connected)
            region_name: AWS region where the secret is stored (default: us-west-2)
            secret_arn: ARN of the secret in AWS Secrets Manager containing credentials (optional if already connected)
            resource_arn: ARN of the RDS cluster or instance (optional if already connected)
            database: Database name to connect to (optional if already connected)
            host: Database host (optional if already connected)
            port: Database port (optional if already connected)
            user: Database username (optional if already connected)
            password: Database password (optional if already connected)
            debug: Enable detailed debug output (default: false)
        
        Returns:
            A comprehensive analysis of the database structure with optimization recommendations
        """
        try:
            # Get or create a database connection
            connector, is_new = await get_or_create_connection(
                ctx,
                secret_name=secret_name,
                region_name=region_name,
                secret_arn=secret_arn,
                resource_arn=resource_arn,
                database=database,
                host=host,
                port=port,
                user=user,
                password=password
            )
            
            if not connector:
                return "Failed to connect to database. Please check your credentials and connection parameters."
            
            try:
                # Get comprehensive database structure
                db_structure = get_database_structure(connector)
                
                # Generate the formatted response
                response = analyze_database_structure_for_response(db_structure)
                
                return response
                
            except Exception as e:
                error_details = traceback.format_exc()
                error_msg = f"Error analyzing database structure: {str(e)}\n\n"
                
                if debug:
                    error_msg += f"Error details:\n{error_details}\n\n"
                
                error_msg += "Troubleshooting tips:\n"
                error_msg += "- Check that your user has permissions to access information_schema\n"
                error_msg += "- Verify that the database contains tables\n"
                
                return error_msg
                
        except Exception as e:
            error_details = traceback.format_exc()
            error_msg = f"Unexpected error: {str(e)}\n\n"
            
            if debug:
                error_msg += f"Error details:\n{error_details}\n\n"
            
            return error_msg
            
        finally:
            # Only disconnect if we created a new connection
            if is_new and connector:
                connector.disconnect()
    
    @mcp_instance.tool()
    async def analyze_query(
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
        debug: bool = False,
        ctx: Context = None
    ) -> str:
        """
        Analyze a SQL query and provide optimization recommendations.
        
        Args:
            query: The SQL query to analyze
            secret_name: Name of the secret in AWS Secrets Manager containing database credentials (optional if already connected)
            region_name: AWS region where the secret is stored (default: us-west-2)
            secret_arn: ARN of the secret in AWS Secrets Manager containing credentials (optional if already connected)
            resource_arn: ARN of the RDS cluster or instance (optional if already connected)
            database: Database name to connect to (optional if already connected)
            host: Database host (optional if already connected)
            port: Database port (optional if already connected)
            user: Database username (optional if already connected)
            password: Database password (optional if already connected)
            debug: Enable detailed debug output (default: false)
        
        Returns:
            Analysis of the query execution plan and optimization suggestions
        """
        try:
            # Get or create a database connection
            connector, is_new = await get_or_create_connection(
                ctx,
                secret_name=secret_name,
                region_name=region_name,
                secret_arn=secret_arn,
                resource_arn=resource_arn,
                database=database,
                host=host,
                port=port,
                user=user,
                password=password
            )
            
            if not connector:
                return "Failed to connect to database. Please check your credentials and connection parameters."
            
            # Clean the query before analysis
            query = query.strip()
            
            # Get the execution plan
            explain_query = f"EXPLAIN (FORMAT JSON) {query};"
            explain_results = connector.execute_query(explain_query)
            
            if not explain_results:
                return "Failed to generate execution plan for the query. The EXPLAIN command returned no results."
            
            if not explain_results[0]:
                return "Failed to generate execution plan for the query. Empty result returned."
            
            # Extract the plan JSON
            plan_json = None
            # The column name might be 'QUERY PLAN' for PostgreSQL
            if 'QUERY PLAN' in explain_results[0]:
                try:
                    plan_json = json.loads(explain_results[0]['QUERY PLAN'][0])
                except:
                    return f"Error: Could not parse JSON from explain result: {explain_results[0]}"
            else:
                # Try to get the first column value
                first_col = list(explain_results[0].keys())[0]
                if explain_results[0][first_col]:
                    try:
                        plan_json = json.loads(explain_results[0][first_col])
                    except:
                        return f"Error: Could not parse JSON from explain result: {explain_results[0]}"
                else:
                    return f"Error: Could not find query plan in EXPLAIN results: {explain_results[0]}"
            
            # Get database structure information for tables involved in the query
            tables_involved = extract_tables_from_query(query)
            if not tables_involved:
                return "Could not identify any tables in the query. Please check the query syntax."
            
            table_stats = get_table_statistics(connector, tables_involved)
            schema_info = get_schema_information(connector, tables_involved)
            index_info = get_index_information(connector, tables_involved)
            
            # Detect query patterns and anti-patterns
            patterns = detect_query_patterns(plan_json)
            anti_patterns = detect_query_anti_patterns(query)
            
            # Analyze query complexity
            complexity = connector.analyze_query_complexity(query)
            
            # Format the response
            response = format_query_analysis_response(
                query=query,
                plan_json=plan_json,
                tables_involved=tables_involved,
                table_stats=table_stats,
                schema_info=schema_info,
                index_info=index_info,
                patterns=patterns,
                anti_patterns=anti_patterns,
                complexity=complexity
            )
            
            return response
        except Exception as e:
            error_details = traceback.format_exc()
            error_msg = f"Error analyzing query: {str(e)}\n\n"
            
            if debug:
                error_msg += f"Error details:\n{error_details}\n\n"
            
            return error_msg
        finally:
            # Only disconnect if we created a new connection
            if is_new and connector:
                connector.disconnect()
    
    @mcp_instance.tool()
    async def execute_read_only_query(
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
        debug: bool = False,
        ctx: Context = None
    ) -> str:
        """
        Execute a read-only SQL query and return the results.
        
        Args:
            query: The SQL query to execute (must be SELECT, EXPLAIN, or SHOW only)
            secret_name: Name of the secret in AWS Secrets Manager containing database credentials (optional if already connected)
            region_name: AWS region where the secret is stored (default: us-west-2)
            secret_arn: ARN of the secret in AWS Secrets Manager containing credentials (optional if already connected)
            resource_arn: ARN of the RDS cluster or instance (optional if already connected)
            database: Database name to connect to (optional if already connected)
            host: Database host (optional if already connected)
            port: Database port (optional if already connected)
            user: Database username (optional if already connected)
            password: Database password (optional if already connected)
            max_rows: Maximum number of rows to return (default: 100)
            debug: Enable detailed debug output (default: false)
        
        Returns:
            Query results in a formatted table
        """
        # Validate that this is a read-only query
        is_valid, error_message = validate_read_only_query(query)
        if not is_valid:
            return f"Error: {error_message}"
        
        try:
            # Get or create a database connection
            connector, is_new = await get_or_create_connection(
                ctx,
                secret_name=secret_name,
                region_name=region_name,
                secret_arn=secret_arn,
                resource_arn=resource_arn,
                database=database,
                host=host,
                port=port,
                user=user,
                password=password,
                readonly=True  # Force readonly for this operation
            )
            
            if not connector:
                return "Failed to connect to database. Please check your credentials and connection parameters."
            
            # Execute the query
            start_time = time.time()
            results = connector.execute_query(query)
            execution_time = time.time() - start_time
            
            if not results:
                return f"Query executed successfully in {execution_time:.2f} seconds, but returned no results."
            
            # Limit the number of rows returned
            if len(results) > max_rows:
                truncated = True
                results = results[:max_rows]
            else:
                truncated = False
            
            # Format the results as a markdown table
            response = f"## Query Results\n\n"
            response += f"Executed in {execution_time:.2f} seconds\n\n"
            
            if truncated:
                response += f"*Results truncated to {max_rows} rows*\n\n"
            
            # Get column names from the first row
            columns = list(results[0].keys())
            
            # Create the header row
            response += "| " + " | ".join(columns) + " |\n"
            response += "| " + " | ".join(["---" for _ in columns]) + " |\n"
            
            # Add data rows
            for row in results:
                # Convert each value to string and handle None values
                row_values = []
                for col in columns:
                    val = row.get(col)
                    if val is None:
                        row_values.append("NULL")
                    else:
                        # Escape pipe characters in the data to prevent breaking the markdown table
                        row_values.append(str(val).replace("|", "\\|"))
                
                response += "| " + " | ".join(row_values) + " |\n"
            
            response += f"\n{len(results)} rows returned" + (" (truncated)" if truncated else "")
            
            return response
        except Exception as e:
            error_details = traceback.format_exc()
            error_msg = f"Error executing query: {str(e)}\n\n"
            
            if debug:
                error_msg += f"Error details:\n{error_details}\n\n"
            
            return error_msg
        finally:
            # Only disconnect if we created a new connection
            if is_new and connector:
                connector.disconnect()

    @mcp_instance.tool()
    async def show_postgresql_settings(
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
        debug: bool = False,
        ctx: Context = None
    ) -> str:
        """
        Show PostgreSQL configuration settings with optional filtering.
        
        Args:
            pattern: Optional pattern to filter settings (e.g., "wal" for all WAL-related settings)
            secret_name: Name of the secret in AWS Secrets Manager containing database credentials (optional if already connected)
            region_name: AWS region where the secret is stored (default: us-west-2)
            secret_arn: ARN of the secret in AWS Secrets Manager containing credentials (optional if already connected)
            resource_arn: ARN of the RDS cluster or instance (optional if already connected)
            database: Database name to connect to (optional if already connected)
            host: Database host (optional if already connected)
            port: Database port (optional if already connected)
            user: Database username (optional if already connected)
            password: Database password (optional if already connected)
            debug: Enable detailed debug output (default: false)
        
        Returns:
            Current PostgreSQL configuration settings in a formatted table
        """
        try:
            # Get or create a database connection
            connector, is_new = await get_or_create_connection(
                ctx,
                secret_name=secret_name,
                region_name=region_name,
                secret_arn=secret_arn,
                resource_arn=resource_arn,
                database=database,
                host=host,
                port=port,
                user=user,
                password=password
            )
            
            if not connector:
                return "Failed to connect to database. Please check your credentials and connection parameters."
            
            # Build the query based on whether a pattern is provided
            if pattern:
                query = """
                    SELECT name, setting, unit, category, short_desc, context, source
                    FROM pg_settings
                    WHERE name ILIKE :pattern
                    ORDER BY category, name
                """
                results = connector.execute_query(query, {"pattern": f"%{pattern}%"})
            else:
                query = """
                    SELECT name, setting, unit, category, short_desc, context, source
                    FROM pg_settings
                    ORDER BY category, name
                """
                results = connector.execute_query(query)
            
            if not results:
                if pattern:
                    return f"No settings found matching pattern '{pattern}'."
                else:
                    return "No settings found."
            
            # Group settings by category for better organization
            settings_by_category = {}
            for setting in results:
                category = setting['category']
                if category not in settings_by_category:
                    settings_by_category[category] = []
                settings_by_category[category].append(setting)
            
            # Format the response
            response = "# PostgreSQL Configuration Settings\n\n"
            
            if pattern:
                response += f"Showing settings matching pattern: '{pattern}'\n\n"
            
            for category, settings in settings_by_category.items():
                response += f"## {category}\n\n"
                response += "| Name | Setting | Unit | Context | Source | Description |\n"
                response += "| ---- | ------- | ---- | ------- | ------ | ----------- |\n"
                
                for setting in settings:
                    name = setting['name']
                    value = setting['setting']
                    unit = setting['unit'] or ''
                    context = setting['context']
                    source = setting['source']
                    desc = setting['short_desc']
                    
                    response += f"| {name} | {value} | {unit} | {context} | {source} | {desc} |\n"
                
                response += "\n"
            
            response += f"\n{len(results)} setting(s) displayed."
            
            return response
        except Exception as e:
            error_details = traceback.format_exc()
            error_msg = f"Error retrieving PostgreSQL settings: {str(e)}\n\n"
            
            if debug:
                error_msg += f"Error details:\n{error_details}\n\n"
            
            return error_msg
        finally:
            # Only disconnect if we created a new connection
            if is_new and connector:
                connector.disconnect()


# Register all tools with the MCP server
register_all_tools(mcp)


def main():
    """Main entry point for the MCP server application."""
    parser = argparse.ArgumentParser(description='PostgreSQL MCP Server')
    parser.add_argument('--port', type=int, default=8000, help='Port to run the server on')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to bind the server to')
    parser.add_argument('--session-timeout', type=int, default=1800,
                        help='Session timeout in seconds (default: 1800)')
    parser.add_argument('--request-timeout', type=int, default=300,
                        help='Request timeout in seconds (default: 300)')
    parser.add_argument('--resource_arn', help='ARN of the RDS cluster')
    parser.add_argument('--secret_arn', help='ARN of the Secrets Manager secret for database credentials')
    parser.add_argument('--database', help='Database name')
    parser.add_argument('--region', help='AWS region for RDS Data API (default: us-west-2)')
    parser.add_argument('--readonly', help='Enforce NL to SQL to only allow readonly sql statement')
    
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
    
    # Initialize DB connection if RDS parameters are provided
    if args.resource_arn and args.secret_arn and args.database and args.region:
        logger.info(
            'PostgreSQL MCP init with CLUSTER_ARN:{}, SECRET_ARN:{}, REGION:{}, DATABASE:{}, READONLY:{}',
            args.resource_arn,
            args.secret_arn,
            args.region,
            args.database,
            args.readonly,
        )

        try:
            DBConnectionSingleton.initialize(
                args.resource_arn, args.secret_arn, args.database, args.region, args.readonly
            )
        except BotoCoreError:
            logger.exception('Failed to RDS API client object for PostgreSQL. Exit the MCP server')
            sys.exit(1)

        # Test RDS API connection
        ctx = DummyCtx()
        response = asyncio.run(run_query('SELECT 1', ctx))
        if (
            isinstance(response, list)
            and len(response) == 1
            and isinstance(response[0], dict)
            and 'error' in response[0]
        ):
            logger.error('Failed to validate RDS API db connection to PostgreSQL. Exit the MCP server')
            sys.exit(1)

        logger.success('Successfully validated RDS API db connection to PostgreSQL')
    
    try:
        # Run server with appropriate transport
        logger.info('Starting PostgreSQL MCP server')
        mcp.run(transport='streamable-http')
    except Exception as e:
        logger.error(f"Server error: {str(e)}")
        # If the server crashes, try to restart it
        time.sleep(5)  # Wait 5 seconds before restarting
        logger.info("Attempting to restart server...")
        mcp.run(transport='streamable-http')


if __name__ == "__main__":
    main()
