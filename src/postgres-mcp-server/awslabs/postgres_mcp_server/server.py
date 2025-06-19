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

"""awslabs postgres MCP Server implementation."""

import argparse
import asyncio
import boto3
import sys
from awslabs.postgres_mcp_server.mutable_sql_detector import (
    check_sql_injection_risk,
    detect_mutating_keywords,
)
from botocore.exceptions import BotoCoreError, ClientError
from loguru import logger
from mcp.server.fastmcp import Context, FastMCP
from pydantic import Field
from typing import Annotated, Any, Dict, List, Optional


client_error_code_key = 'run_query ClientError code'
unexpected_error_key = 'run_query unexpected error'
write_query_prohibited_key = 'Your MCP tool only allows readonly query. If you want to write, change the MCP configuration per README.md'
query_comment_prohibited_key = 'The comment in query is prohibited because of injection risk'
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


mcp = FastMCP(
    'apg-mcp MCP server. This is the starting point for all solutions created',
    dependencies=[
        'loguru',
    ],
)

# Configure MCP settings for better compatibility
mcp.settings.host = '0.0.0.0'  # Listen on all interfaces
mcp.settings.port = 8000       # Use port 8000
mcp.settings.json_response = True  # Enable JSON responses
mcp.settings.stateless_http = True  # Enable stateless HTTP mode


@mcp.tool(name='run_query', description='Run a SQL query using boto3 execute_statement')
async def run_query(
    sql: Annotated[str, Field(description='The SQL query to run')],
    ctx: Context,
    db_connection=None,
    query_parameters: Annotated[
        Optional[List[Dict[str, Any]]], Field(description='Parameters for the SQL query')
    ] = None,
) -> list[dict]:  # type: ignore
    """Run a SQL query using boto3 execute_statement.

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
        try:
            # First try to get the connection from the session handler
            from awslabs.postgres_mcp_server.session_handler import session_handler
            session_id = getattr(ctx, 'session_id', 'qchat_default_session')
            db_connection = session_handler.get_connection(session_id)
            
            if not db_connection:
                # Fall back to singleton if session connection doesn't exist
                db_connection = DBConnectionSingleton.get().db_connection
        except Exception as e:
            await ctx.error(f"No database connection available. Please configure the database first: {str(e)}")
            return [{'error': 'No database connection available'}]

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

        if db_connection.readonly_query:
            response = await asyncio.to_thread(
                execute_readonly_query, db_connection, sql, query_parameters
            )
        else:
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
    description='Fetch table columns and comments from Postgres using RDS Data API',
)
async def get_table_schema(
    table_name: Annotated[str, Field(description='name of the table')], ctx: Context
) -> list[dict]:
    """Get a table's schema information given the table name.

    Args:
        table_name: name of the table
        ctx: MCP context for logging and state management

    Returns:
        List of dictionary that contains query response rows
    """
    logger.info(f'get_table_schema: {table_name}')

    sql = """
        SELECT
            a.attname AS column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
            col_description(a.attrelid, a.attnum) AS column_comment
        FROM
            pg_attribute a
        WHERE
            a.attrelid = :table_name::regclass
            AND a.attnum > 0
            AND NOT a.attisdropped
        ORDER BY a.attnum
    """

    params = [{'name': 'table_name', 'value': {'stringValue': table_name}}]

    return await run_query(sql=sql, ctx=ctx, query_parameters=params)


def execute_readonly_query(
    db_connection: DBConnection, query: str, parameters: Optional[List[Dict[str, Any]]] = None
) -> dict:
    """Execute a query under readonly transaction.

    Args:
        db_connection: connection object
        query: query to run
        parameters: parameters

    Returns:
        List of dictionary that contains query response rows
    """
    tx_id = ''
    try:
        # Begin read-only transaction
        tx = db_connection.data_client.begin_transaction(
            resourceArn=db_connection.cluster_arn,
            secretArn=db_connection.secret_arn,
            database=db_connection.database,
        )

        tx_id = tx['transactionId']

        db_connection.data_client.execute_statement(
            resourceArn=db_connection.cluster_arn,
            secretArn=db_connection.secret_arn,
            database=db_connection.database,
            sql='SET TRANSACTION READ ONLY',
            transactionId=tx_id,
        )

        execute_params = {
            'resourceArn': db_connection.cluster_arn,
            'secretArn': db_connection.secret_arn,
            'database': db_connection.database,
            'sql': query,
            'includeResultMetadata': True,
            'transactionId': tx_id,
        }

        if parameters is not None:
            execute_params['parameters'] = parameters

        result = db_connection.data_client.execute_statement(**execute_params)

        db_connection.data_client.commit_transaction(
            resourceArn=db_connection.cluster_arn,
            secretArn=db_connection.secret_arn,
            transactionId=tx_id,
        )
        return result
    except Exception as e:
        if tx_id:
            db_connection.data_client.rollback_transaction(
                resourceArn=db_connection.cluster_arn,
                secretArn=db_connection.secret_arn,
                transactionId=tx_id,
            )
        raise e


def main():
    """Main entry point for the MCP server application."""
    global client_error_code_key

    """Run the MCP server with CLI argument support."""
    parser = argparse.ArgumentParser(
        description='An AWS Labs Model Context Protocol (MCP) server for postgres'
    )
    parser.add_argument('--resource_arn', required=True, help='ARN of the RDS cluster')
    parser.add_argument(
        '--secret_arn',
        required=True,
        help='ARN of the Secrets Manager secret for database credentials',
    )
    parser.add_argument('--database', required=True, help='Database name')
    parser.add_argument(
        '--region', required=True, help='AWS region for RDS Data API (default: us-west-2)'
    )
    parser.add_argument(
        '--readonly', required=True, help='Enforce NL to SQL to only allow readonly sql statement'
    )
    args = parser.parse_args()

    logger.info(
        'Postgres MCP init with CLUSTER_ARN:{}, SECRET_ARN:{}, REGION:{}, DATABASE:{}, READONLY:{}',
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
        logger.exception('Failed to RDS API client object for Postgres. Exit the MCP server')
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
        logger.error('Failed to validate RDS API db connection to Postgres. Exit the MCP server')
        sys.exit(1)

    logger.success('Successfully validated RDS API db connection to Postgres')

    # Use HTTP transport for standalone operation
    logger.info('Starting Postgres MCP server with HTTP transport on 0.0.0.0:8000')
    mcp.run(transport="http", host="0.0.0.0", port=8000)


@mcp.tool(
    name='connect_database',
    description='Connect to a PostgreSQL database and store the connection in the session'
)
async def connect_database(
    ctx: Context,
    secret_name: Annotated[Optional[str], Field(description='Name of the secret in AWS Secrets Manager')] = None,
    region_name: Annotated[str, Field(description='AWS region where the secret is stored')] = "us-west-2",
    secret_arn: Annotated[Optional[str], Field(description='ARN of the secret in AWS Secrets Manager')] = None,
    resource_arn: Annotated[Optional[str], Field(description='ARN of the RDS cluster or instance')] = None,
    database: Annotated[Optional[str], Field(description='Database name to connect to')] = None,
    host: Annotated[Optional[str], Field(description='Database host')] = None,
    port: Annotated[Optional[int], Field(description='Database port')] = None,
    user: Annotated[Optional[str], Field(description='Database username')] = None,
    password: Annotated[Optional[str], Field(description='Database password')] = None,
    readonly: Annotated[bool, Field(description='Whether to enforce read-only mode')] = True
) -> str:
    """
    Connect to a PostgreSQL database and store the connection in the session.
    
    Args:
        ctx: MCP context
        secret_name: Name of the secret in AWS Secrets Manager
        region_name: AWS region where the secret is stored
        secret_arn: ARN of the secret in AWS Secrets Manager
        resource_arn: ARN of the RDS cluster or instance
        database: Database name to connect to
        host: Database host
        port: Database port
        user: Database username
        password: Database password
        readonly: Whether to enforce read-only mode
        
    Returns:
        Success or error message
    """
    from awslabs.postgres_mcp_server.connection_manager import initialize_connection
    
    # If secret_name is provided but not secret_arn, convert it to ARN
    if secret_name and not secret_arn:
        try:
            import boto3
            sm_client = boto3.client('secretsmanager', region_name=region_name)
            response = sm_client.describe_secret(SecretId=secret_name)
            secret_arn = response['ARN']
            logger.info(f"Converted secret name {secret_name} to ARN: {secret_arn}")
        except Exception as e:
            error_msg = f"Failed to convert secret name to ARN: {str(e)}"
            await ctx.error(error_msg)
            return error_msg
    
    # Initialize the connection
    success = await initialize_connection(
        ctx=ctx,
        secret_arn=secret_arn,
        region_name=region_name,
        resource_arn=resource_arn,
        database=database,
        hostname=host,
        port=port,
        readonly=readonly
    )
    
    if success:
        return "Successfully connected to the PostgreSQL database"
    else:
        return "Failed to connect to the PostgreSQL database. Check the error message for details."


@mcp.tool(
    name='disconnect_database',
    description='Disconnect from the PostgreSQL database and remove the connection from the session'
)
async def disconnect_database(
    ctx: Context
) -> str:
    """
    Disconnect from the PostgreSQL database and remove the connection from the session.
    
    Args:
        ctx: MCP context
        
    Returns:
        Success or error message
    """
    from awslabs.postgres_mcp_server.connection_manager import close_connection
    
    success = await close_connection(ctx)
    
    if success:
        return "Successfully disconnected from the PostgreSQL database"
    else:
        return "No active database connection to disconnect"


@mcp.tool(name='debug_connection', description='Debug database connection status')
async def debug_connection(
    ctx: Context
) -> str:
    """
    Debug the database connection status.
    
    Args:
        ctx: MCP context
        
    Returns:
        Connection status information
    """
    # Check session connection
    from awslabs.postgres_mcp_server.session_handler import session_handler
    session_id = getattr(ctx, 'session_id', 'qchat_default_session')
    session_connection = session_handler.get_connection(session_id)
    session_params = session_handler.get_connection_params(session_id)
    
    # Check singleton connection
    singleton_connection = None
    singleton_error = "Not checked"
    try:
        singleton = DBConnectionSingleton.get()
        singleton_connection = singleton._connection if hasattr(singleton, '_connection') else None
    except Exception as e:
        singleton_error = str(e)
    
    # Build debug info
    debug_info = {
        "session_connection": {
            "exists": session_connection is not None,
            "type": type(session_connection).__name__ if session_connection else None,
            "connected": session_connection.is_connected() if session_connection else False,
            "info": session_connection.connection_info if session_connection else None,
            "params": session_params
        },
        "singleton_connection": {
            "exists": singleton_connection is not None,
            "type": type(singleton_connection).__name__ if singleton_connection else None,
            "error": singleton_error if not singleton_connection else None
        }
    }
    
    # Format as readable string
    result = "Database Connection Debug Information:\n\n"
    
    # Session connection info
    result += "Session Connection:\n"
    if debug_info["session_connection"]["exists"]:
        result += f"  ✅ Connection exists: {debug_info['session_connection']['type']}\n"
        result += f"  ✅ Connected: {debug_info['session_connection']['connected']}\n"
        result += f"  ℹ️ Connection info: {debug_info['session_connection']['info']}\n"
        result += f"  ℹ️ Connection params: {debug_info['session_connection']['params']}\n"
    else:
        result += "  ❌ No session connection\n"
    
    # Singleton connection info
    result += "\nSingleton Connection:\n"
    if debug_info["singleton_connection"]["exists"]:
        result += f"  ✅ Connection exists: {debug_info['singleton_connection']['type']}\n"
    else:
        result += f"  ❌ No singleton connection: {debug_info['singleton_connection']['error']}\n"
    
    # Recommendation
    if not debug_info["session_connection"]["exists"]:
        result += "\nRecommendation: Run configure_database to establish a connection\n"
    
    return result


@mcp.tool(
    name='connect_database',
    description='Connect to a PostgreSQL database and store the connection in the session'
)
async def connect_database(
    ctx: Context,
    secret_name: Annotated[Optional[str], Field(description='Name of the secret in AWS Secrets Manager')] = None,
    region_name: Annotated[str, Field(description='AWS region where the secret is stored')] = "us-west-2",
    secret_arn: Annotated[Optional[str], Field(description='ARN of the secret in AWS Secrets Manager')] = None,
    resource_arn: Annotated[Optional[str], Field(description='ARN of the RDS cluster or instance')] = None,
    database: Annotated[Optional[str], Field(description='Database name to connect to')] = None,
    host: Annotated[Optional[str], Field(description='Database host')] = None,
    port: Annotated[Optional[int], Field(description='Database port')] = None,
    user: Annotated[Optional[str], Field(description='Database username')] = None,
    password: Annotated[Optional[str], Field(description='Database password')] = None,
    readonly: Annotated[bool, Field(description='Whether to enforce read-only mode')] = True
) -> str:
    """
    Connect to a PostgreSQL database and store the connection in the session.
    
    Args:
        ctx: MCP context
        secret_name: Name of the secret in AWS Secrets Manager
        region_name: AWS region where the secret is stored
        secret_arn: ARN of the secret in AWS Secrets Manager
        resource_arn: ARN of the RDS cluster or instance
        database: Database name to connect to
        host: Database host
        port: Database port
        user: Database username
        password: Database password
        readonly: Whether to enforce read-only mode
        
    Returns:
        Success or error message
    """
    from awslabs.postgres_mcp_server.connection_manager import initialize_connection
    
    # If secret_name is provided but not secret_arn, convert it to ARN
    if secret_name and not secret_arn:
        try:
            import boto3
            sm_client = boto3.client('secretsmanager', region_name=region_name)
            response = sm_client.describe_secret(SecretId=secret_name)
            secret_arn = response['ARN']
            logger.info(f"Converted secret name {secret_name} to ARN: {secret_arn}")
        except Exception as e:
            error_msg = f"Failed to convert secret name to ARN: {str(e)}"
            await ctx.error(error_msg)
            return error_msg
    
    # Initialize the connection
    success = await initialize_connection(
        ctx=ctx,
        secret_arn=secret_arn,
        region_name=region_name,
        resource_arn=resource_arn,
        database=database,
        hostname=host,
        port=port,
        readonly=readonly
    )
    
    if success:
        return "Successfully connected to the PostgreSQL database"
    else:
        return "Failed to connect to the PostgreSQL database. Check the error message for details."


@mcp.tool(
    name='disconnect_database',
    description='Disconnect from the PostgreSQL database and remove the connection from the session'
)
async def disconnect_database(
    ctx: Context
) -> str:
    """
    Disconnect from the PostgreSQL database and remove the connection from the session.
    
    Args:
        ctx: MCP context
        
    Returns:
        Success or error message
    """
    from awslabs.postgres_mcp_server.connection_manager import close_connection
    
    success = await close_connection(ctx)
    
    if success:
        return "Successfully disconnected from the PostgreSQL database"
    else:
        return "No active database connection to disconnect"


@mcp.tool(
    name='analyze_database_structure',
    description='Analyze the database structure and provide insights on schema design, indexes, and potential optimizations'
)
async def analyze_database_structure(
    ctx: Context,
    secret_name: Annotated[Optional[str], Field(description='Name of the secret in AWS Secrets Manager')] = None,
    region_name: Annotated[str, Field(description='AWS region where the secret is stored')] = "us-west-2",
    secret_arn: Annotated[Optional[str], Field(description='ARN of the secret in AWS Secrets Manager')] = None,
    resource_arn: Annotated[Optional[str], Field(description='ARN of the RDS cluster or instance')] = None,
    database: Annotated[Optional[str], Field(description='Database name to connect to')] = None,
    host: Annotated[Optional[str], Field(description='Database host')] = None,
    port: Annotated[Optional[int], Field(description='Database port')] = None,
    user: Annotated[Optional[str], Field(description='Database username')] = None,
    password: Annotated[Optional[str], Field(description='Database password')] = None,
    debug: Annotated[bool, Field(description='Whether to include debug information')] = False
) -> str:
    """
    Analyze the database structure and provide insights on schema design, indexes, and potential optimizations.
    
    Args:
        ctx: MCP context
        secret_name: Name of the secret in AWS Secrets Manager
        region_name: AWS region where the secret is stored
        secret_arn: ARN of the secret in AWS Secrets Manager
        resource_arn: ARN of the RDS cluster or instance
        database: Database name to connect to
        host: Database host
        port: Database port
        user: Database username
        password: Database password
        debug: Whether to include debug information
        
    Returns:
        Analysis of the database structure
    """
    from awslabs.postgres_mcp_server.connection_manager import get_or_create_connection
    from awslabs.postgres_mcp_server.analysis.structure import get_database_structure
    
    # Get or create a connection
    connector, is_new = await get_or_create_connection(
        ctx=ctx,
        secret_arn=secret_arn,
        region_name=region_name,
        resource_arn=resource_arn,
        database=database,
        hostname=host,
        port=port
    )
    
    if not connector:
        return "Failed to connect to the database. Please check your connection parameters."
    
    try:
        # Get database structure
        structure = get_database_structure(connector)
        
        # Format the analysis
        result = "# Database Structure Analysis\n\n"
        
        # Tables summary
        result += "## Tables Summary\n\n"
        result += f"Total tables: {len(structure['tables'])}\n"
        result += f"Total size: {sum(table['size_bytes'] for table in structure['tables'])/(1024*1024):.2f} MB\n\n"
        
        # Top 10 largest tables
        result += "## Top 10 Largest Tables\n\n"
        result += "| Table | Schema | Size | Rows (est.) |\n"
        result += "|-------|--------|------|-------------|\n"
        for table in sorted(structure['tables'], key=lambda t: t['size_bytes'], reverse=True)[:10]:
            result += f"| {table['table_name']} | {table['table_schema']} | {table['total_size']} | {table['row_estimate']:,} |\n"
        result += "\n"
        
        # Indexes summary
        result += "## Indexes Summary\n\n"
        result += f"Total indexes: {len(structure['indexes'])}\n"
        result += f"Total index size: {sum(idx['index_size_bytes'] for idx in structure['indexes'])/(1024*1024):.2f} MB\n\n"
        
        # Tables without primary keys
        tables_without_pk = [t['table_name'] for t in structure['tables_without_pk']]
        if tables_without_pk:
            result += "## Tables Without Primary Keys\n\n"
            result += "The following tables don't have primary keys, which can lead to performance issues:\n\n"
            for table in tables_without_pk:
                result += f"- {table}\n"
            result += "\n"
        
        # Tables without indexes
        tables_without_indexes = [t['table_name'] for t in structure['tables_without_indexes']]
        if tables_without_indexes:
            result += "## Tables Without Indexes\n\n"
            result += "The following tables don't have any indexes, which can lead to full table scans:\n\n"
            for table in tables_without_indexes:
                result += f"- {table}\n"
            result += "\n"
        
        # Foreign key constraints without indexes
        if structure['fk_without_index']:
            result += "## Foreign Keys Without Indexes\n\n"
            result += "The following foreign key constraints don't have corresponding indexes:\n\n"
            result += "| Table | Column | Referenced Table | Referenced Column |\n"
            result += "|-------|--------|------------------|-------------------|\n"
            for fk in structure['fk_without_index']:
                result += f"| {fk['table_name']} | {fk['column_name']} | {fk['foreign_table_name']} | {fk['foreign_column_name']} |\n"
            result += "\n"
        
        # Recommendations
        result += "## Recommendations\n\n"
        
        if tables_without_pk:
            result += "1. **Add Primary Keys**: Tables without primary keys can cause performance issues. Consider adding primary keys to these tables.\n\n"
        
        if tables_without_indexes:
            result += "2. **Add Indexes**: Tables without indexes will require full table scans for queries. Consider adding appropriate indexes based on query patterns.\n\n"
        
        if structure['fk_without_index']:
            result += "3. **Index Foreign Keys**: Foreign keys without indexes can cause performance issues during joins. Consider adding indexes to these columns.\n\n"
        
        result += "4. **Review Large Tables**: Consider partitioning or archiving data for very large tables.\n\n"
        
        if debug:
            result += "## Debug Information\n\n"
            result += f"Connection type: {type(connector).__name__}\n"
            result += f"Database: {connector.database if hasattr(connector, 'database') else 'Unknown'}\n"
            result += f"Connected: {connector.is_connected() if hasattr(connector, 'is_connected') else 'Unknown'}\n"
        
        return result
    except Exception as e:
        error_msg = f"Error analyzing database structure: {str(e)}"
        await ctx.error(error_msg)
        return error_msg


@mcp.tool(
    name='analyze_query',
    description='Analyze a SQL query and provide optimization recommendations'
)
async def analyze_query(
    ctx: Context,
    query: Annotated[str, Field(description='The SQL query to analyze')],
    secret_name: Annotated[Optional[str], Field(description='Name of the secret in AWS Secrets Manager')] = None,
    region_name: Annotated[str, Field(description='AWS region where the secret is stored')] = "us-west-2",
    secret_arn: Annotated[Optional[str], Field(description='ARN of the secret in AWS Secrets Manager')] = None,
    resource_arn: Annotated[Optional[str], Field(description='ARN of the RDS cluster or instance')] = None,
    database: Annotated[Optional[str], Field(description='Database name to connect to')] = None,
    host: Annotated[Optional[str], Field(description='Database host')] = None,
    port: Annotated[Optional[int], Field(description='Database port')] = None,
    user: Annotated[Optional[str], Field(description='Database username')] = None,
    password: Annotated[Optional[str], Field(description='Database password')] = None,
    debug: Annotated[bool, Field(description='Whether to include debug information')] = False
) -> str:
    """
    Analyze a SQL query and provide optimization recommendations.
    
    Args:
        ctx: MCP context
        query: The SQL query to analyze
        secret_name: Name of the secret in AWS Secrets Manager
        region_name: AWS region where the secret is stored
        secret_arn: ARN of the secret in AWS Secrets Manager
        resource_arn: ARN of the RDS cluster or instance
        database: Database name to connect to
        host: Database host
        port: Database port
        user: Database username
        password: Database password
        debug: Whether to include debug information
        
    Returns:
        Analysis of the SQL query with optimization recommendations
    """
    from awslabs.postgres_mcp_server.connection_manager import get_or_create_connection
    from awslabs.postgres_mcp_server.analysis.query import analyze_query_performance
    
    # Get or create a connection
    connector, is_new = await get_or_create_connection(
        ctx=ctx,
        secret_arn=secret_arn,
        region_name=region_name,
        resource_arn=resource_arn,
        database=database,
        hostname=host,
        port=port
    )
    
    if not connector:
        return "Failed to connect to the database. Please check your connection parameters."
    
    try:
        # Analyze the query
        analysis = analyze_query_performance(connector, query)
        
        # Format the analysis
        result = "# SQL Query Analysis\n\n"
        
        # Query plan
        result += "## Query Plan\n\n"
        result += "```\n"
        result += analysis['query_plan']
        result += "\n```\n\n"
        
        # Cost analysis
        result += "## Cost Analysis\n\n"
        result += f"Estimated startup cost: {analysis['startup_cost']}\n"
        result += f"Estimated total cost: {analysis['total_cost']}\n"
        result += f"Estimated rows: {analysis['plan_rows']}\n"
        result += f"Estimated width: {analysis['plan_width']} bytes\n\n"
        
        # Optimization issues
        if analysis['optimization_issues']:
            result += "## Optimization Issues\n\n"
            for issue in analysis['optimization_issues']:
                result += f"- {issue}\n"
            result += "\n"
        
        # Recommendations
        result += "## Recommendations\n\n"
        for recommendation in analysis['recommendations']:
            result += f"- {recommendation}\n"
        result += "\n"
        
        # Tables involved
        result += "## Tables Involved\n\n"
        for table in analysis['tables_involved']:
            result += f"- {table}\n"
        result += "\n"
        
        if debug:
            result += "## Debug Information\n\n"
            result += f"Connection type: {type(connector).__name__}\n"
            result += f"Database: {connector.database if hasattr(connector, 'database') else 'Unknown'}\n"
            result += f"Connected: {connector.is_connected() if hasattr(connector, 'is_connected') else 'Unknown'}\n"
        
        return result
    except Exception as e:
        error_msg = f"Error analyzing query: {str(e)}"
        await ctx.error(error_msg)
        return error_msg


@mcp.tool(
    name='recommend_indexes',
    description='Recommend indexes for a given SQL query'
)
async def recommend_indexes(
    ctx: Context,
    query: Annotated[str, Field(description='The SQL query to analyze for index recommendations')],
    secret_name: Annotated[Optional[str], Field(description='Name of the secret in AWS Secrets Manager')] = None,
    region_name: Annotated[str, Field(description='AWS region where the secret is stored')] = "us-west-2",
    secret_arn: Annotated[Optional[str], Field(description='ARN of the secret in AWS Secrets Manager')] = None,
    resource_arn: Annotated[Optional[str], Field(description='ARN of the RDS cluster or instance')] = None,
    database: Annotated[Optional[str], Field(description='Database name to connect to')] = None,
    host: Annotated[Optional[str], Field(description='Database host')] = None,
    port: Annotated[Optional[int], Field(description='Database port')] = None,
    user: Annotated[Optional[str], Field(description='Database username')] = None,
    password: Annotated[Optional[str], Field(description='Database password')] = None,
    debug: Annotated[bool, Field(description='Whether to include debug information')] = False
) -> str:
    """
    Recommend indexes for a given SQL query.
    
    Args:
        ctx: MCP context
        query: The SQL query to analyze for index recommendations
        secret_name: Name of the secret in AWS Secrets Manager
        region_name: AWS region where the secret is stored
        secret_arn: ARN of the secret in AWS Secrets Manager
        resource_arn: ARN of the RDS cluster or instance
        database: Database name to connect to
        host: Database host
        port: Database port
        user: Database username
        password: Database password
        debug: Whether to include debug information
        
    Returns:
        Index recommendations for the SQL query
    """
    from awslabs.postgres_mcp_server.connection_manager import get_or_create_connection
    from awslabs.postgres_mcp_server.analysis.indexes import recommend_query_indexes
    
    # Get or create a connection
    connector, is_new = await get_or_create_connection(
        ctx=ctx,
        secret_arn=secret_arn,
        region_name=region_name,
        resource_arn=resource_arn,
        database=database,
        hostname=host,
        port=port
    )
    
    if not connector:
        return "Failed to connect to the database. Please check your connection parameters."
    
    try:
        # Get index recommendations
        recommendations = recommend_query_indexes(connector, query)
        
        # Format the recommendations
        result = "# Index Recommendations\n\n"
        
        # Original query
        result += "## Original Query\n\n"
        result += "```sql\n"
        result += query
        result += "\n```\n\n"
        
        # Recommended indexes
        result += "## Recommended Indexes\n\n"
        if recommendations['recommended_indexes']:
            for idx in recommendations['recommended_indexes']:
                result += f"### Index on {idx['table']}\n\n"
                result += "```sql\n"
                result += idx['create_statement']
                result += "\n```\n\n"
                result += f"Estimated improvement: {idx['estimated_improvement']}%\n\n"
        else:
            result += "No additional indexes recommended for this query.\n\n"
        
        # Existing indexes
        result += "## Existing Relevant Indexes\n\n"
        if recommendations['existing_indexes']:
            for idx in recommendations['existing_indexes']:
                result += f"- {idx['index_name']} on {idx['table']} ({idx['columns']})\n"
        else:
            result += "No existing indexes found that are relevant to this query.\n\n"
        
        # Cost comparison
        result += "## Cost Comparison\n\n"
        result += f"Current query cost: {recommendations['current_cost']}\n"
        result += f"Estimated cost with recommended indexes: {recommendations['estimated_cost']}\n"
        result += f"Potential improvement: {recommendations['potential_improvement']}%\n\n"
        
        if debug:
            result += "## Debug Information\n\n"
            result += f"Connection type: {type(connector).__name__}\n"
            result += f"Database: {connector.database if hasattr(connector, 'database') else 'Unknown'}\n"
            result += f"Connected: {connector.is_connected() if hasattr(connector, 'is_connected') else 'Unknown'}\n"
        
        return result
    except Exception as e:
        error_msg = f"Error recommending indexes: {str(e)}"
        await ctx.error(error_msg)
        return error_msg
@mcp.tool(
    name='execute_read_only_query',
    description='Execute a read-only SQL query and return the results'
)
async def execute_read_only_query(
    ctx: Context,
    query: Annotated[str, Field(description='The SQL query to execute')],
    secret_name: Annotated[Optional[str], Field(description='Name of the secret in AWS Secrets Manager')] = None,
    region_name: Annotated[str, Field(description='AWS region where the secret is stored')] = "us-west-2",
    secret_arn: Annotated[Optional[str], Field(description='ARN of the secret in AWS Secrets Manager')] = None,
    resource_arn: Annotated[Optional[str], Field(description='ARN of the RDS cluster or instance')] = None,
    database: Annotated[Optional[str], Field(description='Database name to connect to')] = None,
    host: Annotated[Optional[str], Field(description='Database host')] = None,
    port: Annotated[Optional[int], Field(description='Database port')] = None,
    user: Annotated[Optional[str], Field(description='Database username')] = None,
    password: Annotated[Optional[str], Field(description='Database password')] = None,
    max_rows: Annotated[int, Field(description='Maximum number of rows to return')] = 100,
    debug: Annotated[bool, Field(description='Whether to include debug information')] = False
) -> str:
    """
    Execute a read-only SQL query and return the results.
    
    Args:
        ctx: MCP context
        query: The SQL query to execute
        secret_name: Name of the secret in AWS Secrets Manager
        region_name: AWS region where the secret is stored
        secret_arn: ARN of the secret in AWS Secrets Manager
        resource_arn: ARN of the RDS cluster or instance
        database: Database name to connect to
        host: Database host
        port: Database port
        user: Database username
        password: Database password
        max_rows: Maximum number of rows to return
        debug: Whether to include debug information
        
    Returns:
        Results of the SQL query
    """
    from awslabs.postgres_mcp_server.connection_manager import get_or_create_connection
    
    # Check if the query is read-only
    matches = detect_mutating_keywords(query)
    if matches:
        error_msg = f"Query contains mutating keywords: {', '.join(matches)}. Only read-only queries are allowed."
        await ctx.error(error_msg)
        return error_msg
    
    # Get or create a connection
    connector, is_new = await get_or_create_connection(
        ctx=ctx,
        secret_arn=secret_arn,
        region_name=region_name,
        resource_arn=resource_arn,
        database=database,
        hostname=host,
        port=port,
        readonly=True
    )
    
    if not connector:
        return "Failed to connect to the database. Please check your connection parameters."
    
    try:
        # Execute the query
        results = await run_query(query, ctx)
        
        # Format the results
        result = "# Query Results\n\n"
        
        # Original query
        result += "## Query\n\n"
        result += "```sql\n"
        result += query
        result += "\n```\n\n"
        
        # Results
        result += "## Results\n\n"
        
        if not results:
            result += "No results returned.\n\n"
        elif isinstance(results, list) and len(results) > 0 and isinstance(results[0], dict) and 'error' in results[0]:
            result += f"Error: {results[0]['error']}\n\n"
        else:
            # Limit the number of rows
            limited_results = results[:max_rows]
            
            # Get column names
            if limited_results and len(limited_results) > 0:
                columns = list(limited_results[0].keys())
                
                # Create markdown table
                result += "| " + " | ".join(columns) + " |\n"
                result += "| " + " | ".join(["---" for _ in columns]) + " |\n"
                
                for row in limited_results:
                    result += "| " + " | ".join([str(row.get(col, "")) for col in columns]) + " |\n"
                
                if len(results) > max_rows:
                    result += f"\n*Results limited to {max_rows} rows. Total rows: {len(results)}*\n\n"
            else:
                result += "No results returned.\n\n"
        
        if debug:
            result += "## Debug Information\n\n"
            result += f"Connection type: {type(connector).__name__}\n"
            result += f"Database: {connector.database if hasattr(connector, 'database') else 'Unknown'}\n"
            result += f"Connected: {connector.is_connected() if hasattr(connector, 'is_connected') else 'Unknown'}\n"
        
        return result
    except Exception as e:
        error_msg = f"Error executing query: {str(e)}"
        await ctx.error(error_msg)
        return error_msg


@mcp.tool(
    name='analyze_table_fragmentation',
    description='Analyze table fragmentation and provide optimization recommendations'
)
async def analyze_table_fragmentation(
    ctx: Context,
    secret_name: Annotated[Optional[str], Field(description='Name of the secret in AWS Secrets Manager')] = None,
    region_name: Annotated[str, Field(description='AWS region where the secret is stored')] = "us-west-2",
    secret_arn: Annotated[Optional[str], Field(description='ARN of the secret in AWS Secrets Manager')] = None,
    resource_arn: Annotated[Optional[str], Field(description='ARN of the RDS cluster or instance')] = None,
    database: Annotated[Optional[str], Field(description='Database name to connect to')] = None,
    host: Annotated[Optional[str], Field(description='Database host')] = None,
    port: Annotated[Optional[int], Field(description='Database port')] = None,
    user: Annotated[Optional[str], Field(description='Database username')] = None,
    password: Annotated[Optional[str], Field(description='Database password')] = None,
    threshold: Annotated[float, Field(description='Fragmentation threshold percentage')] = 10.0,
    debug: Annotated[bool, Field(description='Whether to include debug information')] = False
) -> str:
    """
    Analyze table fragmentation and provide optimization recommendations.
    
    Args:
        ctx: MCP context
        secret_name: Name of the secret in AWS Secrets Manager
        region_name: AWS region where the secret is stored
        secret_arn: ARN of the secret in AWS Secrets Manager
        resource_arn: ARN of the RDS cluster or instance
        database: Database name to connect to
        host: Database host
        port: Database port
        user: Database username
        password: Database password
        threshold: Fragmentation threshold percentage
        debug: Whether to include debug information
        
    Returns:
        Analysis of table fragmentation with optimization recommendations
    """
    from awslabs.postgres_mcp_server.connection_manager import get_or_create_connection
    from awslabs.postgres_mcp_server.analysis.fragmentation import analyze_fragmentation
    
    # Get or create a connection
    connector, is_new = await get_or_create_connection(
        ctx=ctx,
        secret_arn=secret_arn,
        region_name=region_name,
        resource_arn=resource_arn,
        database=database,
        hostname=host,
        port=port
    )
    
    if not connector:
        return "Failed to connect to the database. Please check your connection parameters."
    
    try:
        # Analyze fragmentation
        fragmentation = analyze_fragmentation(connector, threshold)
        
        # Format the analysis
        result = "# Table Fragmentation Analysis\n\n"
        
        # Summary
        result += "## Summary\n\n"
        result += f"Total tables analyzed: {fragmentation['total_tables']}\n"
        result += f"Tables with fragmentation above {threshold}%: {len(fragmentation['fragmented_tables'])}\n\n"
        
        # Fragmented tables
        if fragmentation['fragmented_tables']:
            result += "## Fragmented Tables\n\n"
            result += "| Table | Schema | Fragmentation % | Size | Dead Tuples | Live Tuples |\n"
            result += "|-------|--------|----------------|------|-------------|-------------|\n"
            for table in fragmentation['fragmented_tables']:
                result += f"| {table['table_name']} | {table['schema_name']} | {table['fragmentation_pct']:.2f}% | {table['size']} | {table['dead_tuples']:,} | {table['live_tuples']:,} |\n"
            result += "\n"
        else:
            result += "No tables found with fragmentation above the threshold.\n\n"
        
        # Recommendations
        result += "## Recommendations\n\n"
        
        if fragmentation['fragmented_tables']:
            result += "### VACUUM Commands\n\n"
            result += "Run the following commands to reclaim space:\n\n"
            result += "```sql\n"
            for table in fragmentation['fragmented_tables']:
                result += f"VACUUM FULL {table['schema_name']}.{table['table_name']};\n"
            result += "```\n\n"
            
            result += "### ANALYZE Commands\n\n"
            result += "Run the following commands to update statistics:\n\n"
            result += "```sql\n"
            for table in fragmentation['fragmented_tables']:
                result += f"ANALYZE {table['schema_name']}.{table['table_name']};\n"
            result += "```\n\n"
            
            result += "### Autovacuum Settings\n\n"
            result += "Consider adjusting autovacuum settings for these tables:\n\n"
            result += "```sql\n"
            for table in fragmentation['fragmented_tables']:
                result += f"ALTER TABLE {table['schema_name']}.{table['table_name']} SET (autovacuum_vacuum_scale_factor = 0.05);\n"
            result += "```\n\n"
        else:
            result += "No specific recommendations needed. Table fragmentation is below the threshold.\n\n"
        
        if debug:
            result += "## Debug Information\n\n"
            result += f"Connection type: {type(connector).__name__}\n"
            result += f"Database: {connector.database if hasattr(connector, 'database') else 'Unknown'}\n"
            result += f"Connected: {connector.is_connected() if hasattr(connector, 'is_connected') else 'Unknown'}\n"
        
        return result
    except Exception as e:
        error_msg = f"Error analyzing table fragmentation: {str(e)}"
        await ctx.error(error_msg)
        return error_msg


@mcp.tool(
    name='analyze_vacuum_stats',
    description='Analyze vacuum statistics and provide recommendations for vacuum settings'
)
async def analyze_vacuum_stats(
    ctx: Context,
    secret_name: Annotated[Optional[str], Field(description='Name of the secret in AWS Secrets Manager')] = None,
    region_name: Annotated[str, Field(description='AWS region where the secret is stored')] = "us-west-2",
    secret_arn: Annotated[Optional[str], Field(description='ARN of the secret in AWS Secrets Manager')] = None,
    resource_arn: Annotated[Optional[str], Field(description='ARN of the RDS cluster or instance')] = None,
    database: Annotated[Optional[str], Field(description='Database name to connect to')] = None,
    host: Annotated[Optional[str], Field(description='Database host')] = None,
    port: Annotated[Optional[int], Field(description='Database port')] = None,
    user: Annotated[Optional[str], Field(description='Database username')] = None,
    password: Annotated[Optional[str], Field(description='Database password')] = None,
    debug: Annotated[bool, Field(description='Whether to include debug information')] = False
) -> str:
    """
    Analyze vacuum statistics and provide recommendations for vacuum settings.
    
    Args:
        ctx: MCP context
        secret_name: Name of the secret in AWS Secrets Manager
        region_name: AWS region where the secret is stored
        secret_arn: ARN of the secret in AWS Secrets Manager
        resource_arn: ARN of the RDS cluster or instance
        database: Database name to connect to
        host: Database host
        port: Database port
        user: Database username
        password: Database password
        debug: Whether to include debug information
        
    Returns:
        Analysis of vacuum statistics with recommendations
    """
    from awslabs.postgres_mcp_server.connection_manager import get_or_create_connection
    from awslabs.postgres_mcp_server.analysis.vacuum import analyze_vacuum_statistics
    
    # Get or create a connection
    connector, is_new = await get_or_create_connection(
        ctx=ctx,
        secret_arn=secret_arn,
        region_name=region_name,
        resource_arn=resource_arn,
        database=database,
        hostname=host,
        port=port
    )
    
    if not connector:
        return "Failed to connect to the database. Please check your connection parameters."
    
    try:
        # Analyze vacuum statistics
        vacuum_stats = analyze_vacuum_statistics(connector)
        
        # Format the analysis
        result = "# Vacuum Statistics Analysis\n\n"
        
        # Summary
        result += "## Summary\n\n"
        result += f"Total tables analyzed: {vacuum_stats['total_tables']}\n"
        result += f"Tables needing vacuum: {len(vacuum_stats['tables_needing_vacuum'])}\n"
        result += f"Tables needing analyze: {len(vacuum_stats['tables_needing_analyze'])}\n\n"
        
        # Tables needing vacuum
        if vacuum_stats['tables_needing_vacuum']:
            result += "## Tables Needing VACUUM\n\n"
            result += "| Table | Schema | Dead Tuples | Live Tuples | Last Vacuum | Last Auto Vacuum |\n"
            result += "|-------|--------|-------------|-------------|-------------|------------------|\n"
            for table in vacuum_stats['tables_needing_vacuum']:
                result += f"| {table['table_name']} | {table['schema_name']} | {table['dead_tuples']:,} | {table['live_tuples']:,} | {table['last_vacuum'] or 'Never'} | {table['last_autovacuum'] or 'Never'} |\n"
            result += "\n"
        
        # Tables needing analyze
        if vacuum_stats['tables_needing_analyze']:
            result += "## Tables Needing ANALYZE\n\n"
            result += "| Table | Schema | Modified Rows | Last Analyze | Last Auto Analyze |\n"
            result += "|-------|--------|--------------|--------------|------------------|\n"
            for table in vacuum_stats['tables_needing_analyze']:
                result += f"| {table['table_name']} | {table['schema_name']} | {table['modified_rows']:,} | {table['last_analyze'] or 'Never'} | {table['last_autoanalyze'] or 'Never'} |\n"
            result += "\n"
        
        # Recommendations
        result += "## Recommendations\n\n"
        
        if vacuum_stats['tables_needing_vacuum']:
            result += "### VACUUM Commands\n\n"
            result += "Run the following commands to reclaim space:\n\n"
            result += "```sql\n"
            for table in vacuum_stats['tables_needing_vacuum']:
                result += f"VACUUM {table['schema_name']}.{table['table_name']};\n"
            result += "```\n\n"
        
        if vacuum_stats['tables_needing_analyze']:
            result += "### ANALYZE Commands\n\n"
            result += "Run the following commands to update statistics:\n\n"
            result += "```sql\n"
            for table in vacuum_stats['tables_needing_analyze']:
                result += f"ANALYZE {table['schema_name']}.{table['table_name']};\n"
            result += "```\n\n"
        
        # Autovacuum settings
        result += "### Autovacuum Settings\n\n"
        result += "Current autovacuum settings:\n\n"
        result += "```\n"
        for setting, value in vacuum_stats['autovacuum_settings'].items():
            result += f"{setting} = {value}\n"
        result += "```\n\n"
        
        if vacuum_stats['autovacuum_recommendations']:
            result += "Recommended autovacuum settings:\n\n"
            result += "```sql\n"
            for recommendation in vacuum_stats['autovacuum_recommendations']:
                result += f"{recommendation}\n"
            result += "```\n\n"
        
        if debug:
            result += "## Debug Information\n\n"
            result += f"Connection type: {type(connector).__name__}\n"
            result += f"Database: {connector.database if hasattr(connector, 'database') else 'Unknown'}\n"
            result += f"Connected: {connector.is_connected() if hasattr(connector, 'is_connected') else 'Unknown'}\n"
        
        return result
    except Exception as e:
        error_msg = f"Error analyzing vacuum statistics: {str(e)}"
        await ctx.error(error_msg)
        return error_msg


@mcp.tool(
    name='identify_slow_queries',
    description='Identify slow-running queries in the database'
)
async def identify_slow_queries(
    ctx: Context,
    secret_name: Annotated[Optional[str], Field(description='Name of the secret in AWS Secrets Manager')] = None,
    region_name: Annotated[str, Field(description='AWS region where the secret is stored')] = "us-west-2",
    secret_arn: Annotated[Optional[str], Field(description='ARN of the secret in AWS Secrets Manager')] = None,
    resource_arn: Annotated[Optional[str], Field(description='ARN of the RDS cluster or instance')] = None,
    database: Annotated[Optional[str], Field(description='Database name to connect to')] = None,
    host: Annotated[Optional[str], Field(description='Database host')] = None,
    port: Annotated[Optional[int], Field(description='Database port')] = None,
    user: Annotated[Optional[str], Field(description='Database username')] = None,
    password: Annotated[Optional[str], Field(description='Database password')] = None,
    min_execution_time: Annotated[float, Field(description='Minimum execution time in milliseconds')] = 100.0,
    limit: Annotated[int, Field(description='Maximum number of slow queries to return')] = 20,
    debug: Annotated[bool, Field(description='Whether to include debug information')] = False
) -> str:
    """
    Identify slow-running queries in the database.
    
    Args:
        ctx: MCP context
        secret_name: Name of the secret in AWS Secrets Manager
        region_name: AWS region where the secret is stored
        secret_arn: ARN of the secret in AWS Secrets Manager
        resource_arn: ARN of the RDS cluster or instance
        database: Database name to connect to
        host: Database host
        port: Database port
        user: Database username
        password: Database password
        min_execution_time: Minimum execution time in milliseconds
        limit: Maximum number of slow queries to return
        debug: Whether to include debug information
        
    Returns:
        Analysis of slow-running queries with optimization recommendations
    """
    from awslabs.postgres_mcp_server.connection_manager import get_or_create_connection
    from awslabs.postgres_mcp_server.analysis.slow_queries import identify_slow_queries
    
    # Get or create a connection
    connector, is_new = await get_or_create_connection(
        ctx=ctx,
        secret_arn=secret_arn,
        region_name=region_name,
        resource_arn=resource_arn,
        database=database,
        hostname=host,
        port=port
    )
    
    if not connector:
        return "Failed to connect to the database. Please check your connection parameters."
    
    try:
        # Identify slow queries
        slow_queries = identify_slow_queries(connector, min_execution_time, limit)
        
        # Format the analysis
        result = "# Slow Query Analysis\n\n"
        
        # Summary
        result += "## Summary\n\n"
        result += f"Minimum execution time: {min_execution_time} ms\n"
        result += f"Total slow queries found: {len(slow_queries['queries'])}\n\n"
        
        # Slow queries
        if slow_queries['queries']:
            result += "## Top Slow Queries\n\n"
            
            for i, query in enumerate(slow_queries['queries'], 1):
                result += f"### Query {i}\n\n"
                result += f"**Execution Time**: {query['mean_exec_time']:.2f} ms\n"
                result += f"**Calls**: {query['calls']:,}\n"
                result += f"**Rows**: {query['rows']:,}\n"
                result += f"**Database**: {query['database']}\n"
                result += f"**User**: {query['username']}\n\n"
                
                result += "**Query**:\n```sql\n"
                result += query['query']
                result += "\n```\n\n"
                
                if query['recommendations']:
                    result += "**Recommendations**:\n"
                    for rec in query['recommendations']:
                        result += f"- {rec}\n"
                    result += "\n"
        else:
            result += "No slow queries found above the minimum execution time.\n\n"
        
        # General recommendations
        result += "## General Recommendations\n\n"
        for rec in slow_queries['general_recommendations']:
            result += f"- {rec}\n"
        result += "\n"
        
        if debug:
            result += "## Debug Information\n\n"
            result += f"Connection type: {type(connector).__name__}\n"
            result += f"Database: {connector.database if hasattr(connector, 'database') else 'Unknown'}\n"
            result += f"Connected: {connector.is_connected() if hasattr(connector, 'is_connected') else 'Unknown'}\n"
            result += f"pg_stat_statements extension: {slow_queries['pg_stat_statements_enabled']}\n"
        
        return result
    except Exception as e:
        error_msg = f"Error identifying slow queries: {str(e)}"
        await ctx.error(error_msg)
        return error_msg


@mcp.tool(
    name='show_postgresql_settings',
    description='Show PostgreSQL configuration settings with optional filtering'
)
async def show_postgresql_settings(
    ctx: Context,
    pattern: Annotated[Optional[str], Field(description='Pattern to filter settings (e.g., "vacuum" or "work_mem")')] = None,
    secret_name: Annotated[Optional[str], Field(description='Name of the secret in AWS Secrets Manager')] = None,
    region_name: Annotated[str, Field(description='AWS region where the secret is stored')] = "us-west-2",
    secret_arn: Annotated[Optional[str], Field(description='ARN of the secret in AWS Secrets Manager')] = None,
    resource_arn: Annotated[Optional[str], Field(description='ARN of the RDS cluster or instance')] = None,
    database: Annotated[Optional[str], Field(description='Database name to connect to')] = None,
    host: Annotated[Optional[str], Field(description='Database host')] = None,
    port: Annotated[Optional[int], Field(description='Database port')] = None,
    user: Annotated[Optional[str], Field(description='Database username')] = None,
    password: Annotated[Optional[str], Field(description='Database password')] = None,
    debug: Annotated[bool, Field(description='Whether to include debug information')] = False
) -> str:
    """
    Show PostgreSQL configuration settings with optional filtering.
    
    Args:
        ctx: MCP context
        pattern: Pattern to filter settings (e.g., "vacuum" or "work_mem")
        secret_name: Name of the secret in AWS Secrets Manager
        region_name: AWS region where the secret is stored
        secret_arn: ARN of the secret in AWS Secrets Manager
        resource_arn: ARN of the RDS cluster or instance
        database: Database name to connect to
        host: Database host
        port: Database port
        user: Database username
        password: Database password
        debug: Whether to include debug information
        
    Returns:
        PostgreSQL configuration settings
    """
    from awslabs.postgres_mcp_server.connection_manager import get_or_create_connection
    
    # Get or create a connection
    connector, is_new = await get_or_create_connection(
        ctx=ctx,
        secret_arn=secret_arn,
        region_name=region_name,
        resource_arn=resource_arn,
        database=database,
        hostname=host,
        port=port
    )
    
    if not connector:
        return "Failed to connect to the database. Please check your connection parameters."
    
    try:
        # Build the query
        query = """
            SELECT 
                name, 
                setting, 
                unit, 
                context, 
                vartype, 
                source, 
                short_desc
            FROM 
                pg_settings
        """
        
        if pattern:
            query += f" WHERE name LIKE '%{pattern}%' OR short_desc LIKE '%{pattern}%'"
        
        query += " ORDER BY name"
        
        # Execute the query
        settings = await run_query(query, ctx)
        
        # Format the results
        result = "# PostgreSQL Configuration Settings\n\n"
        
        if pattern:
            result += f"Filtered by pattern: '{pattern}'\n\n"
        
        result += f"Total settings: {len(settings)}\n\n"
        
        # Group settings by context
        settings_by_context = {}
        for setting in settings:
            context = setting['context']
            if context not in settings_by_context:
                settings_by_context[context] = []
            settings_by_context[context].append(setting)
        
        # Display settings by context
        for context, context_settings in settings_by_context.items():
            result += f"## {context} Settings\n\n"
            result += "| Name | Value | Unit | Type | Source | Description |\n"
            result += "|------|-------|------|------|--------|-------------|\n"
            
            for setting in context_settings:
                name = setting['name']
                value = setting['setting']
                unit = setting['unit'] or ''
                vartype = setting['vartype']
                source = setting['source']
                desc = setting['short_desc']
                
                result += f"| {name} | {value} | {unit} | {vartype} | {source} | {desc} |\n"
            
            result += "\n"
        
        if debug:
            result += "## Debug Information\n\n"
            result += f"Connection type: {type(connector).__name__}\n"
            result += f"Database: {connector.database if hasattr(connector, 'database') else 'Unknown'}\n"
            result += f"Connected: {connector.is_connected() if hasattr(connector, 'is_connected') else 'Unknown'}\n"
        
        return result
    except Exception as e:
        error_msg = f"Error retrieving PostgreSQL settings: {str(e)}"
        await ctx.error(error_msg)
        return error_msg


@mcp.tool(
    name='health_check',
    description='Check if the server is running and responsive'
)
async def health_check() -> Dict[str, Any]:
    """
    Check if the server is running and responsive.
    
    Returns:
        Dictionary with health check information
    """
    try:
        # Check if the server is running
        server_status = "running"
        
        # Check if the connection pool is initialized
        from awslabs.postgres_mcp_server.connection.pool_manager import connection_pool_manager
        pool_status = "initialized" if connection_pool_manager.is_initialized() else "not initialized"
        
        # Get pool statistics
        pool_stats = connection_pool_manager.get_pool_stats() if connection_pool_manager.is_initialized() else {}
        
        # Check session handler
        from awslabs.postgres_mcp_server.session_handler import session_handler
        session_count = len(session_handler.sessions) if hasattr(session_handler, 'sessions') else 0
        
        return {
            "status": "healthy",
            "server": server_status,
            "connection_pool": pool_status,
            "pool_statistics": pool_stats,
            "active_sessions": session_count,
            "timestamp": time.time()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": time.time()
        }


if __name__ == '__main__':
    main()
