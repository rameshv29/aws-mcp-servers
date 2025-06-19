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

"""Simplified PostgreSQL MCP Server for Q Chat integration."""

import argparse
import asyncio
import boto3
import sys
from typing import Annotated, Any, Dict, List, Optional

from loguru import logger
from mcp.server.fastmcp import Context, FastMCP
from pydantic import Field
from botocore.exceptions import BotoCoreError, ClientError

from .mutable_sql_detector import check_sql_injection_risk, detect_mutating_keywords


# Error message constants
CLIENT_ERROR_KEY = 'run_query ClientError code'
UNEXPECTED_ERROR_KEY = 'run_query unexpected error'
WRITE_QUERY_PROHIBITED_KEY = 'Your MCP tool only allows readonly query. If you want to write, change the MCP configuration per README.md'
QUERY_INJECTION_RISK_KEY = 'Your query contains risky injection patterns'


class DBConnection:
    """Class that wraps DB connection client by RDS API."""

    def __init__(self, cluster_arn, secret_arn, database, region, readonly, is_test=False):
        """Initialize a new DB connection."""
        self.cluster_arn = cluster_arn
        self.secret_arn = secret_arn
        self.database = database
        self.readonly = readonly
        if not is_test:
            self.data_client = boto3.client('rds-data', region_name=region)

    @property
    def readonly_query(self):
        """Get whether this connection is read-only."""
        return self.readonly


class DBConnectionSingleton:
    """Manages a single DBConnection instance across the application."""

    _instance = None

    def __init__(self, resource_arn, secret_arn, database, region, readonly, is_test=False):
        """Initialize a new DB connection singleton."""
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
        """Initialize the singleton instance if it doesn't exist."""
        if cls._instance is None:
            cls._instance = cls(resource_arn, secret_arn, database, region, readonly, is_test)

    @classmethod
    def get(cls):
        """Get the singleton instance."""
        if cls._instance is None:
            raise RuntimeError('DBConnectionSingleton is not initialized.')
        return cls._instance

    @property
    def db_connection(self):
        """Get the database connection."""
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


def execute_readonly_query(
    db_connection: DBConnection, query: str, parameters: Optional[List[Dict[str, Any]]] = None
) -> dict:
    """Execute a query under readonly transaction."""
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


# Initialize FastMCP server
mcp = FastMCP('Simplified PostgreSQL MCP Server for Q Chat')


@mcp.tool(name='run_query', description='Run a SQL query using boto3 execute_statement')
async def run_query(
    sql: Annotated[str, Field(description='The SQL query to run')],
    ctx: Context,
    query_parameters: Annotated[
        Optional[List[Dict[str, Any]]], Field(description='Parameters for the SQL query')
    ] = None,
) -> list[dict]:
    """Run a SQL query using boto3 execute_statement."""
    try:
        db_connection = DBConnectionSingleton.get().db_connection
    except Exception as e:
        await ctx.error(f"No database connection available. Please configure the database first: {str(e)}")
        return [{'error': 'No database connection available'}]

    if db_connection.readonly_query:
        matches = detect_mutating_keywords(sql)
        if matches:
            logger.info(f'Query rejected - readonly mode, detected keywords: {matches}')
            await ctx.error(WRITE_QUERY_PROHIBITED_KEY)
            return [{'error': WRITE_QUERY_PROHIBITED_KEY}]

    issues = check_sql_injection_risk(sql)
    if issues:
        logger.info(f'Query rejected - injection risk: {issues}')
        await ctx.error(str({'message': 'Query contains suspicious patterns', 'details': issues}))
        return [{'error': QUERY_INJECTION_RISK_KEY}]

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

        logger.success('Query executed successfully')
        return parse_execute_response(response)
    except ClientError as e:
        logger.exception(CLIENT_ERROR_KEY)
        await ctx.error(str({'code': e.response['Error']['Code'], 'message': e.response['Error']['Message']}))
        return [{'error': CLIENT_ERROR_KEY}]
    except Exception as e:
        logger.exception(UNEXPECTED_ERROR_KEY)
        error_details = f'{type(e).__name__}: {str(e)}'
        await ctx.error(str({'message': error_details}))
        return [{'error': UNEXPECTED_ERROR_KEY}]


@mcp.tool(name='get_table_schema', description='Fetch table columns and comments from Postgres using RDS Data API')
async def get_table_schema(
    table_name: Annotated[str, Field(description='name of the table')], ctx: Context
) -> list[dict]:
    """Get a table's schema information given the table name."""
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


@mcp.tool(name='health_check', description='Check if the server is running and responsive')
async def health_check(ctx: Context) -> Dict[str, Any]:
    """Check if the server is running and responsive."""
    try:
        # Test database connectivity
        connection_test = False
        try:
            db_connection = DBConnectionSingleton.get().db_connection
            test_result = await run_query("SELECT 1", ctx)
            connection_test = len(test_result) > 0 and 'error' not in test_result[0]
        except Exception as e:
            logger.warning(f"Health check database test failed: {str(e)}")
        
        return {
            "status": "healthy",
            "timestamp": "2025-06-19T13:30:00Z",
            "database_connection": connection_test,
            "server_version": "simple-v1.0"
        }
        
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": "2025-06-19T13:30:00Z"
        }


def main():
    """Main entry point for the MCP server application."""
    parser = argparse.ArgumentParser(
        description='Simplified PostgreSQL MCP Server for Q Chat'
    )
    parser.add_argument('--resource_arn', required=True, help='ARN of the RDS cluster')
    parser.add_argument('--secret_arn', required=True, help='ARN of the Secrets Manager secret for database credentials')
    parser.add_argument('--database', required=True, help='Database name')
    parser.add_argument('--region', required=True, help='AWS region for RDS Data API')
    parser.add_argument('--readonly', required=True, help='Enforce readonly SQL statements')
    args = parser.parse_args()

    logger.info(f'Simplified PostgreSQL MCP Server starting with CLUSTER_ARN:{args.resource_arn}, SECRET_ARN:{args.secret_arn}, REGION:{args.region}, DATABASE:{args.database}, READONLY:{args.readonly}')

    try:
        DBConnectionSingleton.initialize(
            args.resource_arn, args.secret_arn, args.database, args.region, args.readonly == 'true'
        )
    except BotoCoreError:
        logger.exception('Failed to create RDS API client. Exiting.')
        sys.exit(1)

    # Test RDS API connection
    class DummyCtx:
        async def error(self, message):
            pass

    ctx = DummyCtx()
    response = asyncio.run(run_query('SELECT 1', ctx))
    if isinstance(response, list) and len(response) == 1 and isinstance(response[0], dict) and 'error' in response[0]:
        logger.error('Failed to validate RDS API db connection. Exiting.')
        sys.exit(1)

    logger.success('Successfully validated RDS API db connection')
    logger.info('Starting server with stdio transport')
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
