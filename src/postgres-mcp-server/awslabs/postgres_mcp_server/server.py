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

"""PostgreSQL MCP Server implementation."""

import argparse
import asyncio
import boto3
import json
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
mcp = FastMCP('PostgreSQL MCP Server with Database Analysis Tools')


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


@mcp.tool(name='analyze_database_structure', description='Analyze the database structure and provide insights on schema design, indexes, and potential optimizations')
async def analyze_database_structure(
    ctx: Context,
    debug: Annotated[bool, Field(description='Whether to include debug information')] = False
) -> str:
    """Analyze the database structure and provide optimization insights."""
    try:
        logger.info("Starting database structure analysis")
        
        # Get schemas
        schemas_sql = """
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY schema_name
        """
        schemas_result = await run_query(schemas_sql, ctx)
        schemas = [row['schema_name'] for row in schemas_result if 'error' not in row]
        
        # Get tables with detailed information
        tables_sql = """
            SELECT 
                t.table_schema,
                t.table_name,
                pg_size_pretty(pg_total_relation_size(quote_ident(t.table_schema)||'.'||quote_ident(t.table_name))) as size,
                pg_total_relation_size(quote_ident(t.table_schema)||'.'||quote_ident(t.table_name)) as size_bytes,
                COALESCE(c.reltuples, 0)::bigint as estimated_rows
            FROM information_schema.tables t
            LEFT JOIN pg_class c ON c.relname = t.table_name
            LEFT JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema
            WHERE t.table_type = 'BASE TABLE'
            AND t.table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY t.table_schema, t.table_name
        """
        tables_result = await run_query(tables_sql, ctx)
        
        # Get indexes
        indexes_sql = """
            SELECT
                schemaname,
                tablename,
                indexname,
                indexdef
            FROM pg_indexes
            WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY schemaname, tablename, indexname
        """
        indexes_result = await run_query(indexes_sql, ctx)
        
        # Format results
        result = {
            "status": "success",
            "data": {
                "schemas": schemas,
                "tables": [row for row in tables_result if 'error' not in row],
                "indexes": [row for row in indexes_result if 'error' not in row]
            },
            "metadata": {
                "analysis_timestamp": "2025-06-19T13:35:00Z",
                "total_schemas": len(schemas),
                "total_tables": len([row for row in tables_result if 'error' not in row]),
                "total_indexes": len([row for row in indexes_result if 'error' not in row])
            },
            "recommendations": [
                "Database structure analysis completed successfully",
                "Review table sizes and consider partitioning for large tables",
                "Ensure proper indexing on frequently queried columns"
            ]
        }
        
        logger.success("Database structure analysis completed")
        return json.dumps(result, indent=2 if debug else None)
        
    except Exception as e:
        logger.error(f"Database structure analysis failed: {str(e)}")
        return json.dumps({"status": "error", "error": str(e)})


@mcp.tool(name='show_postgresql_settings', description='Show PostgreSQL configuration settings with optional filtering')
async def show_postgresql_settings(
    ctx: Context,
    pattern: Annotated[Optional[str], Field(description='Pattern to filter settings (SQL LIKE pattern)')] = None,
    debug: Annotated[bool, Field(description='Include debug information')] = False
) -> str:
    """Show PostgreSQL configuration settings with optional filtering."""
    try:
        logger.info(f"Getting PostgreSQL settings with pattern: {pattern}")
        
        if pattern:
            settings_sql = f"""
                SELECT 
                    name,
                    setting,
                    unit,
                    category,
                    short_desc,
                    context,
                    vartype,
                    source
                FROM pg_settings
                WHERE name ILIKE '%{pattern}%'
                ORDER BY category, name
            """
        else:
            settings_sql = """
                SELECT 
                    name,
                    setting,
                    unit,
                    category,
                    short_desc,
                    context,
                    vartype,
                    source
                FROM pg_settings
                ORDER BY category, name
            """
        
        settings_result = await run_query(settings_sql, ctx)
        
        # Categorize settings
        categorized = {}
        for row in settings_result:
            if 'error' not in row:
                category = row.get('category', 'Unknown')
                if category not in categorized:
                    categorized[category] = []
                categorized[category].append(row)
        
        result = {
            "status": "success",
            "data": {
                "settings": [row for row in settings_result if 'error' not in row],
                "categorized_settings": categorized,
                "filter_pattern": pattern
            },
            "metadata": {
                "analysis_timestamp": "2025-06-19T13:35:00Z",
                "total_settings": len([row for row in settings_result if 'error' not in row]),
                "categories": len(categorized)
            },
            "recommendations": [
                "Review memory settings for optimization opportunities",
                "Check connection limits and adjust if needed",
                "Ensure logging settings match your monitoring requirements"
            ]
        }
        
        logger.success("PostgreSQL settings analysis completed")
        return json.dumps(result, indent=2 if debug else None)
        
    except Exception as e:
        logger.error(f"PostgreSQL settings analysis failed: {str(e)}")
        return json.dumps({"status": "error", "error": str(e)})


@mcp.tool(name='identify_slow_queries', description='Identify slow-running queries in the database')
async def identify_slow_queries(
    ctx: Context,
    min_execution_time: Annotated[float, Field(description='Minimum execution time in milliseconds')] = 100.0,
    limit: Annotated[int, Field(description='Maximum number of queries to return')] = 20,
    debug: Annotated[bool, Field(description='Include debug information')] = False
) -> str:
    """Identify slow-running queries in the database."""
    try:
        logger.info(f"Identifying slow queries (min_time: {min_execution_time}ms, limit: {limit})")
        
        # Check if pg_stat_statements extension is available
        check_extension_sql = """
            SELECT EXISTS (
                SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
            ) as extension_exists
        """
        
        extension_result = await run_query(check_extension_sql, ctx)
        has_extension = False
        if extension_result and len(extension_result) > 0 and 'error' not in extension_result[0]:
            has_extension = extension_result[0].get('extension_exists', False)
        
        if not has_extension:
            return json.dumps({
                "status": "error",
                "error": {
                    "step": "checking_pg_stat_statements",
                    "message": "pg_stat_statements extension is not available",
                    "suggestions": [
                        "Install pg_stat_statements extension: CREATE EXTENSION pg_stat_statements;",
                        "Add 'pg_stat_statements' to shared_preload_libraries in postgresql.conf",
                        "Restart PostgreSQL server after configuration change"
                    ]
                }
            })
        
        # Get slow queries from pg_stat_statements
        slow_queries_sql = f"""
            SELECT 
                query,
                calls,
                total_exec_time,
                mean_exec_time,
                max_exec_time,
                min_exec_time,
                rows
            FROM pg_stat_statements 
            WHERE mean_exec_time >= {min_execution_time}
            ORDER BY mean_exec_time DESC
            LIMIT {limit}
        """
        
        slow_queries_result = await run_query(slow_queries_sql, ctx)
        
        result = {
            "status": "success",
            "data": {
                "slow_queries": [row for row in slow_queries_result if 'error' not in row],
                "min_execution_time_ms": min_execution_time,
                "limit": limit
            },
            "metadata": {
                "analysis_timestamp": "2025-06-19T13:35:00Z",
                "slow_queries_found": len([row for row in slow_queries_result if 'error' not in row])
            },
            "recommendations": [
                "Review the slowest queries for optimization opportunities",
                "Consider adding indexes for frequently filtered columns",
                "Analyze query execution plans for expensive operations"
            ]
        }
        
        logger.success("Slow query analysis completed")
        return json.dumps(result, indent=2 if debug else None)
        
    except Exception as e:
        logger.error(f"Slow query analysis failed: {str(e)}")
        return json.dumps({"status": "error", "error": str(e)})


@mcp.tool(name='analyze_table_fragmentation', description='Analyze table fragmentation and provide optimization recommendations')
async def analyze_table_fragmentation(
    ctx: Context,
    threshold: Annotated[float, Field(description='Bloat percentage threshold for recommendations')] = 10.0,
    debug: Annotated[bool, Field(description='Include debug information')] = False
) -> str:
    """Analyze table fragmentation and provide optimization recommendations."""
    try:
        logger.info(f"Analyzing table fragmentation with threshold {threshold}%")
        
        # Get table bloat information using pg_stat_user_tables
        bloat_sql = """
            SELECT 
                schemaname,
                relname as tablename,
                n_tup_ins as inserts,
                n_tup_upd as updates,
                n_tup_del as deletes,
                n_live_tup as live_tuples,
                n_dead_tup as dead_tuples,
                CASE 
                    WHEN n_live_tup > 0 
                    THEN round(100.0 * n_dead_tup / (n_live_tup + n_dead_tup), 2)
                    ELSE 0 
                END as bloat_percent,
                last_vacuum,
                last_autovacuum
            FROM pg_stat_user_tables
            ORDER BY 
                CASE 
                    WHEN n_live_tup > 0 
                    THEN 100.0 * n_dead_tup / (n_live_tup + n_dead_tup)
                    ELSE 0 
                END DESC
        """
        
        bloat_result = await run_query(bloat_sql, ctx)
        
        # Filter tables above threshold
        problematic_tables = []
        for row in bloat_result:
            if 'error' not in row:
                # Convert bloat_percent from string to float for comparison
                bloat_percent_value = row.get('bloat_percent', '0')
                try:
                    # Handle both string and numeric values
                    if isinstance(bloat_percent_value, str):
                        bloat_percent_float = float(bloat_percent_value)
                    else:
                        bloat_percent_float = float(bloat_percent_value) if bloat_percent_value is not None else 0.0
                    
                    if bloat_percent_float > threshold:
                        # Add the converted value back to the row for consistency
                        row['bloat_percent_numeric'] = bloat_percent_float
                        problematic_tables.append(row)
                except (ValueError, TypeError):
                    # If conversion fails, skip this row but log it
                    logger.warning(f"Could not convert bloat_percent '{bloat_percent_value}' to float for table {row.get('tablename', 'unknown')}")
                    continue
        
        result = {
            "status": "success",
            "data": {
                "table_bloat": [row for row in bloat_result if 'error' not in row],
                "problematic_tables": problematic_tables,
                "threshold_percent": threshold
            },
            "metadata": {
                "analysis_timestamp": "2025-06-19T13:40:00Z",
                "total_tables_analyzed": len([row for row in bloat_result if 'error' not in row]),
                "tables_above_threshold": len(problematic_tables)
            },
            "recommendations": [
                f"Found {len(problematic_tables)} tables above {threshold}% bloat threshold",
                "Consider running VACUUM on tables with high dead tuple percentages",
                "Review autovacuum settings for frequently updated tables",
                "Monitor vacuum operations and adjust frequency as needed"
            ]
        }
        
        logger.success("Table fragmentation analysis completed")
        return json.dumps(result, indent=2 if debug else None)
        
    except Exception as e:
        logger.error(f"Table fragmentation analysis failed: {str(e)}")
        return json.dumps({"status": "error", "error": str(e)})


@mcp.tool(name='analyze_query_performance', description='Analyze query performance and provide optimization recommendations')
async def analyze_query_performance(
    ctx: Context,
    query: Annotated[str, Field(description='SQL query to analyze')],
    debug: Annotated[bool, Field(description='Include debug information')] = False
) -> str:
    """Analyze query performance and provide optimization recommendations."""
    try:
        logger.info(f"Analyzing query performance for: {query[:100]}...")
        
        # Get query execution plan
        explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {query}"
        
        try:
            explain_result = await run_query(explain_sql, ctx)
            execution_plan = [row for row in explain_result if 'error' not in row]
        except Exception as e:
            # Fallback to basic EXPLAIN if ANALYZE fails
            logger.warning(f"EXPLAIN ANALYZE failed, trying basic EXPLAIN: {str(e)}")
            basic_explain_sql = f"EXPLAIN {query}"
            explain_result = await run_query(basic_explain_sql, ctx)
            execution_plan = [row for row in explain_result if 'error' not in row]
        
        # Analyze the plan for common issues
        recommendations = []
        expensive_operations = []
        
        for row in execution_plan:
            plan_line = str(row.get('QUERY PLAN', ''))
            
            if 'Seq Scan' in plan_line:
                recommendations.append("Query uses sequential scans - consider adding indexes on filtered columns")
            
            if 'Nested Loop' in plan_line and 'rows=' in plan_line:
                recommendations.append("Nested loop joins detected - verify join conditions and indexes")
            
            if 'Sort' in plan_line and 'cost=' in plan_line:
                recommendations.append("Expensive sort operations detected - consider indexes for ORDER BY clauses")
            
            if 'Hash' in plan_line:
                recommendations.append("Hash operations detected - monitor memory usage for large datasets")
        
        if not recommendations:
            recommendations.append("Query execution plan looks reasonable - no obvious optimization opportunities")
        
        result = {
            "status": "success",
            "data": {
                "query": query,
                "execution_plan": execution_plan,
                "expensive_operations": expensive_operations
            },
            "metadata": {
                "analysis_timestamp": "2025-06-19T13:40:00Z",
                "plan_lines": len(execution_plan)
            },
            "recommendations": recommendations
        }
        
        logger.success("Query performance analysis completed")
        return json.dumps(result, indent=2 if debug else None)
        
    except Exception as e:
        logger.error(f"Query performance analysis failed: {str(e)}")
        return json.dumps({"status": "error", "error": str(e)})


def main():
    """Main entry point for the MCP server application."""
    parser = argparse.ArgumentParser(
        description='PostgreSQL MCP Server'
    )
    parser.add_argument('--resource_arn', required=True, help='ARN of the RDS cluster')
    parser.add_argument('--secret_arn', required=True, help='ARN of the Secrets Manager secret for database credentials')
    parser.add_argument('--database', required=True, help='Database name')
    parser.add_argument('--region', required=True, help='AWS region for RDS Data API')
    parser.add_argument('--readonly', required=True, help='Enforce readonly SQL statements')
    args = parser.parse_args()

    logger.info(f'PostgreSQL MCP Server starting with CLUSTER_ARN:{args.resource_arn}, DATABASE:{args.database}, READONLY:{args.readonly}')

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
    logger.info('Starting PostgreSQL MCP Server with stdio transport')
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
