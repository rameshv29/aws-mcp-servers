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

"""
Consolidated MCP tools for PostgreSQL database analysis.
"""
import json
import traceback
import time
import datetime
from typing import List, Dict, Any, Optional
from mcp.server.fastmcp import Context, FastMCP

from awslabs.postgresql_mcp_server.db.connector import UniversalConnector
from awslabs.postgresql_mcp_server.connection_manager import get_or_create_connection, initialize_connection, close_connection
from awslabs.postgresql_mcp_server.analysis.structure import (
    get_database_structure, 
    organize_db_structure_by_table,
    analyze_database_structure_for_response
)
from awslabs.postgresql_mcp_server.analysis.query import (
    extract_tables_from_query, 
    get_table_statistics, 
    get_schema_information, 
    get_index_information,
    format_query_analysis_response
)
from awslabs.postgresql_mcp_server.analysis.patterns import (
    detect_query_patterns, 
    detect_query_anti_patterns, 
    validate_read_only_query
)
from awslabs.postgresql_mcp_server.analysis.indexes import (
    extract_potential_indexes,
    get_table_structure_for_index,
    check_existing_indexes,
    format_index_recommendations_response
)

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

def register_all_tools(mcp: FastMCP):
    """Register all tools with the MCP server"""
    
    @mcp.tool()
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
    
    @mcp.tool()
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
    
    @mcp.tool()
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
    
    @mcp.tool()
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
    
    @mcp.tool()
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
    
    @mcp.tool()
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

    @mcp.tool()
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
    
    # Register additional tools like analyze_table_fragmentation, analyze_vacuum_stats, etc.
    # following the same pattern of using get_or_create_connection
