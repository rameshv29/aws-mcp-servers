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
    async def health_check(ctx: Context = None) -> str:
        """
        Check if the server is running and responsive.
        
        Returns:
            A message indicating the server is healthy
        """
        return "PostgreSQL MCP server is running and healthy!"
    
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
            secret_name: Name of the secret in AWS Secrets Manager containing database credentials
            region_name: AWS region where the secret is stored (default: us-west-2)
            secret_arn: ARN of the secret in AWS Secrets Manager containing credentials (for RDS Data API)
            resource_arn: ARN of the RDS cluster or instance (for RDS Data API)
            database: Database name to connect to
            host: Database host (for direct connection)
            port: Database port (for direct connection)
            user: Database username (for direct connection)
            password: Database password (for direct connection)
            debug: Enable detailed debug output (default: false)
        
        Returns:
            A comprehensive analysis of the database structure with optimization recommendations
        """
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
            password=password
        )
        
        try:
            if not connector.connect():
                error_msg = "Failed to connect to database. Please check your credentials and connection parameters.\n\n"
                
                # Add troubleshooting tips
                error_msg += "Troubleshooting tips:\n"
                if secret_arn and resource_arn:
                    error_msg += "- Verify that your RDS cluster has Data API enabled\n"
                    error_msg += "- Check that the secret ARN and resource ARN are correct\n"
                    error_msg += "- Ensure your IAM role has rds-data:ExecuteStatement permission\n"
                elif host:
                    error_msg += "- Check if the database host is reachable\n"
                    error_msg += "- Verify that security groups allow access from your IP\n"
                    error_msg += "- Confirm database credentials are correct\n"
                
                return error_msg
            
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
            # Always disconnect when done
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
            secret_name: Name of the secret in AWS Secrets Manager containing database credentials
            region_name: AWS region where the secret is stored (default: us-west-2)
            secret_arn: ARN of the secret in AWS Secrets Manager containing credentials (for RDS Data API)
            resource_arn: ARN of the RDS cluster or instance (for RDS Data API)
            database: Database name to connect to
            host: Database host (for direct connection)
            port: Database port (for direct connection)
            user: Database username (for direct connection)
            password: Database password (for direct connection)
            debug: Enable detailed debug output (default: false)
        
        Returns:
            Analysis of the query execution plan and optimization suggestions
        """
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
            password=password
        )
        
        try:
            if not connector.connect():
                error_msg = "Failed to connect to database. Please check your credentials and connection parameters.\n\n"
                
                # Add troubleshooting tips
                error_msg += "Troubleshooting tips:\n"
                if secret_arn and resource_arn:
                    error_msg += "- Verify that your RDS cluster has Data API enabled\n"
                    error_msg += "- Check that the secret ARN and resource ARN are correct\n"
                    error_msg += "- Ensure your IAM role has rds-data:ExecuteStatement permission\n"
                elif host:
                    error_msg += "- Check if the database host is reachable\n"
                    error_msg += "- Verify that security groups allow access from your IP\n"
                    error_msg += "- Confirm database credentials are correct\n"
                
                return error_msg
            
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
            # Always disconnect when done
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
            secret_name: Name of the secret in AWS Secrets Manager containing database credentials
            region_name: AWS region where the secret is stored (default: us-west-2)
            secret_arn: ARN of the secret in AWS Secrets Manager containing credentials (for RDS Data API)
            resource_arn: ARN of the RDS cluster or instance (for RDS Data API)
            database: Database name to connect to
            host: Database host (for direct connection)
            port: Database port (for direct connection)
            user: Database username (for direct connection)
            password: Database password (for direct connection)
            max_rows: Maximum number of rows to return (default: 100)
            debug: Enable detailed debug output (default: false)
        
        Returns:
            Query results in a formatted table
        """
        # Validate that this is a read-only query
        is_valid, error_message = validate_read_only_query(query)
        if not is_valid:
            return f"Error: {error_message}"
        
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
            password=password
        )
        
        try:
            if not connector.connect():
                error_msg = "Failed to connect to database. Please check your credentials and connection parameters.\n\n"
                
                # Add troubleshooting tips
                error_msg += "Troubleshooting tips:\n"
                if secret_arn and resource_arn:
                    error_msg += "- Verify that your RDS cluster has Data API enabled\n"
                    error_msg += "- Check that the secret ARN and resource ARN are correct\n"
                    error_msg += "- Ensure your IAM role has rds-data:ExecuteStatement permission\n"
                elif host:
                    error_msg += "- Check if the database host is reachable\n"
                    error_msg += "- Verify that security groups allow access from your IP\n"
                    error_msg += "- Confirm database credentials are correct\n"
                
                return error_msg
            
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
            # Always disconnect when done
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
            secret_name: Name of the secret in AWS Secrets Manager containing database credentials
            region_name: AWS region where the secret is stored (default: us-west-2)
            secret_arn: ARN of the secret in AWS Secrets Manager containing credentials (for RDS Data API)
            resource_arn: ARN of the RDS cluster or instance (for RDS Data API)
            database: Database name to connect to
            host: Database host (for direct connection)
            port: Database port (for direct connection)
            user: Database username (for direct connection)
            password: Database password (for direct connection)
            debug: Enable detailed debug output (default: false)
        
        Returns:
            Current PostgreSQL configuration settings in a formatted table
        """
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
            password=password
        )
        
        try:
            if not connector.connect():
                error_msg = "Failed to connect to database. Please check your credentials and connection parameters.\n\n"
                return error_msg
            
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
            return f"Error retrieving PostgreSQL settings: {str(e)}"
        finally:
            # Always disconnect when done
            connector.disconnect()
            
    @mcp.tool()
    async def analyze_table_fragmentation(
        secret_name: str = None, 
        region_name: str = "us-west-2",
        secret_arn: str = None, 
        resource_arn: str = None, 
        database: str = None,
        host: str = None,
        port: int = None,
        user: str = None,
        password: str = None,
        threshold: float = 10.0,
        debug: bool = True,
        ctx: Context = None
    ) -> str:
        """
        Analyze table fragmentation and provide optimization recommendations.
        
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
            threshold: Fragmentation threshold percentage to report (default: 10.0)
            debug: Enable detailed debug output (default: true)
        
        Returns:
            Analysis of table fragmentation with optimization recommendations
        """
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
            password=password
        )
        
        try:
            if not connector.connect():
                return "Failed to connect to database. Please check your credentials and connection parameters."
            
            # Simple test query to verify connection
            test_query = "SELECT 1 as test"
            test_result = connector.execute_query(test_query)
            if not test_result:
                return "Failed to execute test query. Database connection may be unstable."
            
            # Get basic table statistics
            stats_query = """
                SELECT
                    schemaname,
                    relname,
                    n_live_tup,
                    n_dead_tup,
                    CASE WHEN n_live_tup + n_dead_tup > 0 
                         THEN round((100 * n_dead_tup::numeric / (n_live_tup + n_dead_tup)::numeric)::numeric, 2)
                         ELSE 0
                    END as dead_tup_ratio,
                    last_vacuum,
                    last_autovacuum,
                    vacuum_count,
                    autovacuum_count
                FROM
                    pg_stat_user_tables
                WHERE 
                    schemaname NOT IN ('pg_catalog', 'information_schema')
                ORDER BY
                    dead_tup_ratio DESC
            """
            
            tables = connector.execute_query(stats_query)
            
            if not tables:
                return "No tables found in the database."
            
            # Format the response
            response = "# Table Fragmentation Analysis\n\n"
            
            # Summary
            total_tables = len(tables)
            # Convert dead_tup_ratio to float before comparison
            fragmented_tables = [t for t in tables if t.get('dead_tup_ratio') and float(t['dead_tup_ratio']) > threshold]
            total_dead_tuples = sum(t.get('n_dead_tup', 0) for t in tables)
            total_live_tuples = sum(t.get('n_live_tup', 0) for t in tables)
            
            response += "## Summary\n\n"
            response += f"- **Total Tables**: {total_tables}\n"
            response += f"- **Fragmented Tables**: {len(fragmented_tables)} (above {threshold}% threshold)\n"
            response += f"- **Total Live Tuples**: {total_live_tuples:,}\n"
            response += f"- **Total Dead Tuples**: {total_dead_tuples:,}\n"
            response += f"- **Overall Dead Tuple Percentage**: {(total_dead_tuples / (total_live_tuples + total_dead_tuples) * 100) if (total_live_tuples + total_dead_tuples) > 0 else 0:.2f}%\n\n"
            
            # Table details
            response += "## Table Details\n\n"
            response += "| Table | Live Tuples | Dead Tuples | Dead Tuple % | Last Vacuum | Last Autovacuum |\n"
            response += "| ----- | ----------- | ----------- | ------------ | ----------- | --------------- |\n"
            
            for table in tables:
                schema = table['schemaname']
                name = table['relname']
                live_tuples = f"{table['n_live_tup']:,}" if table.get('n_live_tup') else "0"
                dead_tuples = f"{table['n_dead_tup']:,}" if table.get('n_dead_tup') else "0"
                dead_pct = f"{float(table['dead_tup_ratio']):.2f}%" if table.get('dead_tup_ratio') else "0.00%"
                # Convert string to datetime if needed
                last_vacuum = table['last_vacuum'].strftime("%Y-%m-%d %H:%M:%S") if table.get('last_vacuum') and hasattr(table['last_vacuum'], 'strftime') else table.get('last_vacuum', "Never")
                last_autovacuum = table['last_autovacuum'].strftime("%Y-%m-%d %H:%M:%S") if table.get('last_autovacuum') and hasattr(table['last_autovacuum'], 'strftime') else table.get('last_autovacuum', "Never")
                
                response += f"| {schema}.{name} | {live_tuples} | {dead_tuples} | {dead_pct} | {last_vacuum} | {last_autovacuum} |\n"
            
            response += "\n"
            
            # Optimization recommendations
            if fragmented_tables:
                response += "## Optimization Recommendations\n\n"
                response += f"The following tables have dead tuple percentage above the {threshold}% threshold and should be optimized:\n\n"
                
                for table in fragmented_tables:
                    schema = table['schemaname']
                    name = table['relname']
                    response += f"### {schema}.{name}\n\n"
                    response += f"- **Dead Tuple Percentage**: {float(table['dead_tup_ratio']):.2f}%\n"
                    response += f"- **Dead Tuples**: {table['n_dead_tup']:,}\n"
                    # Handle datetime or string for last_vacuum
                    if table.get('last_vacuum'):
                        if hasattr(table['last_vacuum'], 'strftime'):
                            vacuum_time = table['last_vacuum'].strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            vacuum_time = str(table['last_vacuum'])
                    else:
                        vacuum_time = 'Never'
                    response += f"- **Last Vacuum**: {vacuum_time}\n"
                    response += "- **Recommendation**: Run VACUUM to reclaim space\n\n"
                    response += "```sql\n"
                    response += f"VACUUM (VERBOSE, ANALYZE) \"{schema}\".\"{name}\";\n"
                    response += "```\n\n"
                    response += "For more aggressive space reclamation, consider:\n\n"
                    response += "```sql\n"
                    response += f"VACUUM FULL \"{schema}\".\"{name}\";\n"
                    response += "```\n\n"
                    response += "Note: VACUUM FULL locks the table during operation. Consider running during off-peak hours.\n\n"
            else:
                response += "## Optimization Recommendations\n\n"
                response += "No tables with significant fragmentation were detected. Your database appears to be well-optimized in terms of storage.\n\n"
            
            # General recommendations
            response += "## General Recommendations\n\n"
            response += "1. **Regular Maintenance**: Schedule regular VACUUM operations for large tables during off-peak hours.\n\n"
            response += "2. **Monitor Growth**: Keep an eye on tables that grow rapidly, as they may fragment more quickly.\n\n"
            response += "3. **Consider Autovacuum Settings**: Adjust autovacuum settings for tables with high update/delete activity.\n\n"
            response += "4. **Check Bloat**: Use pgstattuple extension for more detailed bloat analysis.\n\n"
            
            return response
        except Exception as e:
            error_details = traceback.format_exc()
            error_msg = f"Error analyzing table fragmentation: {str(e)}\n\n"
            
            if debug:
                error_msg += f"Error details:\n{error_details}\n\n"
            
            return error_msg
        finally:
            # Always disconnect when done
            connector.disconnect()
    @mcp.tool()
    async def analyze_vacuum_stats(
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
        Analyze vacuum statistics and provide recommendations for vacuum settings.
        
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
            debug: Enable detailed debug output (default: false)
        
        Returns:
            Analysis of vacuum statistics with recommendations
        """
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
            password=password
        )
        
        try:
            if not connector.connect():
                error_msg = "Failed to connect to database. Please check your credentials and connection parameters.\n\n"
                return error_msg
            
            # Get autovacuum settings
            settings_query = """
                SELECT name, setting, unit, short_desc
                FROM pg_settings
                WHERE name LIKE 'autovacuum%' OR name LIKE '%vacuum%'
                ORDER BY name
            """
            
            settings = connector.execute_query(settings_query)
            
            # Get vacuum statistics
            stats_query = """
                SELECT
                    schemaname,
                    relname,
                    n_live_tup,
                    n_dead_tup,
                    CASE WHEN n_live_tup + n_dead_tup > 0 
                         THEN round((100 * n_dead_tup::numeric / (n_live_tup + n_dead_tup)::numeric)::numeric, 2)
                         ELSE 0
                    END as dead_tup_ratio,
                    last_vacuum,
                    last_autovacuum,
                    vacuum_count,
                    autovacuum_count,
                    last_analyze,
                    last_autoanalyze,
                    analyze_count,
                    autoanalyze_count
                FROM
                    pg_stat_user_tables
                ORDER BY
                    dead_tup_ratio DESC
            """
            
            stats = connector.execute_query(stats_query)
            
            # Format the response
            response = "# PostgreSQL Vacuum Analysis\n\n"
            
            # Current autovacuum settings
            response += "## Current Autovacuum Settings\n\n"
            response += "| Parameter | Value | Description |\n"
            response += "| --------- | ----- | ----------- |\n"
            
            for setting in settings:
                name = setting['name']
                value = setting['setting']
                unit = setting['unit'] if setting['unit'] else ""
                desc = setting['short_desc']
                
                response += f"| {name} | {value}{unit} | {desc} |\n"
            
            response += "\n"
            
            # Vacuum statistics
            response += "## Vacuum Statistics\n\n"
            response += "| Table | Live Tuples | Dead Tuples | Dead Ratio | Last Vacuum | Last Autovacuum | Vacuum Count | Autovacuum Count |\n"
            response += "| ----- | ----------- | ----------- | ---------- | ----------- | --------------- | ------------ | ---------------- |\n"
            
            for table in stats[:20]:  # Show top 20 tables by dead tuple ratio
                schema = table['schemaname']
                name = table['relname']
                live_tuples = f"{table['n_live_tup']:,}" if table['n_live_tup'] else "0"
                dead_tuples = f"{table['n_dead_tup']:,}" if table['n_dead_tup'] else "0"
                dead_ratio = f"{float(table['dead_tup_ratio']):.2f}%" if table['dead_tup_ratio'] else "0%"
                # Handle datetime or string for vacuum timestamps
                last_vacuum = table['last_vacuum'].strftime("%Y-%m-%d %H:%M:%S") if table['last_vacuum'] and hasattr(table['last_vacuum'], 'strftime') else str(table['last_vacuum']) if table['last_vacuum'] else "Never"
                last_autovacuum = table['last_autovacuum'].strftime("%Y-%m-%d %H:%M:%S") if table['last_autovacuum'] and hasattr(table['last_autovacuum'], 'strftime') else str(table['last_autovacuum']) if table['last_autovacuum'] else "Never"
                vacuum_count = table['vacuum_count'] or 0
                autovacuum_count = table['autovacuum_count'] or 0
                
                response += f"| {schema}.{name} | {live_tuples} | {dead_tuples} | {dead_ratio} | {last_vacuum} | {last_autovacuum} | {vacuum_count} | {autovacuum_count} |\n"
            
            response += "\n"
            
            # Tables that need vacuum
            tables_needing_vacuum = [t for t in stats if t.get('dead_tup_ratio') and float(t['dead_tup_ratio']) > 10]
            
            if tables_needing_vacuum:
                response += "## Tables Needing VACUUM\n\n"
                response += "The following tables have a high percentage of dead tuples and should be vacuumed:\n\n"
                
                for table in tables_needing_vacuum[:10]:  # Show top 10
                    schema = table['schemaname']
                    name = table['relname']
                    # Convert to float for safe comparison and formatting
                    dead_ratio = float(table['dead_tup_ratio']) if table['dead_tup_ratio'] else 0
                    
                    response += f"- **{schema}.{name}**: {dead_ratio:.2f}% dead tuples\n"
                
                response += "\n"
                response += "```sql\n"
                for table in tables_needing_vacuum[:10]:
                    schema = table['schemaname']
                    name = table['relname']
                    response += f"VACUUM ANALYZE \"{schema}\".\"{name}\";\n"
                response += "```\n\n"
            
            # Tables that haven't been vacuumed recently
            one_month_ago = datetime.datetime.now() - datetime.timedelta(days=30)
            tables_not_vacuumed = []
            
            # Safely check vacuum dates with type handling
            for t in stats:
                not_vacuumed = False
                
                # Check if no vacuum has been performed
                if not t.get('last_vacuum') and not t.get('last_autovacuum'):
                    not_vacuumed = True
                else:
                    # Check last_vacuum date if it exists
                    if t.get('last_vacuum'):
                        # Convert string to datetime if needed
                        last_vacuum_date = t['last_vacuum']
                        if not hasattr(last_vacuum_date, 'strftime'):
                            try:
                                # Try to parse the date string if it's not already a datetime
                                if isinstance(last_vacuum_date, str) and last_vacuum_date != "Never":
                                    last_vacuum_date = datetime.datetime.strptime(last_vacuum_date, "%Y-%m-%d %H:%M:%S")
                            except:
                                # If parsing fails, assume it needs vacuum
                                not_vacuumed = True
                                continue
                                
                        # Check if vacuum is older than one month
                        if hasattr(last_vacuum_date, 'strftime') and last_vacuum_date < one_month_ago:
                            # Check last_autovacuum date
                            if not t.get('last_autovacuum'):
                                not_vacuumed = True
                            else:
                                last_autovacuum_date = t['last_autovacuum']
                                if not hasattr(last_autovacuum_date, 'strftime'):
                                    try:
                                        if isinstance(last_autovacuum_date, str) and last_autovacuum_date != "Never":
                                            last_autovacuum_date = datetime.datetime.strptime(last_autovacuum_date, "%Y-%m-%d %H:%M:%S")
                                    except:
                                        not_vacuumed = True
                                        continue
                                
                                if hasattr(last_autovacuum_date, 'strftime') and last_autovacuum_date < one_month_ago:
                                    not_vacuumed = True
                
                if not_vacuumed:
                    tables_not_vacuumed.append(t)
            
            if tables_not_vacuumed:
                response += "## Tables Not Vacuumed Recently\n\n"
                response += "The following tables haven't been vacuumed in the last 30 days:\n\n"
                
                for table in tables_not_vacuumed[:10]:  # Show top 10
                    schema = table['schemaname']
                    name = table['relname']
                    # Handle datetime or string for vacuum timestamps
                    last_vacuum = table['last_vacuum'].strftime("%Y-%m-%d") if table['last_vacuum'] and hasattr(table['last_vacuum'], 'strftime') else str(table['last_vacuum']) if table['last_vacuum'] else "Never"
                    last_autovacuum = table['last_autovacuum'].strftime("%Y-%m-%d") if table['last_autovacuum'] and hasattr(table['last_autovacuum'], 'strftime') else str(table['last_autovacuum']) if table['last_autovacuum'] else "Never"
                    
                    response += f"- **{schema}.{name}**: Last vacuum: {last_vacuum}, Last autovacuum: {last_autovacuum}\n"
                
                response += "\n"
            
            # Recommendations
            response += "## Recommendations\n\n"
            
            # Check if autovacuum is enabled
            autovacuum_enabled = next((s for s in settings if s['name'] == 'autovacuum' and s['setting'] == 'on'), None)
            
            if not autovacuum_enabled:
                response += "- **Enable Autovacuum**: Autovacuum is currently disabled. Enable it to automatically reclaim space and update statistics.\n\n"
            
            # General recommendations
            response += "### General Recommendations\n\n"
            response += "1. **Regular Maintenance**: Schedule regular VACUUM operations for large tables during off-peak hours.\n\n"
            response += "2. **Adjust Autovacuum Settings**: Consider these settings for better autovacuum performance:\n\n"
            response += "```sql\n"
            response += "-- More aggressive autovacuum for busy databases\n"
            response += "ALTER SYSTEM SET autovacuum_vacuum_scale_factor = 0.05;  -- Default is 0.2 (20%)\n"
            response += "ALTER SYSTEM SET autovacuum_analyze_scale_factor = 0.05;  -- Default is 0.1 (10%)\n"
            response += "ALTER SYSTEM SET autovacuum_vacuum_threshold = 50;  -- Default is 50 rows\n"
            response += "ALTER SYSTEM SET autovacuum_analyze_threshold = 50;  -- Default is 50 rows\n"
            response += "ALTER SYSTEM SET autovacuum_naptime = '1min';  -- Default is 1min\n"
            response += "ALTER SYSTEM SET autovacuum_max_workers = 6;  -- Default is 3\n"
            response += "```\n\n"
            
            response += "3. **Table-Specific Settings**: For tables with high update/delete activity, consider table-specific autovacuum settings:\n\n"
            response += "```sql\n"
            response += "-- Example for a high-churn table\n"
            response += "ALTER TABLE high_churn_table SET (autovacuum_vacuum_scale_factor = 0.01, autovacuum_vacuum_threshold = 100);\n"
            response += "```\n\n"
            
            response += "4. **Monitor Bloat**: Regularly check for table bloat and vacuum accordingly.\n\n"
            
            response += "5. **VACUUM FULL**: For tables with significant bloat, consider running VACUUM FULL during maintenance windows:\n\n"
            response += "```sql\n"
            response += "-- Warning: VACUUM FULL locks the table and rewrites it completely\n"
            response += "VACUUM FULL table_name;\n"
            response += "```\n\n"
            
            return response
        except Exception as e:
            error_details = traceback.format_exc()
            error_msg = f"Error analyzing vacuum statistics: {str(e)}\n\n"
            
            if debug:
                error_msg += f"Error details:\n{error_details}\n\n"
            
            return error_msg
        finally:
            # Always disconnect when done
            connector.disconnect()
    @mcp.tool()
    async def identify_slow_queries(
        secret_name: str = None, 
        region_name: str = "us-west-2",
        secret_arn: str = None, 
        resource_arn: str = None, 
        database: str = None,
        host: str = None,
        port: int = None,
        user: str = None,
        password: str = None,
        min_execution_time: float = 100.0,  # minimum execution time in milliseconds
        limit: int = 20,  # limit number of slow queries to return
        debug: bool = False,
        ctx: Context = None
    ) -> str:
        """
        Identify slow queries using pg_stat_statements extension.
        
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
            min_execution_time: Minimum average execution time in milliseconds to consider a query as slow (default: 100ms)
            limit: Maximum number of slow queries to return (default: 20)
            debug: Enable detailed debug output (default: false)
        
        Returns:
            Analysis of slow queries with optimization recommendations
        """
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
            password=password
        )
        
        try:
            if not connector.connect():
                error_msg = "Failed to connect to database. Please check your credentials and connection parameters.\n\n"
                
                # Add troubleshooting tips
                error_msg += "Troubleshooting tips:\n"
                if secret_arn and resource_arn:
                    error_msg += "- Verify that your RDS cluster has Data API enabled\n"
                    error_msg += "- Check that the secret ARN and resource ARN are correct\n"
                    error_msg += "- Ensure your IAM role has rds-data:ExecuteStatement permission\n"
                elif host:
                    error_msg += "- Check if the database host is reachable\n"
                    error_msg += "- Verify that security groups allow access from your IP\n"
                    error_msg += "- Confirm database credentials are correct\n"
                
                return error_msg
            
            # Check if pg_stat_statements extension is installed
            check_extension_query = """
                SELECT COUNT(*) as count FROM pg_extension WHERE extname = 'pg_stat_statements'
            """
            
            extension_result = connector.execute_query(check_extension_query)
            
            if not extension_result or extension_result[0]['count'] == 0:
                return """
                The pg_stat_statements extension is not installed or enabled.
                
                To enable pg_stat_statements, follow these steps:
                
                1. Add pg_stat_statements to shared_preload_libraries in postgresql.conf:
                   ```
                   shared_preload_libraries = 'pg_stat_statements'
                   ```
                
                2. Restart the PostgreSQL server
                
                3. Create the extension in your database:
                   ```
                   CREATE EXTENSION pg_stat_statements;
                   ```
                
                4. Verify the extension is installed:
                   ```
                   SELECT * FROM pg_extension WHERE extname = 'pg_stat_statements';
                   ```
                
                Note: For Amazon RDS, you can enable pg_stat_statements by modifying the parameter group.
                """
            
            # Get slow queries
            slow_queries_query = """
                SELECT
                    query,
                    calls,
                    total_exec_time / calls as avg_exec_time_ms,
                    total_exec_time as total_time_ms,
                    rows / calls as avg_rows,
                    max_exec_time as max_time_ms,
                    mean_exec_time as mean_time_ms,
                    stddev_exec_time as stddev_time_ms,
                    min_exec_time as min_time_ms
                FROM
                    pg_stat_statements
                WHERE
                    total_exec_time / calls >= :min_time
                ORDER BY
                    avg_exec_time_ms DESC
                LIMIT :limit_rows
            """
            
            slow_queries = connector.execute_query(slow_queries_query, {"min_time": min_execution_time, "limit_rows": limit})
            
            if not slow_queries:
                return f"No queries found with average execution time >= {min_execution_time} ms."
            
            # Format the response
            response = "# Slow Query Analysis\n\n"
            
            response += f"Found {len(slow_queries)} queries with average execution time >= {min_execution_time} ms.\n\n"
            
            # Add summary table
            response += "## Summary\n\n"
            response += "| Query | Calls | Avg Time (ms) | Total Time (ms) | Avg Rows | Max Time (ms) |\n"
            response += "| ----- | ----- | ------------- | --------------- | -------- | ------------- |\n"
            
            for query in slow_queries:
                # Truncate and clean query for table display
                truncated_query = query['query']
                if len(truncated_query) > 80:
                    truncated_query = truncated_query[:77] + "..."
                truncated_query = truncated_query.replace("\n", " ").replace("|", "\\|")
                
                calls = f"{query['calls']:,}"
                avg_time = f"{query['avg_exec_time_ms']:.2f}"
                total_time = f"{query['total_time_ms']:.2f}"
                avg_rows = f"{query['avg_rows']:.1f}" if query['avg_rows'] else "0"
                max_time = f"{query['max_time_ms']:.2f}"
                
                response += f"| {truncated_query} | {calls} | {avg_time} | {total_time} | {avg_rows} | {max_time} |\n"
            
            response += "\n"
            
            # Add detailed analysis for each slow query
            response += "## Detailed Analysis\n\n"
            
            for i, query in enumerate(slow_queries, 1):
                sql = query['query']
                calls = query['calls']
                avg_time = query['avg_exec_time_ms']
                total_time = query['total_time_ms']
                avg_rows = query['avg_rows'] if query['avg_rows'] else 0
                max_time = query['max_time_ms']
                min_time = query['min_time_ms']
                stddev_time = query['stddev_time_ms']
                
                response += f"### Query {i}\n\n"
                response += "```sql\n"
                response += sql + "\n"
                response += "```\n\n"
                
                response += "**Statistics:**\n\n"
                response += f"- **Calls:** {calls:,}\n"
                response += f"- **Average Time:** {avg_time:.2f} ms\n"
                response += f"- **Total Time:** {total_time:.2f} ms\n"
                response += f"- **Average Rows:** {avg_rows:.1f}\n"
                response += f"- **Maximum Time:** {max_time:.2f} ms\n"
                response += f"- **Minimum Time:** {min_time:.2f} ms\n"
                response += f"- **Standard Deviation:** {stddev_time:.2f} ms\n\n"
                
                # Extract tables from the query to provide more specific recommendations
                tables_involved = extract_tables_from_query(sql)
                
                # Add optimization recommendations
                response += "**Optimization Recommendations:**\n\n"
                
                # Check for common issues and provide recommendations
                if "SELECT *" in sql.upper():
                    response += "- **Avoid SELECT ***: Specify only the columns you need to reduce I/O and network traffic.\n\n"
                
                if " LIKE '%" in sql.upper() or " LIKE \"%"  in sql.upper():
                    response += "- **Leading Wildcard LIKE**: Queries with leading wildcards (LIKE '%...') cannot use standard indexes effectively. Consider using a trigram index or full-text search instead.\n\n"
                
                if "ORDER BY" in sql.upper() and avg_rows > 1000:
                    response += "- **Large Result Sorting**: This query sorts a large result set. Consider adding an index on the sorted columns or limiting the result set.\n\n"
                
                if "GROUP BY" in sql.upper():
                    response += "- **Grouping Operation**: Consider adding indexes on the grouped columns to speed up the operation.\n\n"
                
                if "JOIN" in sql.upper():
                    response += "- **Join Performance**: Ensure that joined columns are properly indexed and consider analyzing the join conditions.\n\n"
                
                if stddev_time > avg_time * 0.5:
                    response += "- **High Variability**: This query has high execution time variability, which might indicate parameter sensitivity or data skew.\n\n"
                
                if tables_involved:
                    response += "- **Tables Involved**: " + ", ".join(tables_involved) + "\n"
                    response += "  - Consider analyzing these tables with EXPLAIN ANALYZE to identify bottlenecks.\n"
                    response += "  - Check if appropriate indexes exist for the query conditions.\n\n"
                
                # Add EXPLAIN suggestion
                response += "- **Analyze with EXPLAIN**: Run the following to analyze the query execution plan:\n\n"
                response += "```sql\n"
                response += "EXPLAIN (ANALYZE, BUFFERS) " + sql + ";\n"
                response += "```\n\n"
                
                # Add a separator between queries
                if i < len(slow_queries):
                    response += "---\n\n"
            
            # General recommendations
            response += "## General Recommendations\n\n"
            response += "1. **Review Indexes**: Ensure appropriate indexes exist for frequently queried columns.\n\n"
            response += "2. **Update Statistics**: Run ANALYZE regularly to keep statistics up to date.\n\n"
            response += "3. **Query Rewriting**: Consider rewriting complex queries or breaking them into simpler parts.\n\n"
            response += "4. **Connection Pooling**: Use connection pooling to reduce connection overhead.\n\n"
            response += "5. **Regular Maintenance**: Schedule regular VACUUM and ANALYZE operations.\n\n"
            response += "6. **Monitor Resource Usage**: Check if the database server has sufficient resources (CPU, memory, disk I/O).\n\n"
            
            return response
        except Exception as e:
            error_details = traceback.format_exc()
            error_msg = f"Error identifying slow queries: {str(e)}\n\n"
            
            if debug:
                error_msg += f"Error details:\n{error_details}\n\n"
            
            return error_msg
        finally:
            # Always disconnect when done
            connector.disconnect()
