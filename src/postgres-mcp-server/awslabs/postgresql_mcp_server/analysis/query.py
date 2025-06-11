"""
Functions for analyzing PostgreSQL queries.
"""
import re
import json
from typing import Dict, List, Any, Optional

def extract_tables_from_query(query: str) -> List[str]:
    """
    Extract table names from a SQL query.
    
    Args:
        query: SQL query to analyze
        
    Returns:
        List of table names found in the query
    """
    # Normalize query: remove comments and extra whitespace
    query = re.sub(r'--.*?$', '', query, flags=re.MULTILINE)  # Remove single-line comments
    query = re.sub(r'/\*.*?\*/', '', query, flags=re.DOTALL)  # Remove multi-line comments
    query = ' '.join(query.split())  # Normalize whitespace
    
    # Find tables in FROM and JOIN clauses
    from_pattern = r'(?:FROM|JOIN)\s+([^\s,()]+)'
    tables = re.findall(from_pattern, query, re.IGNORECASE)
    
    # Clean up table names (remove aliases, schema prefixes for now)
    clean_tables = []
    for table in tables:
        # Skip common non-table keywords that might appear after FROM/JOIN
        if table.upper() in ('SELECT', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT', 'OFFSET'):
            continue
            
        # Handle schema.table format
        if '.' in table:
            parts = table.split('.')
            if len(parts) == 2:
                clean_tables.append(parts[1])  # Just use table name without schema
            else:
                clean_tables.append(table)
        else:
            clean_tables.append(table)
    
    # Remove duplicates while preserving order
    unique_tables = []
    for table in clean_tables:
        if table not in unique_tables:
            unique_tables.append(table)
    
    return unique_tables

def get_table_statistics(connector, tables: List[str]) -> List[Dict[str, Any]]:
    """
    Get statistics for the specified tables.
    
    Args:
        connector: Database connector instance
        tables: List of table names
        
    Returns:
        List of dictionaries with table statistics
    """
    if not tables:
        return []
    
    # Build a query to get statistics for all tables at once
    placeholders = ', '.join(['%s'] * len(tables))
    query = f"""
        SELECT 
            schemaname as table_schema,
            relname as table_name,
            seq_scan,
            seq_tup_read,
            idx_scan,
            idx_tup_fetch,
            n_tup_ins,
            n_tup_upd,
            n_tup_del,
            n_live_tup,
            n_dead_tup,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze
        FROM 
            pg_stat_user_tables
        WHERE 
            relname IN ({placeholders})
    """
    
    return connector.execute_query(query, tables)

def get_schema_information(connector, tables: List[str]) -> List[Dict[str, Any]]:
    """
    Get schema information for the specified tables.
    
    Args:
        connector: Database connector instance
        tables: List of table names
        
    Returns:
        List of dictionaries with column information
    """
    if not tables:
        return []
    
    # Build a query to get column information for all tables at once
    placeholders = ', '.join(['%s'] * len(tables))
    query = f"""
        SELECT 
            c.table_schema,
            c.table_name,
            c.column_name,
            c.data_type,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale,
            c.is_nullable,
            c.column_default,
            col_description(format('%s.%s', c.table_schema, c.table_name)::regclass::oid, c.ordinal_position) as column_description
        FROM 
            information_schema.columns c
        WHERE 
            c.table_name IN ({placeholders})
        ORDER BY 
            c.table_schema, c.table_name, c.ordinal_position
    """
    
    return connector.execute_query(query, tables)

def get_index_information(connector, tables: List[str]) -> List[Dict[str, Any]]:
    """
    Get index information for the specified tables.
    
    Args:
        connector: Database connector instance
        tables: List of table names
        
    Returns:
        List of dictionaries with index information
    """
    if not tables:
        return []
    
    # Build a query to get index information for all tables at once
    placeholders = ', '.join(['%s'] * len(tables))
    query = f"""
        SELECT 
            t.schemaname as table_schema,
            t.tablename as table_name,
            i.indexname as index_name,
            pg_get_indexdef(i.indexrelid) as index_definition,
            idx.indisunique as is_unique,
            idx.indisprimary as is_primary,
            am.amname as index_type,
            pg_size_pretty(pg_relation_size(i.indexrelid)) as index_size,
            s.idx_scan as index_scans
        FROM 
            pg_tables t
        JOIN 
            pg_class c ON c.relname = t.tablename
        JOIN 
            pg_namespace n ON n.nspname = t.schemaname
        JOIN 
            pg_indexes i ON i.tablename = t.tablename AND i.schemaname = t.schemaname
        JOIN 
            pg_class ic ON ic.relname = i.indexname
        JOIN 
            pg_index idx ON idx.indexrelid = ic.oid
        JOIN 
            pg_am am ON am.oid = ic.relam
        LEFT JOIN 
            pg_stat_all_indexes s ON s.indexrelid = ic.oid
        WHERE 
            t.tablename IN ({placeholders})
        ORDER BY 
            t.schemaname, t.tablename, i.indexname
    """
    
    return connector.execute_query(query, tables)

def detect_query_patterns(plan_json: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Detect patterns in the query execution plan.
    
    Args:
        plan_json: Query execution plan in JSON format
        
    Returns:
        List of dictionaries with pattern information
    """
    patterns = []
    
    if not plan_json or 'Plan' not in plan_json:
        return patterns
    
    # Check for sequential scans
    seq_scans = find_nodes_by_type(plan_json['Plan'], 'Seq Scan')
    if seq_scans:
        tables = [node.get('Relation Name', 'Unknown') for node in seq_scans]
        patterns.append({
            'pattern': 'Sequential Scan',
            'description': f"Sequential scan detected on tables: {', '.join(tables)}",
            'impact': 'High',
            'suggestion': 'Consider adding indexes to improve query performance'
        })
    
    # Check for hash joins
    hash_joins = find_nodes_by_type(plan_json['Plan'], 'Hash Join')
    if hash_joins and len(hash_joins) > 2:
        patterns.append({
            'pattern': 'Multiple Hash Joins',
            'description': f"Query uses {len(hash_joins)} hash joins",
            'impact': 'Medium',
            'suggestion': 'For large tables, consider optimizing join order or adding indexes'
        })
    
    # Check for nested loops with many iterations
    nested_loops = find_nodes_by_type(plan_json['Plan'], 'Nested Loop')
    if nested_loops and len(nested_loops) > 2:
        patterns.append({
            'pattern': 'Multiple Nested Loops',
            'description': f"Query uses {len(nested_loops)} nested loops",
            'impact': 'Medium',
            'suggestion': 'Nested loops can be inefficient for large datasets. Consider adding indexes or rewriting the query.'
        })
    
    # Check for sorts
    sorts = find_nodes_by_type(plan_json['Plan'], 'Sort')
    if sorts:
        patterns.append({
            'pattern': 'Explicit Sort',
            'description': f"Query requires sorting results",
            'impact': 'Medium',
            'suggestion': 'Consider adding an index that matches your ORDER BY clause'
        })
    
    # Check for high-cost operations
    if 'Total Cost' in plan_json['Plan'] and plan_json['Plan']['Total Cost'] > 1000:
        patterns.append({
            'pattern': 'High Cost Query',
            'description': f"Query has a high estimated cost: {plan_json['Plan']['Total Cost']:.2f}",
            'impact': 'High',
            'suggestion': 'Review query structure and consider optimization'
        })
    
    return patterns

def find_nodes_by_type(plan, node_type):
    """
    Recursively find all nodes of a specific type in the execution plan.
    
    Args:
        plan: Plan node to search
        node_type: Type of node to find
        
    Returns:
        List of matching nodes
    """
    result = []
    
    if isinstance(plan, dict):
        if plan.get('Node Type') == node_type:
            result.append(plan)
        
        # Check Plans array if it exists
        if 'Plans' in plan and isinstance(plan['Plans'], list):
            for subplan in plan['Plans']:
                result.extend(find_nodes_by_type(subplan, node_type))
    
    return result

def detect_query_anti_patterns(query: str) -> List[Dict[str, str]]:
    """
    Detect anti-patterns in the SQL query.
    
    Args:
        query: SQL query to analyze
        
    Returns:
        List of dictionaries with anti-pattern information
    """
    anti_patterns = []
    query_lower = query.lower()
    
    # Check for SELECT *
    if re.search(r'select\s+\*\s+from', query_lower):
        anti_patterns.append({
            'issue': 'Using SELECT *',
            'suggestion': 'Specify only the columns you need instead of using SELECT *',
            'example': 'SELECT id, name, email FROM users  # Instead of SELECT * FROM users'
        })
    
    # Check for LIKE with leading wildcard
    if re.search(r'like\s+[\'"]%', query_lower):
        anti_patterns.append({
            'issue': 'LIKE with leading wildcard',
            'suggestion': 'Queries with leading wildcards (LIKE \'%text\') cannot use indexes effectively',
            'example': 'Consider using a full-text search index or trigram indexes for this type of search'
        })
    
    # Check for functions on indexed columns
    function_patterns = [
        r'(lower|upper|substring|trim|to_char|date_trunc)\s*\(\s*(\w+)',
        r'(\w+)\s*(\+|\-|\*|\/)'
    ]
    
    for pattern in function_patterns:
        if re.search(pattern, query_lower):
            anti_patterns.append({
                'issue': 'Function on column in WHERE clause',
                'suggestion': 'Applying functions to columns in WHERE clauses prevents index usage',
                'example': 'WHERE email = lower(\'EMAIL\')  # Instead use: WHERE lower(email) = lower(\'EMAIL\') and create a functional index'
            })
            break
    
    # Check for OR conditions
    if re.search(r'where.*?\s+or\s+', query_lower):
        anti_patterns.append({
            'issue': 'Multiple OR conditions',
            'suggestion': 'Multiple OR conditions can lead to suboptimal query plans',
            'example': 'Consider using UNION or IN clauses instead, or ensure indexes exist for all OR conditions'
        })
    
    # Check for implicit conversions
    type_patterns = [
        r'::text', r'::int', r'::timestamp', r'::date',
        r'to_char', r'to_number', r'to_date', r'to_timestamp'
    ]
    
    for pattern in type_patterns:
        if pattern in query_lower:
            anti_patterns.append({
                'issue': 'Implicit type conversion',
                'suggestion': 'Type conversions can prevent index usage',
                'example': 'Store data in the correct type to avoid conversions in queries'
            })
            break
    
    # Check for NOT IN or NOT EXISTS
    if re.search(r'not\s+in\s*\(', query_lower) or 'not exists' in query_lower:
        anti_patterns.append({
            'issue': 'Using NOT IN or NOT EXISTS',
            'suggestion': 'NOT IN and NOT EXISTS can lead to poor performance, especially with large datasets',
            'example': 'Consider rewriting using LEFT JOIN / IS NULL pattern'
        })
    
    # Check for complex subqueries
    subquery_count = query_lower.count('select')
    if subquery_count > 2:
        anti_patterns.append({
            'issue': 'Multiple subqueries',
            'suggestion': f'Query contains {subquery_count} SELECT statements which may indicate complex nesting',
            'example': 'Consider simplifying using CTEs (WITH clause) or temporary tables'
        })
    
    return anti_patterns

def format_query_analysis_response(
    query: str,
    plan_json: Dict[str, Any],
    tables_involved: List[str],
    table_stats: List[Dict[str, Any]],
    schema_info: List[Dict[str, Any]],
    index_info: List[Dict[str, Any]],
    patterns: List[Dict[str, str]],
    anti_patterns: List[Dict[str, str]],
    complexity: Dict[str, Any]
) -> str:
    """
    Format the query analysis results into a comprehensive response.
    
    Args:
        query: Original SQL query
        plan_json: Query execution plan
        tables_involved: List of tables involved in the query
        table_stats: Statistics for the tables
        schema_info: Schema information for the tables
        index_info: Index information for the tables
        patterns: Detected query patterns
        anti_patterns: Detected query anti-patterns
        complexity: Query complexity metrics
        
    Returns:
        Formatted string with query analysis
    """
    response = "# PostgreSQL Query Analysis\n\n"
    
    # Original query
    response += "## Original Query\n\n"
    response += f"```sql\n{query}\n```\n\n"
    
    # Query complexity
    response += "## Query Complexity\n\n"
    response += f"- **Complexity Score**: {complexity['complexity_score']}\n"
    response += f"- **Join Count**: {complexity['join_count']}\n"
    response += f"- **Subquery Count**: {complexity['subquery_count']}\n"
    response += f"- **Aggregation Count**: {complexity['aggregation_count']}\n"
    
    if complexity['warnings']:
        response += "- **Warnings**:\n"
        for warning in complexity['warnings']:
            response += f"  - {warning}\n"
    
    response += "\n"
    
    # Execution plan summary
    response += "## Execution Plan Summary\n\n"
    
    if plan_json and 'Plan' in plan_json:
        plan = plan_json['Plan']
        response += f"- **Plan Type**: {plan.get('Node Type', 'Unknown')}\n"
        response += f"- **Estimated Cost**: {plan.get('Total Cost', 'Unknown')}\n"
        response += f"- **Estimated Rows**: {plan.get('Plan Rows', 'Unknown')}\n"
        
        # Add execution time if available
        if 'Execution Time' in plan_json:
            response += f"- **Execution Time**: {plan_json['Execution Time']:.2f} ms\n"
        
        # Add planning time if available
        if 'Planning Time' in plan_json:
            response += f"- **Planning Time**: {plan_json['Planning Time']:.2f} ms\n"
        
        response += "\n"
        
        # Simplified plan visualization
        response += "### Simplified Plan\n\n"
        response += "```\n"
        response += format_plan_node(plan, 0)
        response += "```\n\n"
    else:
        response += "No execution plan available.\n\n"
    
    # Tables involved
    response += "## Tables Involved\n\n"
    
    if tables_involved:
        response += "| Table | Rows | Sequential Scans | Index Scans |\n"
        response += "| ----- | ---- | --------------- | ----------- |\n"
        
        for table_name in tables_involved:
            # Find stats for this table
            table_stat = next((stat for stat in table_stats if stat['table_name'] == table_name), None)
            
            if table_stat:
                rows = table_stat.get('n_live_tup', 'Unknown')
                seq_scans = table_stat.get('seq_scan', 'Unknown')
                idx_scans = table_stat.get('idx_scan', 'Unknown')
                response += f"| {table_name} | {rows} | {seq_scans} | {idx_scans} |\n"
            else:
                response += f"| {table_name} | Unknown | Unknown | Unknown |\n"
        
        response += "\n"
    else:
        response += "No tables identified in the query.\n\n"
    
    # Indexes
    response += "## Available Indexes\n\n"
    
    if index_info:
        response += "| Table | Index | Type | Definition | Scans |\n"
        response += "| ----- | ----- | ---- | ---------- | ----- |\n"
        
        for idx in index_info:
            table_name = idx['table_name']
            index_name = idx['index_name']
            index_type = idx['index_type']
            definition = idx['index_definition']
            scans = idx['index_scans'] or 0
            
            # Truncate definition if too long
            if len(definition) > 80:
                definition = definition[:77] + "..."
            
            response += f"| {table_name} | {index_name} | {index_type} | {definition} | {scans} |\n"
        
        response += "\n"
    else:
        response += "No index information available for the tables in this query.\n\n"
    
    # Detected patterns
    if patterns:
        response += "## Detected Patterns\n\n"
        
        for i, pattern in enumerate(patterns, 1):
            response += f"### Pattern {i}: {pattern['pattern']}\n"
            response += f"**Description**: {pattern['description']}\n"
            response += f"**Impact**: {pattern['impact']}\n"
            response += f"**Suggestion**: {pattern['suggestion']}\n\n"
    
    # Anti-patterns
    if anti_patterns:
        response += "## Detected Anti-Patterns\n\n"
        
        for i, issue in enumerate(anti_patterns, 1):
            response += f"### Issue {i}: {issue['issue']}\n"
            response += f"**Suggestion**: {issue['suggestion']}\n"
            if "example" in issue and issue["example"]:
                response += f"**Example**: ```sql\n{issue['example']}\n```\n\n"
    
    # Recommendations
    response += "## Recommendations\n\n"
    
    # Generate recommendations based on the analysis
    recommendations = []
    
    # Check for sequential scans
    seq_scan_patterns = [p for p in patterns if p['pattern'] == 'Sequential Scan']
    if seq_scan_patterns:
        recommendations.append("- **Add Indexes**: Consider adding indexes to tables that are being sequentially scanned.")
    
    # Check for sort operations
    sort_patterns = [p for p in patterns if p['pattern'] == 'Explicit Sort']
    if sort_patterns:
        recommendations.append("- **Index for Sorting**: Add indexes that match your ORDER BY clause to avoid explicit sorts.")
    
    # Check for high-cost queries
    high_cost_patterns = [p for p in patterns if p['pattern'] == 'High Cost Query']
    if high_cost_patterns:
        recommendations.append("- **Optimize Query Structure**: Review and simplify the query structure to reduce its cost.")
    
    # Add recommendations from anti-patterns
    if any(ap['issue'] == 'Using SELECT *' for ap in anti_patterns):
        recommendations.append("- **Specify Columns**: Select only the columns you need instead of using SELECT *.")
    
    if any(ap['issue'] == 'LIKE with leading wildcard' for ap in anti_patterns):
        recommendations.append("- **Avoid Leading Wildcards**: Use trigram indexes or full-text search for pattern matching.")
    
    if any(ap['issue'] == 'Function on column in WHERE clause' for ap in anti_patterns):
        recommendations.append("- **Avoid Functions in WHERE**: Move functions to the right side of the comparison or use functional indexes.")
    
    # Add general recommendations
    recommendations.extend([
        "- **Keep Statistics Updated**: Run ANALYZE regularly to ensure the query planner has accurate statistics.",
        "- **Monitor Query Performance**: Use EXPLAIN ANALYZE to track actual vs. estimated performance.",
        "- **Consider Query Rewriting**: If performance issues persist, consider rewriting the query or using CTEs."
    ])
    
    for recommendation in recommendations:
        response += f"{recommendation}\n"
    
    return response

def format_plan_node(node, depth=0):
    """
    Format a plan node for display.
    
    Args:
        node: Plan node to format
        depth: Current depth in the plan tree
        
    Returns:
        Formatted string representation of the plan node
    """
    indent = "  " * depth
    result = f"{indent}-> {node.get('Node Type', 'Unknown')}"
    
    # Add key information based on node type
    if node.get('Node Type') == 'Seq Scan':
        result += f" on {node.get('Relation Name', 'Unknown')}"
    elif node.get('Node Type') == 'Index Scan':
        result += f" on {node.get('Relation Name', 'Unknown')} using {node.get('Index Name', 'Unknown')}"
    elif node.get('Node Type') == 'Hash Join':
        result += f" {node.get('Join Type', 'Unknown')}"
    
    # Add cost and rows
    result += f" (cost={node.get('Startup Cost', 0):.2f}..{node.get('Total Cost', 0):.2f} rows={node.get('Plan Rows', 0)})"
    
    # Add filter if present
    if 'Filter' in node:
        result += f"\n{indent}  Filter: {node['Filter']}"
    
    result += "\n"
    
    # Recursively format child nodes
    if 'Plans' in node:
        for child in node['Plans']:
            result += format_plan_node(child, depth + 1)
    
    return result