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
Functions for analyzing and recommending PostgreSQL indexes.
"""
import re
from typing import Dict, List, Any, Tuple

def extract_potential_indexes(query: str) -> List[Dict[str, Any]]:
    """
    Extract potential index candidates from a SQL query.
    
    Args:
        query: SQL query to analyze
        
    Returns:
        List of dictionaries with potential index information
    """
    # Normalize query: remove comments and extra whitespace
    query = re.sub(r'--.*?$', '', query, flags=re.MULTILINE)  # Remove single-line comments
    query = re.sub(r'/\*.*?\*/', '', query, flags=re.DOTALL)  # Remove multi-line comments
    query = ' '.join(query.split())  # Normalize whitespace
    query_lower = query.lower()
    
    potential_indexes = []
    
    # Extract WHERE conditions
    where_match = re.search(r'where\s+(.*?)(?:group by|order by|limit|$)', query_lower, re.IGNORECASE | re.DOTALL)
    if where_match:
        where_clause = where_match.group(1).strip()
        
        # Split by AND to get individual conditions
        conditions = re.split(r'\s+and\s+', where_clause, flags=re.IGNORECASE)
        
        for condition in conditions:
            # Look for column comparisons
            # Pattern: column_name = value or column_name IN (...) or column_name BETWEEN ... AND ...
            column_match = re.search(r'(\w+\.\w+|\w+)\s*(=|<|>|<=|>=|like|in|between)', condition, re.IGNORECASE)
            
            if column_match:
                column_name = column_match.group(1)
                operator = column_match.group(2).upper()
                
                # Handle table.column format
                if '.' in column_name:
                    parts = column_name.split('.')
                    table_name = parts[0]
                    column_name = parts[1]
                else:
                    # We don't know the table, will need to be resolved later
                    table_name = None
                
                potential_indexes.append({
                    'table': table_name,
                    'column': column_name,
                    'operator': operator,
                    'condition': condition.strip(),
                    'source': 'WHERE clause'
                })
    
    # Extract JOIN conditions
    join_matches = re.finditer(r'(?:inner|left|right|full)?\s*join\s+(\w+)\s+(?:as\s+(\w+)\s+)?on\s+(.*?)(?:(?:inner|left|right|full)?\s*join|where|group by|order by|limit|$)', 
                              query_lower, re.IGNORECASE | re.DOTALL)
    
    for match in join_matches:
        table_name = match.group(1)
        table_alias = match.group(2) if match.group(2) else table_name
        join_condition = match.group(3).strip()
        
        # Look for column comparisons in join condition
        # Pattern: table1.column1 = table2.column2
        join_column_match = re.search(r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)', join_condition, re.IGNORECASE)
        
        if join_column_match:
            left_table = join_column_match.group(1)
            left_column = join_column_match.group(2)
            right_table = join_column_match.group(3)
            right_column = join_column_match.group(4)
            
            # Add potential index for the table being joined
            if left_table == table_alias:
                potential_indexes.append({
                    'table': table_name,
                    'column': left_column,
                    'operator': '=',
                    'condition': join_condition.strip(),
                    'source': 'JOIN condition'
                })
            elif right_table == table_alias:
                potential_indexes.append({
                    'table': table_name,
                    'column': right_column,
                    'operator': '=',
                    'condition': join_condition.strip(),
                    'source': 'JOIN condition'
                })
    
    # Extract ORDER BY columns
    order_by_match = re.search(r'order by\s+(.*?)(?:limit|$)', query_lower, re.IGNORECASE | re.DOTALL)
    if order_by_match:
        order_by_clause = order_by_match.group(1).strip()
        
        # Split by comma to get individual columns
        order_columns = re.split(r'\s*,\s*', order_by_clause)
        
        for column_expr in order_columns:
            # Remove ASC/DESC if present
            column_name = re.sub(r'\s+(asc|desc)(\s+|$)', '', column_expr, flags=re.IGNORECASE).strip()
            
            # Handle table.column format
            if '.' in column_name:
                parts = column_name.split('.')
                table_name = parts[0]
                column_name = parts[1]
            else:
                # We don't know the table, will need to be resolved later
                table_name = None
            
            potential_indexes.append({
                'table': table_name,
                'column': column_name,
                'operator': 'ORDER',
                'condition': f"ORDER BY {column_expr}",
                'source': 'ORDER BY clause'
            })
    
    # Extract GROUP BY columns
    group_by_match = re.search(r'group by\s+(.*?)(?:having|order by|limit|$)', query_lower, re.IGNORECASE | re.DOTALL)
    if group_by_match:
        group_by_clause = group_by_match.group(1).strip()
        
        # Split by comma to get individual columns
        group_columns = re.split(r'\s*,\s*', group_by_clause)
        
        for column_expr in group_columns:
            column_name = column_expr.strip()
            
            # Handle table.column format
            if '.' in column_name:
                parts = column_name.split('.')
                table_name = parts[0]
                column_name = parts[1]
            else:
                # We don't know the table, will need to be resolved later
                table_name = None
            
            potential_indexes.append({
                'table': table_name,
                'column': column_name,
                'operator': 'GROUP',
                'condition': f"GROUP BY {column_expr}",
                'source': 'GROUP BY clause'
            })
    
    return potential_indexes

def get_table_structure_for_index(connector, tables: List[str]) -> Dict[str, Any]:
    """
    Get table structure information for index analysis.
    
    Args:
        connector: Database connector instance
        tables: List of table names
        
    Returns:
        Dictionary with table structure information
    """
    if not tables:
        return {}
    
    db_structure = {
        'tables': {},
        'columns': {},
        'indexes': {}
    }
    
    # Get table information
    placeholders = ', '.join(['%s'] * len(tables))
    tables_query = f"""
        SELECT 
            t.table_name,
            t.table_schema,
            pg_size_pretty(pg_total_relation_size(c.oid)) as total_size,
            pg_total_relation_size(c.oid) as size_bytes,
            c.reltuples::bigint as row_estimate
        FROM 
            information_schema.tables t
        JOIN 
            pg_class c ON c.relname = t.table_name
        JOIN 
            pg_namespace n ON n.nspname = t.table_schema AND n.oid = c.relnamespace
        WHERE 
            t.table_name IN ({placeholders})
            AND t.table_type = 'BASE TABLE'
    """
    
    table_results = connector.execute_query(tables_query, tables)
    
    for table in table_results:
        table_name = table['table_name']
        schema_name = table['table_schema']
        full_name = f"{schema_name}.{table_name}"
        
        db_structure['tables'][full_name] = {
            'name': table_name,
            'schema': schema_name,
            'size': table['total_size'],
            'size_bytes': table['size_bytes'],
            'row_estimate': table['row_estimate']
        }
    
    # Get column information
    columns_query = f"""
        SELECT 
            c.table_schema,
            c.table_name,
            c.column_name,
            c.data_type,
            c.is_nullable
        FROM 
            information_schema.columns c
        WHERE 
            c.table_name IN ({placeholders})
        ORDER BY 
            c.table_schema, c.table_name, c.ordinal_position
    """
    
    column_results = connector.execute_query(columns_query, tables)
    
    for column in column_results:
        table_name = column['table_name']
        schema_name = column['table_schema']
        full_name = f"{schema_name}.{table_name}"
        column_name = column['column_name']
        
        if full_name not in db_structure['columns']:
            db_structure['columns'][full_name] = {}
        
        db_structure['columns'][full_name][column_name] = {
            'data_type': column['data_type'],
            'is_nullable': column['is_nullable']
        }
    
    # Get index information
    indexes_query = f"""
        SELECT 
            t.schemaname as table_schema,
            t.tablename as table_name,
            i.indexname as index_name,
            pg_get_indexdef(ic.oid) as index_definition,
            idx.indisunique as is_unique,
            idx.indisprimary as is_primary,
            am.amname as index_type,
            pg_size_pretty(pg_relation_size(ic.oid)) as index_size,
            pg_relation_size(ic.oid) as index_size_bytes,
            s.idx_scan as index_scans,
            array_to_string(array_agg(a.attname), ', ') as column_names
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
        JOIN 
            pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(idx.indkey)
        WHERE 
            t.tablename IN ({placeholders})
        GROUP BY
            t.schemaname, t.tablename, i.indexname, idx.indisunique, idx.indisprimary, 
            am.amname, index_size, index_size_bytes, s.idx_scan, ic.oid
        ORDER BY 
            t.schemaname, t.tablename, i.indexname
    """
    
    index_results = connector.execute_query(indexes_query, tables)
    
    for index in index_results:
        table_name = index['table_name']
        schema_name = index['table_schema']
        full_name = f"{schema_name}.{table_name}"
        index_name = index['index_name']
        
        if full_name not in db_structure['indexes']:
            db_structure['indexes'][full_name] = {}
        
        db_structure['indexes'][full_name][index_name] = {
            'definition': index['index_definition'],
            'is_unique': index['is_unique'],
            'is_primary': index['is_primary'],
            'type': index['index_type'],
            'size': index['index_size'],
            'size_bytes': index['index_size_bytes'],
            'scans': index['index_scans'],
            'columns': index['column_names'].split(', ')
        }
    
    return db_structure

def check_existing_indexes(potential_indexes: List[Dict[str, Any]], db_structure: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Check which potential indexes already exist.
    
    Args:
        potential_indexes: List of potential indexes
        db_structure: Database structure information
        
    Returns:
        Tuple of (existing_indexes, missing_indexes)
    """
    existing_indexes = []
    missing_indexes = []
    
    for index_candidate in potential_indexes:
        candidate_table = index_candidate['table']
        candidate_column = index_candidate['column']
        
        # Skip if we don't know the table
        if not candidate_table:
            continue
        
        # Check if the table exists in our structure
        table_found = False
        for full_table_name in db_structure['tables']:
            table_name = db_structure['tables'][full_table_name]['name']
            
            if table_name.lower() == candidate_table.lower():
                table_found = True
                
                # Check if the column exists in this table
                if full_table_name in db_structure['columns'] and candidate_column.lower() in [col.lower() for col in db_structure['columns'][full_table_name]]:
                    # Check if an index exists for this column
                    index_exists = False
                    
                    if full_table_name in db_structure['indexes']:
                        for index_name, index_info in db_structure['indexes'][full_table_name].items():
                            if candidate_column.lower() in [col.lower() for col in index_info['columns']]:
                                # Index exists for this column
                                index_exists = True
                                existing_indexes.append({
                                    'table': full_table_name,
                                    'column': candidate_column,
                                    'index_name': index_name,
                                    'index_type': index_info['type'],
                                    'is_unique': index_info['is_unique'],
                                    'is_primary': index_info['is_primary'],
                                    'definition': index_info['definition'],
                                    'scans': index_info['scans'],
                                    'source': index_candidate['source']
                                })
                                break
                    
                    if not index_exists:
                        # No index exists for this column
                        missing_indexes.append({
                            'table': full_table_name,
                            'column': candidate_column,
                            'operator': index_candidate['operator'],
                            'condition': index_candidate['condition'],
                            'source': index_candidate['source']
                        })
                
                break
        
        if not table_found:
            # We couldn't find the table, so we can't determine if an index exists
            continue
    
    return existing_indexes, missing_indexes

def format_index_recommendations_response(
    query: str,
    plan_json: Dict[str, Any],
    db_structure: Dict[str, Any],
    existing_indexes: List[Dict[str, Any]],
    missing_indexes: List[Dict[str, Any]]
) -> str:
    """
    Format the index recommendations into a comprehensive response.
    
    Args:
        query: Original SQL query
        plan_json: Query execution plan
        db_structure: Database structure information
        existing_indexes: List of existing indexes
        missing_indexes: List of missing indexes
        
    Returns:
        Formatted string with index recommendations
    """
    response = "# PostgreSQL Index Recommendations\n\n"
    
    # Original query
    response += "## Original Query\n\n"
    response += f"```sql\n{query}\n```\n\n"
    
    # Execution plan summary
    response += "## Execution Plan Summary\n\n"
    
    if plan_json and 'Plan' in plan_json:
        plan = plan_json['Plan']
        response += f"- **Plan Type**: {plan.get('Node Type', 'Unknown')}\n"
        response += f"- **Estimated Cost**: {plan.get('Total Cost', 'Unknown')}\n"
        response += f"- **Estimated Rows**: {plan.get('Plan Rows', 'Unknown')}\n"
        
        # Check for sequential scans
        seq_scans = find_nodes_by_type(plan, 'Seq Scan')
        if seq_scans:
            response += "- **Sequential Scans Detected**: "
            response += ", ".join([node.get('Relation Name', 'Unknown') for node in seq_scans])
            response += "\n"
        
        response += "\n"
    else:
        response += "No execution plan available.\n\n"
    
    # Tables involved
    response += "## Tables Involved\n\n"
    
    if db_structure['tables']:
        response += "| Table | Rows (est.) | Size | Columns | Indexes |\n"
        response += "| ----- | ----------- | ---- | ------- | ------- |\n"
        
        for full_name, table_info in db_structure['tables'].items():
            table_name = table_info['name']
            rows = table_info['row_estimate']
            size = table_info['size']
            
            # Count columns and indexes
            column_count = len(db_structure['columns'].get(full_name, {}))
            index_count = len(db_structure['indexes'].get(full_name, {}))
            
            response += f"| {full_name} | {rows:,} | {size} | {column_count} | {index_count} |\n"
        
        response += "\n"
    else:
        response += "No table information available.\n\n"
    
    # Existing indexes
    response += "## Existing Indexes\n\n"
    
    if existing_indexes:
        response += "| Table | Column | Index Name | Type | Scans | Source |\n"
        response += "| ----- | ------ | ---------- | ---- | ----- | ------ |\n"
        
        for index in existing_indexes:
            table = index['table']
            column = index['column']
            index_name = index['index_name']
            index_type = index['index_type']
            scans = index['scans'] or 0
            source = index['source']
            
            response += f"| {table} | {column} | {index_name} | {index_type} | {scans} | {source} |\n"
        
        response += "\n"
    else:
        response += "No existing indexes found for the query conditions.\n\n"
    
    # Missing indexes
    response += "## Recommended Indexes\n\n"
    
    if missing_indexes:
        response += "| Table | Column | Condition | Source |\n"
        response += "| ----- | ------ | --------- | ------ |\n"
        
        for index in missing_indexes:
            table = index['table']
            column = index['column']
            condition = index['condition']
            source = index['source']
            
            response += f"| {table} | {column} | {condition} | {source} |\n"
        
        response += "\n"
        
        # Generate CREATE INDEX statements
        response += "### SQL Statements for Recommended Indexes\n\n"
        response += "```sql\n"
        
        for i, index in enumerate(missing_indexes, 1):
            table_parts = index['table'].split('.')
            schema = table_parts[0]
            table = table_parts[1]
            column = index['column']
            
            # Generate a unique index name
            index_name = f"idx_{table}_{column}_{i}"
            
            # Determine index type based on operator
            if index['operator'] in ('=', 'IN'):
                # B-tree is good for equality
                response += f"CREATE INDEX {index_name} ON {schema}.{table} ({column});\n"
            elif index['operator'] in ('<', '>', '<=', '>=', 'BETWEEN'):
                # B-tree is also good for range queries
                response += f"CREATE INDEX {index_name} ON {schema}.{table} ({column});\n"
            elif index['operator'] == 'LIKE':
                # For LIKE queries, consider a trigram index
                response += f"-- For LIKE queries with wildcards, consider a trigram index\n"
                response += f"CREATE EXTENSION IF NOT EXISTS pg_trgm;\n"
                response += f"CREATE INDEX {index_name} ON {schema}.{table} USING gin ({column} gin_trgm_ops);\n"
            elif index['operator'] == 'ORDER':
                # For ORDER BY, a regular B-tree index works
                response += f"-- For ORDER BY optimization\n"
                response += f"CREATE INDEX {index_name} ON {schema}.{table} ({column});\n"
            elif index['operator'] == 'GROUP':
                # For GROUP BY, a regular B-tree index works
                response += f"-- For GROUP BY optimization\n"
                response += f"CREATE INDEX {index_name} ON {schema}.{table} ({column});\n"
            else:
                # Default to B-tree
                response += f"CREATE INDEX {index_name} ON {schema}.{table} ({column});\n"
        
        response += "```\n\n"
    else:
        response += "No additional indexes recommended for this query.\n\n"
    
    # Recommendations
    response += "## Recommendations\n\n"
    
    if missing_indexes:
        response += "### Index Recommendations\n\n"
        response += "Based on the query analysis, the following recommendations are made:\n\n"
        
        for index in missing_indexes:
            table = index['table']
            column = index['column']
            source = index['source']
            
            if source == 'WHERE clause':
                response += f"- Add an index on `{table}.{column}` to optimize filtering in the WHERE clause\n"
            elif source == 'JOIN condition':
                response += f"- Add an index on `{table}.{column}` to optimize the JOIN operation\n"
            elif source == 'ORDER BY clause':
                response += f"- Add an index on `{table}.{column}` to avoid sorting operations for ORDER BY\n"
            elif source == 'GROUP BY clause':
                response += f"- Add an index on `{table}.{column}` to optimize GROUP BY operations\n"
        
        response += "\n"
    
    # General recommendations
    response += "### General Recommendations\n\n"
    
    recommendations = [
        "- **Monitor Index Usage**: After creating indexes, monitor their usage with `pg_stat_all_indexes` to ensure they're being used",
        "- **Consider Index Size**: Indexes speed up queries but require storage space and slow down writes",
        "- **Analyze Regularly**: Run ANALYZE regularly to keep statistics up to date for the query planner",
        "- **Test Performance**: Measure query performance before and after adding indexes to confirm improvements"
    ]
    
    for recommendation in recommendations:
        response += f"{recommendation}\n"
    
    return response

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
