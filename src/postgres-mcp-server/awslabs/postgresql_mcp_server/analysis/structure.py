"""
Functions for analyzing PostgreSQL database structure.
"""
from typing import Dict, List, Any

def get_database_structure(connector):
    """
    Get comprehensive database structure information.
    
    Args:
        connector: Database connector instance
        
    Returns:
        Dictionary containing tables, columns, indexes, foreign keys, and table statistics
    """
    # Get all tables in the database
    tables_query = """
        SELECT 
            t.table_name,
            t.table_schema,
            obj_description(c.oid, 'pg_class')::text as table_description,
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
            t.table_schema NOT IN ('pg_catalog', 'information_schema')
            AND t.table_type = 'BASE TABLE'
        ORDER BY 
            pg_total_relation_size(c.oid) DESC
    """
    tables = connector.execute_query(tables_query)
    
    # Get all columns in the database
    columns_query = """
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
            col_description(format('%s.%s', c.table_schema, c.table_name)::regclass::oid, c.ordinal_position)::text as column_description
        FROM 
            information_schema.columns c
        JOIN 
            information_schema.tables t ON c.table_name = t.table_name AND c.table_schema = t.table_schema
        WHERE 
            c.table_schema NOT IN ('pg_catalog', 'information_schema')
            AND t.table_type = 'BASE TABLE'
        ORDER BY 
            c.table_schema, c.table_name, c.ordinal_position
    """
    columns = connector.execute_query(columns_query)
    
    # Get all indexes in the database - modified to avoid oidvector type
    indexes_query = """
        SELECT 
            t.schemaname as table_schema,
            t.tablename as table_name,
            i.indexname as index_name,
            pg_get_indexdef(ic.oid)::text as index_definition,
            idx.indisunique as is_unique,
            idx.indisprimary as is_primary,
            am.amname as index_type,
            pg_size_pretty(pg_relation_size(ic.oid)) as index_size,
            pg_relation_size(ic.oid) as index_size_bytes,
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
            t.schemaname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY 
            t.schemaname, t.tablename, i.indexname
    """
    indexes = connector.execute_query(indexes_query)
    
    # Get all foreign keys in the database
    foreign_keys_query = """
        SELECT 
            tc.table_schema,
            tc.table_name,
            kcu.column_name,
            ccu.table_schema AS foreign_table_schema,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name,
            tc.constraint_name
        FROM 
            information_schema.table_constraints tc
        JOIN 
            information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN 
            information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        WHERE 
            tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY 
            tc.table_schema, tc.table_name
    """
    foreign_keys = connector.execute_query(foreign_keys_query)
    
    # Get table statistics - modified to avoid potential RDS Data API issues
    table_stats_query = """
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
            n_tup_hot_upd,
            n_live_tup,
            n_dead_tup,
            vacuum_count,
            autovacuum_count,
            analyze_count,
            autoanalyze_count
        FROM 
            pg_stat_user_tables
        WHERE
            schemaname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY 
            schemaname, relname
    """
    table_stats = connector.execute_query(table_stats_query)
    
    return {
        'tables': tables or [],
        'columns': columns or [],
        'indexes': indexes or [],
        'foreign_keys': foreign_keys or [],
        'table_stats': table_stats or []
    }

def organize_db_structure_by_table(db_structure):
    """
    Organize database structure by table for easier analysis.
    
    Args:
        db_structure: Database structure from get_database_structure()
        
    Returns:
        Dictionary with tables as keys and their details as values
    """
    tables = {}
    
    # Process tables
    for table in db_structure['tables']:
        table_name = table['table_name']
        table_schema = table['table_schema']
        full_table_name = f"{table_schema}.{table_name}"
        
        tables[full_table_name] = {
            'name': table_name,
            'schema': table_schema,
            'description': table['table_description'],
            'size': table['total_size'],
            'size_bytes': table['size_bytes'],
            'row_estimate': table['row_estimate'],
            'columns': [],
            'indexes': [],
            'foreign_keys': [],
            'stats': None
        }
    
    # Process columns
    for column in db_structure['columns']:
        table_name = column['table_name']
        table_schema = column['table_schema']
        full_table_name = f"{table_schema}.{table_name}"
        
        if full_table_name in tables:
            tables[full_table_name]['columns'].append({
                'name': column['column_name'],
                'data_type': column['data_type'],
                'max_length': column['character_maximum_length'],
                'numeric_precision': column['numeric_precision'],
                'numeric_scale': column['numeric_scale'],
                'is_nullable': column['is_nullable'],
                'default': column['column_default'],
                'description': column['column_description']
            })
    
    # Process indexes
    for index in db_structure['indexes']:
        table_name = index['table_name']
        table_schema = index['table_schema']
        full_table_name = f"{table_schema}.{table_name}"
        
        if full_table_name in tables:
            tables[full_table_name]['indexes'].append({
                'name': index['index_name'],
                'definition': index['index_definition'],
                'is_unique': index['is_unique'],
                'is_primary': index['is_primary'],
                'type': index['index_type'],
                'size': index['index_size'],
                'size_bytes': index['index_size_bytes'],
                'scans': index['index_scans']
            })
    
    # Process foreign keys
    for fk in db_structure['foreign_keys']:
        table_name = fk['table_name']
        table_schema = fk['table_schema']
        full_table_name = f"{table_schema}.{table_name}"
        
        if full_table_name in tables:
            tables[full_table_name]['foreign_keys'].append({
                'column': fk['column_name'],
                'foreign_table': f"{fk['foreign_table_schema']}.{fk['foreign_table_name']}",
                'foreign_column': fk['foreign_column_name'],
                'constraint_name': fk['constraint_name']
            })
    
    # Process table stats
    for stat in db_structure['table_stats']:
        table_name = stat['table_name']
        table_schema = stat['table_schema']
        full_table_name = f"{table_schema}.{table_name}"
        
        if full_table_name in tables:
            tables[full_table_name]['stats'] = {
                'seq_scan': stat['seq_scan'],
                'seq_tup_read': stat['seq_tup_read'],
                'idx_scan': stat['idx_scan'],
                'idx_tup_fetch': stat['idx_tup_fetch'],
                'n_tup_ins': stat['n_tup_ins'],
                'n_tup_upd': stat['n_tup_upd'],
                'n_tup_del': stat['n_tup_del'],
                'n_tup_hot_upd': stat['n_tup_hot_upd'],
                'n_live_tup': stat['n_live_tup'],
                'n_dead_tup': stat['n_dead_tup'],
                'vacuum_count': stat['vacuum_count'],
                'autovacuum_count': stat['autovacuum_count'],
                'analyze_count': stat['analyze_count'],
                'autoanalyze_count': stat['autoanalyze_count']
            }
    
    return tables

def analyze_database_structure_for_response(db_structure):
    """
    Analyze database structure and format a comprehensive response.
    
    Args:
        db_structure: Database structure from get_database_structure()
        
    Returns:
        Formatted string with database analysis
    """
    # Organize data by table
    tables_by_name = organize_db_structure_by_table(db_structure)
    
    # Sort tables by size (largest first)
    sorted_tables = sorted(
        tables_by_name.values(), 
        key=lambda x: int(x['size_bytes'] or 0), 
        reverse=True
    )
    
    # Calculate database summary
    total_tables = len(sorted_tables)
    total_columns = sum(len(table['columns']) for table in sorted_tables)
    total_indexes = sum(len(table['indexes']) for table in sorted_tables)
    total_foreign_keys = sum(len(table['foreign_keys']) for table in sorted_tables)
    total_size_bytes = sum(int(table['size_bytes'] or 0) for table in sorted_tables)
    
    # Format the response
    response = "# PostgreSQL Database Structure Analysis\n\n"
    
    # Database summary
    response += "## Database Summary\n\n"
    response += f"- **Tables**: {total_tables}\n"
    response += f"- **Columns**: {total_columns}\n"
    response += f"- **Indexes**: {total_indexes}\n"
    response += f"- **Foreign Keys**: {total_foreign_keys}\n"
    response += f"- **Total Size**: {format_bytes(total_size_bytes)}\n\n"
    
    # Largest tables
    response += "## Largest Tables\n\n"
    response += "| Table | Size | Rows (est.) | Columns | Indexes | Foreign Keys |\n"
    response += "| ----- | ---- | ----------- | ------- | ------- | ------------ |\n"
    
    for table in sorted_tables[:10]:  # Show top 10 largest tables
        response += f"| {table['schema']}.{table['name']} | {table['size']} | {table['row_estimate']:,} | {len(table['columns'])} | {len(table['indexes'])} | {len(table['foreign_keys'])} |\n"
    
    response += "\n"
    
    # Tables with most rows
    sorted_by_rows = sorted(
        sorted_tables, 
        key=lambda x: int(x['row_estimate'] or 0), 
        reverse=True
    )
    
    response += "## Tables with Most Rows\n\n"
    response += "| Table | Rows (est.) | Size | Columns | Indexes |\n"
    response += "| ----- | ----------- | ---- | ------- | ------- |\n"
    
    for table in sorted_by_rows[:10]:  # Show top 10 tables by row count
        response += f"| {table['schema']}.{table['name']} | {table['row_estimate']:,} | {table['size']} | {len(table['columns'])} | {len(table['indexes'])} |\n"
    
    response += "\n"
    
    # Tables with most indexes
    sorted_by_indexes = sorted(
        sorted_tables, 
        key=lambda x: len(x['indexes']), 
        reverse=True
    )
    
    response += "## Tables with Most Indexes\n\n"
    response += "| Table | Indexes | Size | Rows (est.) |\n"
    response += "| ----- | ------- | ---- | ----------- |\n"
    
    for table in sorted_by_indexes[:10]:  # Show top 10 tables by index count
        if len(table['indexes']) > 0:
            response += f"| {table['schema']}.{table['name']} | {len(table['indexes'])} | {table['size']} | {table['row_estimate']:,} |\n"
    
    response += "\n"
    
    # Tables without indexes
    tables_without_indexes = [
        table for table in sorted_tables 
        if len(table['indexes']) == 0 and int(table['row_estimate'] or 0) > 0
    ]
    
    if tables_without_indexes:
        response += "## Tables Without Indexes\n\n"
        response += "| Table | Rows (est.) | Size |\n"
        response += "| ----- | ----------- | ---- |\n"
        
        for table in tables_without_indexes:
            response += f"| {table['schema']}.{table['name']} | {table['row_estimate']:,} | {table['size']} |\n"
        
        response += "\n"
    
    # Tables with potential issues
    response += "## Potential Issues\n\n"
    
    # Tables with high dead tuple ratio
    tables_with_dead_tuples = []
    for table in sorted_tables:
        if table['stats'] and table['stats']['n_live_tup'] > 0:
            dead_ratio = table['stats']['n_dead_tup'] / (table['stats']['n_live_tup'] + table['stats']['n_dead_tup'])
            if dead_ratio > 0.1 and table['stats']['n_dead_tup'] > 1000:
                tables_with_dead_tuples.append({
                    'table': f"{table['schema']}.{table['name']}",
                    'dead_tuples': table['stats']['n_dead_tup'],
                    'live_tuples': table['stats']['n_live_tup'],
                    'dead_ratio': dead_ratio,
                    'size': table['size']
                })
    
    if tables_with_dead_tuples:
        response += "### Tables with High Dead Tuple Ratio\n\n"
        response += "| Table | Dead Tuples | Live Tuples | Dead Ratio | Size |\n"
        response += "| ----- | ----------- | ----------- | ---------- | ---- |\n"
        
        for table in sorted(tables_with_dead_tuples, key=lambda x: x['dead_ratio'], reverse=True)[:10]:
            response += f"| {table['table']} | {table['dead_tuples']:,} | {table['live_tuples']:,} | {table['dead_ratio']:.1%} | {table['size']} |\n"
        
        response += "\n**Recommendation**: Consider running VACUUM on these tables to reclaim space.\n\n"
    
    # Tables with sequential scans but no index scans
    tables_with_seq_scans = []
    for table in sorted_tables:
        if table['stats'] and table['stats']['seq_scan'] > 10 and table['stats']['idx_scan'] == 0 and int(table['row_estimate'] or 0) > 1000:
            tables_with_seq_scans.append({
                'table': f"{table['schema']}.{table['name']}",
                'seq_scans': table['stats']['seq_scan'],
                'rows': table['row_estimate'],
                'size': table['size']
            })
    
    if tables_with_seq_scans:
        response += "### Tables with Sequential Scans but No Index Scans\n\n"
        response += "| Table | Sequential Scans | Rows (est.) | Size |\n"
        response += "| ----- | --------------- | ----------- | ---- |\n"
        
        for table in sorted(tables_with_seq_scans, key=lambda x: x['seq_scans'], reverse=True)[:10]:
            response += f"| {table['table']} | {table['seq_scans']:,} | {table['rows']:,} | {table['size']} |\n"
        
        response += "\n**Recommendation**: Consider adding indexes to these tables to improve query performance.\n\n"
    
    # Unused indexes
    unused_indexes = []
    for table in sorted_tables:
        for index in table['indexes']:
            if index['scans'] == 0 and not index['is_primary']:
                unused_indexes.append({
                    'table': f"{table['schema']}.{table['name']}",
                    'index': index['name'],
                    'definition': index['definition'],
                    'size': index['size']
                })
    
    if unused_indexes:
        response += "### Unused Indexes\n\n"
        response += "| Table | Index | Size | Definition |\n"
        response += "| ----- | ----- | ---- | ---------- |\n"
        
        for index in sorted(unused_indexes, key=lambda x: x['size'], reverse=True)[:10]:
            # Truncate definition if too long
            definition = index['definition']
            if len(definition) > 80:
                definition = definition[:77] + "..."
            
            response += f"| {index['table']} | {index['index']} | {index['size']} | {definition} |\n"
        
        response += "\n**Recommendation**: Consider dropping these unused indexes to improve write performance and reduce storage.\n\n"
    
    # Recommendations section
    response += "## General Recommendations\n\n"
    
    # Add general recommendations based on analysis
    recommendations = []
    
    if tables_with_dead_tuples:
        recommendations.append("- **Vacuum Regularly**: Set up regular VACUUM operations to reclaim space from dead tuples.")
    
    if tables_with_seq_scans:
        recommendations.append("- **Add Indexes**: Consider adding indexes to tables with frequent sequential scans.")
    
    if unused_indexes:
        recommendations.append("- **Remove Unused Indexes**: Drop unused indexes to improve write performance and reduce storage.")
    
    # Add standard recommendations
    recommendations.extend([
        "- **Analyze Regularly**: Ensure ANALYZE is run regularly to keep statistics up to date.",
        "- **Monitor Table Growth**: Keep an eye on rapidly growing tables and consider partitioning if necessary.",
        "- **Check Query Performance**: Use EXPLAIN ANALYZE to identify slow queries and optimize them.",
        "- **Review Foreign Keys**: Ensure all foreign keys have corresponding indexes to prevent performance issues."
    ])
    
    for recommendation in recommendations:
        response += f"{recommendation}\n"
    
    return response

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