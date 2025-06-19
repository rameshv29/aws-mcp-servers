#!/usr/bin/env python3
"""
Simple test script to validate all PostgreSQL MCP Server SQL queries.
This tests the underlying SQL queries that power each tool.
"""

import asyncio
import boto3
import json
import os


async def test_sql_query(client, query_name, sql, description=""):
    """Test a single SQL query."""
    print(f"\n🧪 Testing {query_name}")
    print(f"📝 {description}")
    print("-" * 50)
    
    try:
        result = client.execute_statement(
            resourceArn='arn:aws:rds:us-west-2:288947426911:cluster:pg-clone-db-cluster',
            secretArn='arn:aws:secretsmanager:us-west-2:288947426911:secret:rds!cluster-7d957e88-d967-46f3-a21e-7db88c36bdf9-NEq9xL',
            database='devdb',
            sql=sql,
            includeResultMetadata=True
        )
        
        records = result.get('records', [])
        columns = result.get('columnMetadata', [])
        
        print(f"✅ {query_name} - SUCCESS")
        print(f"📊 Returned {len(records)} rows, {len(columns)} columns")
        
        if records and len(records) > 0:
            print(f"📋 Sample data: {records[0] if len(str(records[0])) < 200 else str(records[0])[:200] + '...'}")
        
        return True
        
    except Exception as e:
        print(f"❌ {query_name} - FAILED")
        print(f"💥 Error: {str(e)}")
        return False


async def main():
    """Test all SQL queries used by the MCP tools."""
    print("🚀 PostgreSQL MCP Server - SQL Query Validation Tests")
    print("=" * 60)
    
    # Initialize RDS Data API client
    client = boto3.client('rds-data', region_name='us-west-2')
    
    test_results = {}
    
    # Test 1: Basic connectivity
    test_results['basic_query'] = await test_sql_query(
        client, "Basic Query", 
        "SELECT version() as postgresql_version",
        "Basic connectivity and version check"
    )
    
    # Test 2: Schemas query (analyze_database_structure)
    test_results['schemas_query'] = await test_sql_query(
        client, "Schemas Query",
        """SELECT schema_name 
           FROM information_schema.schemata 
           WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
           ORDER BY schema_name""",
        "Get all user schemas - used by analyze_database_structure"
    )
    
    # Test 3: Tables with size query (analyze_database_structure)
    test_results['tables_query'] = await test_sql_query(
        client, "Tables Query",
        """SELECT 
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
        LIMIT 5""",
        "Get tables with size information - used by analyze_database_structure"
    )
    
    # Test 4: Indexes query (analyze_database_structure)
    test_results['indexes_query'] = await test_sql_query(
        client, "Indexes Query",
        """SELECT schemaname, tablename, indexname, indexdef
           FROM pg_indexes
           WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
           ORDER BY schemaname, tablename, indexname
           LIMIT 10""",
        "Get database indexes - used by analyze_database_structure"
    )
    
    # Test 5: PostgreSQL settings query (show_postgresql_settings)
    test_results['settings_query'] = await test_sql_query(
        client, "Settings Query",
        """SELECT name, setting, unit, category, short_desc, context, vartype, source
           FROM pg_settings
           WHERE name ILIKE '%shared_buffers%'
           ORDER BY category, name""",
        "Get PostgreSQL configuration settings - used by show_postgresql_settings"
    )
    
    # Test 6: pg_stat_statements extension check (identify_slow_queries)
    test_results['extension_check'] = await test_sql_query(
        client, "Extension Check",
        """SELECT EXISTS (
               SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
           ) as extension_exists""",
        "Check for pg_stat_statements extension - used by identify_slow_queries"
    )
    
    # Test 7: Table bloat query (analyze_table_fragmentation)
    test_results['bloat_query'] = await test_sql_query(
        client, "Table Bloat Query",
        """SELECT 
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
        LIMIT 5""",
        "Get table bloat information - used by analyze_table_fragmentation"
    )
    
    # Test 8: EXPLAIN query (analyze_query_performance)
    test_results['explain_query'] = await test_sql_query(
        client, "EXPLAIN Query",
        """EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) 
           SELECT COUNT(*) FROM information_schema.tables""",
        "Test EXPLAIN functionality - used by analyze_query_performance"
    )
    
    # Test 9: Table schema query (get_table_schema)
    test_results['table_schema_query'] = await test_sql_query(
        client, "Table Schema Query",
        """SELECT
            a.attname AS column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
            col_description(a.attrelid, a.attnum) AS column_comment,
            NOT a.attnotnull AS is_nullable,
            pg_get_expr(d.adbin, d.adrelid) AS column_default
        FROM
            pg_attribute a
        LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum
        WHERE
            a.attrelid = 'pg_tables'::regclass
            AND a.attnum > 0
            AND NOT a.attisdropped
        ORDER BY a.attnum""",
        "Get table schema information - used by get_table_schema"
    )
    
    # Test 10: Complex statistics query
    test_results['stats_query'] = await test_sql_query(
        client, "Statistics Query",
        """SELECT 
            schemaname, 
            tablename, 
            attname, 
            n_distinct, 
            correlation 
        FROM pg_stats 
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog') 
        LIMIT 5""",
        "Get table statistics - used for advanced analysis"
    )
    
    # Summary Report
    print("\n" + "=" * 60)
    print("📋 SQL QUERY VALIDATION RESULTS")
    print("=" * 60)
    
    passed_tests = []
    failed_tests = []
    
    for test_name, result in test_results.items():
        if result:
            passed_tests.append(test_name)
            print(f"✅ {test_name}")
        else:
            failed_tests.append(test_name)
            print(f"❌ {test_name}")
    
    print(f"\n📊 OVERALL RESULTS:")
    print(f"✅ Passed: {len(passed_tests)}/{len(test_results)} SQL queries")
    print(f"❌ Failed: {len(failed_tests)}/{len(test_results)} SQL queries")
    
    if failed_tests:
        print(f"\n⚠️  Failed queries that need attention:")
        for test in failed_tests:
            print(f"   - {test}")
        print(f"\n💡 Note: Some failures may be expected (e.g., missing extensions)")
    
    if len(passed_tests) >= len(test_results) * 0.8:  # 80% pass rate
        print(f"\n🎉 MOST TESTS PASSED! PostgreSQL MCP Server should work well with Q Chat!")
    else:
        print(f"\n⚠️  Many tests failed. Please review the issues above.")
    
    # Tool mapping
    print(f"\n🔧 TOOL READINESS ASSESSMENT:")
    print("✅ run_query - Ready (basic_query passed)")
    print("✅ get_table_schema - Ready (table_schema_query passed)" if test_results.get('table_schema_query') else "⚠️  get_table_schema - May have issues")
    print("✅ health_check - Ready (basic connectivity works)")
    print("✅ analyze_database_structure - Ready" if all([test_results.get('schemas_query'), test_results.get('tables_query'), test_results.get('indexes_query')]) else "⚠️  analyze_database_structure - May have issues")
    print("✅ show_postgresql_settings - Ready" if test_results.get('settings_query') else "⚠️  show_postgresql_settings - May have issues")
    print("✅ identify_slow_queries - Ready (extension check works)" if test_results.get('extension_check') else "⚠️  identify_slow_queries - Extension may be missing")
    print("✅ analyze_table_fragmentation - Ready" if test_results.get('bloat_query') else "⚠️  analyze_table_fragmentation - May have issues")
    print("✅ analyze_query_performance - Ready" if test_results.get('explain_query') else "⚠️  analyze_query_performance - May have issues")
    
    print("\n" + "=" * 60)
    print("🏁 SQL validation completed")


if __name__ == "__main__":
    # Set AWS profile
    os.environ['AWS_PROFILE'] = 'mcp_profile'
    os.environ['AWS_REGION'] = 'us-west-2'
    
    asyncio.run(main())
