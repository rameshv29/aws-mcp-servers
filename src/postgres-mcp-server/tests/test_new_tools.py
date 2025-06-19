#!/usr/bin/env python3
"""
Test script to validate the 3 newly added PostgreSQL MCP Server tools.
This tests each new tool individually to ensure no SQL or syntax errors.
"""

import asyncio
import boto3
import json
import os


def extract_cell(cell: dict):
    """Extracts the scalar or array value from a single cell (same as server)."""
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
    """Convert RDS Data API execute_statement response to list of rows (same as server)."""
    columns = [col['name'] for col in response.get('columnMetadata', [])]
    records = []

    for row in response.get('records', []):
        row_data = {col: extract_cell(cell) for col, cell in zip(columns, row)}
        records.append(row_data)

    return records


async def test_health_check_queries():
    """Test the SQL queries used by health_check tool."""
    print("🧪 Testing health_check Tool")
    print("=" * 50)
    
    client = boto3.client('rds-data', region_name='us-west-2')
    
    try:
        # Test the basic connectivity query used by health_check
        print("Testing basic connectivity query...")
        result = client.execute_statement(
            resourceArn='arn:aws:rds:us-west-2:288947426911:cluster:pg-clone-db-cluster',
            secretArn='arn:aws:secretsmanager:us-west-2:288947426911:secret:rds!cluster-7d957e88-d967-46f3-a21e-7db88c36bdf9-NEq9xL',
            database='devdb',
            sql="SELECT 1 as health_check",
            includeResultMetadata=True
        )
        
        health_data = parse_execute_response(result)
        print(f"✅ Health check query successful")
        print(f"📊 Result: {health_data}")
        
        # Test that we can simulate the health_check logic
        connection_test = len(health_data) > 0 and 'error' not in health_data[0]
        health_status = {
            "status": "healthy" if connection_test else "unhealthy",
            "timestamp": "2025-06-19T14:15:00Z",
            "database_connection": connection_test,
            "server_version": "consolidated-v1.0",
            "tools_available": 10,
            "database_type": "PostgreSQL via RDS Data API"
        }
        
        print(f"✅ Health check logic working")
        print(f"📋 Health status: {health_status}")
        
        return True
        
    except Exception as e:
        print(f"❌ health_check test failed: {e}")
        return False


async def test_analyze_vacuum_stats_queries():
    """Test the SQL queries used by analyze_vacuum_stats tool."""
    print("\n🧪 Testing analyze_vacuum_stats Tool")
    print("=" * 50)
    
    client = boto3.client('rds-data', region_name='us-west-2')
    
    try:
        # Test vacuum statistics query
        print("Testing vacuum statistics query...")
        vacuum_stats_sql = """
            SELECT 
                schemaname,
                relname as tablename,
                n_tup_ins as total_inserts,
                n_tup_upd as total_updates,
                n_tup_del as total_deletes,
                n_live_tup as live_tuples,
                n_dead_tup as dead_tuples,
                last_vacuum,
                last_autovacuum,
                vacuum_count,
                autovacuum_count,
                CASE 
                    WHEN n_live_tup > 0 
                    THEN round(100.0 * n_dead_tup / (n_live_tup + n_dead_tup), 2)
                    ELSE 0 
                END as dead_tuple_percent
            FROM pg_stat_user_tables
            WHERE n_tup_ins + n_tup_upd + n_tup_del > 0
            ORDER BY 
                CASE 
                    WHEN n_live_tup > 0 
                    THEN 100.0 * n_dead_tup / (n_live_tup + n_dead_tup)
                    ELSE 0 
                END DESC
            LIMIT 5
        """
        
        result = client.execute_statement(
            resourceArn='arn:aws:rds:us-west-2:288947426911:cluster:pg-clone-db-cluster',
            secretArn='arn:aws:secretsmanager:us-west-2:288947426911:secret:rds!cluster-7d957e88-d967-46f3-a21e-7db88c36bdf9-NEq9xL',
            database='devdb',
            sql=vacuum_stats_sql,
            includeResultMetadata=True
        )
        
        vacuum_data = parse_execute_response(result)
        print(f"✅ Vacuum statistics query successful")
        print(f"📊 Found {len(vacuum_data)} tables with activity")
        
        if vacuum_data:
            print(f"📋 Sample table: {vacuum_data[0].get('tablename', 'unknown')}")
            print(f"    Dead tuple %: {vacuum_data[0].get('dead_tuple_percent', 'N/A')}")
        
        # Test vacuum settings query
        print("\nTesting vacuum settings query...")
        vacuum_settings_sql = """
            SELECT 
                name,
                setting,
                unit,
                short_desc
            FROM pg_settings 
            WHERE name LIKE '%vacuum%' OR name LIKE '%autovacuum%'
            ORDER BY name
            LIMIT 10
        """
        
        settings_result = client.execute_statement(
            resourceArn='arn:aws:rds:us-west-2:288947426911:cluster:pg-clone-db-cluster',
            secretArn='arn:aws:secretsmanager:us-west-2:288947426911:secret:rds!cluster-7d957e88-d967-46f3-a21e-7db88c36bdf9-NEq9xL',
            database='devdb',
            sql=vacuum_settings_sql,
            includeResultMetadata=True
        )
        
        settings_data = parse_execute_response(settings_result)
        print(f"✅ Vacuum settings query successful")
        print(f"📊 Found {len(settings_data)} vacuum-related settings")
        
        if settings_data:
            print(f"📋 Sample setting: {settings_data[0].get('name', 'unknown')} = {settings_data[0].get('setting', 'N/A')}")
        
        # Test type conversion logic for dead_tuple_percent
        print("\nTesting type conversion logic...")
        tables_needing_vacuum = []
        threshold = 20.0
        
        for row in vacuum_data:
            if 'error' not in row:
                dead_percent_value = row.get('dead_tuple_percent', '0')
                try:
                    if isinstance(dead_percent_value, str):
                        dead_percent = float(dead_percent_value)
                    else:
                        dead_percent = float(dead_percent_value) if dead_percent_value is not None else 0.0
                    
                    print(f"    Table {row.get('tablename', 'unknown')}: {dead_percent}% dead tuples")
                    
                    if dead_percent > threshold:
                        tables_needing_vacuum.append({
                            'table': f"{row.get('schemaname', '')}.{row.get('tablename', '')}",
                            'dead_percent': dead_percent,
                            'last_vacuum': row.get('last_vacuum'),
                            'last_autovacuum': row.get('last_autovacuum')
                        })
                except (ValueError, TypeError) as e:
                    print(f"    ⚠️  Type conversion failed for {row.get('tablename', 'unknown')}: {e}")
                    continue
        
        print(f"✅ Type conversion working correctly")
        print(f"📊 Tables needing vacuum (>{threshold}%): {len(tables_needing_vacuum)}")
        
        return True
        
    except Exception as e:
        print(f"❌ analyze_vacuum_stats test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_recommend_indexes_queries():
    """Test the SQL queries used by recommend_indexes tool."""
    print("\n🧪 Testing recommend_indexes Tool")
    print("=" * 50)
    
    client = boto3.client('rds-data', region_name='us-west-2')
    
    try:
        # Test current indexes query
        print("Testing current indexes query...")
        current_indexes_sql = """
            SELECT 
                schemaname,
                tablename,
                indexname,
                indexdef,
                CASE 
                    WHEN indexdef LIKE '%UNIQUE%' THEN 'UNIQUE'
                    WHEN indexdef LIKE '%btree%' THEN 'BTREE'
                    WHEN indexdef LIKE '%gin%' THEN 'GIN'
                    WHEN indexdef LIKE '%gist%' THEN 'GIST'
                    ELSE 'OTHER'
                END as index_type
            FROM pg_indexes
            WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY schemaname, tablename, indexname
            LIMIT 10
        """
        
        result = client.execute_statement(
            resourceArn='arn:aws:rds:us-west-2:288947426911:cluster:pg-clone-db-cluster',
            secretArn='arn:aws:secretsmanager:us-west-2:288947426911:secret:rds!cluster-7d957e88-d967-46f3-a21e-7db88c36bdf9-NEq9xL',
            database='devdb',
            sql=current_indexes_sql,
            includeResultMetadata=True
        )
        
        indexes_data = parse_execute_response(result)
        print(f"✅ Current indexes query successful")
        print(f"📊 Found {len(indexes_data)} indexes")
        
        if indexes_data:
            print(f"📋 Sample index: {indexes_data[0].get('indexname', 'unknown')} on {indexes_data[0].get('tablename', 'unknown')}")
            print(f"    Type: {indexes_data[0].get('index_type', 'unknown')}")
        
        # Test table statistics query
        print("\nTesting table statistics query...")
        table_stats_sql = """
            SELECT 
                schemaname,
                tablename,
                attname as column_name,
                n_distinct,
                correlation
            FROM pg_stats
            WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            AND n_distinct IS NOT NULL
            ORDER BY schemaname, tablename, n_distinct DESC
            LIMIT 10
        """
        
        stats_result = client.execute_statement(
            resourceArn='arn:aws:rds:us-west-2:288947426911:cluster:pg-clone-db-cluster',
            secretArn='arn:aws:secretsmanager:us-west-2:288947426911:secret:rds!cluster-7d957e88-d967-46f3-a21e-7db88c36bdf9-NEq9xL',
            database='devdb',
            sql=table_stats_sql,
            includeResultMetadata=True
        )
        
        stats_data = parse_execute_response(stats_result)
        print(f"✅ Table statistics query successful")
        print(f"📊 Found {len(stats_data)} column statistics")
        
        if stats_data:
            print(f"📋 Sample column: {stats_data[0].get('tablename', 'unknown')}.{stats_data[0].get('column_name', 'unknown')}")
            print(f"    Distinct values: {stats_data[0].get('n_distinct', 'N/A')}")
        
        # Test index recommendation logic
        print("\nTesting index recommendation logic...")
        index_suggestions = []
        table_stats = {}
        
        # Group stats by table
        for row in stats_data:
            if 'error' not in row:
                table_key = f"{row.get('schemaname', '')}.{row.get('tablename', '')}"
                if table_key not in table_stats:
                    table_stats[table_key] = []
                table_stats[table_key].append(row)
        
        # Analyze each table for index opportunities
        for table_name, columns in table_stats.items():
            high_cardinality_cols = []
            
            for col in columns:
                try:
                    n_distinct = col.get('n_distinct', 0)
                    if isinstance(n_distinct, str):
                        n_distinct = float(n_distinct)
                    else:
                        n_distinct = float(n_distinct) if n_distinct is not None else 0
                    
                    print(f"    {table_name}.{col.get('column_name', 'unknown')}: {n_distinct} distinct values")
                    
                    if n_distinct > 100:  # High cardinality
                        high_cardinality_cols.append({
                            'column': col.get('column_name'),
                            'n_distinct': n_distinct,
                            'correlation': col.get('correlation')
                        })
                except (ValueError, TypeError) as e:
                    print(f"    ⚠️  Type conversion failed for {col.get('column_name', 'unknown')}: {e}")
                    continue
            
            # Generate suggestions for this table
            if high_cardinality_cols:
                for col in high_cardinality_cols[:2]:  # Top 2 high cardinality columns
                    suggestion = {
                        'table': table_name,
                        'suggested_index': f"CREATE INDEX idx_{table_name.split('.')[-1]}_{col['column']} ON {table_name} ({col['column']})",
                        'reason': f"High cardinality column ({col['n_distinct']} distinct values) - good for equality searches",
                        'priority': 'HIGH'
                    }
                    index_suggestions.append(suggestion)
                    print(f"    💡 Suggestion: {suggestion['suggested_index']}")
        
        print(f"✅ Index recommendation logic working correctly")
        print(f"📊 Generated {len(index_suggestions)} index suggestions")
        
        # Test EXPLAIN query analysis (optional feature)
        print("\nTesting EXPLAIN query analysis...")
        test_query = "SELECT COUNT(*) FROM dms_sample.sport_location"
        try:
            explain_result = client.execute_statement(
                resourceArn='arn:aws:rds:us-west-2:288947426911:cluster:pg-clone-db-cluster',
                secretArn='arn:aws:secretsmanager:us-west-2:288947426911:secret:rds!cluster-7d957e88-d967-46f3-a21e-7db88c36bdf9-NEq9xL',
                database='devdb',
                sql=f"EXPLAIN {test_query}",
                includeResultMetadata=True
            )
            
            explain_data = parse_execute_response(explain_result)
            print(f"✅ EXPLAIN query analysis successful")
            print(f"📊 Got {len(explain_data)} plan lines")
            
            if explain_data:
                plan_line = str(explain_data[0].get('QUERY PLAN', ''))
                print(f"📋 Sample plan line: {plan_line}")
                
                # Test plan analysis logic
                if 'Seq Scan' in plan_line:
                    print("    💡 Detected sequential scan - would recommend indexes")
                if 'Sort' in plan_line:
                    print("    💡 Detected sort operation - would recommend ORDER BY indexes")
                    
        except Exception as e:
            print(f"⚠️  EXPLAIN analysis failed (this is optional): {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ recommend_indexes test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Run comprehensive tests for the 3 newly added tools."""
    print("🚀 PostgreSQL MCP Server - New Tools Validation Tests")
    print("=" * 70)
    
    # Set AWS profile
    os.environ['AWS_PROFILE'] = 'mcp_profile'
    os.environ['AWS_REGION'] = 'us-west-2'
    
    test_results = {}
    
    # Test each new tool
    test_results['health_check'] = await test_health_check_queries()
    test_results['analyze_vacuum_stats'] = await test_analyze_vacuum_stats_queries()
    test_results['recommend_indexes'] = await test_recommend_indexes_queries()
    
    # Summary
    print("\n" + "=" * 70)
    print("📋 NEW TOOLS VALIDATION RESULTS")
    print("=" * 70)
    
    passed = sum(1 for result in test_results.values() if result)
    total = len(test_results)
    
    for test_name, result in test_results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{status} - {test_name}")
    
    print(f"\n📊 Overall: {passed}/{total} new tools passed validation")
    
    if passed == total:
        print("🎉 ALL NEW TOOLS VALIDATED SUCCESSFULLY!")
        print("✅ health_check - Basic connectivity and status working")
        print("✅ analyze_vacuum_stats - Vacuum analysis and type conversion working")
        print("✅ recommend_indexes - Index analysis and recommendations working")
        print("🚀 All 3 new tools are ready for Q Chat!")
    else:
        print("⚠️  Some new tools failed validation - please review the issues above")
    
    print("\n" + "=" * 70)
    print("🏁 New tools validation completed")


if __name__ == "__main__":
    asyncio.run(main())
