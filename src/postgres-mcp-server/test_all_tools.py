#!/usr/bin/env python3
"""
Comprehensive test script for all PostgreSQL MCP Server tools.
This script tests each tool individually to ensure they work correctly.
"""

import asyncio
import json
import sys
import os

# Add the package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'awslabs'))

from awslabs.postgres_mcp_server.server import (
    DBConnectionSingleton, 
    run_query, 
    get_table_schema,
    health_check,
    analyze_database_structure,
    show_postgresql_settings,
    identify_slow_queries,
    analyze_table_fragmentation,
    analyze_query_performance
)


class TestContext:
    """Mock context for testing tools."""
    def __init__(self):
        self.errors = []
    
    async def error(self, message):
        self.errors.append(message)
        print(f"❌ Error: {message}")


async def test_tool(tool_name, tool_func, *args, **kwargs):
    """Test a single tool and return results."""
    print(f"\n🧪 Testing {tool_name}...")
    print("=" * 50)
    
    try:
        ctx = TestContext()
        result = await tool_func(ctx, *args, **kwargs)
        
        if ctx.errors:
            print(f"❌ {tool_name} failed with errors:")
            for error in ctx.errors:
                print(f"   - {error}")
            return False
        
        # Check if result is valid
        if isinstance(result, str):
            try:
                # Try to parse JSON results
                if result.startswith('{') or result.startswith('['):
                    parsed = json.loads(result)
                    print(f"✅ {tool_name} returned valid JSON")
                    if isinstance(parsed, dict) and parsed.get('status') == 'success':
                        print(f"✅ {tool_name} completed successfully")
                        if 'data' in parsed:
                            print(f"📊 Data keys: {list(parsed['data'].keys())}")
                        if 'recommendations' in parsed:
                            print(f"💡 Recommendations: {len(parsed['recommendations'])} items")
                    elif isinstance(parsed, dict) and parsed.get('status') == 'error':
                        print(f"⚠️  {tool_name} returned error status: {parsed.get('error', 'Unknown error')}")
                        return False
                else:
                    print(f"✅ {tool_name} returned text result (length: {len(result)})")
            except json.JSONDecodeError:
                print(f"✅ {tool_name} returned non-JSON text result")
        elif isinstance(result, (list, dict)):
            print(f"✅ {tool_name} returned structured data")
            if isinstance(result, list):
                print(f"📊 Result list length: {len(result)}")
            elif isinstance(result, dict):
                print(f"📊 Result keys: {list(result.keys())}")
        else:
            print(f"✅ {tool_name} returned result of type: {type(result)}")
        
        return True
        
    except Exception as e:
        print(f"❌ {tool_name} failed with exception: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Run comprehensive tests for all tools."""
    print("🚀 Starting Comprehensive PostgreSQL MCP Server Tool Tests")
    print("=" * 60)
    
    # Initialize database connection
    print("🔧 Initializing database connection...")
    try:
        DBConnectionSingleton.initialize(
            'arn:aws:rds:us-west-2:288947426911:cluster:pg-clone-db-cluster',
            'arn:aws:secretsmanager:us-west-2:288947426911:secret:rds!cluster-7d957e88-d967-46f3-a21e-7db88c36bdf9-NEq9xL',
            'devdb',
            'us-west-2',
            True  # readonly
        )
        print("✅ Database connection initialized successfully")
    except Exception as e:
        print(f"❌ Failed to initialize database connection: {e}")
        return
    
    # Test results tracking
    test_results = {}
    
    # Test 1: Basic query execution
    test_results['run_query'] = await test_tool(
        "run_query", 
        run_query, 
        "SELECT version() as postgresql_version"
    )
    
    # Test 2: Health check
    test_results['health_check'] = await test_tool(
        "health_check", 
        health_check
    )
    
    # Test 3: Get table schema (using a system table that should exist)
    test_results['get_table_schema'] = await test_tool(
        "get_table_schema", 
        get_table_schema, 
        "pg_tables"
    )
    
    # Test 4: Database structure analysis
    test_results['analyze_database_structure'] = await test_tool(
        "analyze_database_structure", 
        analyze_database_structure,
        debug=False
    )
    
    # Test 5: PostgreSQL settings (with pattern)
    test_results['show_postgresql_settings'] = await test_tool(
        "show_postgresql_settings", 
        show_postgresql_settings,
        pattern="shared_buffers",
        debug=False
    )
    
    # Test 6: PostgreSQL settings (without pattern)
    test_results['show_postgresql_settings_all'] = await test_tool(
        "show_postgresql_settings (all)", 
        show_postgresql_settings,
        pattern=None,
        debug=False
    )
    
    # Test 7: Slow queries identification
    test_results['identify_slow_queries'] = await test_tool(
        "identify_slow_queries", 
        identify_slow_queries,
        min_execution_time=100.0,
        limit=10,
        debug=False
    )
    
    # Test 8: Table fragmentation analysis
    test_results['analyze_table_fragmentation'] = await test_tool(
        "analyze_table_fragmentation", 
        analyze_table_fragmentation,
        threshold=10.0,
        debug=False
    )
    
    # Test 9: Query performance analysis
    test_results['analyze_query_performance'] = await test_tool(
        "analyze_query_performance", 
        analyze_query_performance,
        query="SELECT COUNT(*) FROM information_schema.tables",
        debug=False
    )
    
    # Test 10: Complex query execution
    test_results['run_query_complex'] = await test_tool(
        "run_query (complex)", 
        run_query, 
        """
        SELECT 
            schemaname, 
            tablename, 
            attname, 
            n_distinct, 
            correlation 
        FROM pg_stats 
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog') 
        LIMIT 5
        """
    )
    
    # Summary Report
    print("\n" + "=" * 60)
    print("📋 COMPREHENSIVE TEST RESULTS SUMMARY")
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
    print(f"✅ Passed: {len(passed_tests)}/{len(test_results)} tests")
    print(f"❌ Failed: {len(failed_tests)}/{len(test_results)} tests")
    
    if failed_tests:
        print(f"\n⚠️  Failed tests that need attention:")
        for test in failed_tests:
            print(f"   - {test}")
    
    if len(passed_tests) == len(test_results):
        print(f"\n🎉 ALL TESTS PASSED! PostgreSQL MCP Server is ready for Q Chat!")
    else:
        print(f"\n⚠️  Some tests failed. Please review the issues above.")
    
    print("\n" + "=" * 60)
    print("🏁 Test execution completed")


if __name__ == "__main__":
    # Set AWS profile
    os.environ['AWS_PROFILE'] = 'mcp_profile'
    os.environ['AWS_REGION'] = 'us-west-2'
    
    asyncio.run(main())
