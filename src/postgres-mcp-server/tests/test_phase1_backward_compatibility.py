#!/usr/bin/env python3
"""
Test Phase 1: Backward Compatibility for Multi-Database Support

This test validates that:
1. Existing single-database usage continues to work unchanged
2. Optional database_id parameter works correctly
3. New database management tools function properly
4. No breaking changes to existing functionality
"""

import asyncio
import os
import sys
import time
sys.path.insert(0, '/Users/reachrk/Downloads/awslabs/aws-mcp-servers/src/postgres-mcp-server')

from awslabs.postgres_mcp_server.multi_database_manager import (
    get_multi_database_manager, 
    initialize_single_database_mode
)


async def test_backward_compatibility():
    """Test that existing single-database usage works unchanged."""
    print("🧪 Testing Backward Compatibility")
    print("=" * 50)
    
    try:
        # Initialize in single-database mode (existing approach)
        initialize_single_database_mode(
            connection_type="rds_data_api",
            resource_arn="arn:aws:rds:us-west-2:288947426911:cluster:pg-clone-db-cluster",
            secret_arn="arn:aws:secretsmanager:us-west-2:288947426911:secret:rds!cluster-7d957e88-d967-46f3-a21e-7db88c36bdf9-NEq9xL",
            database="devdb",
            region="us-west-2",
            readonly=True
        )
        
        print("✅ Single-database initialization successful")
        
        # Test that manager is properly initialized
        db_manager = get_multi_database_manager()
        assert db_manager.is_initialized(), "Manager should be initialized"
        assert db_manager.get_database_count() == 1, "Should have exactly 1 database"
        
        print("✅ Manager state validation successful")
        
        # Test getting connection without database_id (backward compatibility)
        connection = db_manager.get_connection()  # No database_id specified
        assert connection is not None, "Should get default connection"
        
        print("✅ Default connection retrieval successful")
        
        # Test getting connection with explicit database_id
        connection_explicit = db_manager.get_connection("default")
        assert connection_explicit is not None, "Should get connection by ID"
        
        print("✅ Explicit connection retrieval successful")
        
        # Test database listing
        databases = db_manager.list_databases()
        assert len(databases) == 1, "Should list exactly 1 database"
        assert databases[0]["id"] == "default", "Database ID should be 'default'"
        assert databases[0]["is_default"] == True, "Should be marked as default"
        
        print("✅ Database listing successful")
        print(f"   Database info: {databases[0]}")
        
        return True
        
    except Exception as e:
        print(f"❌ Backward compatibility test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


async def test_database_id_parameter():
    """Test that database_id parameter works correctly."""
    print("\n🧪 Testing Database ID Parameter")
    print("=" * 50)
    
    try:
        db_manager = get_multi_database_manager()
        
        # Test getting config without database_id
        config_default = db_manager.get_database_config()
        assert config_default.database_id == "default", "Should get default config"
        
        print("✅ Default config retrieval successful")
        
        # Test getting config with explicit database_id
        config_explicit = db_manager.get_database_config("default")
        assert config_explicit.database_id == "default", "Should get explicit config"
        assert config_explicit.database == "devdb", "Should have correct database name"
        
        print("✅ Explicit config retrieval successful")
        
        # Test error handling for non-existent database
        try:
            db_manager.get_connection("non_existent")
            assert False, "Should raise error for non-existent database"
        except ValueError as e:
            assert "not found" in str(e), "Should provide helpful error message"
            print("✅ Error handling for non-existent database successful")
        
        return True
        
    except Exception as e:
        print(f"❌ Database ID parameter test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


async def test_configuration_validation():
    """Test configuration validation and data integrity."""
    print("\n🧪 Testing Configuration Validation")
    print("=" * 50)
    
    try:
        db_manager = get_multi_database_manager()
        config = db_manager.get_database_config()
        
        # Validate configuration data
        assert config.connection_type == "rds_data_api", "Should have correct connection type"
        assert config.database == "devdb", "Should have correct database name"
        assert config.region == "us-west-2", "Should have correct region"
        assert config.readonly == True, "Should be readonly"
        assert config.resource_arn is not None, "Should have resource ARN"
        
        print("✅ Configuration validation successful")
        
        # Test configuration serialization
        config_dict = config.to_dict()
        assert "database_id" in config_dict, "Should include database_id in dict"
        assert "connection_type" in config_dict, "Should include connection_type in dict"
        assert "resource_arn" in config_dict, "Should include resource_arn in dict"
        
        print("✅ Configuration serialization successful")
        print(f"   Config keys: {list(config_dict.keys())}")
        
        return True
        
    except Exception as e:
        print(f"❌ Configuration validation test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


async def test_manager_state():
    """Test multi-database manager state management."""
    print("\n🧪 Testing Manager State")
    print("=" * 50)
    
    try:
        db_manager = get_multi_database_manager()
        
        # Test state queries
        assert db_manager.is_initialized() == True, "Should be initialized"
        assert db_manager.get_database_count() == 1, "Should have 1 database"
        assert db_manager.get_default_database_id() == "default", "Should have default database"
        
        print("✅ Manager state validation successful")
        
        # Test database listing structure
        databases = db_manager.list_databases()
        db_info = databases[0]
        
        required_fields = ["id", "database", "connection_type", "readonly", "is_default"]
        for field in required_fields:
            assert field in db_info, f"Database info should include {field}"
        
        print("✅ Database info structure validation successful")
        
        return True
        
    except Exception as e:
        print(f"❌ Manager state test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Run all Phase 1 backward compatibility tests."""
    print("🚀 PostgreSQL MCP Server - Phase 1 Backward Compatibility Tests")
    print("=" * 70)
    
    # Set AWS profile
    os.environ['AWS_PROFILE'] = 'mcp_profile'
    os.environ['AWS_REGION'] = 'us-west-2'
    
    test_results = []
    
    # Run tests
    test_results.append(await test_backward_compatibility())
    test_results.append(await test_database_id_parameter())
    test_results.append(await test_configuration_validation())
    test_results.append(await test_manager_state())
    
    # Summary
    print("\n" + "=" * 70)
    print("📋 PHASE 1 BACKWARD COMPATIBILITY TEST RESULTS")
    print("=" * 70)
    
    passed = sum(test_results)
    total = len(test_results)
    
    print(f"\n📊 OVERALL RESULTS:")
    print(f"✅ Passed: {passed}/{total} tests")
    print(f"❌ Failed: {total - passed}/{total} tests")
    
    if passed == total:
        print(f"\n🎉 ALL PHASE 1 TESTS PASSED!")
        print("✅ Backward compatibility maintained")
        print("✅ Optional database_id parameter working")
        print("✅ Multi-database foundation established")
        print("✅ No breaking changes detected")
    else:
        print(f"\n⚠️  SOME PHASE 1 TESTS FAILED")
        print("🔧 Review the implementation for issues")
    
    print("\n" + "=" * 70)
    print("🏁 Phase 1 test execution completed")
    
    return passed == total


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
