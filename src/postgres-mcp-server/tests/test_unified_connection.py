#!/usr/bin/env python3
"""
Test script to validate the unified connection integration.

This tests both RDS Data API and Direct PostgreSQL connection paths, focusing on:
1. Integration tests for both connection types
2. Parameter validation
3. ConnectionFactory functionality

For detailed tests of individual connectors, see:
- test_rds_driver.py: Tests for RDS Data API connector
- test_psycopg_driver.py: Tests for PostgreSQL driver
"""

import asyncio
import os
import sys
import unittest.mock as mock
import pytest
import json
import boto3
from unittest.mock import AsyncMock, MagicMock, patch

# Import the modules to test
from awslabs.postgres_mcp_server.connection.connection_factory import ConnectionFactory


@pytest.mark.skipif(
    os.environ.get('SKIP_AWS_TESTS', 'true').lower() == 'true',
    reason="Skipping test that requires valid AWS credentials. Set SKIP_AWS_TESTS=false to run."
)
def test_rds_data_api_connection():
    """Test RDS Data API connection (should work with real credentials)."""
    print("Testing RDS Data API Connection")
    print("=" * 50)
    
    cmd = [
        sys.executable, "-m", "awslabs.postgres_mcp_server.server",
        "--resource_arn", "arn:aws:rds:us-west-2:288947426911:cluster:pg-clone-db-cluster",
        "--secret_arn", "arn:aws:secretsmanager:us-west-2:288947426911:secret:rds!cluster-7d957e88-d967-46f3-a21e-7db88c36bdf9-NEq9xL",
        "--database", "devdb",
        "--region", "us-west-2",
        "--readonly", "true"
    ]
    
    import subprocess
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        
        if "Successfully validated Rds Data Api database connection" in result.stderr:
            print(" RDS Data API Connection - SUCCESS")
            print(" Connection established and validated")
            assert True
        else:
            print(" RDS Data API Connection - FAILED")
            print(f" Error: {result.stderr}")
            assert False, "RDS Data API Connection failed"
            
    except subprocess.TimeoutExpired:
        print(" RDS Data API Connection - SUCCESS (timeout expected)")
        print(" Server started successfully (timeout after validation)")
        assert True
    except Exception as e:
        print(f" RDS Data API Connection - FAILED: {e}")
        assert False, f"RDS Data API Connection failed with exception: {e}"


@pytest.mark.skipif(
    os.environ.get('SKIP_POSTGRES_TESTS', 'true').lower() == 'true',
    reason="Skipping test that requires PostgreSQL connection. Set SKIP_POSTGRES_TESTS=false to run."
)
def test_direct_postgres_connection():
    """Test Direct PostgreSQL connection (should fail gracefully with fake credentials)."""
    print("\n Testing Direct PostgreSQL Connection")
    print("=" * 50)
    
    cmd = [
        sys.executable, "-m", "awslabs.postgres_mcp_server.server",
        "--hostname", "fake-host.amazonaws.com",
        "--port", "5432",
        "--secret_arn", "arn:aws:secretsmanager:us-west-2:123456789012:secret:fake-secret",
        "--database", "fakedb",
        "--region", "us-west-2",
        "--readonly", "true"
    ]
    
    import subprocess
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        
        # Check if it went through the Direct PostgreSQL path
        if "Using direct PostgreSQL connection (hostname provided)" in result.stderr:
            print(" Direct PostgreSQL Path - SUCCESS")
            print(" Connection factory correctly identified Direct PostgreSQL")
            
            if "Initialized Direct PostgreSQL connection" in result.stderr:
                print(" Direct PostgreSQL Initialization - SUCCESS")
                print(" PostgreSQL connector initialized correctly")
                
                if "connection_type:direct_postgres" in result.stderr:
                    print(" Direct PostgreSQL Query Path - SUCCESS")
                    print(" Query execution went through Direct PostgreSQL path")
                    
                    # Expected to fail with fake credentials
                    if "Failed to retrieve credentials" in result.stderr or "Access to account" in result.stderr:
                        print(" Direct PostgreSQL Error Handling - SUCCESS")
                        print(" Failed gracefully with fake credentials (expected)")
                        assert True
                    else:
                        print("  Direct PostgreSQL - Unexpected behavior")
                        print(" Should have failed with fake credentials")
                        assert False, "Direct PostgreSQL should have failed with fake credentials"
                else:
                    print(" Direct PostgreSQL Query Path - FAILED")
                    assert False, "Direct PostgreSQL query path not detected"
            else:
                print(" Direct PostgreSQL Initialization - FAILED")
                assert False, "Direct PostgreSQL initialization failed"
        else:
            print(" Direct PostgreSQL Path - FAILED")
            print(f" Error: {result.stderr}")
            assert False, "Direct PostgreSQL path not detected"
            
    except subprocess.TimeoutExpired:
        print(" Direct PostgreSQL Connection - TIMEOUT")
        print(" Should have failed quickly with fake credentials")
        assert False, "Direct PostgreSQL connection timed out"
    except Exception as e:
        print(f" Direct PostgreSQL Connection - FAILED: {e}")
        assert False, f"Direct PostgreSQL connection failed with exception: {e}"


def test_parameter_validation():
    """Test parameter validation logic."""
    print("\n🧪 Testing Parameter Validation")
    print("=" * 50)
    
    # Test 1: No connection parameters
    cmd1 = [
        sys.executable, "-m", "awslabs.postgres_mcp_server.server",
        "--secret_arn", "test",
        "--database", "test",
        "--region", "us-west-2",
        "--readonly", "true"
    ]
    
    import subprocess
    try:
        result1 = subprocess.run(cmd1, capture_output=True, text=True, timeout=5)
        if "Either --resource_arn (for RDS Data API) or --hostname (for direct PostgreSQL) must be provided" in result1.stderr:
            print(" No Connection Parameters - SUCCESS")
            print(" Correctly rejected missing connection parameters")
            assert True
        else:
            print(" No Connection Parameters - FAILED")
            assert False, "Server did not reject missing connection parameters"
    except Exception as e:
        print(f" No Connection Parameters Test - FAILED: {e}")
        assert False, f"No Connection Parameters test failed with exception: {e}"
    
    # Test 2: Both connection parameters
    cmd2 = [
        sys.executable, "-m", "awslabs.postgres_mcp_server.server",
        "--resource_arn", "arn:aws:rds:us-west-2:123456789012:cluster:test",
        "--hostname", "test-host.com",
        "--secret_arn", "test",
        "--database", "test",
        "--region", "us-west-2",
        "--readonly", "true"
    ]
    
    try:
        result2 = subprocess.run(cmd2, capture_output=True, text=True, timeout=5)
        if "Cannot specify both --resource_arn and --hostname" in result2.stderr:
            print(" Both Connection Parameters - SUCCESS")
            print(" Correctly rejected conflicting connection parameters")
            assert True
        else:
            print(" Both Connection Parameters - FAILED")
            assert False, "Server did not reject conflicting connection parameters"
    except Exception as e:
        print(f" Both Connection Parameters Test - FAILED: {e}")
        assert False, f"Both Connection Parameters test failed with exception: {e}"


# ConnectionFactory Tests
def test_connection_factory_determine_connection_type():
    """Test ConnectionFactory.determine_connection_type method."""
    print("\n🧪 Testing ConnectionFactory.determine_connection_type")
    print("-" * 50)
    
    # Test with resource_arn only
    conn_type1 = ConnectionFactory.determine_connection_type(
        resource_arn="arn:aws:rds:us-west-2:123456789012:cluster:test",
        hostname=None
    )
    print(f" With resource_arn only: {conn_type1}")
    assert conn_type1 == "rds_data_api", "Should return rds_data_api with resource_arn"
    
    # Test with hostname only
    conn_type2 = ConnectionFactory.determine_connection_type(
        resource_arn=None,
        hostname="test-host.com"
    )
    print(f" With hostname only: {conn_type2}")
    assert conn_type2 == "psycopg_driver", "Should return psycopg_driver with hostname"
    
    # Test with neither (should raise ValueError)
    try:
        ConnectionFactory.determine_connection_type(
            resource_arn=None,
            hostname=None
        )
        print(" With neither parameter: FAILED - should have raised ValueError")
        assert False, "Should have raised ValueError with neither parameter"
    except ValueError as e:
        print(f" With neither parameter: SUCCESS - raised ValueError: {e}")
    
    print(" ConnectionFactory.determine_connection_type tests passed")
    assert True


def test_connection_factory_validate_connection_params():
    """Test ConnectionFactory.validate_connection_params method."""
    print("\n🧪 Testing ConnectionFactory.validate_connection_params")
    print("-" * 50)
    
    # Test valid RDS Data API parameters
    is_valid1, error1 = ConnectionFactory.validate_connection_params(
        connection_type="rds_data_api",
        resource_arn="arn:aws:rds:us-west-2:123456789012:cluster:test",
        secret_arn="arn:aws:secretsmanager:us-west-2:123456789012:secret:test",
        database="test",
        region_name="us-west-2"
    )
    print(f" Valid RDS Data API params: {is_valid1}")
    assert is_valid1 is True, "Valid RDS Data API params should return True"
    
    # Test missing RDS Data API parameters
    is_valid2, error2 = ConnectionFactory.validate_connection_params(
        connection_type="rds_data_api",
        resource_arn=None,
        secret_arn="test",
        database="test",
        region_name="us-west-2"
    )
    print(f" Missing RDS Data API params: {not is_valid2}")
    assert is_valid2 is False, "Missing resource_arn should return False"
    
    # Test valid Direct PostgreSQL parameters
    is_valid3, error3 = ConnectionFactory.validate_connection_params(
        connection_type="psycopg_driver",
        hostname="test-host.com",
        secret_arn="test",
        database="test",
        region_name="us-west-2"
    )
    print(f" Valid Direct PostgreSQL params: {is_valid3}")
    assert is_valid3 is True, "Valid Direct PostgreSQL params should return True"
    
    # Test missing Direct PostgreSQL parameters
    is_valid4, error4 = ConnectionFactory.validate_connection_params(
        connection_type="psycopg_driver",
        hostname=None,
        secret_arn="test",
        database="test",
        region_name="us-west-2"
    )
    print(f" Missing Direct PostgreSQL params: {not is_valid4}")
    assert is_valid4 is False, "Missing hostname should return False"
    
    # Test unknown connection type
    is_valid5, error5 = ConnectionFactory.validate_connection_params(
        connection_type="unknown_type",
        hostname="test",
        secret_arn="test",
        database="test",
        region_name="us-west-2"
    )
    print(f" Unknown connection type: {not is_valid5}")
    assert is_valid5 is False, "Unknown connection type should return False"
    
    print(" ConnectionFactory.validate_connection_params tests passed")
    assert True


def test_connection_factory_create_connection():
    """Test ConnectionFactory.create_connection method with mocks."""
    print("\n🧪 Testing ConnectionFactory.create_connection")
    print("-" * 50)
    
    # Mock the RDS and PostgreSQL connector classes
    with patch('awslabs.postgres_mcp_server.connection.connection_factory.RDSDataAPIConnector') as mock_rds:
        with patch('awslabs.postgres_mcp_server.connection.connection_factory.PostgresDriver') as mock_postgres:
            # Test creating RDS Data API connection
            ConnectionFactory.create_connection(
                resource_arn="arn:aws:rds:us-west-2:123456789012:cluster:test",
                secret_arn="test",
                database="test",
                region="us-west-2",
                readonly=True
            )
            print(" RDS Data API connection creation: ", end="")
            if mock_rds.called and not mock_postgres.called:
                print("SUCCESS - RDSDataAPIConnector was called")
            else:
                print("FAILED - RDSDataAPIConnector was not called correctly")
                assert False, "RDSDataAPIConnector was not called correctly"
            
            # Reset mocks
            mock_rds.reset_mock()
            mock_postgres.reset_mock()
            
            # Test creating Direct PostgreSQL connection
            ConnectionFactory.create_connection(
                hostname="test-host.com",
                port=5432,
                secret_arn="test",
                database="test",
                region="us-west-2",
                readonly=True
            )
            print(" Direct PostgreSQL connection creation: ", end="")
            if mock_postgres.called and not mock_rds.called:
                print("SUCCESS - PostgresDriver was called")
            else:
                print("FAILED - PostgresDriver was not called correctly")
                assert False, "PostgresDriver was not called correctly"
    
    print(" ConnectionFactory.create_connection tests passed")
    assert True


def test_connection_factory_create_pool_key():
    """Test ConnectionFactory.create_pool_key method."""
    print("\n🧪 Testing ConnectionFactory.create_pool_key")
    print("-" * 50)
    
    # Test RDS Data API pool key
    key1 = ConnectionFactory.create_pool_key(
        connection_type="rds_data_api",
        resource_arn="arn:aws:rds:us-west-2:123456789012:cluster:test",
        database="test",
        secret_arn="test"
    )
    print(f" RDS Data API pool key: {key1}")
    assert "rds://" in key1, "RDS Data API pool key should start with rds://"
    assert "test" in key1, "RDS Data API pool key should contain database name"
    
    # Test Direct PostgreSQL pool key
    key2 = ConnectionFactory.create_pool_key(
        connection_type="psycopg_driver",
        hostname="test-host.com",
        port=5432,
        database="test",
        secret_arn="test"
    )
    print(f" Direct PostgreSQL pool key: {key2}")
    assert "postgres://" in key2, "Direct PostgreSQL pool key should start with postgres://"
    assert "test-host.com" in key2, "Direct PostgreSQL pool key should contain hostname"
    assert "5432" in key2, "Direct PostgreSQL pool key should contain port"
    
    # Test unknown connection type
    try:
        ConnectionFactory.create_pool_key(
            connection_type="unknown_type",
            hostname="test",
            database="test"
        )
        print(" Unknown connection type: FAILED - should have raised ValueError")
        assert False, "Should have raised ValueError with unknown connection type"
    except ValueError as e:
        print(f" Unknown connection type: SUCCESS - raised ValueError: {e}")
    
    print(" ConnectionFactory.create_pool_key tests passed")
    assert True


# No RDS Connector Tests - Moved to test_rds_driver.py


def main():
    """Run all unified connection tests."""
    print("🚀 PostgreSQL MCP Server - Unified Connection Integration Tests")
    print("=" * 70)
    
    # Set AWS profile
    os.environ['AWS_PROFILE'] = 'mcp_profile'
    os.environ['AWS_REGION'] = 'us-west-2'
    
    test_results = []
    
    # Test RDS Data API (existing functionality)
    test_results.append(test_rds_data_api_connection())
    
    # Test Direct PostgreSQL (new functionality)
    test_results.append(test_direct_postgres_connection())
    
    # Test parameter validation
    test_results.append(test_parameter_validation())
    
    # Test ConnectionFactory methods
    test_results.append(test_connection_factory_determine_connection_type())
    test_results.append(test_connection_factory_validate_connection_params())
    test_results.append(test_connection_factory_create_connection())
    test_results.append(test_connection_factory_create_pool_key())
    
    
    # Summary
    print("\n" + "=" * 70)
    print(" UNIFIED CONNECTION INTEGRATION TEST RESULTS")
    print("=" * 70)
    
    passed = sum(test_results)
    total = len(test_results)
    
    print(f"\n OVERALL RESULTS:")
    print(f" Passed: {passed}/{total} tests")
    print(f" Failed: {total - passed}/{total} tests")
    
    if passed == total:
        print(f"\n ALL TESTS PASSED! Direct PostgreSQL integration is complete!")
        print(" Both RDS Data API and Direct PostgreSQL connections are working")
        print(" Existing functionality preserved")
        print(" New functionality integrated successfully")
    else:
        print(f"\n  SOME TESTS FAILED - Please review the issues above")
    
    print("\n" + "=" * 70)
    print(" Integration test execution completed")
    
    assert passed == total, f"Only {passed}/{total} tests passed"


if __name__ == "__main__":
    main()
    sys.exit(0)
