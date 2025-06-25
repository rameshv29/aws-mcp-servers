#!/usr/bin/env python3
"""
Test script for PostgreSQL driver functionality.

This tests the PostgreSQL driver implementation, including connection management,
query execution, parameter handling, and error handling.
"""

import asyncio
import os
import sys
import json
import unittest.mock as mock
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

# Use relative path instead of hard-coded path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the modules to test
from awslabs.postgres_mcp_server.connection.connection_factory import ConnectionFactory
from awslabs.postgres_mcp_server.connection.postgres_driver import PostgresDriver


# PostgreSQL Driver Tests
def test_postgres_driver_init():
    """Test PostgresDriver initialization."""
    print("\n🧪 Testing PostgresDriver initialization")
    print("-" * 50)
    
    # Create a driver
    driver = PostgresDriver(
        hostname="test-host.com",
        database="test-db",
        secret_arn="test-secret",
        region_name="us-west-2",
        port=5432,
        readonly=True
    )
    
    # Check that the driver was initialized correctly
    print(" Checking driver attributes:")
    assert driver.hostname == "test-host.com", "hostname not set correctly"
    assert driver.database == "test-db", "database not set correctly"
    assert driver.secret_arn == "test-secret", "secret_arn not set correctly"
    assert driver.region_name == "us-west-2", "region_name not set correctly"
    assert driver.port == 5432, "port not set correctly"
    assert driver.readonly is True, "readonly not set correctly"
    assert driver._connection is None, "Should start with no connection"
    assert driver._credentials_cached is False, "Should start with no cached credentials"
    
    print(" PostgresDriver initialization tests passed")


@pytest.mark.asyncio
async def test_postgres_driver_get_credentials():
    """Test PostgresDriver _get_credentials method with mocks."""
    print("\n🧪 Testing PostgresDriver._get_credentials")
    print("-" * 50)
    
    # Create a mock boto3 client
    with patch('boto3.client') as mock_boto3_client:
        # Set up the mock to return a successful response
        mock_secrets_client = MagicMock()
        mock_boto3_client.return_value = mock_secrets_client
        mock_secrets_client.get_secret_value.return_value = {
            'SecretString': '{"username": "test_user", "password": "test_password"}'
        }
        
        # Create a driver
        driver = PostgresDriver(
            hostname="test-host.com",
            database="test-db",
            secret_arn="test-secret",
            region_name="us-west-2",
            port=5432,
            readonly=True
        )
        
        # Call the _get_credentials method
        credentials = await driver._get_credentials()
        
        # Check that the credentials were retrieved correctly
        print(" Credentials retrieved:", credentials)
        assert credentials.get('username') == "test_user", "Username not retrieved correctly"
        assert credentials.get('password') == "test_password", "Password not retrieved correctly"
        assert driver._credentials_cached is True, "Credentials should be cached"
        
        # Check that get_secret_value was called with the correct parameters
        mock_secrets_client.get_secret_value.assert_called_once_with(
            SecretId="test-secret"
        )
        
        # Call _get_credentials again to test caching
        mock_secrets_client.get_secret_value.reset_mock()
        credentials2 = await driver._get_credentials()
        
        # Check that get_secret_value was not called again
        mock_secrets_client.get_secret_value.assert_not_called()
        
        print(" PostgresDriver._get_credentials tests passed")


@pytest.mark.asyncio
async def test_postgres_driver_connect():
    """Test PostgresDriver connect method with mocks."""
    print("\n🧪 Testing PostgresDriver.connect")
    print("-" * 50)
    
    # Create mocks for boto3 and psycopg2
    with patch('boto3.client') as mock_boto3_client:
        with patch('psycopg2.connect') as mock_psycopg_connect:
            # Set up the mock to return a successful response
            mock_secrets_client = MagicMock()
            mock_boto3_client.return_value = mock_secrets_client
            mock_secrets_client.get_secret_value.return_value = {
                'SecretString': '{"username": "test_user", "password": "test_password"}'
            }
            
            # Set up the mock connection
            mock_connection = MagicMock()
            mock_psycopg_connect.return_value = mock_connection
            
            # Create a driver
            driver = PostgresDriver(
                hostname="test-host.com",
                database="test-db",
                secret_arn="test-secret",
                region_name="us-west-2",
                port=5432,
                readonly=True
            )
            
            # Call the connect method
            result = await driver.connect()
            
            # Check that the connection was successful
            print(" Connection result:", result)
            assert result is True, "connect() should return True on success"
            assert driver._connection is not None, "Connection should be set"
            assert driver._connection_validated is True, "Connection should be validated"
            
            # Check that psycopg2.connect was called with the correct parameters
            mock_psycopg_connect.assert_called_once()
            call_args = mock_psycopg_connect.call_args[1]
            assert call_args['host'] == "test-host.com"
            assert call_args['port'] == 5432
            assert call_args['database'] == "test-db"
            assert call_args['user'] == "test_user"
            assert call_args['password'] == "test_password"
            
            # Check that autocommit was set for readonly mode
            assert mock_connection.autocommit is True, "autocommit should be True for readonly mode"
            
            print(" PostgresDriver.connect tests passed")


@pytest.mark.asyncio
async def test_postgres_driver_connect_failure():
    """Test PostgresDriver connect method with failure."""
    print("\n🧪 Testing PostgresDriver.connect failure")
    print("-" * 50)
    
    # Create mocks for boto3 and psycopg2
    with patch('boto3.client') as mock_boto3_client:
        with patch('psycopg2.connect') as mock_psycopg_connect:
            # Set up the mock to return a successful response for credentials
            mock_secrets_client = MagicMock()
            mock_boto3_client.return_value = mock_secrets_client
            mock_secrets_client.get_secret_value.return_value = {
                'SecretString': '{"username": "test_user", "password": "test_password"}'
            }
            
            # Set up the mock connection to fail
            mock_psycopg_connect.side_effect = Exception("Connection failed")
            
            # Create a driver
            driver = PostgresDriver(
                hostname="test-host.com",
                database="test-db",
                secret_arn="test-secret",
                region_name="us-west-2",
                port=5432,
                readonly=True
            )
            
            # Call the connect method
            result = await driver.connect()
            
            # Check that the connection failed
            print(" Connection result:", result)
            assert result is False, "connect() should return False on failure"
            assert driver._connection is None, "Connection should be None"
            assert driver._connection_validated is False, "Connection should not be validated"
            
            print(" PostgresDriver.connect failure tests passed")


@pytest.mark.asyncio
async def test_postgres_driver_test_connection_parameters():
    """Test PostgresDriver test_connection_parameters method."""
    print("\n🧪 Testing PostgresDriver.test_connection_parameters")
    print("-" * 50)
    
    # Create mocks for boto3
    with patch('boto3.client') as mock_boto3_client:
        # Set up the mock to return a successful response
        mock_secrets_client = MagicMock()
        mock_boto3_client.return_value = mock_secrets_client
        mock_secrets_client.get_secret_value.return_value = {
            'SecretString': '{"username": "test_user", "password": "test_password"}'
        }
        
        # Create a driver
        driver = PostgresDriver(
            hostname="test-host.com",
            database="test-db",
            secret_arn="test-secret",
            region_name="us-west-2",
            port=5432,
            readonly=True
        )
        
        # Call the test_connection_parameters method
        result = await driver.test_connection_parameters()
        
        # Check that the test was successful
        print(" Test result:", result)
        assert result is True, "test_connection_parameters() should return True on success"
        
        # Check that get_secret_value was called with the correct parameters
        mock_secrets_client.get_secret_value.assert_called_once_with(
            SecretId="test-secret"
        )
        
        print(" PostgresDriver.test_connection_parameters tests passed")


@pytest.mark.asyncio
async def test_postgres_driver_execute_query():
    """Test PostgresDriver execute_query method with mocks."""
    print("\n🧪 Testing PostgresDriver.execute_query")
    print("-" * 50)
    
    # Create mocks for psycopg2
    with patch('psycopg2.extras.RealDictCursor') as mock_dict_cursor:
        # Set up the mock cursor
        mock_cursor = MagicMock()
        mock_dict_cursor.return_value = mock_cursor
        mock_cursor.__enter__.return_value = mock_cursor
        
        # Set up the cursor description and fetchall with proper name attributes
        col1 = MagicMock()
        col1.name = 'id'  # String instead of MagicMock
        col1.type_code = 23
        col2 = MagicMock()
        col2.name = 'name'
        col2.type_code = 25
        mock_cursor.description = [col1, col2]
        mock_cursor.fetchall.return_value = [
            {'id': 1, 'name': 'test'},
            {'id': 2, 'name': 'test2'}
        ]
        
        # Create a driver with a mock connection
        driver = PostgresDriver(
            hostname="test-host.com",
            database="test-db",
            secret_arn="test-secret",
            region_name="us-west-2",
            port=5432,
            readonly=True
        )
        driver._connection = MagicMock()
        driver._connection.cursor.return_value = mock_cursor
        driver._connected = True
        
        # Reset the mock before calling execute_query
        mock_cursor.execute.reset_mock()
        
        # Call the execute_query method
        result = await driver.execute_query("SELECT * FROM test")
        
        # Check that the query was successful
        print(" Query result has expected structure:", 'columnMetadata' in result and 'records' in result)
        assert 'columnMetadata' in result, "Result should have columnMetadata"
        assert 'records' in result, "Result should have records"
        assert len(result['records']) == 2, "Result should have 2 records"
        
        # Check that cursor.execute was called with the correct parameters
        mock_cursor.execute.assert_called_with("SELECT * FROM test", None)
        
        print(" PostgresDriver.execute_query tests passed")


@pytest.mark.asyncio
async def test_postgres_driver_execute_query_with_parameters():
    """Test PostgresDriver execute_query method with parameters."""
    print("\n🧪 Testing PostgresDriver.execute_query with parameters")
    print("-" * 50)
    
    # Create mocks for psycopg2
    with patch('psycopg2.extras.RealDictCursor') as mock_dict_cursor:
        # Set up the mock cursor
        mock_cursor = MagicMock()
        mock_dict_cursor.return_value = mock_cursor
        mock_cursor.__enter__.return_value = mock_cursor
        
        # Set up the cursor description and fetchall with proper name attributes
        col1 = MagicMock()
        col1.name = 'id'  # String instead of MagicMock
        col1.type_code = 23
        col2 = MagicMock()
        col2.name = 'name'
        col2.type_code = 25
        mock_cursor.description = [col1, col2]
        mock_cursor.fetchall.return_value = [
            {'id': 1, 'name': 'test'}
        ]
        
        # Create a driver with a mock connection
        driver = PostgresDriver(
            hostname="test-host.com",
            database="test-db",
            secret_arn="test-secret",
            region_name="us-west-2",
            port=5432,
            readonly=True
        )
        driver._connection = MagicMock()
        driver._connection.cursor.return_value = mock_cursor
        driver._connected = True
        
        # Reset the mock before calling execute_query
        mock_cursor.execute.reset_mock()
        
        # Call the execute_query method with parameters
        params = [
            {'name': 'id', 'value': {'longValue': 1}},
            {'name': 'name', 'value': {'stringValue': 'test'}}
        ]
        result = await driver.execute_query("SELECT * FROM test WHERE id = %(id)s AND name = %(name)s", params)
        
        # Check that the query was successful
        print(" Query result has expected structure:", 'columnMetadata' in result and 'records' in result)
        assert 'columnMetadata' in result, "Result should have columnMetadata"
        assert 'records' in result, "Result should have records"
        
        # Check that cursor.execute was called with the correct parameters
        mock_cursor.execute.assert_called_with("SELECT * FROM test WHERE id = %(id)s AND name = %(name)s", {'id': 1, 'name': 'test'})
        
        print(" PostgresDriver.execute_query with parameters tests passed")


@pytest.mark.asyncio
async def test_postgres_driver_disconnect():
    """Test PostgresDriver disconnect method."""
    print("\n🧪 Testing PostgresDriver.disconnect")
    print("-" * 50)
    
    # Create a driver with a mock connection
    driver = PostgresDriver(
        hostname="test-host.com",
        database="test-db",
        secret_arn="test-secret",
        region_name="us-west-2",
        port=5432,
        readonly=True
    )
    
    # Store a reference to the connection before disconnecting
    connection = MagicMock()
    driver._connection = connection
    
    # Call the disconnect method
    result = await driver.disconnect()
    
    # Check that the disconnect was successful
    print(" Disconnect result:", result)
    
    # Check that the connection was closed (using the saved reference)
    connection.close.assert_called_once()
    
    # Check that the connection is now None
    assert driver._connection is None, "Connection should be None after disconnect"
    
    print(" PostgresDriver.disconnect tests passed")


@pytest.mark.skip(reason="Requires actual database connection")
async def test_direct_postgres_query():
    """Test Direct PostgreSQL query execution directly."""
    print("🧪 Testing Direct PostgreSQL Query Execution")
    print("=" * 50)
    
    try:
        # Create connection using ConnectionFactory
        db_connection = ConnectionFactory.create_connection(
            hostname="pg-clone-db-cluster.cluster-cjvgkx7iusm0.us-west-2.rds.amazonaws.com",
            port=5432,
            secret_arn="arn:aws:secretsmanager:us-west-2:288947426911:secret:rds!cluster-7d957e88-d967-46f3-a21e-7db88c36bdf9-NEq9xL",
            database="devdb",
            region="us-west-2",
            readonly=True
        )
        
        print("✅ Connection initialized")
        
        # Connect to the database
        await db_connection.connect()
        print(f"✅ Connection established successfully")
        
        # Test simple query
        print("\n🔍 Testing simple query: SELECT 1")
        result = await db_connection.execute_query("SELECT 1 as test")
        print(f"✅ Query result: {result}")
        
        # Test version query
        print("\n🔍 Testing version query")
        result = await db_connection.execute_query("SELECT version() as postgresql_version")
        print(f"✅ Version result: {result}")
        
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        pytest.fail(f"Direct PostgreSQL query test failed: {str(e)}")


async def main():
    """Run all PostgreSQL driver tests."""
    print(" PostgreSQL Driver Tests")
    print("=" * 70)
    
    # Set AWS profile
    os.environ['AWS_PROFILE'] = 'mcp_profile'
    os.environ['AWS_REGION'] = 'us-west-2'
    
    test_results = []
    
    # Run synchronous tests
    test_results.append(test_postgres_driver_init())
    
    # Run async tests using pytest
    try:
        import pytest
        pytest.main(["-xvs", __file__, "-k", "test_postgres_driver_get_credentials or test_postgres_driver_connect or test_postgres_driver_connect_failure or test_postgres_driver_test_connection_parameters or test_postgres_driver_execute_query or test_postgres_driver_execute_query_with_parameters or test_postgres_driver_disconnect"])
    except Exception as e:
        print(f"Error running async tests: {e}")
    
    # Run the direct query test
    test_results.append(await test_direct_postgres_query())
    
    # Summary
    print("\n" + "=" * 70)
    print(" POSTGRESQL DRIVER TEST RESULTS")
    print("=" * 70)
    
    passed = sum(1 for result in test_results if result)
    total = len(test_results)
    
    print(f"\n OVERALL RESULTS:")
    print(f" Passed: {passed}/{total} tests")
    print(f" Failed: {total - passed}/{total} tests")
    
    if passed == total:
        print(f"\n ALL TESTS PASSED! PostgreSQL driver is working correctly!")
    else:
        print(f"\n SOME TESTS FAILED - Please review the issues above")
    
    print("\n" + "=" * 70)
    print(" Test execution completed")
    
    return passed == total


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
