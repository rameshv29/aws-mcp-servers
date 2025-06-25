#!/usr/bin/env python3
"""
Test script for RDS Data API driver functionality.

This tests the RDS Data API connector implementation, including connection management,
query execution, transaction handling, and error handling.
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
from awslabs.postgres_mcp_server.connection.rds_connector import RDSDataAPIConnector


# RDS Connector Tests
def test_rds_connector_init():
    """Test RDSDataAPIConnector initialization."""
    print("\n🧪 Testing RDSDataAPIConnector initialization")
    print("-" * 50)
    
    # Create a connector
    connector = RDSDataAPIConnector(
        resource_arn="arn:aws:rds:us-west-2:123456789012:cluster:test",
        secret_arn="test-secret",
        database="test-db",
        region_name="us-west-2",
        readonly=True
    )
    
    # Check that the connector was initialized correctly
    print(" Checking connector attributes:")
    assert connector.resource_arn == "arn:aws:rds:us-west-2:123456789012:cluster:test", "resource_arn not set correctly"
    assert connector.secret_arn == "test-secret", "secret_arn not set correctly"
    assert connector.database == "test-db", "database not set correctly"
    assert connector.region_name == "us-west-2", "region_name not set correctly"
    assert connector.readonly is True, "readonly not set correctly"
    assert connector._connected is False, "Should start disconnected"
    
    print(" RDSDataAPIConnector initialization tests passed")


@pytest.mark.asyncio
async def test_rds_connector_client_property():
    """Test RDSDataAPIConnector client property."""
    print("\n🧪 Testing RDSDataAPIConnector.client property")
    print("-" * 50)
    
    # Create mocks for boto3
    with patch('boto3.client') as mock_boto3_client:
        # Set up the mock to return a client
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        
        # Create a connector
        connector = RDSDataAPIConnector(
            resource_arn="arn:aws:rds:us-west-2:123456789012:cluster:test",
            secret_arn="test-secret",
            database="test-db",
            region_name="us-west-2",
            readonly=True
        )
        
        # Access the client property
        client = connector.client
        
        # Check that boto3.client was called with the correct parameters
        mock_boto3_client.assert_called_once_with('rds-data', region_name="us-west-2")
        
        # Check that the client was returned
        assert client == mock_client, "client property should return the boto3 client"
        
        # Access the client property again to ensure it's cached
        mock_boto3_client.reset_mock()
        client2 = connector.client
        
        # Check that boto3.client was not called again
        mock_boto3_client.assert_not_called()
        
        print(" RDSDataAPIConnector.client property tests passed")


@pytest.mark.asyncio
async def test_rds_connector_connect():
    """Test RDSDataAPIConnector connect method with mocks."""
    print("\n🧪 Testing RDSDataAPIConnector.connect")
    print("-" * 50)
    
    # Create a mock boto3 client
    mock_client = MagicMock()
    
    # Create a connector
    connector = RDSDataAPIConnector(
        resource_arn="arn:aws:rds:us-west-2:123456789012:cluster:test",
        secret_arn="test-secret",
        database="test-db",
        region_name="us-west-2",
        readonly=True
    )
    
    # Replace the real client with our mock
    connector._client = mock_client
    
    # Set up the mock to return a successful response
    mock_client.execute_statement.return_value = {
        'columnMetadata': [{'name': 'version'}],
        'records': [[{'stringValue': 'PostgreSQL 13.4'}]]
    }
    
    # Call the connect method
    result = await connector.connect()
    
    # Check that the connection was successful
    print(" Connection result:", result)
    assert result is True, "connect() should return True on success"
    assert connector._connected is True, "Should be marked as connected"
    
    # Check that execute_statement was called with the correct parameters
    mock_client.execute_statement.assert_called_once()
    call_args = mock_client.execute_statement.call_args[1]
    assert call_args['resourceArn'] == "arn:aws:rds:us-west-2:123456789012:cluster:test"
    assert call_args['secretArn'] == "test-secret"
    assert call_args['database'] == "test-db"
    assert call_args['sql'] == "SELECT 1"
    
    print(" RDSDataAPIConnector.connect tests passed")


@pytest.mark.asyncio
async def test_rds_connector_connect_failure():
    """Test RDSDataAPIConnector connect method with failure."""
    print("\n🧪 Testing RDSDataAPIConnector.connect failure")
    print("-" * 50)
    
    # Create a mock boto3 client
    mock_client = MagicMock()
    
    # Create a connector
    connector = RDSDataAPIConnector(
        resource_arn="arn:aws:rds:us-west-2:123456789012:cluster:test",
        secret_arn="test-secret",
        database="test-db",
        region_name="us-west-2",
        readonly=True
    )
    
    # Replace the real client with our mock
    connector._client = mock_client
    
    # Set up the mock to raise an exception
    mock_client.execute_statement.side_effect = Exception("Connection failed")
    
    # Call the connect method
    result = await connector.connect()
    
    # Check that the connection failed
    print(" Connection result:", result)
    assert result is False, "connect() should return False on failure"
    assert connector._connected is False, "Should not be marked as connected"
    
    print(" RDSDataAPIConnector.connect failure tests passed")


@pytest.mark.asyncio
async def test_rds_connector_is_connected():
    """Test RDSDataAPIConnector is_connected method."""
    print("\n🧪 Testing RDSDataAPIConnector.is_connected")
    print("-" * 50)
    
    # Create a connector
    connector = RDSDataAPIConnector(
        resource_arn="arn:aws:rds:us-west-2:123456789012:cluster:test",
        secret_arn="test-secret",
        database="test-db",
        region_name="us-west-2",
        readonly=True
    )
    
    # Check initial state
    assert connector.is_connected() is False, "Should start disconnected"
    
    # Set connected state
    connector._connected = True
    assert connector.is_connected() is True, "Should be connected after setting _connected"
    
    # Set disconnected state
    connector._connected = False
    assert connector.is_connected() is False, "Should be disconnected after setting _connected"
    
    print(" RDSDataAPIConnector.is_connected tests passed")


@pytest.mark.asyncio
async def test_rds_connector_disconnect():
    """Test RDSDataAPIConnector disconnect method."""
    print("\n🧪 Testing RDSDataAPIConnector.disconnect")
    print("-" * 50)
    
    # Create a connector
    connector = RDSDataAPIConnector(
        resource_arn="arn:aws:rds:us-west-2:123456789012:cluster:test",
        secret_arn="test-secret",
        database="test-db",
        region_name="us-west-2",
        readonly=True
    )
    
    # Set connected state
    connector._connected = True
    
    # Call the disconnect method
    await connector.disconnect()
    
    # Check that the connector is disconnected
    assert connector._connected is False, "Should be disconnected after calling disconnect"
    
    print(" RDSDataAPIConnector.disconnect tests passed")


@pytest.mark.asyncio
async def test_rds_connector_execute_query():
    """Test RDSDataAPIConnector execute_query method with mocks."""
    print("\n🧪 Testing RDSDataAPIConnector.execute_query")
    print("-" * 50)
    
    # Create a mock boto3 client
    mock_client = MagicMock()
    
    # Create a connector
    connector = RDSDataAPIConnector(
        resource_arn="arn:aws:rds:us-west-2:123456789012:cluster:test",
        secret_arn="test-secret",
        database="test-db",
        region_name="us-west-2",
        readonly=False  # Non-readonly for direct execution
    )
    
    # Replace the real client with our mock and mark as connected
    connector._client = mock_client
    connector._connected = True
    
    # Set up the mock to return a successful response
    mock_client.execute_statement.return_value = {
        'columnMetadata': [{'name': 'id'}, {'name': 'name'}],
        'records': [
            [{'longValue': 1}, {'stringValue': 'test'}],
            [{'longValue': 2}, {'stringValue': 'test2'}]
        ]
    }
    
    # Call the execute_query method
    result = await connector.execute_query("SELECT * FROM test")
    
    # Check that the query was successful
    print(" Query result has expected structure:", 'columnMetadata' in result and 'records' in result)
    assert 'columnMetadata' in result, "Result should have columnMetadata"
    assert 'records' in result, "Result should have records"
    assert len(result['records']) == 2, "Result should have 2 records"
    
    # Check that execute_statement was called with the correct parameters
    mock_client.execute_statement.assert_called_once()
    call_args = mock_client.execute_statement.call_args[1]
    assert call_args['resourceArn'] == "arn:aws:rds:us-west-2:123456789012:cluster:test"
    assert call_args['secretArn'] == "test-secret"
    assert call_args['database'] == "test-db"
    assert call_args['sql'] == "SELECT * FROM test"
    
    print(" RDSDataAPIConnector.execute_query tests passed")


@pytest.mark.asyncio
async def test_rds_connector_execute_query_with_parameters():
    """Test RDSDataAPIConnector execute_query method with parameters."""
    print("\n🧪 Testing RDSDataAPIConnector.execute_query with parameters")
    print("-" * 50)
    
    # Create a mock boto3 client
    mock_client = MagicMock()
    
    # Create a connector
    connector = RDSDataAPIConnector(
        resource_arn="arn:aws:rds:us-west-2:123456789012:cluster:test",
        secret_arn="test-secret",
        database="test-db",
        region_name="us-west-2",
        readonly=False  # Non-readonly for direct execution
    )
    
    # Replace the real client with our mock and mark as connected
    connector._client = mock_client
    connector._connected = True
    
    # Set up the mock to return a successful response
    mock_client.execute_statement.return_value = {
        'columnMetadata': [{'name': 'id'}, {'name': 'name'}],
        'records': [
            [{'longValue': 1}, {'stringValue': 'test'}]
        ]
    }
    
    # Call the execute_query method with parameters
    params = [
        {'name': 'id', 'value': {'longValue': 1}},
        {'name': 'name', 'value': {'stringValue': 'test'}}
    ]
    result = await connector.execute_query("SELECT * FROM test WHERE id = :id AND name = :name", params)
    
    # Check that the query was successful
    print(" Query result has expected structure:", 'columnMetadata' in result and 'records' in result)
    assert 'columnMetadata' in result, "Result should have columnMetadata"
    assert 'records' in result, "Result should have records"
    
    # Check that execute_statement was called with the correct parameters
    mock_client.execute_statement.assert_called_once()
    call_args = mock_client.execute_statement.call_args[1]
    assert call_args['resourceArn'] == "arn:aws:rds:us-west-2:123456789012:cluster:test"
    assert call_args['secretArn'] == "test-secret"
    assert call_args['database'] == "test-db"
    assert call_args['sql'] == "SELECT * FROM test WHERE id = :id AND name = :name"
    assert call_args['parameters'] == params
    
    print(" RDSDataAPIConnector.execute_query with parameters tests passed")


@pytest.mark.asyncio
async def test_rds_connector_readonly_query():
    """Test RDSDataAPIConnector readonly query execution with mocks."""
    print("\n🧪 Testing RDSDataAPIConnector readonly query execution")
    print("-" * 50)
    
    # Create a mock boto3 client
    mock_client = MagicMock()
    
    # Create a connector with readonly=True
    connector = RDSDataAPIConnector(
        resource_arn="arn:aws:rds:us-west-2:123456789012:cluster:test",
        secret_arn="test-secret",
        database="test-db",
        region_name="us-west-2",
        readonly=True
    )
    
    # Replace the real client with our mock and mark as connected
    connector._client = mock_client
    connector._connected = True
    
    # Set up the mock responses
    mock_client.begin_transaction.return_value = {'transactionId': 'test-tx-id'}
    mock_client.execute_statement.return_value = {
        'columnMetadata': [{'name': 'id'}],
        'records': [[{'longValue': 1}]]
    }
    mock_client.commit_transaction.return_value = {'transactionStatus': 'COMMITTED'}
    
    # Call the execute_query method
    result = await connector.execute_query("SELECT * FROM test")
    
    # Check that the query was successful
    print(" Query result has expected structure:", 'columnMetadata' in result and 'records' in result)
    assert 'columnMetadata' in result, "Result should have columnMetadata"
    assert 'records' in result, "Result should have records"
    
    # Check that transaction methods were called
    mock_client.begin_transaction.assert_called_once()
    assert mock_client.execute_statement.call_count >= 2, "Should call execute_statement at least twice"
    mock_client.commit_transaction.assert_called_once()
    
    # Check that SET TRANSACTION READ ONLY was executed
    set_tx_call = mock_client.execute_statement.call_args_list[0]
    assert "SET TRANSACTION READ ONLY" in set_tx_call[1]['sql'], "Should set transaction to read-only"
    
    print(" RDSDataAPIConnector readonly query tests passed")


@pytest.mark.asyncio
async def test_rds_connector_readonly_query_error():
    """Test RDSDataAPIConnector readonly query execution with error."""
    print("\n🧪 Testing RDSDataAPIConnector readonly query execution with error")
    print("-" * 50)
    
    # Create a mock boto3 client
    mock_client = MagicMock()
    
    # Create a connector with readonly=True
    connector = RDSDataAPIConnector(
        resource_arn="arn:aws:rds:us-west-2:123456789012:cluster:test",
        secret_arn="test-secret",
        database="test-db",
        region_name="us-west-2",
        readonly=True
    )
    
    # Replace the real client with our mock and mark as connected
    connector._client = mock_client
    connector._connected = True
    
    # Set up the mock responses
    mock_client.begin_transaction.return_value = {'transactionId': 'test-tx-id'}
    mock_client.execute_statement.side_effect = [
        # First call (SET TRANSACTION READ ONLY) succeeds
        {
            'columnMetadata': [],
            'records': []
        },
        # Second call (actual query) fails
        Exception("Query failed")
    ]
    
    # Call the execute_query method
    try:
        await connector.execute_query("SELECT * FROM test")
        print(" FAILED: Should have raised an exception")
        pytest.fail("Should have raised an exception")
    except Exception as e:
        print(f" SUCCESS: Raised exception: {e}")
    
    # Check that rollback was called
    mock_client.rollback_transaction.assert_called_once()
    
    print(" RDSDataAPIConnector readonly query error tests passed")


@pytest.mark.asyncio
async def test_rds_connector_health_check():
    """Test RDSDataAPIConnector health_check method."""
    print("\n🧪 Testing RDSDataAPIConnector.health_check")
    print("-" * 50)
    
    # Create a connector
    connector = RDSDataAPIConnector(
        resource_arn="arn:aws:rds:us-west-2:123456789012:cluster:test",
        secret_arn="test-secret",
        database="test-db",
        region_name="us-west-2",
        readonly=True
    )
    
    # Mock the execute_query method
    connector.execute_query = AsyncMock()
    connector.execute_query.return_value = {"result": "success"}
    
    # Call the health_check method
    result = await connector.health_check()
    
    # Check that the health check was successful
    print(" Health check result:", result)
    assert result is True, "health_check() should return True on success"
    
    # Check that execute_query was called with the correct parameters
    connector.execute_query.assert_called_once_with("SELECT 1")
    
    # Test health check failure
    connector.execute_query.reset_mock()
    connector.execute_query.side_effect = Exception("Health check failed")
    
    result = await connector.health_check()
    
    # Check that the health check failed
    print(" Health check failure result:", result)
    assert result is False, "health_check() should return False on failure"
    
    print(" RDSDataAPIConnector.health_check tests passed")


@pytest.mark.asyncio
async def test_rds_connector_connection_info():
    """Test RDSDataAPIConnector connection_info property."""
    print("\n🧪 Testing RDSDataAPIConnector.connection_info")
    print("-" * 50)
    
    # Create a connector
    connector = RDSDataAPIConnector(
        resource_arn="arn:aws:rds:us-west-2:123456789012:cluster:test",
        secret_arn="test-secret",
        database="test-db",
        region_name="us-west-2",
        readonly=True
    )
    
    # Get connection info
    info = connector.connection_info
    
    # Check that the connection info is correct
    print(" Connection info:", info)
    assert info['type'] == "rds_data_api", "type should be rds_data_api"
    assert info['resource_arn'] == "arn:aws:rds:us-west-2:123456789012:cluster:test", "resource_arn should match"
    assert info['database'] == "test-db", "database should match"
    assert info['region'] == "us-west-2", "region should match"
    assert info['readonly'] is True, "readonly should match"
    assert info['connected'] is False, "connected should match"
    
    # Set connected state and check again
    connector._connected = True
    info = connector.connection_info
    assert info['connected'] is True, "connected should be updated"
    
    print(" RDSDataAPIConnector.connection_info tests passed")


async def main():
    """Run all RDS connector tests."""
    print(" RDS Data API Driver Tests")
    print("=" * 70)
    
    # Set AWS profile
    os.environ['AWS_PROFILE'] = 'mcp_profile'
    os.environ['AWS_REGION'] = 'us-west-2'
    
    test_results = []
    
    # Run synchronous tests
    test_results.append(test_rds_connector_init())
    test_results.append(test_rds_connector_is_connected())
    
    # Run async tests using pytest
    try:
        import pytest
        pytest.main(["-xvs", __file__, "-k", "test_rds_connector_client_property or test_rds_connector_connect or test_rds_connector_connect_failure or test_rds_connector_disconnect or test_rds_connector_execute_query or test_rds_connector_execute_query_with_parameters or test_rds_connector_readonly_query or test_rds_connector_readonly_query_error or test_rds_connector_health_check or test_rds_connector_connection_info"])
    except Exception as e:
        print(f"Error running async tests: {e}")
    
    # Summary
    print("\n" + "=" * 70)
    print(" RDS DATA API DRIVER TEST RESULTS")
    print("=" * 70)
    
    passed = sum(1 for result in test_results if result)
    total = len(test_results)
    
    print(f"\n OVERALL RESULTS:")
    print(f" Passed: {passed}/{total} tests")
    print(f" Failed: {total - passed}/{total} tests")
    
    if passed == total:
        print(f"\n ALL TESTS PASSED! RDS Data API driver is working correctly!")
    else:
        print(f"\n SOME TESTS FAILED - Please review the issues above")
    
    print("\n" + "=" * 70)
    print(" Test execution completed")
    
    return passed == total


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
