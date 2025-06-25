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

"""Tests for the PostgreSQL MCP Server."""

import pytest
from awslabs.postgres_mcp_server.server import (
    extract_cell,
    get_table_schema,
    parse_execute_response,
    run_query,
)
from awslabs.postgres_mcp_server.connection import DBConnector, ConnectionFactory
from unittest.mock import AsyncMock, MagicMock, patch


class TestExtractCell:
    """Tests for the extract_cell function."""

    def test_extract_null_cell(self):
        """Test extracting a null cell."""
        cell = {'isNull': True}
        assert extract_cell(cell) is None

    def test_extract_string_value(self):
        """Test extracting a string value."""
        cell = {'stringValue': 'test'}
        assert extract_cell(cell) == 'test'

    def test_extract_long_value(self):
        """Test extracting a long value."""
        cell = {'longValue': 123}
        assert extract_cell(cell) == 123

    def test_extract_double_value(self):
        """Test extracting a double value."""
        cell = {'doubleValue': 123.45}
        assert extract_cell(cell) == 123.45

    def test_extract_boolean_value(self):
        """Test extracting a boolean value."""
        cell = {'booleanValue': True}
        assert extract_cell(cell) is True

    def test_extract_array_value(self):
        """Test extracting an array value."""
        cell = {'arrayValue': [1, 2, 3]}
        assert extract_cell(cell) == [1, 2, 3]

    def test_extract_unknown_value(self):
        """Test extracting an unknown value type."""
        cell = {'unknownValue': 'test'}
        assert extract_cell(cell) is None


class TestParseExecuteResponse:
    """Tests for the parse_execute_response function."""

    def test_parse_empty_response(self):
        """Test parsing an empty response."""
        response = {}
        assert parse_execute_response(response) == []

    def test_parse_response_with_data(self):
        """Test parsing a response with data."""
        response = {
            'columnMetadata': [
                {'name': 'id'},
                {'name': 'name'},
            ],
            'records': [
                [
                    {'longValue': 1},
                    {'stringValue': 'test'},
                ],
                [
                    {'longValue': 2},
                    {'stringValue': 'test2'},
                ],
            ],
        }
        expected = [
            {'id': 1, 'name': 'test'},
            {'id': 2, 'name': 'test2'},
        ]
        assert parse_execute_response(response) == expected


class TestDBConnector:
    """Tests for the DBConnector implementations."""

    def test_rds_connector_init(self):
        """Test initializing an RDSDataAPIConnector."""
        from awslabs.postgres_mcp_server.connection.rds_connector import RDSDataAPIConnector
        
        connector = RDSDataAPIConnector(
            resource_arn='cluster_arn',
            secret_arn='secret_arn', # pragma: allowlist secret
            database='database',
            region_name='region',
            readonly=True
        )
        assert connector.resource_arn == 'cluster_arn'
        assert connector.secret_arn == 'secret_arn' # pragma: allowlist secret
        assert connector.database == 'database'
        assert connector.readonly is True
        assert connector.connection_info['type'] == 'rds_data_api'

    def test_postgres_driver_init(self):
        """Test initializing a PostgresDriver."""
        from awslabs.postgres_mcp_server.connection.postgres_driver import PostgresDriver
        
        connector = PostgresDriver(
            hostname='hostname',
            port=5432,
            database='database',
            secret_arn='secret_arn', # pragma: allowlist secret
            region_name='region',
            readonly=True
        )
        assert connector.hostname == 'hostname'
        assert connector.port == 5432
        assert connector.database == 'database'
        assert connector.secret_arn == 'secret_arn' # pragma: allowlist secret
        assert connector.readonly is True
        assert connector.connection_info['type'] == 'psycopg_driver'

    def test_readonly_query(self):
        """Test the readonly_query property."""
        # Create a mock DBConnector
        connector = MagicMock(spec=DBConnector)
        connector.readonly = True
        
        # Test the readonly_query property
        assert DBConnector.readonly_query.__get__(connector) is True


class TestConnectionFactory:
    """Tests for the ConnectionFactory class."""

    def test_determine_connection_type_rds(self):
        """Test determining connection type with resource_arn."""
        connection_type = ConnectionFactory.determine_connection_type(
            resource_arn='cluster_arn',
            hostname=None
        )
        assert connection_type == 'rds_data_api'

    def test_determine_connection_type_postgres(self):
        """Test determining connection type with hostname."""
        connection_type = ConnectionFactory.determine_connection_type(
            resource_arn=None,
            hostname='hostname'
        )
        assert connection_type == 'psycopg_driver'

    def test_determine_connection_type_missing_params(self):
        """Test determining connection type with missing parameters."""
        with pytest.raises(ValueError):
            ConnectionFactory.determine_connection_type(
                resource_arn=None,
                hostname=None
            )

    def test_create_connection_rds(self):
        """Test creating an RDS Data API connection."""
        from awslabs.postgres_mcp_server.connection.rds_connector import RDSDataAPIConnector
        
        with patch('awslabs.postgres_mcp_server.connection.connection_factory.RDSDataAPIConnector') as mock_connector:
            mock_connector.return_value = MagicMock(spec=RDSDataAPIConnector)
            
            ConnectionFactory.create_connection(
                resource_arn='cluster_arn',
                secret_arn='secret_arn', # pragma: allowlist secret
                database='database',
                region='region',
                readonly=True
            )
            
            mock_connector.assert_called_once_with(
                resource_arn='cluster_arn',
                secret_arn='secret_arn', # pragma: allowlist secret
                database='database',
                region_name='region',
                readonly=True
            )

    def test_create_connection_postgres(self):
        """Test creating a PostgreSQL connection."""
        from awslabs.postgres_mcp_server.connection.postgres_driver import PostgresDriver
        
        with patch('awslabs.postgres_mcp_server.connection.connection_factory.PostgresDriver') as mock_driver:
            mock_driver.return_value = MagicMock(spec=PostgresDriver)
            
            ConnectionFactory.create_connection(
                hostname='hostname',
                port=5432,
                secret_arn='secret_arn', # pragma: allowlist secret
                database='database',
                region='region',
                readonly=True
            )
            
            mock_driver.assert_called_once_with(
                hostname='hostname',
                port=5432,
                database='database',
                secret_arn='secret_arn', # pragma: allowlist secret
                region_name='region',
                readonly=True
            )

    def test_validate_connection_params_rds_valid(self):
        """Test validating RDS Data API connection parameters."""
        is_valid, _ = ConnectionFactory.validate_connection_params(
            connection_type='rds_data_api',
            resource_arn='cluster_arn',
            secret_arn='secret_arn', # pragma: allowlist secret
            database='database',
            region_name='region'
        )
        assert is_valid is True

    def test_validate_connection_params_rds_invalid(self):
        """Test validating RDS Data API connection parameters with missing parameters."""
        is_valid, error_msg = ConnectionFactory.validate_connection_params(
            connection_type='rds_data_api',
            resource_arn=None,
            secret_arn='secret_arn', # pragma: allowlist secret
            database='database',
            region_name='region'
        )
        assert is_valid is False
        assert 'resource_arn' in error_msg

    def test_validate_connection_params_postgres_valid(self):
        """Test validating PostgreSQL connection parameters."""
        is_valid, _ = ConnectionFactory.validate_connection_params(
            connection_type='psycopg_driver',
            hostname='hostname',
            secret_arn='secret_arn', # pragma: allowlist secret
            database='database',
            region_name='region'
        )
        assert is_valid is True

    def test_validate_connection_params_postgres_invalid(self):
        """Test validating PostgreSQL connection parameters with missing parameters."""
        is_valid, error_msg = ConnectionFactory.validate_connection_params(
            connection_type='psycopg_driver',
            hostname=None,
            secret_arn='secret_arn', # pragma: allowlist secret
            database='database',
            region_name='region'
        )
        assert is_valid is False
        assert 'hostname' in error_msg


class TestRunQuery:
    """Tests for the run_query function."""

    @pytest.mark.asyncio
    async def test_run_query_success(self):
        """Test running a query successfully."""
        # Mock context
        ctx = AsyncMock()

        # Mock DB connection
        mock_db_connection = MagicMock(spec=DBConnector)
        mock_db_connection.readonly_query = False
        mock_db_connection.is_connected.return_value = True
        
        # Mock response from execute_query
        mock_response = {
            'columnMetadata': [{'name': 'id'}],
            'records': [
                [{'longValue': 1}],
            ],
        }
        mock_db_connection.execute_query.return_value = mock_response

        # Patch the global db_connection
        with patch('awslabs.postgres_mcp_server.server.db_connection', mock_db_connection):
            # Run the query
            result = await run_query('SELECT 1', ctx)

            # Check the result
            assert result == [{'id': 1}]

            # Check that execute_query was called with the correct parameters
            mock_db_connection.execute_query.assert_called_once_with('SELECT 1', None)

    @pytest.mark.asyncio
    async def test_run_query_readonly_violation(self):
        """Test running a mutating query in readonly mode."""
        # Mock context
        ctx = AsyncMock()

        # Mock DB connection
        mock_db_connection = MagicMock(spec=DBConnector)
        mock_db_connection.readonly_query = True

        # Patch the global db_connection
        with patch('awslabs.postgres_mcp_server.server.db_connection', mock_db_connection):
            # Run the query
            result = await run_query('UPDATE table SET column = value', ctx)

            # Check the result
            assert result == [
                {
                    'error': 'Your MCP tool only allows readonly query. If you want to write, change the MCP configuration per README.md'
                }
            ]

            # Check that execute_query was not called
            mock_db_connection.execute_query.assert_not_called()

            # Check that error was called
            ctx.error.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_query_injection_risk(self):
        """Test running a query with injection risk."""
        # Mock context
        ctx = AsyncMock()

        # Mock DB connection
        mock_db_connection = MagicMock(spec=DBConnector)
        mock_db_connection.readonly_query = False

        # Patch the global db_connection
        with patch('awslabs.postgres_mcp_server.server.db_connection', mock_db_connection):
            # Run the query with a risky pattern
            result = await run_query(
                "SELECT * FROM users WHERE username = 'admin'; DROP TABLE users;--'",
                ctx,
            )

            # Check the result
            assert result == [{'error': 'Your query contains risky injection patterns'}]

            # Check that execute_query was not called
            mock_db_connection.execute_query.assert_not_called()

            # Check that error was called
            ctx.error.assert_called_once()


class TestGetTableSchema:
    """Tests for the get_table_schema function."""

    @pytest.mark.asyncio
    @patch('awslabs.postgres_mcp_server.server.run_query')
    async def test_get_table_schema(self, mock_run_query):
        """Test getting a table schema."""
        # Mock context
        ctx = AsyncMock()

        # Mock response from run_query
        mock_run_query.return_value = [
            {
                'column_name': 'id',
                'data_type': 'integer',
                'is_nullable': 'NO',
                'column_default': None,
                'character_maximum_length': None,
                'numeric_precision': 32,
                'numeric_scale': 0,
            },
        ]

        # Get the table schema
        result = await get_table_schema('users', ctx)

        # Check the result
        assert result == mock_run_query.return_value

        # Check that run_query was called with the correct parameters
        mock_run_query.assert_called_once()
        args, kwargs = mock_run_query.call_args
        assert 'pg_attribute' in kwargs['sql']
        assert kwargs['ctx'] == ctx
        assert len(kwargs['query_parameters']) == 1
        assert kwargs['query_parameters'][0]['name'] == 'table_name'
        assert kwargs['query_parameters'][0]['value']['stringValue'] == 'users'
