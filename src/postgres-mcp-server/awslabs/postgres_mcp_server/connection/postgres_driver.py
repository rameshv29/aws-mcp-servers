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

"""Direct PostgreSQL driver implementation using psycopg3."""

import asyncio
import boto3
import json
import os
from botocore.exceptions import ClientError
from loguru import logger
from typing import Any, Dict, List, Optional


try:
    import psycopg
    from psycopg_pool import ConnectionPool
    PSYCOPG_AVAILABLE = True
except ImportError:
    psycopg = None
    ConnectionPool = None
    PSYCOPG_AVAILABLE = False

from .base_connection import DBConnector


class PostgresDriver(DBConnector):
    """Driver for direct PostgreSQL connections using psycopg3."""

    def __init__(
        self,
        hostname: str,
        database: str,
        secret_arn: str,
        region_name: str,
        port: int = 5432,
        readonly: bool = True
    ):
        """
        Initialize PostgreSQL driver with connection pool.

        Args:
            hostname: Database hostname
            database: Database name
            secret_arn: ARN of the secret containing credentials
            region_name: AWS region name
            port: Database port
            readonly: Whether connection is read-only
        """
        if not PSYCOPG_AVAILABLE:
            raise ImportError(
                "psycopg is required for direct PostgreSQL connections. "
                "Install with: pip install psycopg[pool] or pip install .[postgres]"
            )

        self.hostname = hostname
        self.database = database
        self.secret_arn = secret_arn
        self.region_name = region_name
        self.port = port
        self.readonly = readonly
        self._credentials = None
        self._credentials_cached = False
        self._pool = None

        # Configure pool size from environment variables
        self._min_size = int(os.getenv('POSTGRES_POOL_MIN_SIZE', '4'))
        self._max_size = int(os.getenv('POSTGRES_POOL_MAX_SIZE', '10'))
        
        logger.info(f"PostgreSQL driver initialized for {hostname}:{port}/{database}")

    async def _get_credentials(self) -> Dict[str, str]:
        """Get database credentials from AWS Secrets Manager with caching."""
        if not self._credentials_cached:
            try:
                sm_client = boto3.client('secretsmanager', region_name=self.region_name)
                response = await asyncio.to_thread(
                    sm_client.get_secret_value,
                    SecretId=self.secret_arn
                )
                self._credentials = json.loads(response['SecretString'])
                self._credentials_cached = True
                logger.info("Successfully retrieved and cached credentials from Secrets Manager")
            except ClientError as e:
                logger.error(f"Failed to retrieve credentials: {str(e)}")
                raise
        return self._credentials or {}

    async def _ensure_pool(self):
        """Ensure the connection pool is initialized."""
        if self._pool is not None:
            return
            
        try:
            logger.info(f"Initializing connection pool to PostgreSQL: {self.hostname}:{self.port}/{self.database}")
            credentials = await self._get_credentials()
            
            # Build connection string
            conninfo = (
                f"host={self.hostname} "
                f"port={self.port} "
                f"dbname={self.database} "
                f"user={credentials.get('username')} "
                f"password={credentials.get('password')} "
                f"application_name=postgres-mcp-server"
            )
            
            # Create the connection pool
            self._pool = ConnectionPool(
                conninfo=conninfo,
                min_size=self._min_size,
                max_size=self._max_size,
                timeout=30.0,
                open=True  
            )
            
            logger.success(f"Successfully initialized PostgreSQL pool: {self.hostname}:{self.port}/{self.database}")
            
        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL pool: {str(e)}")
            self._pool = None
            raise

    # Required by the abstract base class, but simplified
    def is_connected(self) -> bool:
        """Check if the connection pool is active."""
        return self._pool is not None

    # Required by the abstract base class, but simplified
    async def connect(self) -> bool:
        """
        Establish connection pool to PostgreSQL database.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            await self._ensure_pool()
            return True
        except Exception:
            return False

    # Required by the abstract base class, but simplified
    async def disconnect(self):
        """Disconnect from PostgreSQL database."""
        if self._pool:
            try:
                self._pool.close()
                logger.info("Disconnected from PostgreSQL pool")
            except Exception as e:
                logger.warning(f"Error during disconnect: {str(e)}")
            finally:
                self._pool = None

    async def execute_query(
        self,
        query: str,
        parameters: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Execute a query using psycopg3 connection pool.
        
        Args:
            query: SQL query to execute
            parameters: Query parameters (converted from RDS Data API format)
            
        Returns:
            Query result dictionary in RDS Data API format for compatibility
        """
        # Ensure pool is established
        await self._ensure_pool()
        
        try:
            # Convert RDS Data API parameters to psycopg format
            pg_params = self._convert_parameters(parameters) if parameters else None
            
            # Execute query using connection from pool
            with self._pool.connection() as conn:
                with conn.cursor(row_factory=psycopg.rows.dict_row) as cursor:
                    cursor.execute(query, pg_params)
                    
                    # Fetch results if it's a SELECT query
                    if cursor.description:
                        rows = cursor.fetchall()
                        return self._format_response(rows, cursor.description)
                    else:
                        # For non-SELECT queries, return affected row count
                        return {
                            'numberOfRecordsUpdated': cursor.rowcount,
                            'records': [],
                            'columnMetadata': []
                        }
                        
        except Exception as e:
            # Handle connection errors
            if isinstance(e, (psycopg.OperationalError, psycopg.InterfaceError)):
                logger.warning(f"Connection error: {str(e)}")
                # Reset the pool to force reconnection on next query
                self._pool = None
                # Retry once
                await self._ensure_pool()
                return await self.execute_query(query, parameters)
            else:
                logger.error(f"PostgreSQL query error: {str(e)}")
                raise

    def _convert_parameters(self, rds_params: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Convert RDS Data API parameters to psycopg format."""
        pg_params = {}
        for param in rds_params:
            name = param.get('name')
            value_dict = param.get('value', {})

            # Extract value based on type
            if 'stringValue' in value_dict:
                pg_params[name] = value_dict['stringValue']
            elif 'longValue' in value_dict:
                pg_params[name] = value_dict['longValue']
            elif 'doubleValue' in value_dict:
                pg_params[name] = value_dict['doubleValue']
            elif 'booleanValue' in value_dict:
                pg_params[name] = value_dict['booleanValue']
            elif 'isNull' in value_dict and value_dict['isNull']:
                pg_params[name] = None
            else:
                pg_params[name] = str(value_dict)

        return pg_params

    def _format_response(self, rows: List[Dict], description) -> Dict[str, Any]:
        """Format PostgreSQL response to match RDS Data API format."""
        # Create column metadata
        column_metadata = []
        for desc in description:
            column_metadata.append({
                'name': desc.name,
                'type': desc.type_code,
                'typeName': self._get_type_name(desc.type_code)
            })

        # Convert rows to RDS Data API format
        records = []
        for row in rows:
            record = []
            for col_name in [desc.name for desc in description]:
                value = row[col_name]
                record.append(self._format_cell_value(value))
            records.append(record)

        return {
            'records': records,
            'columnMetadata': column_metadata,
            'numberOfRecordsUpdated': 0
        }

    def _format_cell_value(self, value: Any) -> Dict[str, Any]:
        """Format a cell value to RDS Data API format."""
        if value is None:
            return {'isNull': True}
        elif isinstance(value, str):
            return {'stringValue': value}
        elif isinstance(value, int):
            return {'longValue': value}
        elif isinstance(value, float):
            return {'doubleValue': value}
        elif isinstance(value, bool):
            return {'booleanValue': value}
        else:
            return {'stringValue': str(value)}

    def _get_type_name(self, type_code: int) -> str:
        """Get PostgreSQL type name from type code."""
        # Basic type mapping - can be extended
        type_mapping = {
            23: 'INTEGER',
            25: 'TEXT',
            1043: 'VARCHAR',
            16: 'BOOLEAN',
            701: 'FLOAT8',
            1114: 'TIMESTAMP'
        }
        return type_mapping.get(type_code, 'UNKNOWN')

    async def health_check(self) -> bool:
        """
        Perform health check on the connection pool.
        
        Returns:
            True if connection is healthy, False otherwise
        """
        if not self._pool:
            return False
            
        try:
            with self._pool.connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    return True
        except Exception as e:
            logger.warning(f"Health check failed for PostgreSQL: {str(e)}")
            return False

    @property
    def connection_info(self) -> Dict[str, Any]:
        """Get connection information."""
        pool_stats = {}
        if self._pool:
            try:
                pool_stats = self._pool.get_stats()
            except Exception:
                pass
                
        return {
            'type': 'psycopg_driver',
            'hostname': self.hostname,
            'port': self.port,
            'database': self.database,
            'readonly': self.readonly,
            'connected': self._pool is not None,
            'pool_stats': pool_stats
        }
