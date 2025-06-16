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

"""Connection pool manager for PostgreSQL MCP Server."""

import psycopg2
from psycopg2.pool import ThreadedConnectionPool
import logging

logger = logging.getLogger("postgresql-mcp-server")

class PostgresConnectionPool:
    """
    Singleton connection pool manager for PostgreSQL connections.
    
    This class implements a thread-safe connection pool for PostgreSQL
    using psycopg2.pool.ThreadedConnectionPool. It follows the singleton
    pattern to ensure only one pool is created per application instance.
    """
    
    _instance = None
    
    @classmethod
    def get_instance(cls, **kwargs):
        """
        Get or create the singleton instance of the connection pool.
        
        Args:
            **kwargs: Connection parameters to pass to the constructor if
                     a new instance needs to be created.
                     
        Returns:
            PostgresConnectionPool: The singleton instance
        """
        if cls._instance is None:
            cls._instance = cls(**kwargs)
        return cls._instance
    
    def __init__(self, min_size=5, max_size=30, **connection_params):
        """
        Initialize the connection pool manager.
        
        Args:
            min_size: Minimum number of connections to keep in the pool (default: 5)
            max_size: Maximum number of connections allowed in the pool (default: 30)
            **connection_params: Connection parameters to pass to psycopg2.connect()
        """
        self.min_size = min_size
        self.max_size = max_size
        self.connection_params = connection_params
        self.pool = None
        logger.info(f"Connection pool manager initialized with min_size={min_size}, max_size={max_size}")
        
    def initialize(self):
        """
        Initialize the connection pool.
        
        This method creates the actual ThreadedConnectionPool if it doesn't exist yet.
        """
        if self.pool is not None:
            return
            
        logger.info(f"Creating connection pool with min_size={self.min_size}, max_size={self.max_size}")
        
        try:
            self.pool = ThreadedConnectionPool(
                minconn=self.min_size,
                maxconn=self.max_size,
                **self.connection_params
            )
            logger.info("Connection pool initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {str(e)}")
            raise
    
    def get_connection(self):
        """
        Get a connection from the pool.
        
        Returns:
            psycopg2.connection: A PostgreSQL database connection
        
        Raises:
            Exception: If the pool is not initialized or if getting a connection fails
        """
        if self.pool is None:
            self.initialize()
            
        try:
            conn = self.pool.getconn()
            logger.debug("Retrieved connection from pool")
            return conn
        except Exception as e:
            logger.error(f"Error getting connection from pool: {str(e)}")
            raise
    
    def release_connection(self, conn):
        """
        Return a connection to the pool.
        
        Args:
            conn: The connection to return to the pool
        """
        if self.pool is not None and conn is not None:
            try:
                self.pool.putconn(conn)
                logger.debug("Returned connection to pool")
            except Exception as e:
                logger.error(f"Error returning connection to pool: {str(e)}")
    
    def close(self):
        """
        Close all connections in the pool.
        
        This should be called when shutting down the application.
        """
        if self.pool is not None:
            try:
                self.pool.closeall()
                logger.info("Connection pool closed")
            except Exception as e:
                logger.error(f"Error closing connection pool: {str(e)}")
            finally:
                self.pool = None
