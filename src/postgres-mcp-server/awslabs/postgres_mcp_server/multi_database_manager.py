"""
Multi-Database Manager for PostgreSQL MCP Server

This module provides the foundation for managing multiple PostgreSQL database connections
within a single MCP server instance, supporting both backward compatibility and future
multi-database configurations.
"""

from typing import Dict, Optional, Any, List
from loguru import logger

from .unified_connection import UnifiedDBConnection


class DatabaseConfig:
    """Configuration for a single database connection."""
    
    def __init__(
        self,
        database_id: str,
        connection_type: str,
        database: str,
        region: str,
        secret_arn: str,
        readonly: bool = True,
        resource_arn: Optional[str] = None,
        hostname: Optional[str] = None,
        port: int = 5432
    ):
        self.database_id = database_id
        self.connection_type = connection_type
        self.database = database
        self.region = region
        self.secret_arn = secret_arn
        self.readonly = readonly
        self.resource_arn = resource_arn
        self.hostname = hostname
        self.port = port
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        config = {
            "database_id": self.database_id,
            "connection_type": self.connection_type,
            "database": self.database,
            "region": self.region,
            "secret_arn": self.secret_arn,
            "readonly": self.readonly,
            "port": self.port
        }
        
        if self.resource_arn:
            config["resource_arn"] = self.resource_arn
        if self.hostname:
            config["hostname"] = self.hostname
            
        return config


class MultiDatabaseManager:
    """
    Manages multiple PostgreSQL database connections within a single MCP server.
    
    Supports both single-database mode (backward compatibility) and multi-database mode.
    """
    
    def __init__(self):
        self._connections: Dict[str, UnifiedDBConnection] = {}
        self._configs: Dict[str, DatabaseConfig] = {}
        self._default_database_id: Optional[str] = None
        self._initialized = False
    
    def initialize_single_database(
        self,
        connection_type: str,
        database: str,
        region: str,
        secret_arn: str,
        readonly: bool = True,
        resource_arn: Optional[str] = None,
        hostname: Optional[str] = None,
        port: int = 5432,
        database_id: str = "default"
    ) -> None:
        """
        Initialize in single-database mode for backward compatibility.
        
        Args:
            connection_type: Type of connection ("rds_data_api" or "direct_postgres")
            database: Database name
            region: AWS region
            secret_arn: ARN of the secret containing credentials
            readonly: Whether connection is read-only
            resource_arn: ARN of the RDS cluster (for RDS Data API)
            hostname: Database hostname (for direct PostgreSQL)
            port: Database port (for direct PostgreSQL)
            database_id: Internal identifier for this database
        """
        logger.info(f"Initializing single-database mode with database_id: {database_id}")
        
        # Create database configuration
        config = DatabaseConfig(
            database_id=database_id,
            connection_type=connection_type,
            database=database,
            region=region,
            secret_arn=secret_arn,
            readonly=readonly,
            resource_arn=resource_arn,
            hostname=hostname,
            port=port
        )
        
        # Store configuration
        self._configs[database_id] = config
        self._default_database_id = database_id
        
        # Create connection
        connection = UnifiedDBConnection(
            connection_type=connection_type,
            resource_arn=resource_arn,
            hostname=hostname,
            port=port,
            secret_arn=secret_arn,
            database=database,
            region=region,
            readonly=readonly
        )
        
        self._connections[database_id] = connection
        self._initialized = True
        
        logger.success(f"Single-database mode initialized successfully for {database_id}")
    
    def get_connection(self, database_id: Optional[str] = None) -> UnifiedDBConnection:
        """
        Get database connection by ID.
        
        Args:
            database_id: Database identifier. If None, uses default database.
            
        Returns:
            UnifiedDBConnection instance
            
        Raises:
            ValueError: If database_id not found or no default database set
        """
        if not self._initialized:
            raise ValueError("MultiDatabaseManager not initialized")
        
        # Use default database if no database_id specified
        if database_id is None:
            if self._default_database_id is None:
                raise ValueError("No default database configured")
            database_id = self._default_database_id
        
        # Check if database exists
        if database_id not in self._connections:
            available_dbs = list(self._connections.keys())
            raise ValueError(f"Database '{database_id}' not found. Available databases: {available_dbs}")
        
        return self._connections[database_id]
    
    def get_database_config(self, database_id: Optional[str] = None) -> DatabaseConfig:
        """
        Get database configuration by ID.
        
        Args:
            database_id: Database identifier. If None, uses default database.
            
        Returns:
            DatabaseConfig instance
        """
        if database_id is None:
            database_id = self._default_database_id
        
        if database_id not in self._configs:
            available_dbs = list(self._configs.keys())
            raise ValueError(f"Database '{database_id}' not found. Available databases: {available_dbs}")
        
        return self._configs[database_id]
    
    def list_databases(self) -> List[Dict[str, Any]]:
        """
        List all configured databases.
        
        Returns:
            List of database information dictionaries
        """
        databases = []
        for db_id, config in self._configs.items():
            db_info = {
                "id": db_id,
                "database": config.database,
                "connection_type": config.connection_type,
                "readonly": config.readonly,
                "is_default": db_id == self._default_database_id
            }
            databases.append(db_info)
        
        return databases
    
    def get_default_database_id(self) -> Optional[str]:
        """Get the default database ID."""
        return self._default_database_id
    
    def is_initialized(self) -> bool:
        """Check if the manager is initialized."""
        return self._initialized
    
    def get_database_count(self) -> int:
        """Get the number of configured databases."""
        return len(self._connections)


# Global singleton instance for backward compatibility
_multi_db_manager: Optional[MultiDatabaseManager] = None


def get_multi_database_manager() -> MultiDatabaseManager:
    """Get the global MultiDatabaseManager instance."""
    global _multi_db_manager
    if _multi_db_manager is None:
        _multi_db_manager = MultiDatabaseManager()
    return _multi_db_manager


def initialize_single_database_mode(
    connection_type: str,
    database: str,
    region: str,
    secret_arn: str,
    readonly: bool = True,
    resource_arn: Optional[str] = None,
    hostname: Optional[str] = None,
    port: int = 5432
) -> None:
    """
    Initialize the global manager in single-database mode.
    
    This function provides backward compatibility with the existing
    UnifiedDBConnectionSingleton approach.
    """
    manager = get_multi_database_manager()
    manager.initialize_single_database(
        connection_type=connection_type,
        database=database,
        region=region,
        secret_arn=secret_arn,
        readonly=readonly,
        resource_arn=resource_arn,
        hostname=hostname,
        port=port,
        database_id="default"
    )
