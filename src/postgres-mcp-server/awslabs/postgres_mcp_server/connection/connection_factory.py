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

"""Connection factory for determining connection types and creating connections."""

import os
from typing import Optional, Tuple, Dict, Any
from loguru import logger


class ConnectionFactory:
    """Factory class for determining connection types and creating appropriate connections."""
    
    @staticmethod
    def determine_connection_type(
        resource_arn: Optional[str] = None,
        hostname: Optional[str] = None,
        secret_arn: Optional[str] = None,
        database: Optional[str] = None
    ) -> str:
        """
        Determine the connection type based on provided parameters.
        
        Args:
            resource_arn: ARN of the RDS cluster or instance
            hostname: Database hostname
            secret_arn: ARN of the secret containing credentials
            database: Database name
            
        Returns:
            Connection type: 'rds_data_api' or 'direct_postgres'
            
        Raises:
            ValueError: If neither resource_arn nor hostname is provided
        """
        if resource_arn:
            logger.info("Using RDS Data API connection (resource_arn provided)")
            return "rds_data_api"
        elif hostname:
            logger.info("Using direct PostgreSQL connection (hostname provided)")
            return "direct_postgres"
        else:
            raise ValueError("Either resource_arn or hostname must be provided")
    
    @staticmethod
    def create_pool_key(
        connection_type: str,
        resource_arn: Optional[str] = None,
        hostname: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        secret_arn: Optional[str] = None
    ) -> str:
        """
        Create a unique pool key for connection pooling.
        
        Args:
            connection_type: Type of connection ('rds_data_api' or 'direct_postgres')
            resource_arn: ARN of the RDS cluster or instance
            hostname: Database hostname
            port: Database port
            database: Database name
            secret_arn: ARN of the secret containing credentials
            
        Returns:
            Unique pool key string
        """
        if connection_type == "rds_data_api":
            secret_hash = hash(secret_arn) if secret_arn else 0
            return f"rds://{resource_arn}/{database}#{secret_hash}"
        elif connection_type == "direct_postgres":
            port = port or 5432
            secret_hash = hash(secret_arn) if secret_arn else 0
            return f"postgres://{hostname}:{port}/{database}#{secret_hash}"
        else:
            raise ValueError(f"Unknown connection type: {connection_type}")
    
    @staticmethod
    def get_connection_config() -> Dict[str, Any]:
        """
        Get connection configuration from environment variables.
        
        Returns:
            Dictionary containing connection configuration
        """
        return {
            'secret_arn': os.getenv('POSTGRES_SECRET_ARN'),
            'resource_arn': os.getenv('POSTGRES_RESOURCE_ARN'),
            'database': os.getenv('POSTGRES_DATABASE'),
            'hostname': os.getenv('POSTGRES_HOSTNAME'),
            'port': int(os.getenv('POSTGRES_PORT', '5432')),
            'region_name': os.getenv('POSTGRES_REGION', 'us-west-2'),
            'readonly': os.getenv('POSTGRES_READONLY', 'true').lower() == 'true'
        }
    
    @staticmethod
    def validate_connection_params(
        connection_type: str,
        secret_arn: Optional[str] = None,
        resource_arn: Optional[str] = None,
        database: Optional[str] = None,
        hostname: Optional[str] = None,
        region_name: Optional[str] = None
    ) -> Tuple[bool, str]:
        """
        Validate connection parameters for the given connection type.
        
        Args:
            connection_type: Type of connection
            secret_arn: ARN of the secret containing credentials
            resource_arn: ARN of the RDS cluster or instance
            database: Database name
            hostname: Database hostname
            region_name: AWS region name
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if connection_type == "rds_data_api":
            if not all([resource_arn, secret_arn, database, region_name]):
                missing = []
                if not resource_arn:
                    missing.append('resource_arn')
                if not secret_arn:
                    missing.append('secret_arn')
                if not database:
                    missing.append('database')
                if not region_name:
                    missing.append('region_name')
                return False, f"Missing required parameters for RDS Data API: {', '.join(missing)}"
        
        elif connection_type == "direct_postgres":
            if not all([hostname, database, secret_arn, region_name]):
                missing = []
                if not hostname:
                    missing.append('hostname')
                if not database:
                    missing.append('database')
                if not secret_arn:
                    missing.append('secret_arn')
                if not region_name:
                    missing.append('region_name')
                return False, f"Missing required parameters for direct PostgreSQL: {', '.join(missing)}"
        
        else:
            return False, f"Unknown connection type: {connection_type}"
        
        return True, ""
