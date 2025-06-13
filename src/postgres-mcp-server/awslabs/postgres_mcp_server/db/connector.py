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

import json
import psycopg2
import boto3
import base64
from typing import List, Dict, Any, Optional

class UniversalConnector:
    def __init__(self, secret_name=None, region_name=None, 
                 secret_arn=None, resource_arn=None, database=None,
                 host=None, port=None, user=None, password=None):
        """
        Initialize the universal connector that can use either RDS Data API or direct PostgreSQL connection
        
        Args:
            secret_name: Name of the secret in AWS Secrets Manager containing database credentials
            region_name: AWS region name
            secret_arn: ARN of the secret in AWS Secrets Manager (for RDS Data API)
            resource_arn: ARN of the RDS cluster or instance (for RDS Data API)
            database: Database name to connect to
            host: Database host (for direct connection)
            port: Database port (for direct connection)
            user: Database username (for direct connection)
            password: Database password (for direct connection)
        """
        # RDS Data API parameters
        self.secret_arn = secret_arn
        self.resource_arn = resource_arn
        
        # PostgreSQL connector parameters
        self.secret_name = secret_name
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        
        # Common parameters
        self.database = database
        self.region_name = region_name or 'us-west-2'
        
        # Connection objects
        self.rds_client = None
        self.pg_conn = None
        
        # Connection mode
        self.connection_mode = None  # 'rds_data_api' or 'pg_connector'
        
        # Default to read-only mode
        self.read_only = True
    
    def connect(self):
        """
        Connect to the database using either RDS Data API or PostgreSQL connector
        based on available credentials
        """
        print("Attempting to connect to database...")
        
        # IMPORTANT: If we have RDS Data API parameters, ONLY try RDS Data API
        # and don't fall back to PostgreSQL connector
        if self.secret_arn and self.resource_arn and self.database:
            print(f"Trying RDS Data API connection with secret_arn={self.secret_arn}, resource_arn={self.resource_arn}, database={self.database}")
            if self._connect_rds_data_api():
                self.connection_mode = 'rds_data_api'
                print(f"Connected to RDS database using Data API: {self.database}")
                return True
            else:
                print("RDS Data API connection failed")
                print("Not attempting PostgreSQL connection since RDS Data API parameters were provided")
                return False
        
        # If no RDS Data API parameters, try to get credentials from Secrets Manager
        if self.secret_name or self.secret_arn:
            secret_arn = self.secret_arn
            secret_name = self.secret_name
            
            try:
                print(f"Retrieving credentials from AWS Secrets Manager using {'secret_arn' if secret_arn else 'secret_name'}")
                # Get credentials from AWS Secrets Manager
                session = boto3.session.Session()
                client = session.client(
                    service_name='secretsmanager',
                    region_name=self.region_name
                )
                
                if secret_arn:
                    get_secret_value_response = client.get_secret_value(SecretId=secret_arn)
                else:
                    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
                
                if 'SecretString' in get_secret_value_response:
                    secret = json.loads(get_secret_value_response['SecretString'])
                    print("Retrieved secret in string format")
                else:
                    decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
                    secret = json.loads(decoded_binary_secret)
                    print("Retrieved secret in binary format")
                
                # Check for RDS Data API parameters with various possible key names
                rds_api_keys = [
                    ('resourceArn', 'secretArn'),
                    ('resource_arn', 'secret_arn'),
                    ('ResourceArn', 'SecretArn'),
                    ('clusterArn', 'secretArn'),
                    ('cluster_arn', 'secret_arn'),
                    ('ClusterArn', 'SecretArn')
                ]
                
                # Check if any of the RDS Data API key pairs exist in the secret
                for resource_key, secret_key in rds_api_keys:
                    if resource_key in secret and secret_key in secret:
                        print(f"Found RDS Data API parameters in secret: {resource_key}, {secret_key}")
                        self.resource_arn = secret.get(resource_key)
                        self.secret_arn = secret.get(secret_key)
                        self.database = secret.get('database', secret.get('dbname', self.database))
                        
                        if self._connect_rds_data_api():
                            self.connection_mode = 'rds_data_api'
                            print(f"Connected to RDS database using Data API with credentials from secret: {self.database}")
                            return True
                        else:
                            print("RDS Data API connection with credentials from secret failed")
                            # Don't fall back to PostgreSQL if we found RDS Data API parameters
                            return False
                
                # If no RDS Data API parameters found, try PostgreSQL connector
                print("No RDS Data API parameters found, trying PostgreSQL connector with credentials from secret")
                self.host = secret.get('host', self.host)
                self.port = secret.get('port', self.port) or 5432
                self.database = secret.get('dbname', secret.get('database', self.database))
                self.user = secret.get('username', secret.get('user', self.user))
                self.password = secret.get('password', self.password)
                
                print(f"PostgreSQL connection parameters from secret: host={self.host}, port={self.port}, database={self.database}, user={self.user}")
                
                if self._connect_postgresql():
                    self.connection_mode = 'pg_connector'
                    print(f"Connected to PostgreSQL database using direct connection: {self.database} at {self.host}")
                    return True
                else:
                    print("PostgreSQL connection with credentials from secret failed")
            
            except Exception as e:
                print(f"Error retrieving secret or connecting: {str(e)}")
        
        # If we still don't have a connection, try direct PostgreSQL connection with provided parameters
        if self.host and self.database and self.user and self.password:
            print(f"Trying direct PostgreSQL connection with provided parameters: host={self.host}, database={self.database}")
            if self._connect_postgresql():
                self.connection_mode = 'pg_connector'
                print(f"Connected to PostgreSQL database using direct connection: {self.database} at {self.host}")
                return True
            else:
                print("Direct PostgreSQL connection with provided parameters failed")
        
        print("Failed to connect to database. Insufficient connection parameters.")
        return False
    
    def _connect_rds_data_api(self):
        """Connect to RDS database using the Data API"""
        try:
            if not self.secret_arn or not self.resource_arn or not self.database:
                print("Missing required RDS Data API parameters: secret_arn, resource_arn, or database")
                return False
            
            # Create RDS Data API client
            self.rds_client = boto3.client('rds-data', region_name=self.region_name)
            
            # Test connection with a simple query
            test_query = "SELECT 1 as test_connection;"
            try:
                response = self.rds_client.execute_statement(
                    resourceArn=self.resource_arn,
                    secretArn=self.secret_arn,
                    database=self.database,
                    sql=test_query,
                    includeResultMetadata=True
                )
                
                # Verify we got a response
                if 'records' in response and len(response['records']) > 0:
                    print(f"Successfully connected to database {self.database} using RDS Data API")
                    return True
                else:
                    print("RDS Data API connection test returned no results")
                    self.rds_client = None
                    return False
            except self.rds_client.exceptions.BadRequestException as e:
                print(f"RDS Data API error - bad request: {str(e)}")
                self.rds_client = None
                return False
            except self.rds_client.exceptions.StatementTimeoutException as e:
                print(f"RDS Data API error - statement timeout: {str(e)}")
                self.rds_client = None
                return False
            except self.rds_client.exceptions.InternalServerErrorException as e:
                print(f"RDS Data API error - internal server error: {str(e)}")
                self.rds_client = None
                return False
        except Exception as e:
            print(f"Error connecting to database using RDS Data API: {str(e)}")
            self.rds_client = None
            return False
    
    def _connect_postgresql(self):
        """Connect to PostgreSQL database using direct connection"""
        try:
            if not all([self.host, self.database, self.user, self.password]):
                print("Missing required PostgreSQL connection parameters")
                return False
            
            print(f"Attempting PostgreSQL connection to {self.host}:{self.port or 5432}, database={self.database}, user={self.user}")
            
            # Connect to the database with a timeout
            self.pg_conn = psycopg2.connect(
                host=self.host,
                port=self.port or 5432,
                dbname=self.database,
                user=self.user,
                password=self.password,
                connect_timeout=10  # 10-second connection timeout
            )
            
            # Set session to read-only mode for safety
            if self.read_only:
                with self.pg_conn.cursor() as cursor:
                    cursor.execute("SET SESSION TRANSACTION READ ONLY")
                    cursor.execute("SET statement_timeout = '30000'")  # 30-second timeout
            
            print(f"PostgreSQL connection successful to {self.host}")
            return True
        except psycopg2.OperationalError as err:
            print(f"PostgreSQL Error: {str(err)}")
            print("This error typically indicates network connectivity issues:")
            print("1. Check if the host is reachable (network connectivity)")
            print("2. Verify security groups allow access from your IP")
            print("3. Confirm the database port is correct (default is 5432)")
            print("4. For RDS, check if the instance is publicly accessible")
            self.pg_conn = None
            return False
        except Exception as e:
            print(f"Error connecting to database using PostgreSQL connector: {str(e)}")
            self.pg_conn = None
            return False
    
    def disconnect(self):
        """Close the database connection"""
        if self.connection_mode == 'rds_data_api':
            # No explicit disconnect needed for RDS Data API
            self.rds_client = None
        elif self.connection_mode == 'pg_connector':
            if self.pg_conn:
                try:
                    self.pg_conn.close()
                except Exception as e:
                    print(f"Error closing PostgreSQL connection: {str(e)}")
                self.pg_conn = None
        
        self.connection_mode = None
        print("Database connection closed")
    
    def execute_query(self, query, params=None):
        """
        Execute a query and return results as a list of dictionaries
        
        Args:
            query: SQL query to execute
            params: List of parameters for the query
            
        Returns:
            List of dictionaries containing the query results
        """
        if not self.connection_mode:
            print("No database connection. Call connect() first.")
            return []
        
        # Ensure query ends with a semicolon for RDS Data API
        query = query.strip()
        if self.connection_mode == 'rds_data_api' and not query.endswith(';'):
            query += ';'
        
        # For safety, check if this is a potentially dangerous operation
        if self.read_only:
            query_lower = query.lower().strip()
            dangerous_operations = [
                'insert', 'update', 'delete', 'drop', 'alter', 'create', 'truncate', 
                'grant', 'revoke', 'reset', 'load', 'vacuum', 'reindex', 'cluster'
            ]
            
            # Check if query starts with any dangerous operation
            for op in dangerous_operations:
                if query_lower.startswith(op):
                    print(f"Error: Write operation '{op}' attempted in read-only mode")
                    return []
        
        # Handle RDS Data API limitations for PostgreSQL
        if self.connection_mode == 'rds_data_api':
            # Check for unsupported types and functions
            if 'oidvector' in query.lower():
                # Add explicit casts to text for oidvector types
                query = query.replace('oidvector', 'oidvector::text')
                print("Modified query to handle oidvector type for RDS Data API compatibility")
            
            # Add explicit casts for other potentially problematic types
            query = query.replace('regclass', 'regclass::text')
            query = query.replace('regproc', 'regproc::text')
            
            return self._execute_rds_data_api(query, params)
        else:
            return self._execute_postgresql(query, params)
    
    def _execute_rds_data_api(self, query, params=None):
        """Execute query using RDS Data API"""
        try:
            # Convert params to RDS Data API format if provided
            sql_parameters = []
            if params:
                # Handle both list-style and dict-style parameters
                if isinstance(params, list):
                    for i, param in enumerate(params):
                        param_obj = {
                            'name': f'param{i}',
                            'value': {}
                        }
                        
                        if isinstance(param, int):
                            param_obj['value']['longValue'] = param
                        elif isinstance(param, float):
                            param_obj['value']['doubleValue'] = param
                        elif isinstance(param, bool):
                            param_obj['value']['booleanValue'] = param
                        elif param is None:
                            param_obj['value']['isNull'] = True
                        else:
                            param_obj['value']['stringValue'] = str(param)
                        
                        sql_parameters.append(param_obj)
                    
                    # Replace ? placeholders with named parameters
                    for i in range(len(params)):
                        query = query.replace('?', f':{sql_parameters[i]["name"]}', 1)
                elif isinstance(params, dict):
                    # Handle dictionary-style parameters (named parameters)
                    for param_name, param_value in params.items():
                        param_obj = {
                            'name': param_name,
                            'value': {}
                        }
                        
                        if isinstance(param_value, int):
                            param_obj['value']['longValue'] = param_value
                        elif isinstance(param_value, float):
                            param_obj['value']['doubleValue'] = param_value
                        elif isinstance(param_value, bool):
                            param_obj['value']['booleanValue'] = param_value
                        elif param_value is None:
                            param_obj['value']['isNull'] = True
                        else:
                            param_obj['value']['stringValue'] = str(param_value)
                        
                        sql_parameters.append(param_obj)
                    
                    # Replace ? placeholders with named parameters
                    for i in range(len(params)):
                        query = query.replace('?', f':{sql_parameters[i]["name"]}', 1)
                elif isinstance(params, dict):
                    # Handle dictionary-style parameters (named parameters)
                    for param_name, param_value in params.items():
                        param_obj = {
                            'name': param_name,
                            'value': {}
                        }
                        
                        if isinstance(param_value, int):
                            param_obj['value']['longValue'] = param_value
                        elif isinstance(param_value, float):
                            param_obj['value']['doubleValue'] = param_value
                        elif isinstance(param_value, bool):
                            param_obj['value']['booleanValue'] = param_value
                        elif param_value is None:
                            param_obj['value']['isNull'] = True
                        else:
                            param_obj['value']['stringValue'] = str(param_value)
                        
                        sql_parameters.append(param_obj)
            
            # Execute the query
            try:
                response = self.rds_client.execute_statement(
                    resourceArn=self.resource_arn,
                    secretArn=self.secret_arn,
                    database=self.database,
                    sql=query,
                    parameters=sql_parameters if params else [],
                    includeResultMetadata=True
                )
            except self.rds_client.exceptions.BadRequestException as e:
                print(f"RDS Data API error - bad request: {str(e)}")
                if "syntax" in str(e).lower():
                    print("SQL syntax error detected. Check if your query is compatible with RDS Data API.")
                    print("Note: Some PostgreSQL syntax features may not be fully supported by RDS Data API.")
                return []
            except self.rds_client.exceptions.DatabaseErrorException as e:
                print(f"RDS Data API error - database error: {str(e)}")
                if "function" in str(e).lower() and "does not exist" in str(e).lower():
                    print("Function not found error. The function may not exist or requires different parameters.")
                return []
            except self.rds_client.exceptions.UnsupportedResultException as e:
                print(f"RDS Data API error - unsupported result: {str(e)}")
                if "unsupported data type" in str(e).lower():
                    print("Query contains unsupported data types for RDS Data API.")
                    print("Consider using direct PostgreSQL connection for this query.")
                return []
            except Exception as e:
                print(f"Error executing query using RDS Data API: {str(e)}")
                return []
            
            # Process the results
            if 'records' in response and response['records']:
                # Convert RDS Data API format to list of dictionaries
                results = []
                column_metadata = response.get('columnMetadata', [])
                
                # If we have metadata, use column names, otherwise use generic names
                if column_metadata:
                    column_names = [col.get('name', f'col{i}') for i, col in enumerate(column_metadata)]
                else:
                    # Create generic column names if metadata is missing
                    column_names = [f'col{i}' for i in range(len(response['records'][0]))]
                
                for record in response['records']:
                    row = {}
                    for i, value in enumerate(record):
                        if i >= len(column_names):
                            # Skip if we don't have a column name (shouldn't happen)
                            continue
                            
                        col_name = column_names[i]
                        
                        # Extract the value based on its type
                        if 'stringValue' in value:
                            row[col_name] = value['stringValue']
                        elif 'longValue' in value:
                            row[col_name] = value['longValue']
                        elif 'doubleValue' in value:
                            row[col_name] = value['doubleValue']
                        elif 'booleanValue' in value:
                            row[col_name] = value['booleanValue']
                        elif 'blobValue' in value:
                            row[col_name] = value['blobValue']
                        elif 'isNull' in value and value['isNull']:
                            row[col_name] = None
                        else:
                            # If we can't determine the type, store the raw value
                            row[col_name] = str(value)
                    
                    results.append(row)
                
                return results
            
            # For non-SELECT queries or empty results
            if 'numberOfRecordsUpdated' in response:
                # For DML statements that don't return records
                print(f"Query executed successfully. Rows affected: {response['numberOfRecordsUpdated']}")
                return []
            
            # For EXPLAIN queries that might return in a different format
            if query.lower().startswith('explain'):
                # Try to create a synthetic result for EXPLAIN
                if 'formattedRecords' in response:
                    return [{'EXPLAIN': response['formattedRecords']}]
            
            # If we get here, return an empty list
            return []
        except Exception as e:
            print(f"Error executing query using RDS Data API: {str(e)}")
            return []
    
    def _execute_postgresql(self, query, params=None):
        """Execute query using PostgreSQL connector"""
        try:
            with self.pg_conn.cursor() as cursor:
                cursor.execute(query, params or ())
                
                # For SELECT queries, return results
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    results = []
                    for row in cursor.fetchall():
                        results.append(dict(zip(columns, row)))
                    return results
                
                # For non-SELECT queries, commit and return empty list
                self.pg_conn.commit()
                return []
        except Exception as e:
            self.pg_conn.rollback()
            print(f"Error executing query using PostgreSQL connector: {str(e)}")
            return []

    def analyze_query_complexity(self, query):
        """
        Analyze query complexity and potential resource impact
        
        Args:
            query (str): SQL query to analyze
        
        Returns:
            dict: Complexity metrics
        """
        query_lower = query.lower()
        complexity_score = 0
        warnings = []
        
        # Check for joins
        join_count = sum(1 for join_type in ['join', 'inner join', 'left join', 'right join', 'full join'] 
                        if join_type in query_lower)
        complexity_score += join_count * 2
        if join_count > 3:
            warnings.append(f"Query contains {join_count} joins - consider simplifying")
        
        # Check for subqueries
        subquery_count = query_lower.count('(select')
        complexity_score += subquery_count * 3
        if subquery_count > 2:
            warnings.append(f"Query contains {subquery_count} subqueries - consider restructuring")
        
        # Check for aggregations
        agg_functions = ['count(', 'sum(', 'avg(', 'max(', 'min(']
        agg_count = sum(query_lower.count(func) for func in agg_functions)
        complexity_score += agg_count
        
        # Check for window functions (PostgreSQL specific)
        if 'over(' in query_lower or 'partition by' in query_lower:
            complexity_score += 3
            warnings.append("Query uses window functions - monitor performance")
        
        # Check for complex WHERE conditions
        where_pos = query_lower.find('where')
        if where_pos != -1:
            where_clause = query_lower[where_pos:]
            and_count = where_clause.count(' and ')
            or_count = where_clause.count(' or ')
            complexity_score += (and_count + or_count)
            if (and_count + or_count) > 5:
                warnings.append(f"Complex WHERE clause with {and_count + or_count} conditions")
        
        # Check for ORDER BY with multiple columns
        order_by_pos = query_lower.find('order by')
        if order_by_pos != -1:
            order_clause = query_lower[order_by_pos:]
            comma_count = order_clause.count(',')
            complexity_score += comma_count
            if comma_count > 2:
                warnings.append(f"ORDER BY with {comma_count + 1} columns may impact performance")
        
        return {
            'complexity_score': complexity_score,
            'warnings': warnings,
            'join_count': join_count,
            'subquery_count': subquery_count,
            'aggregation_count': agg_count
        }
