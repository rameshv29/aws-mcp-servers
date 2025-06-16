#!/bin/bash
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

# Default connection pool configuration
export POSTGRES_POOL_MIN_SIZE=${POSTGRES_POOL_MIN_SIZE:-5}
export POSTGRES_POOL_MAX_SIZE=${POSTGRES_POOL_MAX_SIZE:-30}

# Log the connection pool configuration
echo "PostgreSQL Connection Pool Configuration:"
echo "  - Minimum connections: $POSTGRES_POOL_MIN_SIZE"
echo "  - Maximum connections: $POSTGRES_POOL_MAX_SIZE"

# Run the PostgreSQL MCP server with proper initialization
cd "$(dirname "$0")"
python -m awslabs.postgresql_mcp_server.main --host 0.0.0.0 --port 8000 --session-timeout 1800 --request-timeout 300
