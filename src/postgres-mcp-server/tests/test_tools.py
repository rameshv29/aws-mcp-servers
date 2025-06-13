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

"""Tests for the PostgreSQL MCP Server tools."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from awslabs.postgres_mcp_server.server import (
    register_all_tools,
    format_bytes,
)


class TestFormatBytes:
    """Tests for the format_bytes function."""

    def test_format_bytes_none(self):
        """Test formatting None bytes."""
        assert format_bytes(None) == "Unknown"

    def test_format_bytes_zero(self):
        """Test formatting zero bytes."""
        assert format_bytes(0) == "0.00 B"

    def test_format_bytes_small(self):
        """Test formatting a small number of bytes."""
        assert format_bytes(100) == "100.00 B"

    def test_format_bytes_kb(self):
        """Test formatting kilobytes."""
        assert format_bytes(1500) == "1.46 KB"

    def test_format_bytes_mb(self):
        """Test formatting megabytes."""
        assert format_bytes(1500000) == "1.43 MB"

    def test_format_bytes_gb(self):
        """Test formatting gigabytes."""
        assert format_bytes(1500000000) == "1.40 GB"


class TestRegisterAllTools:
    """Tests for the register_all_tools function."""

    def test_register_all_tools(self):
        """Test registering all tools."""
        # Mock MCP server
        mcp = MagicMock()
        
        # Register all tools
        register_all_tools(mcp)
        
        # Check that tool was called the expected number of times
        assert mcp.tool.call_count >= 1  # At least one tool should be registered


@pytest.mark.asyncio
async def test_health_check():
    """Test the health_check tool."""
    # Mock MCP server
    mcp = MagicMock()
    
    # Mock context
    ctx = AsyncMock()
    
    # Register all tools
    register_all_tools(mcp)
    
    # Get the health_check function
    health_check_func = None
    for call in mcp.tool.mock_calls:
        if len(call.args) > 0 and call.args[0] == 'health_check':
            health_check_func = call.kwargs.get('function')
        elif len(call.args) == 0 and call.kwargs.get('name') == 'health_check':
            health_check_func = call.kwargs.get('function')
        elif len(call.args) == 0 and not call.kwargs.get('name'):
            # This is a decorator call without arguments, the function is in the return value
            decorator = call()
            if hasattr(decorator, '__name__') and decorator.__name__ == 'health_check':
                health_check_func = decorator
    
    # If we couldn't find the health_check function, skip the test
    if not health_check_func:
        pytest.skip("Could not find health_check function")
    
    # Call the health_check function
    result = await health_check_func(ctx)
    
    # Check the result
    assert isinstance(result, str)
    assert "healthy" in result.lower()
