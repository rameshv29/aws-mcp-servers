# PostgreSQL MCP Server - Testing Guide

This document describes the test files available for validating the PostgreSQL MCP Server.

## Test Files

### Core Server Tests

#### `test_all_tools_simple.py`
**Purpose**: Comprehensive validation of all SQL queries used by the 10 MCP tools
- Tests all underlying SQL queries against the actual RDS database
- Validates data types and response formats
- Confirms RDS Data API compatibility
- **Usage**: `python test_all_tools_simple.py`
- **Expected Result**: 10/10 SQL queries should pass

#### `test_new_tools.py`
**Purpose**: Specific validation of the 3 newly added tools
- Tests `health_check`, `analyze_vacuum_stats`, and `recommend_indexes`
- Validates type conversions and error handling
- Tests RDS Data API compatibility fixes
- **Usage**: `python test_new_tools.py`
- **Expected Result**: 3/3 new tools should pass

#### `test_type_conversions.py`
**Purpose**: Validates critical type conversion fixes
- Tests string to float conversions for bloat percentages
- Validates table fragmentation analysis type handling
- Ensures no "str vs float" comparison errors
- **Usage**: `python test_type_conversions.py`
- **Expected Result**: Type conversion tests should pass

### Legacy Tests

#### `tests/test_server.py`
**Purpose**: Original unit tests from the base implementation
- Legacy test file from the original server
- May not be compatible with current consolidated server
- Kept for reference but not actively maintained

## Running Tests

### Prerequisites
```bash
# Activate virtual environment
source .venv/bin/activate

# Set AWS profile
export AWS_PROFILE=mcp_profile
export AWS_REGION=us-west-2
```

### Run All Tests
```bash
# Test all SQL queries (most comprehensive)
python test_all_tools_simple.py

# Test new tools specifically
python test_new_tools.py

# Test type conversions
python test_type_conversions.py
```

### Expected Results
- **All tests should pass** for a healthy server
- **SQL syntax errors** indicate query compatibility issues
- **Type conversion errors** indicate RDS Data API data type issues
- **Connection errors** indicate AWS credentials or network issues

## Test Coverage

The test suite covers:
- ✅ All 10 MCP tools
- ✅ All SQL queries and syntax
- ✅ RDS Data API compatibility
- ✅ Type conversions (string ↔ numeric)
- ✅ Error handling and edge cases
- ✅ JSON response formatting

## Troubleshooting

### Common Issues
1. **AWS Credentials**: Ensure `mcp_profile` is configured
2. **Database Access**: Verify RDS cluster is accessible
3. **Extensions**: Some tests require `pg_stat_statements` extension
4. **Data Types**: RDS Data API doesn't support PostgreSQL arrays

### Test Failures
- Review error messages for specific SQL syntax issues
- Check AWS credentials and database connectivity
- Verify RDS Data API permissions and cluster status
