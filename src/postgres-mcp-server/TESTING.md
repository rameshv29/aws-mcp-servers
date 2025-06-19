# PostgreSQL MCP Server - Testing Guide

This document describes the test files available for validating the PostgreSQL MCP Server.

## Test Directory Structure

All test files are organized in the `tests/` directory following Python project conventions:

```
tests/
├── test_all_tools_simple.py     # Comprehensive SQL query validation
├── test_new_tools.py            # New tools validation
├── test_type_conversions.py     # Type conversion validation
└── test_server.py               # Legacy unit tests
```

## Test Files

### Core Server Tests

#### `tests/test_all_tools_simple.py`
**Purpose**: Comprehensive validation of all SQL queries used by the 10 MCP tools
- Tests all underlying SQL queries against the actual RDS database
- Validates data types and response formats
- Confirms RDS Data API compatibility
- **Usage**: `python tests/test_all_tools_simple.py`
- **Expected Result**: 10/10 SQL queries should pass

#### `tests/test_new_tools.py`
**Purpose**: Specific validation of the 3 newly added tools
- Tests `health_check`, `analyze_vacuum_stats`, and `recommend_indexes`
- Validates type conversions and error handling
- Tests RDS Data API compatibility fixes
- **Usage**: `python tests/test_new_tools.py`
- **Expected Result**: 3/3 new tools should pass

#### `tests/test_type_conversions.py`
**Purpose**: Validates critical type conversion fixes
- Tests string to float conversions for bloat percentages
- Validates table fragmentation analysis type handling
- Ensures no "str vs float" comparison errors
- **Usage**: `python tests/test_type_conversions.py`
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

### Run Individual Tests
```bash
# Test all SQL queries (most comprehensive)
python tests/test_all_tools_simple.py

# Test new tools specifically
python tests/test_new_tools.py

# Test type conversions
python tests/test_type_conversions.py

# Run legacy unit tests
python tests/test_server.py
```

### Run All Tests
```bash
# Run all tests using pytest (if available)
pytest tests/

# Or run all tests manually
python tests/test_all_tools_simple.py && \
python tests/test_new_tools.py && \
python tests/test_type_conversions.py
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

## Development Workflow

### Adding New Tests
1. Create new test files in the `tests/` directory
2. Follow the naming convention: `test_<feature_name>.py`
3. Include comprehensive error handling and validation
4. Update this documentation with new test descriptions

### Before Committing Changes
```bash
# Run the comprehensive test suite
python tests/test_all_tools_simple.py
python tests/test_new_tools.py
python tests/test_type_conversions.py
```

All tests should pass before pushing changes to the repository.
