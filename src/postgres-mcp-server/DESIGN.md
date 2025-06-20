# PostgreSQL MCP Server - Multi-Database Architecture Design

## Overview

This document outlines the design for supporting multiple PostgreSQL databases within a single MCP server instance, addressing the challenge of managing 100+ databases without requiring 100+ server processes.

## Problem Statement

### Current Limitations
- **Single Database per Server**: Each server instance can only connect to one database
- **Resource Inefficiency**: 100 databases require 100 server processes
- **Operational Complexity**: Managing 100+ ports, configurations, and deployments
- **Streamable-HTTP Challenge**: HTTP clients cannot pass database parameters like stdio clients

### Requirements
1. **Resource Efficiency**: Single server process for multiple databases
2. **Backward Compatibility**: Existing single-database usage must continue to work
3. **Transport Flexibility**: Support both stdio and streamable-http transports
4. **Operational Simplicity**: Single configuration file and deployment
5. **Scalability**: Dynamic database addition without server restart

## Architecture Design

### Multi-Tenant Single Server Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                PostgreSQL MCP Server                        │
├─────────────────────────────────────────────────────────────┤
│  Multi-Database Manager                                     │
│  ┌─────────────────┐ ┌─────────────────┐ ┌───────────────┐ │
│  │   Database 1    │ │   Database 2    │ │  Database N   │ │
│  │  (RDS Data API) │ │ (Direct Postgres)│ │   (Mixed)     │ │
│  │                 │ │                 │ │               │ │
│  │ Connection Pool │ │ Connection Pool │ │Connection Pool│ │
│  └─────────────────┘ └─────────────────┘ └───────────────┘ │
├─────────────────────────────────────────────────────────────┤
│  Transport Layer (stdio | streamable-http | sse)           │
└─────────────────────────────────────────────────────────────┘
```

## Implementation Phases

### Phase 1: Backward Compatibility ✅ (Current Implementation)
**Goal**: Add optional database selection while maintaining existing behavior

#### Changes:
1. **Optional database_id parameter** added to all MCP tools
2. **Default database concept** - uses existing single database if no database_id specified
3. **No breaking changes** - all existing usage patterns continue to work
4. **Foundation classes** for multi-database support

#### Tool Interface:
```python
@mcp.tool()
async def run_query(
    sql: str,
    database_id: Optional[str] = None,  # NEW: Optional database selector
    ctx: Context
) -> List[Dict[str, Any]]:
    # Uses default database if database_id not provided
```

### Phase 2: Multi-Database Support 🚧 (Future)
**Goal**: Full multi-database configuration and management

#### Features:
- Configuration file support for multiple databases
- Database discovery and listing tools
- Per-database connection management
- Health monitoring per database

### Phase 3: Advanced Features 🔮 (Future)
**Goal**: Production-ready multi-database operations

#### Features:
- Dynamic database addition/removal
- Per-database connection pooling
- Database-specific settings and policies
- Advanced monitoring and metrics

## Configuration Design

### Single Database Mode (Current - Backward Compatible)
```bash
# Command line (existing approach)
python -m awslabs.postgres_mcp_server.server \
  --resource_arn "arn:aws:rds:region:account:cluster:primary" \
  --secret_arn "arn:aws:secretsmanager:region:account:secret:primary" \
  --database "production" \
  --region "us-west-2" \
  --readonly "true"
```

### Multi-Database Mode (Phase 2)
```yaml
# config.yaml
default_database: "primary"
databases:
  primary:
    type: "rds_data_api"
    resource_arn: "arn:aws:rds:region:account:cluster:primary"
    secret_arn: "arn:aws:secretsmanager:region:account:secret:primary"
    database: "production"
    region: "us-west-2"
    readonly: true
  
  analytics:
    type: "direct_postgres"
    hostname: "analytics.company.com"
    port: 5432
    secret_arn: "arn:aws:secretsmanager:region:account:secret:analytics"
    database: "warehouse"
    region: "us-west-2"
    readonly: true
```

## Transport Support

### stdio Transport (Q Chat)
```json
{
  "mcpServers": {
    "postgresql-enhanced": {
      "command": "python",
      "args": [
        "-m", "awslabs.postgres_mcp_server.server",
        "--resource_arn", "arn:aws:rds:region:account:cluster:primary",
        "--database", "production"
      ]
    }
  }
}
```

### streamable-http Transport
```bash
# Environment variables for server configuration
export POSTGRES_RESOURCE_ARN="arn:aws:rds:region:account:cluster:primary"
export POSTGRES_DATABASE="production"
export FASTMCP_HOST="0.0.0.0"
export FASTMCP_PORT="8000"

python -m awslabs.postgres_mcp_server.server --transport streamable-http
```

## Client Usage Patterns

### Backward Compatible (Single Database)
```python
# Existing usage - no changes required
await client.call_tool("run_query", {
    "sql": "SELECT * FROM users"
})
```

### Multi-Database (Phase 2)
```python
# Specify database explicitly
await client.call_tool("run_query", {
    "sql": "SELECT * FROM users",
    "database_id": "production"
})

await client.call_tool("run_query", {
    "sql": "SELECT * FROM analytics_data",
    "database_id": "analytics"
})

# List available databases
databases = await client.call_tool("list_databases", {})
```

## Benefits Analysis

### Resource Efficiency
| Approach | Servers | Ports | Memory | CPU | Maintenance |
|----------|---------|-------|--------|-----|-------------|
| **100 Single-DB Servers** | 100 | 8000-8099 | ~10GB | High | Complex |
| **1 Multi-DB Server** | 1 | 8000 | ~1GB | Low | Simple |
| **Savings** | 99% less | 99% less | 90% less | 80% less | 95% less |

### Operational Benefits
- **Single Configuration**: One file vs 100 files
- **Single Deployment**: One container vs 100 containers
- **Unified Monitoring**: One health endpoint vs 100 endpoints
- **Simplified Networking**: One port vs 100 ports
- **Easier Scaling**: Add databases to config vs deploy new servers

## Migration Strategy

### For Existing Users
1. **No immediate changes required** - existing single-database usage continues to work
2. **Gradual migration** - can add database_id parameter when ready
3. **Configuration flexibility** - choose single-database or multi-database mode

### For New Users
1. **Start with single-database** for simplicity
2. **Upgrade to multi-database** when managing multiple databases
3. **Full flexibility** from day one

## Risk Mitigation

### Backward Compatibility Risks
- **Mitigation**: Extensive testing of existing usage patterns
- **Validation**: All current MCP tools work without database_id parameter

### Performance Risks
- **Connection pooling**: Per-database connection limits
- **Resource isolation**: Database-specific resource management
- **Monitoring**: Per-database health checks and metrics

### Security Risks
- **Credential isolation**: Separate secrets per database
- **Access control**: Database-specific readonly settings
- **Audit logging**: Per-database query logging

## Success Metrics

### Phase 1 Success Criteria
- ✅ All existing functionality works unchanged
- ✅ Optional database_id parameter added to all tools
- ✅ Foundation for multi-database support established
- ✅ No performance regression
- ✅ Comprehensive test coverage

### Phase 2 Success Criteria (Future)
- Configuration file support for multiple databases
- Database discovery and management tools
- Per-database connection management
- Health monitoring per database

## Conclusion

The multi-database architecture provides a scalable, resource-efficient solution for managing multiple PostgreSQL databases within a single MCP server instance. Phase 1 implementation maintains full backward compatibility while establishing the foundation for future multi-database capabilities.

This design addresses the core challenge of managing 100+ databases without requiring 100+ server processes, while maintaining operational simplicity and client flexibility.
