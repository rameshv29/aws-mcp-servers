# Q Chat Integration Guide

## Quick Setup for Amazon Q Developer CLI

### 1. MCP Configuration

Add to your `~/.aws/amazonq/mcp.json`:

```json
{
  "mcpServers": {
    "postgresql-enhanced": {
      "command": "python",
      "args": [
        "-m", "awslabs.postgres_mcp_server.server",
        "--transport", "stdio",
        "--secret-arn", "arn:aws:secretsmanager:us-west-2:123456789012:secret:my-db-secret-AbCdEf",
        "--resource-arn", "arn:aws:rds:us-west-2:123456789012:cluster:my-cluster",
        "--database", "mydb",
        "--readonly"
      ],
      "disabled": false,
      "autoApprove": []
    }
  }
}
```

### 2. Environment Variables (Alternative)

Set environment variables and use minimal MCP config:

```bash
export POSTGRES_SECRET_ARN="arn:aws:secretsmanager:us-west-2:123456789012:secret:my-db-secret-AbCdEf"
export POSTGRES_RESOURCE_ARN="arn:aws:rds:us-west-2:123456789012:cluster:my-cluster"
export POSTGRES_DATABASE="mydb"
export POSTGRES_READONLY="true"
```

```json
{
  "mcpServers": {
    "postgresql-enhanced": {
      "command": "python",
      "args": ["-m", "awslabs.postgres_mcp_server.server"],
      "disabled": false,
      "autoApprove": []
    }
  }
}
```

### 3. Direct PostgreSQL Connection

For direct PostgreSQL connections (non-RDS):

```json
{
  "mcpServers": {
    "postgresql-enhanced": {
      "command": "python",
      "args": [
        "-m", "awslabs.postgres_mcp_server.server",
        "--secret-arn", "arn:aws:secretsmanager:us-west-2:123456789012:secret:my-db-secret-AbCdEf",
        "--hostname", "my-postgres-server.example.com",
        "--database", "mydb",
        "--port", "5432"
      ],
      "disabled": false,
      "autoApprove": []
    }
  }
}
```

## Example Q Chat Conversations

### Database Analysis
```
You: "Analyze the structure of my PostgreSQL database and provide optimization recommendations"

Q Chat: I'll analyze your database structure using the PostgreSQL MCP server.

[Calls analyze_database_structure tool]

Based on the analysis of your database:

**Database Overview:**
- 3 schemas: public, analytics, staging
- 15 tables with 2.3GB total size
- 23 indexes consuming 450MB

**Key Findings:**
- Table 'orders' (1.2M rows) lacks indexes on frequently queried columns
- 3 tables show foreign key relationships forming a proper normalized structure
- Table 'logs' has 25% dead tuples indicating need for vacuum

**Recommendations:**
1. Add index on orders.customer_id for better join performance
2. Run VACUUM on logs table to reclaim space
3. Consider partitioning the large events table by date
```

### Query Optimization
```
You: "This query is running slowly: SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id WHERE c.region = 'US' ORDER BY o.created_at DESC LIMIT 100"

Q Chat: I'll analyze this query's performance and provide optimization recommendations.

[Calls analyze_query_performance and recommend_indexes tools]

**Query Performance Analysis:**
- Current execution time: 2.3 seconds
- Using sequential scan on customers table
- Expensive sort operation on orders.created_at

**Index Recommendations:**
1. CREATE INDEX idx_customers_region ON customers (region);
2. CREATE INDEX idx_orders_created_at ON orders (created_at DESC);
3. CREATE INDEX idx_orders_customer_id ON orders (customer_id);

**Expected Improvement:** 70-85% performance improvement with these indexes.
```

### Database Health Check
```
You: "Check the health of my PostgreSQL database and identify any maintenance needs"

Q Chat: I'll perform a comprehensive health check of your database.

[Calls multiple analysis tools: analyze_vacuum_stats, analyze_table_fragmentation, identify_slow_queries]

**Database Health Report:**

**Vacuum Status:** ✅ Good
- Autovacuum is enabled and running regularly
- Most tables have <5% dead tuples

**Fragmentation:** ⚠️ Attention Needed
- Table 'user_sessions' has 18% bloat (45MB wasted space)
- Index 'idx_events_timestamp' shows signs of bloat

**Slow Queries:** ⚠️ 3 queries averaging >1 second
- Complex reporting query needs optimization
- Missing index on frequently filtered columns

**Immediate Actions:**
1. Run VACUUM FULL on user_sessions table
2. Add index on events.user_id column
3. Consider query rewrite for the reporting dashboard
```

## Available Tools

### Core Database Operations
- `run_query` - Execute SQL queries
- `get_table_schema` - Get table structure
- `connect_database` - Test connections
- `health_check` - Server status

### Advanced Analysis Tools
- `analyze_database_structure` - Complete database analysis
- `analyze_query_performance` - Query optimization analysis
- `recommend_indexes` - Smart index recommendations
- `analyze_table_fragmentation` - Bloat and fragmentation analysis
- `analyze_vacuum_stats` - Vacuum optimization
- `identify_slow_queries` - Performance bottleneck identification
- `show_postgresql_settings` - Configuration analysis

## Tips for Best Results

1. **Be Specific**: Ask for specific analysis types for focused results
2. **Provide Context**: Mention performance issues or specific tables of concern
3. **Follow Recommendations**: The analysis tools provide actionable suggestions
4. **Regular Monitoring**: Use the tools periodically to maintain database health

## Troubleshooting

### Common Issues

**"pg_stat_statements extension not available"**
- Solution: Enable the extension in your PostgreSQL instance
- For RDS: Add to parameter group and restart instance

**"No database connection available"**
- Check your secret ARN and resource ARN are correct
- Verify AWS credentials have access to Secrets Manager and RDS
- Ensure the database is accessible from your network

**"Permission denied for table pg_constraint"**
- Grant necessary permissions to your database user
- Consider using a user with pg_read_all_stats role

---

Ready to use with Amazon Q Developer CLI! 🚀
