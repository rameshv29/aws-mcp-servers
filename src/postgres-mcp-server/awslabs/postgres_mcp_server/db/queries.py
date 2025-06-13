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

"""
Common database queries used by the PostgreSQL MCP server.
"""

# Query to get table information
GET_TABLE_INFO = """
SELECT
    c.relname as table_name,
    c.reltuples::bigint as row_count,
    pg_size_pretty(pg_total_relation_size(c.oid)) as total_size,
    pg_size_pretty(pg_relation_size(c.oid)) as table_size,
    pg_size_pretty(pg_total_relation_size(c.oid) - pg_relation_size(c.oid)) as index_size,
    (SELECT count(*) FROM pg_index i WHERE i.indrelid = c.oid) as index_count,
    obj_description(c.oid, 'pg_class') as description
FROM
    pg_class c
JOIN
    pg_namespace n ON n.oid = c.relnamespace
WHERE
    c.relkind = 'r'
    AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    AND c.relname = %s
    AND n.nspname = %s
"""

# Query to get column information for a table
GET_COLUMN_INFO = """
SELECT
    a.attname as column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type,
    CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END as is_nullable,
    (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid)
     FROM pg_catalog.pg_attrdef d
     WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum
     AND a.atthasdef) as column_default,
    col_description(a.attrelid, a.attnum) as description
FROM
    pg_catalog.pg_attribute a
JOIN
    pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN
    pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE
    a.attnum > 0
    AND NOT a.attisdropped
    AND c.relname = %s
    AND n.nspname = %s
ORDER BY
    a.attnum
"""

# Query to get index information for a table
GET_INDEX_INFO = """
SELECT
    i.relname as index_name,
    a.attname as column_name,
    am.amname as index_type,
    idx.indisunique as is_unique,
    idx.indisprimary as is_primary,
    pg_get_indexdef(idx.indexrelid) as index_definition,
    pg_size_pretty(pg_relation_size(i.oid)) as index_size,
    s.idx_scan as index_scans
FROM
    pg_index idx
JOIN
    pg_class i ON i.oid = idx.indexrelid
JOIN
    pg_class c ON c.oid = idx.indrelid
JOIN
    pg_namespace n ON n.oid = c.relnamespace
JOIN
    pg_am am ON am.oid = i.relam
JOIN
    pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(idx.indkey)
LEFT JOIN
    pg_stat_all_indexes s ON s.indexrelid = i.oid
WHERE
    c.relname = %s
    AND n.nspname = %s
ORDER BY
    i.relname, a.attnum
"""

# Query to get table statistics
GET_TABLE_STATS = """
SELECT
    relname as table_name,
    seq_scan,
    seq_tup_read,
    idx_scan,
    idx_tup_fetch,
    n_tup_ins,
    n_tup_upd,
    n_tup_del,
    n_live_tup,
    n_dead_tup,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze
FROM
    pg_stat_user_tables
WHERE
    relname = %s
"""

# Query to get foreign key relationships
GET_FOREIGN_KEYS = """
SELECT
    conname as constraint_name,
    pg_catalog.pg_get_constraintdef(c.oid, true) as constraint_definition,
    conrelid::regclass as table_name,
    confrelid::regclass as referenced_table,
    confupdtype as update_action,
    confdeltype as delete_action
FROM
    pg_catalog.pg_constraint c
JOIN
    pg_catalog.pg_namespace n ON n.oid = c.connamespace
WHERE
    c.contype = 'f'
    AND (conrelid::regclass::text = %s OR confrelid::regclass::text = %s)
    AND n.nspname NOT IN ('pg_catalog', 'information_schema')
"""

# Query to get slow queries from pg_stat_statements
GET_SLOW_QUERIES = """
SELECT
    query,
    calls,
    total_exec_time / calls as avg_exec_time_ms,
    total_exec_time as total_time_ms,
    rows / calls as avg_rows,
    max_exec_time as max_time_ms,
    mean_exec_time as mean_time_ms,
    stddev_exec_time as stddev_time_ms,
    min_exec_time as min_time_ms
FROM
    pg_stat_statements
WHERE
    total_exec_time / calls >= %s
ORDER BY
    avg_exec_time_ms DESC
LIMIT %s
"""

# Query to check if pg_stat_statements extension is installed
CHECK_PG_STAT_STATEMENTS = """
SELECT
    COUNT(*) as count
FROM
    pg_extension
WHERE
    extname = 'pg_stat_statements'
"""

# Query to get database size
GET_DATABASE_SIZE = """
SELECT
    pg_size_pretty(pg_database_size(current_database())) as database_size
"""

# Query to get all tables in the current database
GET_ALL_TABLES = """
SELECT
    c.relname as table_name,
    n.nspname as schema_name,
    c.reltuples::bigint as row_count,
    pg_size_pretty(pg_total_relation_size(c.oid)) as total_size
FROM
    pg_class c
JOIN
    pg_namespace n ON n.oid = c.relnamespace
WHERE
    c.relkind = 'r'
    AND n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY
    pg_total_relation_size(c.oid) DESC
"""

# Query to get table bloat information
GET_TABLE_BLOAT = """
SELECT
    schemaname,
    tablename,
    pg_size_pretty(bloat_size) as bloat_size,
    round(bloat_ratio * 100, 2) as bloat_ratio
FROM (
    SELECT
        schemaname,
        tablename,
        bs*tblpages as real_size,
        (tblpages-est_tblpages)*bs as bloat_size,
        CASE WHEN tblpages > 0 THEN (tblpages-est_tblpages)/tblpages::float ELSE 0 END as bloat_ratio
    FROM (
        SELECT
            schemaname,
            tablename,
            (data_length+(CASE WHEN otta>0 THEN otta*(ceil(fillfactor/100.0)) ELSE 0 END))::bigint as est_tblpages,
            tblpages,
            bs
        FROM (
            SELECT
                n.nspname as schemaname,
                c.relname as tablename,
                (CASE WHEN c.relhassubclass=TRUE THEN 0 ELSE (array_to_string(c.reloptions, ' ')::text~'.*fillfactor=([0-9]+).*')::boolean END) as fillfactor,
                CASE WHEN c.relhassubclass=TRUE THEN 0 ELSE (regexp_matches(array_to_string(c.reloptions, ' '), 'fillfactor=([0-9]+)'))[1]::int END as fillfactor_value,
                CASE WHEN c.relhassubclass=TRUE THEN 0 ELSE (CASE WHEN c.reltoastrelid = 0 THEN NULL ELSE (SELECT 100 FROM pg_toast.pg_toast_2619 WHERE reltoastrelid = c.reltoastrelid LIMIT 1) END) END as toast_fillfactor,
                current_setting('block_size')::numeric as bs,
                CASE WHEN version()~'mingw32' OR version()~'64-bit|x86_64|ppc64|ia64|amd64' THEN 8 ELSE 4 END as ma,
                24 as page_hdr,
                23 + CASE WHEN MAX(coalesce(s.null_frac,0)) > 0 THEN ( 7 + count(*) ) / 8 ELSE 0::int END as tpl_hdr_size,
                sum( (1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 1024) ) as data_length,
                max(s.null_frac) as max_null_frac,
                8 as alignment,
                reltuples,
                relpages as tblpages,
                coalesce(substring(array_to_string(c.reloptions, ' ') from 'fillfactor=([0-9]+)')::smallint, 100) as fillfactor,
                reltoastrelid
            FROM
                pg_class c
            JOIN
                pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN
                pg_stats s ON s.schemaname = n.nspname AND s.tablename = c.relname
            WHERE
                c.relkind = 'r'
                AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                AND c.relname = %s
            GROUP BY
                n.nspname, c.relname, bs, ma, page_hdr, fillfactor, reltuples, relpages, reltoastrelid, relhassubclass
        ) as foo
    ) as foo2
) as foo3
WHERE
    bloat_ratio > 0.1
    AND bloat_size > 1024 * 1024  -- Only show tables with at least 1MB of bloat
"""

# Query to get index bloat information
GET_INDEX_BLOAT = """
SELECT
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(bloat_size) as bloat_size,
    round(bloat_ratio * 100, 2) as bloat_ratio
FROM (
    SELECT
        schemaname,
        tablename,
        indexname,
        bs*relpages as real_size,
        (relpages-est_pages)*bs as bloat_size,
        CASE WHEN relpages > 0 THEN (relpages-est_pages)/relpages::float ELSE 0 END as bloat_ratio
    FROM (
        SELECT
            n.nspname as schemaname,
            c.relname as tablename,
            i.relname as indexname,
            current_setting('block_size')::numeric as bs,
            i.relpages,
            (CASE WHEN version()~'mingw32' OR version()~'64-bit|x86_64|ppc64|ia64|amd64' THEN 8 ELSE 4 END)::float as nulldatawidth,
            (CASE WHEN i.reltuples > 0 THEN ceil((i.relpages * (current_setting('block_size')::numeric) - 
                (CASE WHEN version()~'mingw32' OR version()~'64-bit|x86_64|ppc64|ia64|amd64' THEN 8 ELSE 4 END)::float) / 
                (i.reltuples * (CASE WHEN version()~'mingw32' OR version()~'64-bit|x86_64|ppc64|ia64|amd64' THEN 8 ELSE 4 END)::float)) ELSE 0 END)::float as est_pages
        FROM
            pg_class c
        JOIN
            pg_namespace n ON n.oid = c.relnamespace
        JOIN
            pg_index x ON x.indrelid = c.oid
        JOIN
            pg_class i ON i.oid = x.indexrelid
        WHERE
            c.relkind = 'r'
            AND n.nspname NOT IN ('pg_catalog', 'information_schema')
            AND c.relname = %s
    ) as foo
) as foo2
WHERE
    bloat_ratio > 0.1
    AND bloat_size > 1024 * 1024  -- Only show indexes with at least 1MB of bloat
"""
