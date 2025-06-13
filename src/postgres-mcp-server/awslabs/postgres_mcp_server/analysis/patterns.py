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
Functions for detecting patterns in PostgreSQL queries.
"""
import re
from typing import List, Dict, Tuple, Optional

def detect_query_patterns(plan_json):
    """
    Detect patterns in the query execution plan.
    
    Args:
        plan_json: Query execution plan in JSON format
        
    Returns:
        List of dictionaries with pattern information
    """
    patterns = []
    
    if not plan_json or 'Plan' not in plan_json:
        return patterns
    
    # Extract plan nodes
    nodes = extract_plan_nodes(plan_json['Plan'])
    
    # Check for sequential scans
    seq_scans = [node for node in nodes if node.get('Node Type') == 'Seq Scan']
    if seq_scans:
        tables = [node.get('Relation Name', 'Unknown') for node in seq_scans]
        patterns.append({
            'pattern': 'Sequential Scan',
            'description': f"Sequential scan detected on tables: {', '.join(tables)}",
            'impact': 'High',
            'suggestion': 'Consider adding indexes to improve query performance'
        })
    
    # Check for hash joins
    hash_joins = [node for node in nodes if node.get('Node Type') == 'Hash Join']
    if hash_joins and len(hash_joins) > 2:
        patterns.append({
            'pattern': 'Multiple Hash Joins',
            'description': f"Query uses {len(hash_joins)} hash joins",
            'impact': 'Medium',
            'suggestion': 'For large tables, consider optimizing join order or adding indexes'
        })
    
    # Check for nested loops with many iterations
    nested_loops = [node for node in nodes if node.get('Node Type') == 'Nested Loop']
    if nested_loops and len(nested_loops) > 2:
        patterns.append({
            'pattern': 'Multiple Nested Loops',
            'description': f"Query uses {len(nested_loops)} nested loops",
            'impact': 'Medium',
            'suggestion': 'Nested loops can be inefficient for large datasets. Consider adding indexes or rewriting the query.'
        })
    
    # Check for sorts
    sorts = [node for node in nodes if node.get('Node Type') == 'Sort']
    if sorts:
        patterns.append({
            'pattern': 'Explicit Sort',
            'description': f"Query requires sorting results",
            'impact': 'Medium',
            'suggestion': 'Consider adding an index that matches your ORDER BY clause'
        })
    
    # Check for high-cost operations
    if 'Total Cost' in plan_json['Plan'] and plan_json['Plan']['Total Cost'] > 1000:
        patterns.append({
            'pattern': 'High Cost Query',
            'description': f"Query has a high estimated cost: {plan_json['Plan']['Total Cost']:.2f}",
            'impact': 'High',
            'suggestion': 'Review query structure and consider optimization'
        })
    
    return patterns

def extract_plan_nodes(plan):
    """
    Recursively extract all nodes from the execution plan.
    
    Args:
        plan: Plan node to extract from
        
    Returns:
        List of plan nodes
    """
    nodes = [plan]
    
    if 'Plans' in plan:
        for subplan in plan['Plans']:
            nodes.extend(extract_plan_nodes(subplan))
    
    return nodes

def detect_query_anti_patterns(query: str) -> List[Dict[str, str]]:
    """
    Detect anti-patterns in the SQL query.
    
    Args:
        query: SQL query to analyze
        
    Returns:
        List of dictionaries with anti-pattern information
    """
    anti_patterns = []
    query_lower = query.lower()
    
    # Check for SELECT *
    if re.search(r'select\s+\*\s+from', query_lower):
        anti_patterns.append({
            'issue': 'Using SELECT *',
            'suggestion': 'Specify only the columns you need instead of using SELECT *',
            'example': 'SELECT id, name, email FROM users  # Instead of SELECT * FROM users'
        })
    
    # Check for LIKE with leading wildcard
    if re.search(r'like\s+[\'"]%', query_lower):
        anti_patterns.append({
            'issue': 'LIKE with leading wildcard',
            'suggestion': 'Queries with leading wildcards (LIKE \'%text\') cannot use indexes effectively',
            'example': 'Consider using a full-text search index or trigram indexes for this type of search'
        })
    
    # Check for functions on indexed columns
    function_patterns = [
        r'(lower|upper|substring|trim|to_char|date_trunc)\s*\(\s*(\w+)',
        r'(\w+)\s*(\+|\-|\*|\/)'
    ]
    
    for pattern in function_patterns:
        if re.search(pattern, query_lower):
            anti_patterns.append({
                'issue': 'Function on column in WHERE clause',
                'suggestion': 'Applying functions to columns in WHERE clauses prevents index usage',
                'example': 'WHERE email = lower(\'EMAIL\')  # Instead use: WHERE lower(email) = lower(\'EMAIL\') and create a functional index'
            })
            break
    
    # Check for OR conditions
    if re.search(r'where.*?\s+or\s+', query_lower):
        anti_patterns.append({
            'issue': 'Multiple OR conditions',
            'suggestion': 'Multiple OR conditions can lead to suboptimal query plans',
            'example': 'Consider using UNION or IN clauses instead, or ensure indexes exist for all OR conditions'
        })
    
    # Check for implicit conversions
    type_patterns = [
        r'::text', r'::int', r'::timestamp', r'::date',
        r'to_char', r'to_number', r'to_date', r'to_timestamp'
    ]
    
    for pattern in type_patterns:
        if pattern in query_lower:
            anti_patterns.append({
                'issue': 'Implicit type conversion',
                'suggestion': 'Type conversions can prevent index usage',
                'example': 'Store data in the correct type to avoid conversions in queries'
            })
            break
    
    # Check for NOT IN or NOT EXISTS
    if re.search(r'not\s+in\s*\(', query_lower) or 'not exists' in query_lower:
        anti_patterns.append({
            'issue': 'Using NOT IN or NOT EXISTS',
            'suggestion': 'NOT IN and NOT EXISTS can lead to poor performance, especially with large datasets',
            'example': 'Consider rewriting using LEFT JOIN / IS NULL pattern'
        })
    
    # Check for complex subqueries
    subquery_count = query_lower.count('select')
    if subquery_count > 2:
        anti_patterns.append({
            'issue': 'Multiple subqueries',
            'suggestion': f'Query contains {subquery_count} SELECT statements which may indicate complex nesting',
            'example': 'Consider simplifying using CTEs (WITH clause) or temporary tables'
        })
    
    return anti_patterns

def validate_read_only_query(query: str) -> Tuple[bool, Optional[str]]:
    """
    Validate that a query is read-only.
    
    Args:
        query: The SQL query to check
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    # Normalize query: remove comments and extra whitespace
    query = re.sub(r'--.*?$', '', query, flags=re.MULTILINE)  # Remove single-line comments
    query = re.sub(r'/\*.*?\*/', '', query, flags=re.DOTALL)  # Remove multi-line comments
    query = ' '.join(query.split())  # Normalize whitespace
    query_lower = query.lower()
    
    # Check for allowed operations
    allowed_prefixes = ['select', 'explain', 'show', 'with']
    
    # For WITH queries, we need to check that they're not data-modifying
    if query_lower.startswith('with'):
        # Check if the WITH clause is followed by a data-modifying statement
        after_with = re.sub(r'^with\s+.*?\s+as\s+\([^)]*\)', '', query_lower, flags=re.DOTALL | re.IGNORECASE)
        
        # Check if what remains starts with a read-only operation
        if not any(after_with.strip().startswith(prefix) for prefix in ['select', 'explain', 'show']):
            return False, "WITH queries must be followed by a read-only operation (SELECT, EXPLAIN, SHOW)"
    elif not any(query_lower.startswith(prefix) for prefix in allowed_prefixes):
        return False, f"Query must start with one of: {', '.join(allowed_prefixes).upper()}"
    
    # Check for disallowed operations
    disallowed_operations = [
        'insert', 'update', 'delete', 'drop', 'alter', 'create', 'truncate',
        'grant', 'revoke', 'vacuum', 'reindex', 'cluster', 'reset', 'load',
        'copy'
    ]
    
    # Use regex to find these operations at the start of the query or after a semicolon
    for operation in disallowed_operations:
        pattern = rf'(^|\s*;\s*){operation}\s+'
        if re.search(pattern, query_lower):
            return False, f"Query contains disallowed operation: {operation.upper()}"
    
    return True, None
