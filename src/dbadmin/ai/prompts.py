"""Prompt templates for different database operations."""

# System prompt for the database assistant
SYSTEM_PROMPT = """You are DbAdmin AI, an expert database administrator assistant. 
You help users manage and optimize PostgreSQL, MySQL, MongoDB, and Redis databases.

Your capabilities:
1. Execute SQL queries and database commands
2. Analyze query performance and suggest optimizations
3. Recommend indexes and schema improvements
4. Diagnose database issues
5. Explain database concepts in simple terms

Important guidelines:
- When asked to show data, write and execute the appropriate query
- Always explain what you're doing before executing commands
- For destructive operations (DELETE, DROP, TRUNCATE), warn the user first
- Provide performance estimates when suggesting changes
- Use the database schema context to write accurate queries

When generating SQL:
- Use proper table and column names from the schema
- Include appropriate WHERE clauses for safety
- Add LIMIT clauses for SELECT queries unless the user wants all data
- Format SQL for readability

Current database context:
{database_context}

Schema information:
{schema_info}
"""

# Prompt for natural language to SQL conversion
NL_TO_SQL_PROMPT = """Convert the following natural language request to a valid SQL query.

Database type: {db_type}
Schema:
{schema}

User request: {question}

Guidelines:
1. Use only tables and columns that exist in the schema
2. Add appropriate LIMIT clause (default 100) unless counting/aggregating
3. Use proper SQL syntax for the database type
4. For ambiguous requests, make reasonable assumptions

Respond with ONLY the SQL query, no explanations.
"""

# Prompt for query optimization
QUERY_OPTIMIZATION_PROMPT = """Analyze this SQL query and suggest optimizations.

Database: {db_type}
Query:
```sql
{query}
```

Explain plan:
{explain_plan}

Schema context:
{schema}

Provide:
1. Performance analysis (what makes it slow)
2. Specific index recommendations with CREATE INDEX statements
3. Rewritten query if improvements possible
4. Expected performance improvement estimate

Format your response as:
## Analysis
[Your analysis]

## Recommendations
[Numbered list of improvements]

## Optimized Query (if applicable)
```sql
[Rewritten query]
```
"""

# Prompt for health diagnosis
HEALTH_DIAGNOSIS_PROMPT = """Analyze these database health metrics and provide recommendations.

Database: {db_type} ({database_name})
Health Score: {health_score}/100

Metrics:
{metrics}

Recent issues:
{issues}

Provide:
1. Summary of database health status
2. Top 3 concerns that need attention
3. Specific actionable recommendations
4. Preventive measures for future issues
"""

# Prompt for error diagnosis
ERROR_DIAGNOSIS_PROMPT = """Help diagnose this database error.

Database: {db_type}
Error message:
{error}

Query that caused the error (if applicable):
{query}

Recent context:
{context}

Provide:
1. What the error means in simple terms
2. Most likely cause
3. Step by step fix
4. How to prevent this in the future
"""

# Prompt for index recommendation
INDEX_RECOMMENDATION_PROMPT = """Recommend indexes for this database based on query patterns.

Database: {db_type}
Table: {table_name}

Current indexes:
{current_indexes}

Query patterns (frequency, query):
{query_patterns}

For each recommended index:
1. Index definition (CREATE INDEX statement)
2. Which queries it helps
3. Expected improvement
4. Trade-offs (write penalty, space)
"""


def format_schema_for_prompt(schema: dict, db_type: str) -> str:
    """Format database schema for inclusion in prompts."""
    lines = []
    
    if db_type in ("postgresql", "mysql"):
        tables = schema.get("tables", {})
        for table_name, table_info in tables.items():
            columns = table_info.get("columns", [])
            col_strs = []
            for col in columns:
                nullable = "NULL" if col.get("nullable") else "NOT NULL"
                col_strs.append(f"  {col['name']} {col['type']} {nullable}")
            
            lines.append(f"TABLE {table_name} (")
            lines.append(",\n".join(col_strs))
            lines.append(")")
            lines.append("")
    
    elif db_type == "mongodb":
        collections = schema.get("collections", {})
        for coll_name, coll_info in collections.items():
            fields = coll_info.get("fields", [])
            field_strs = [f"  {f['name']}: {f['type']}" for f in fields]
            
            lines.append(f"COLLECTION {coll_name} {{")
            lines.append(",\n".join(field_strs))
            lines.append("}")
            lines.append("")
    
    elif db_type == "redis":
        key_types = schema.get("key_types", {})
        for key_type, examples in key_types.items():
            lines.append(f"KEY TYPE: {key_type}")
            lines.append(f"  Examples: {', '.join(examples[:3])}")
            lines.append("")
    
    return "\n".join(lines) if lines else "Schema not available"
