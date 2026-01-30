"""PostgreSQL database connector."""

import time
from typing import Any
from urllib.parse import urlparse

import psycopg

from dbadmin.connectors.base import BaseConnector, QueryResult, ExplainPlan


class PostgreSQLConnector(BaseConnector):
    """PostgreSQL database connector using psycopg3."""
    
    def connect(self) -> None:
        """Establish PostgreSQL connection."""
        self._connection = psycopg.connect(self.url)
    
    def disconnect(self) -> None:
        """Close PostgreSQL connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
    
    def test_connection(self) -> dict[str, Any]:
        """Test connection and return database info."""
        with psycopg.connect(self.url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version(), current_database(), inet_server_addr(), inet_server_port()")
                row = cur.fetchone()
                
                parsed = urlparse(self.url)
                
                return {
                    "version": row[0] if row else "Unknown",
                    "database": row[1] if row else parsed.path.strip("/"),
                    "host": str(row[2]) if row and row[2] else parsed.hostname,
                    "port": row[3] if row and row[3] else parsed.port or 5432,
                    "type": "postgresql",
                }
    
    def execute(self, query: str, params: tuple | None = None) -> QueryResult:
        """Execute a query and return results."""
        if not self._connection:
            self.connect()
        
        start = time.perf_counter()
        
        try:
            with self._connection.cursor() as cur:
                cur.execute(query, params)
                
                if cur.description:
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                else:
                    columns = []
                    rows = []
                
                self._connection.commit()
                
                return QueryResult(
                    columns=columns,
                    rows=rows,
                    row_count=cur.rowcount,
                    execution_time_ms=(time.perf_counter() - start) * 1000,
                )
        except Exception as e:
            self._connection.rollback()
            return QueryResult(error=str(e))
    
    def execute_read_only(self, query: str, params: tuple | None = None) -> QueryResult:
        """Execute a read-only query."""
        # Wrap in read-only transaction for safety
        query_upper = query.strip().upper()
        if not query_upper.startswith(("SELECT", "EXPLAIN", "SHOW", "WITH")):
            return QueryResult(error="Only SELECT, EXPLAIN, SHOW, and WITH queries allowed in read-only mode")
        
        return self.execute(query, params)
    
    def get_schema(self) -> dict[str, Any]:
        """Get database schema."""
        if not self._connection:
            self.connect()
        
        schema = {"tables": {}}
        
        with self._connection.cursor() as cur:
            # Get tables
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
            
            for (table_name,) in cur.fetchall():
                # Get columns for each table
                cur.execute("""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s
                    ORDER BY ordinal_position
                """, (table_name,))
                
                columns = []
                for col_name, data_type, nullable, default in cur.fetchall():
                    columns.append({
                        "name": col_name,
                        "type": data_type,
                        "nullable": nullable == "YES",
                        "default": default,
                    })
                
                schema["tables"][table_name] = {
                    "columns": columns,
                }
        
        return schema
    
    def get_table_info(self, table: str) -> dict[str, Any]:
        """Get detailed table information."""
        if not self._connection:
            self.connect()
        
        info = {"table": table, "columns": [], "indexes": [], "size": 0, "row_count": 0}
        
        with self._connection.cursor() as cur:
            # Get columns
            cur.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = %s AND table_schema = 'public'
                ORDER BY ordinal_position
            """, (table,))
            info["columns"] = [{"name": n, "type": t, "nullable": nul == "YES"} 
                               for n, t, nul in cur.fetchall()]
            
            # Get indexes
            cur.execute("""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE tablename = %s AND schemaname = 'public'
            """, (table,))
            info["indexes"] = [{"name": n, "definition": d} for n, d in cur.fetchall()]
            
            # Get size and count
            cur.execute(f"SELECT pg_total_relation_size(%s), count(*) FROM {table}", (table,))
            row = cur.fetchone()
            if row:
                info["size"] = row[0]
                info["row_count"] = row[1]
        
        return info
    
    def explain_query(self, query: str) -> ExplainPlan:
        """Get execution plan for a query."""
        if not self._connection:
            self.connect()
        
        with self._connection.cursor() as cur:
            cur.execute(f"EXPLAIN (FORMAT JSON, ANALYZE false) {query}")
            result = cur.fetchone()
            
            if result:
                plan_data = result[0][0]
                plan = plan_data.get("Plan", {})
                
                return ExplainPlan(
                    raw_plan=str(result[0]),
                    cost=plan.get("Total Cost", 0),
                    rows=plan.get("Plan Rows", 0),
                    scan_type=plan.get("Node Type", "Unknown"),
                    warnings=self._extract_plan_warnings(plan),
                )
        
        return ExplainPlan()
    
    def _extract_plan_warnings(self, plan: dict) -> list[str]:
        """Extract warnings from execution plan."""
        warnings = []
        
        node_type = plan.get("Node Type", "")
        if node_type == "Seq Scan":
            warnings.append("Sequential scan detected - consider adding an index")
        
        if plan.get("Plan Rows", 0) > 10000:
            warnings.append("Query may return many rows - consider adding LIMIT")
        
        return warnings
    
    def get_slow_queries(self, limit: int = 10) -> list[dict[str, Any]]:
        """Get slow queries from pg_stat_statements if available."""
        if not self._connection:
            self.connect()
        
        queries = []
        
        with self._connection.cursor() as cur:
            try:
                cur.execute("""
                    SELECT query, calls, mean_exec_time, total_exec_time
                    FROM pg_stat_statements
                    ORDER BY mean_exec_time DESC
                    LIMIT %s
                """, (limit,))
                
                for query, calls, mean_time, total_time in cur.fetchall():
                    queries.append({
                        "query": query,
                        "calls": calls,
                        "mean_time_ms": mean_time,
                        "total_time_ms": total_time,
                    })
            except Exception:
                # pg_stat_statements extension might not be installed
                pass
        
        return queries
    
    def get_index_stats(self) -> list[dict[str, Any]]:
        """Get index usage statistics."""
        if not self._connection:
            self.connect()
        
        with self._connection.cursor() as cur:
            cur.execute("""
                SELECT 
                    schemaname, tablename, indexname,
                    idx_scan, idx_tup_read, idx_tup_fetch,
                    pg_relation_size(indexrelid) as size
                FROM pg_stat_user_indexes
                ORDER BY idx_scan DESC
            """)
            
            return [
                {
                    "schema": row[0],
                    "table": row[1],
                    "index": row[2],
                    "scans": row[3],
                    "tuples_read": row[4],
                    "tuples_fetched": row[5],
                    "size_bytes": row[6],
                }
                for row in cur.fetchall()
            ]
    
    def get_health_metrics(self) -> dict[str, Any]:
        """Get PostgreSQL health metrics."""
        if not self._connection:
            self.connect()
        
        metrics = {}
        
        with self._connection.cursor() as cur:
            # Connection count
            cur.execute("SELECT count(*) FROM pg_stat_activity")
            metrics["active_connections"] = cur.fetchone()[0]
            
            # Database size
            cur.execute("SELECT pg_database_size(current_database())")
            metrics["database_size_bytes"] = cur.fetchone()[0]
            
            # Cache hit ratio
            cur.execute("""
                SELECT 
                    CASE WHEN blks_hit + blks_read = 0 THEN 0
                    ELSE round(blks_hit::numeric / (blks_hit + blks_read) * 100, 2)
                    END as cache_hit_ratio
                FROM pg_stat_database 
                WHERE datname = current_database()
            """)
            row = cur.fetchone()
            metrics["cache_hit_ratio"] = float(row[0]) if row else 0
            
            # Dead tuples (needs vacuum)
            cur.execute("""
                SELECT sum(n_dead_tup) 
                FROM pg_stat_user_tables
            """)
            row = cur.fetchone()
            metrics["dead_tuples"] = row[0] or 0
            
            # Long running queries
            cur.execute("""
                SELECT count(*) FROM pg_stat_activity 
                WHERE state = 'active' 
                AND now() - query_start > interval '1 minute'
            """)
            metrics["long_running_queries"] = cur.fetchone()[0]
        
        return metrics
