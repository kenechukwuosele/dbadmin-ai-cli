"""MySQL database connector."""

import re
import time
from typing import Any
from urllib.parse import urlparse

import mysql.connector

from dbadmin.connectors.base import BaseConnector, QueryResult, ExplainPlan


# Valid identifier pattern (alphanumeric + underscore, no special chars)
_VALID_IDENTIFIER = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')


def _validate_identifier(name: str) -> str:
    """Validate and return a safe SQL identifier.
    
    Raises ValueError if the identifier contains potentially dangerous characters.
    """
    if not name or not _VALID_IDENTIFIER.match(name):
        raise ValueError(f"Invalid identifier: {name!r}. Only alphanumeric characters and underscores allowed.")
    if len(name) > 64:  # MySQL identifier limit
        raise ValueError(f"Identifier too long: {name!r}")
    return name


class MySQLConnector(BaseConnector):
    """MySQL/MariaDB database connector."""
    
    def _parse_url(self) -> dict[str, Any]:
        """Parse MySQL connection URL."""
        parsed = urlparse(self.url)
        return {
            "host": parsed.hostname or "localhost",
            "port": parsed.port or 3306,
            "user": parsed.username or "root",
            "password": parsed.password or "",
            "database": parsed.path.strip("/") or None,
        }
    
    def connect(self) -> None:
        """Establish MySQL connection."""
        config = self._parse_url()
        self._connection = mysql.connector.connect(**config)
    
    def disconnect(self) -> None:
        """Close MySQL connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
    
    def test_connection(self) -> dict[str, Any]:
        """Test connection and return database info."""
        config = self._parse_url()
        
        with mysql.connector.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT VERSION(), DATABASE(), @@hostname, @@port")
                row = cur.fetchone()
                
                return {
                    "version": row[0] if row else "Unknown",
                    "database": row[1] if row else config["database"],
                    "host": row[2] if row else config["host"],
                    "port": row[3] if row else config["port"],
                    "type": "mysql",
                }
    
    def execute(self, query: str, params: tuple | None = None) -> QueryResult:
        """Execute a query and return results."""
        if not self._connection:
            self.connect()
        
        start = time.perf_counter()
        
        try:
            cur = self._connection.cursor()
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
        finally:
            cur.close()
    
    def execute_read_only(self, query: str, params: tuple | None = None) -> QueryResult:
        """Execute a read-only query."""
        query_upper = query.strip().upper()
        if not query_upper.startswith(("SELECT", "EXPLAIN", "SHOW", "DESCRIBE")):
            return QueryResult(error="Only SELECT, EXPLAIN, SHOW, DESCRIBE queries allowed")
        return self.execute(query, params)
    
    def get_schema(self) -> dict[str, Any]:
        """Get database schema."""
        if not self._connection:
            self.connect()
        
        schema = {"tables": {}}
        cur = self._connection.cursor()
        
        try:
            cur.execute("SHOW TABLES")
            tables = [row[0] for row in cur.fetchall()]
            
            for table in tables:
                # Tables from SHOW TABLES are safe, but validate anyway
                safe_table = _validate_identifier(table)
                cur.execute(f"DESCRIBE `{safe_table}`")
                columns = []
                for row in cur.fetchall():
                    columns.append({
                        "name": row[0],
                        "type": row[1],
                        "nullable": row[2] == "YES",
                        "key": row[3],
                        "default": row[4],
                    })
                schema["tables"][table] = {"columns": columns}
        finally:
            cur.close()
        
        return schema
    
    def get_table_info(self, table: str) -> dict[str, Any]:
        """Get detailed table information."""
        if not self._connection:
            self.connect()
        
        info = {"table": table, "columns": [], "indexes": [], "size": 0, "row_count": 0}
        cur = self._connection.cursor()
        
        try:
            # Validate table name to prevent SQL injection
            safe_table = _validate_identifier(table)
            
            # Columns
            cur.execute(f"DESCRIBE `{safe_table}`")
            info["columns"] = [{"name": r[0], "type": r[1], "nullable": r[2] == "YES"} 
                               for r in cur.fetchall()]
            
            # Indexes
            cur.execute(f"SHOW INDEX FROM `{safe_table}`")
            indexes = {}
            for row in cur.fetchall():
                idx_name = row[2]
                if idx_name not in indexes:
                    indexes[idx_name] = {"name": idx_name, "columns": [], "unique": not row[1]}
                indexes[idx_name]["columns"].append(row[4])
            info["indexes"] = list(indexes.values())
            
            # Size and count - use parameterized query for table name in WHERE
            cur.execute(f"SELECT COUNT(*) FROM `{safe_table}`")
            info["row_count"] = cur.fetchone()[0]
            
            cur.execute("""
                SELECT data_length + index_length 
                FROM information_schema.tables 
                WHERE table_name = %s
            """, (table,))
            row = cur.fetchone()
            info["size"] = row[0] if row else 0
        finally:
            cur.close()
        
        return info
    
    def explain_query(self, query: str) -> ExplainPlan:
        """Get execution plan for a query.
        
        Note: Only SELECT queries are allowed for EXPLAIN to prevent 
        abuse through other statement types.
        """
        if not self._connection:
            self.connect()
        
        # Security: Only allow EXPLAIN on SELECT statements
        query_upper = query.strip().upper()
        if not query_upper.startswith('SELECT'):
            return ExplainPlan(
                raw_plan="",
                warnings=["EXPLAIN only allowed for SELECT queries"]
            )
        
        cur = self._connection.cursor()
        try:
            # Use EXPLAIN with the query (query is already validated as SELECT)
            cur.execute("EXPLAIN " + query)
            rows = cur.fetchall()
            
            if rows:
                # MySQL EXPLAIN format
                first_row = rows[0]
                return ExplainPlan(
                    raw_plan=str(rows),
                    cost=0,  # MySQL doesn't show cost in basic EXPLAIN
                    rows=first_row[9] if len(first_row) > 9 else 0,
                    scan_type=first_row[3] if len(first_row) > 3 else "Unknown",
                    warnings=self._extract_warnings(rows),
                )
        finally:
            cur.close()
        
        return ExplainPlan()
    
    def _extract_warnings(self, explain_rows) -> list[str]:
        """Extract warnings from execution plan."""
        warnings = []
        for row in explain_rows:
            if len(row) > 3 and row[3] == "ALL":
                warnings.append("Full table scan detected - consider adding an index")
        return warnings
    
    def get_slow_queries(self, limit: int = 10) -> list[dict[str, Any]]:
        """Get slow queries from slow query log."""
        # Note: Requires slow_query_log to be enabled
        return []
    
    def get_index_stats(self) -> list[dict[str, Any]]:
        """Get index usage statistics."""
        if not self._connection:
            self.connect()
        
        cur = self._connection.cursor()
        stats = []
        
        try:
            cur.execute("""
                SELECT table_name, index_name, stat_value
                FROM mysql.innodb_index_stats
                WHERE stat_name = 'n_leaf_pages'
            """)
            
            for table, index, pages in cur.fetchall():
                stats.append({
                    "table": table,
                    "index": index,
                    "leaf_pages": pages,
                })
        except Exception:
            pass
        finally:
            cur.close()
        
        return stats
    
    def get_health_metrics(self) -> dict[str, Any]:
        """Get MySQL health metrics."""
        if not self._connection:
            self.connect()
        
        metrics = {}
        cur = self._connection.cursor()
        
        try:
            # Connection count
            cur.execute("SHOW STATUS LIKE 'Threads_connected'")
            row = cur.fetchone()
            metrics["active_connections"] = int(row[1]) if row else 0
            
            # Buffer pool stats
            cur.execute("SHOW STATUS LIKE 'Innodb_buffer_pool_read_requests'")
            reads = int(cur.fetchone()[1])
            cur.execute("SHOW STATUS LIKE 'Innodb_buffer_pool_reads'")
            disk_reads = int(cur.fetchone()[1])
            
            if reads > 0:
                metrics["buffer_pool_hit_ratio"] = round((reads - disk_reads) / reads * 100, 2)
            else:
                metrics["buffer_pool_hit_ratio"] = 0
            
            # Uptime
            cur.execute("SHOW STATUS LIKE 'Uptime'")
            metrics["uptime_seconds"] = int(cur.fetchone()[1])
            
            # Queries per second
            cur.execute("SHOW STATUS LIKE 'Questions'")
            questions = int(cur.fetchone()[1])
            metrics["queries_per_second"] = round(questions / max(metrics["uptime_seconds"], 1), 2)
            
        finally:
            cur.close()
        
        return metrics
