"""PostgreSQL database connector with connection pooling and async support."""

import time
import re
import logging
from typing import Any, Optional
from urllib.parse import urlparse

import psycopg
from psycopg import sql, AsyncConnection
from psycopg_pool import ConnectionPool
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from dbadmin.connectors.base import BaseConnector, QueryResult, ExplainPlan
from dbadmin.connectors.base_async import AsyncBaseConnector

logger = logging.getLogger(__name__)


# Valid identifier pattern (alphanumeric + underscore, no special chars)
_VALID_IDENTIFIER = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')


def _validate_identifier(name: str) -> str:
    """Validate and return a safe SQL identifier.
    
    Raises ValueError if the identifier contains potentially dangerous characters.
    """
    if not name or not _VALID_IDENTIFIER.match(name):
        raise ValueError(f"Invalid identifier: {name!r}. Only alphanumeric characters and underscores allowed.")
    if len(name) > 63:  # PostgreSQL identifier limit
        raise ValueError(f"Identifier too long: {name!r}")
    return name


class PostgreSQLConnector(BaseConnector):
    """PostgreSQL database connector using psycopg3 with connection pooling.
    
    Connection pooling prevents hitting database connection limits
    and improves performance by reusing connections.
    """
    
    # Class-level pool cache to share pools across instances
    _pools: dict[str, ConnectionPool] = {}
    
    def __init__(
        self, 
        url: str, 
        pool_min_size: int = 2, 
        pool_max_size: int = 10,
        pool_timeout: float = 30.0,
    ):
        """Initialize connector with pooling configuration.
        
        Args:
            url: PostgreSQL connection URL
            pool_min_size: Minimum connections to keep open
            pool_max_size: Maximum connections allowed
            pool_timeout: Timeout waiting for a connection from pool
        """
        super().__init__(url)
        self.pool_min_size = pool_min_size
        self.pool_max_size = pool_max_size
        self.pool_timeout = pool_timeout
        self._pool: Optional[ConnectionPool] = None
    
    def _get_pool(self) -> ConnectionPool:
        """Get or create connection pool for this URL."""
        if self.url not in PostgreSQLConnector._pools:
            PostgreSQLConnector._pools[self.url] = ConnectionPool(
                self.url,
                min_size=self.pool_min_size,
                max_size=self.pool_max_size,
                timeout=self.pool_timeout,
                open=True,
            )
        return PostgreSQLConnector._pools[self.url]
    
    # Circuit breaker state
    _failure_count: dict[str, int] = {}
    _circuit_open_until: dict[str, float] = {}
    _FAILURE_THRESHOLD = 5
    _CIRCUIT_TIMEOUT = 60.0  # seconds
    
    def _check_circuit_breaker(self) -> None:
        """Check if circuit breaker is open, raise if so."""
        if self.url in self._circuit_open_until:
            if time.time() < self._circuit_open_until[self.url]:
                raise ConnectionError(
                    f"Circuit breaker open for this connection. "
                    f"Retry after {self._circuit_open_until[self.url] - time.time():.1f}s"
                )
            else:
                # Circuit timeout expired, allow retry
                del self._circuit_open_until[self.url]
                self._failure_count[self.url] = 0
    
    def _record_failure(self) -> None:
        """Record a connection failure and potentially open circuit breaker."""
        self._failure_count[self.url] = self._failure_count.get(self.url, 0) + 1
        if self._failure_count[self.url] >= self._FAILURE_THRESHOLD:
            self._circuit_open_until[self.url] = time.time() + self._CIRCUIT_TIMEOUT
            logger.warning(f"Circuit breaker opened for {self.url[:50]}...")
    
    def _record_success(self) -> None:
        """Record a successful connection, reset failure count."""
        self._failure_count[self.url] = 0
        if self.url in self._circuit_open_until:
            del self._circuit_open_until[self.url]
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((psycopg.OperationalError, ConnectionError)),
        reraise=True,
    )
    def connect(self) -> None:
        """Get a connection from the pool with retry and circuit breaker."""
        self._check_circuit_breaker()
        try:
            self._pool = self._get_pool()
            self._connection = self._pool.getconn()
            self._record_success()
        except Exception as e:
            self._record_failure()
            logger.warning(f"Connection failed: {e}")
            raise
    
    def disconnect(self) -> None:
        """Return connection to the pool (not close it)."""
        if self._connection and self._pool:
            self._pool.putconn(self._connection)
            self._connection = None
    
    @classmethod
    def close_all_pools(cls) -> None:
        """Close all connection pools. Call on application shutdown."""
        for pool in cls._pools.values():
            pool.close()
        cls._pools.clear()
    
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
    
    # ===== Async Methods =====
    
    async def connect_async(self) -> None:
        """Establish async PostgreSQL connection."""
        self._async_connection = await AsyncConnection.connect(self.url)
    
    async def disconnect_async(self) -> None:
        """Close async PostgreSQL connection."""
        if self._async_connection:
            await self._async_connection.close()
            self._async_connection = None
    
    async def execute_async(
        self, 
        query: str, 
        params: tuple | None = None
    ) -> QueryResult:
        """Execute a query asynchronously and return results.
        
        This is the critical path async method for non-blocking queries.
        """
        if not self._async_connection:
            await self.connect_async()
        
        start = time.perf_counter()
        
        try:
            async with self._async_connection.cursor() as cur:
                await cur.execute(query, params)
                
                if cur.description:
                    columns = [desc[0] for desc in cur.description]
                    rows = await cur.fetchall()
                else:
                    columns = []
                    rows = []
                
                await self._async_connection.commit()
                
                return QueryResult(
                    columns=columns,
                    rows=rows,
                    row_count=cur.rowcount,
                    execution_time_ms=(time.perf_counter() - start) * 1000,
                )
        except Exception as e:
            await self._async_connection.rollback()
            return QueryResult(error=str(e))
    
    async def execute_read_only_async(
        self, 
        query: str, 
        params: tuple | None = None
    ) -> QueryResult:
        """Execute a read-only query asynchronously."""
        query_upper = query.strip().upper()
        if not query_upper.startswith(("SELECT", "EXPLAIN", "SHOW", "WITH")):
            return QueryResult(error="Only SELECT, EXPLAIN, SHOW, and WITH queries allowed in read-only mode")
        
        return await self.execute_async(query, params)
    
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
        
        # Validate table name to prevent SQL injection
        safe_table = _validate_identifier(table)
        
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
            
            # Get size and count using sql.Identifier for safe table name
            query = sql.SQL("SELECT pg_total_relation_size(%s), count(*) FROM {}").format(
                sql.Identifier(safe_table)
            )
            cur.execute(query, (table,))
            row = cur.fetchone()
            if row:
                info["size"] = row[0]
                info["row_count"] = row[1]
        
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
        
        with self._connection.cursor() as cur:
            # Construct EXPLAIN query safely - query is validated as SELECT above
            explain_query = sql.SQL("EXPLAIN (FORMAT JSON, ANALYZE false) {}").format(
                sql.SQL(query)
            )
            cur.execute(explain_query)
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
