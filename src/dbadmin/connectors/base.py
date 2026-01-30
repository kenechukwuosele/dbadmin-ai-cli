"""Base database connector interface."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ConnectionInfo:
    """Database connection information."""
    
    host: str = ""
    port: int = 0
    database: str = ""
    version: str = ""
    db_type: str = ""
    connected: bool = False
    error: str = ""


@dataclass
class QueryResult:
    """Result from executing a query."""
    
    columns: list[str] = field(default_factory=list)
    rows: list[tuple[Any, ...]] = field(default_factory=list)
    row_count: int = 0
    execution_time_ms: float = 0.0
    error: str = ""
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "columns": self.columns,
            "rows": [list(row) for row in self.rows],
            "row_count": self.row_count,
            "execution_time_ms": self.execution_time_ms,
        }


@dataclass
class ExplainPlan:
    """Query execution plan."""
    
    raw_plan: str = ""
    cost: float = 0.0
    rows: int = 0
    scan_type: str = ""
    warnings: list[str] = field(default_factory=list)
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "cost": self.cost,
            "rows": self.rows,
            "scan_type": self.scan_type,
            "warnings": self.warnings,
            "raw_plan": self.raw_plan,
        }


class BaseConnector(ABC):
    """Abstract base class for database connectors."""
    
    def __init__(self, url: str):
        """Initialize connector with connection URL."""
        self.url = url
        self._connection = None
    
    @abstractmethod
    def connect(self) -> None:
        """Establish database connection."""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close database connection."""
        pass
    
    @abstractmethod
    def test_connection(self) -> dict[str, Any]:
        """Test connection and return database info."""
        pass
    
    @abstractmethod
    def execute(self, query: str, params: tuple | None = None) -> QueryResult:
        """Execute a query and return results."""
        pass
    
    @abstractmethod
    def execute_read_only(self, query: str, params: tuple | None = None) -> QueryResult:
        """Execute a read-only query (SELECT, EXPLAIN, etc.)."""
        pass
    
    @abstractmethod
    def get_schema(self) -> dict[str, Any]:
        """Get database schema (tables, columns, types)."""
        pass
    
    @abstractmethod
    def get_table_info(self, table: str) -> dict[str, Any]:
        """Get detailed information about a table."""
        pass
    
    @abstractmethod
    def explain_query(self, query: str) -> ExplainPlan:
        """Get execution plan for a query."""
        pass
    
    @abstractmethod
    def get_slow_queries(self, limit: int = 10) -> list[dict[str, Any]]:
        """Get slow queries from database."""
        pass
    
    @abstractmethod
    def get_index_stats(self) -> list[dict[str, Any]]:
        """Get index usage statistics."""
        pass
    
    @abstractmethod
    def get_health_metrics(self) -> dict[str, Any]:
        """Get database health metrics."""
        pass
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
