"""Async base connector interface for critical path operations.

This provides async versions of the most commonly used database operations
to prevent blocking the event loop in async applications.
"""

from abc import ABC, abstractmethod
from typing import Any

from dbadmin.connectors.base import QueryResult


class AsyncBaseConnector(ABC):
    """Abstract base class for async database operations.
    
    This is a minimal async interface for critical path operations.
    For full functionality, use the synchronous connectors.
    """
    
    def __init__(self, url: str):
        """Initialize connector with connection URL."""
        self.url = url
        self._async_connection = None
    
    @abstractmethod
    async def connect_async(self) -> None:
        """Establish async database connection."""
        pass
    
    @abstractmethod
    async def disconnect_async(self) -> None:
        """Close async database connection."""
        pass
    
    @abstractmethod
    async def execute_async(
        self, 
        query: str, 
        params: tuple | None = None
    ) -> QueryResult:
        """Execute a query asynchronously and return results."""
        pass
    
    @abstractmethod
    async def execute_read_only_async(
        self, 
        query: str, 
        params: tuple | None = None
    ) -> QueryResult:
        """Execute a read-only query asynchronously."""
        pass
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect_async()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect_async()
