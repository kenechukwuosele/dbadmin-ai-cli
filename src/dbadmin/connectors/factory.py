"""Connector factory for creating database-specific connectors."""

import re
from typing import Any
from urllib.parse import urlparse

from dbadmin.connectors.base import BaseConnector


def detect_db_type(url: str) -> str:
    """Detect database type from connection URL."""
    url_lower = url.lower()
    
    if url_lower.startswith(("postgresql://", "postgres://")):
        return "postgresql"
    elif url_lower.startswith("mysql://"):
        return "mysql"
    elif url_lower.startswith("mongodb://"):
        return "mongodb"
    elif url_lower.startswith("redis://"):
        return "redis"
    elif url_lower.startswith("mariadb://"):
        return "mysql"  # MariaDB uses MySQL connector
    else:
        raise ValueError(f"Unknown database type in URL: {url}")


def get_connector(url_or_name: str) -> BaseConnector:
    """Get appropriate connector for database URL or saved connection name.
    
    Args:
        url_or_name: Database connection URL or saved connection name
        
    Returns:
        Database connector instance
    """
    # Check if it's a saved connection name
    url = _resolve_connection_name(url_or_name)
    
    # Detect database type and create connector
    db_type = detect_db_type(url)
    
    if db_type == "postgresql":
        from dbadmin.connectors.postgresql import PostgreSQLConnector
        return PostgreSQLConnector(url)
    elif db_type == "mysql":
        from dbadmin.connectors.mysql import MySQLConnector
        return MySQLConnector(url)
    elif db_type == "mongodb":
        from dbadmin.connectors.mongodb import MongoDBConnector
        return MongoDBConnector(url)
    elif db_type == "redis":
        from dbadmin.connectors.redis import RedisConnector
        return RedisConnector(url)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")


def _resolve_connection_name(url_or_name: str) -> str:
    """Resolve a connection name to URL if it's a saved connection.
    
    Returns the original string if it looks like a URL.
    """
    # If it looks like a URL, return as-is
    if "://" in url_or_name:
        return url_or_name
    
    # Try to load from saved connections
    import json
    from pathlib import Path
    
    config_file = Path.home() / ".dbadmin" / "connections.json"
    
    if config_file.exists():
        connections = json.loads(config_file.read_text())
        if url_or_name in connections:
            return connections[url_or_name]["url"]
    
    # Check environment variables
    from dbadmin.config import get_settings
    settings = get_settings()
    dbs = settings.get_configured_databases()
    
    if url_or_name in dbs:
        return dbs[url_or_name]
    
    raise ValueError(f"Unknown connection: {url_or_name}")
