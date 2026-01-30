"""Redis database connector."""

import time
from typing import Any
from urllib.parse import urlparse

import redis

from dbadmin.connectors.base import BaseConnector, QueryResult, ExplainPlan


class RedisConnector(BaseConnector):
    """Redis database connector."""
    
    def __init__(self, url: str):
        super().__init__(url)
        self._client: redis.Redis | None = None
    
    def connect(self) -> None:
        """Establish Redis connection."""
        self._client = redis.from_url(self.url)
    
    def disconnect(self) -> None:
        """Close Redis connection."""
        if self._client:
            self._client.close()
            self._client = None
    
    def test_connection(self) -> dict[str, Any]:
        """Test connection and return Redis info."""
        client = redis.from_url(self.url)
        info = client.info()
        parsed = urlparse(self.url)
        
        return {
            "version": info.get("redis_version", "Unknown"),
            "database": parsed.path.strip("/") or "0",
            "host": parsed.hostname or "localhost",
            "port": parsed.port or 6379,
            "type": "redis",
        }
    
    def execute(self, command: str, params: tuple | None = None) -> QueryResult:
        """Execute a Redis command."""
        if not self._client:
            self.connect()
        
        start = time.perf_counter()
        
        try:
            parts = command.split()
            cmd = parts[0].upper()
            args = parts[1:] if len(parts) > 1 else []
            
            result = self._client.execute_command(cmd, *args)
            
            # Format result based on type
            if isinstance(result, bytes):
                result = result.decode()
            elif isinstance(result, list):
                result = [r.decode() if isinstance(r, bytes) else r for r in result]
            
            return QueryResult(
                columns=["result"],
                rows=[(result,)],
                row_count=1,
                execution_time_ms=(time.perf_counter() - start) * 1000,
            )
        except Exception as e:
            return QueryResult(error=str(e))
    
    def execute_read_only(self, command: str, params: tuple | None = None) -> QueryResult:
        """Execute a read-only Redis command."""
        read_commands = {"GET", "MGET", "HGET", "HGETALL", "LRANGE", "SMEMBERS", 
                        "ZRANGE", "KEYS", "SCAN", "INFO", "DBSIZE", "EXISTS", "TYPE"}
        
        cmd = command.split()[0].upper()
        if cmd not in read_commands:
            return QueryResult(error=f"Command {cmd} not allowed in read-only mode")
        
        return self.execute(command, params)
    
    def get_schema(self) -> dict[str, Any]:
        """Get Redis key patterns and types."""
        if not self._client:
            self.connect()
        
        schema = {"key_types": {}}
        
        # Scan keys (limited sample)
        cursor = 0
        sample_size = 100
        keys_seen = 0
        
        while keys_seen < sample_size:
            cursor, keys = self._client.scan(cursor, count=100)
            
            for key in keys[:sample_size - keys_seen]:
                key_type = self._client.type(key)
                if isinstance(key_type, bytes):
                    key_type = key_type.decode()
                if isinstance(key, bytes):
                    key = key.decode()
                
                if key_type not in schema["key_types"]:
                    schema["key_types"][key_type] = []
                
                if len(schema["key_types"][key_type]) < 5:
                    schema["key_types"][key_type].append(key)
                
                keys_seen += 1
            
            if cursor == 0:
                break
        
        return schema
    
    def get_table_info(self, key: str) -> dict[str, Any]:
        """Get information about a Redis key."""
        if not self._client:
            self.connect()
        
        key_type = self._client.type(key)
        if isinstance(key_type, bytes):
            key_type = key_type.decode()
        
        ttl = self._client.ttl(key)
        memory = self._client.memory_usage(key) or 0
        
        info = {
            "key": key,
            "type": key_type,
            "ttl": ttl if ttl > 0 else "No expiry",
            "memory_bytes": memory,
        }
        
        # Get size based on type
        if key_type == "string":
            info["length"] = self._client.strlen(key)
        elif key_type == "list":
            info["length"] = self._client.llen(key)
        elif key_type == "set":
            info["cardinality"] = self._client.scard(key)
        elif key_type == "zset":
            info["cardinality"] = self._client.zcard(key)
        elif key_type == "hash":
            info["fields"] = self._client.hlen(key)
        
        return info
    
    def explain_query(self, query: str) -> ExplainPlan:
        """Redis doesn't have explain plans."""
        return ExplainPlan(raw_plan="Redis commands execute directly without query planning")
    
    def get_slow_queries(self, limit: int = 10) -> list[dict[str, Any]]:
        """Get slow log entries."""
        if not self._client:
            self.connect()
        
        try:
            slow_log = self._client.slowlog_get(limit)
            return [
                {
                    "id": entry.get("id"),
                    "timestamp": entry.get("start_time"),
                    "duration_us": entry.get("duration"),
                    "command": " ".join(str(c) for c in entry.get("command", [])),
                }
                for entry in slow_log
            ]
        except Exception:
            return []
    
    def get_index_stats(self) -> list[dict[str, Any]]:
        """Redis doesn't have traditional indexes."""
        return []
    
    def get_health_metrics(self) -> dict[str, Any]:
        """Get Redis health metrics."""
        if not self._client:
            self.connect()
        
        info = self._client.info()
        
        return {
            "connected_clients": info.get("connected_clients", 0),
            "used_memory_bytes": info.get("used_memory", 0),
            "used_memory_peak_bytes": info.get("used_memory_peak", 0),
            "total_commands_processed": info.get("total_commands_processed", 0),
            "keyspace_hits": info.get("keyspace_hits", 0),
            "keyspace_misses": info.get("keyspace_misses", 0),
            "hit_rate": self._calculate_hit_rate(info),
            "uptime_seconds": info.get("uptime_in_seconds", 0),
            "role": info.get("role", "unknown"),
        }
    
    def _calculate_hit_rate(self, info: dict) -> float:
        """Calculate cache hit rate."""
        hits = info.get("keyspace_hits", 0)
        misses = info.get("keyspace_misses", 0)
        total = hits + misses
        
        if total == 0:
            return 0.0
        return round(hits / total * 100, 2)
