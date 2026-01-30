"""MongoDB database connector."""

import time
from typing import Any
from urllib.parse import urlparse

from pymongo import MongoClient

from dbadmin.connectors.base import BaseConnector, QueryResult, ExplainPlan


class MongoDBConnector(BaseConnector):
    """MongoDB database connector."""
    
    def __init__(self, url: str):
        super().__init__(url)
        self._client: MongoClient | None = None
        self._db = None
    
    def _parse_url(self) -> tuple[str, str]:
        """Parse MongoDB URL and extract database name."""
        parsed = urlparse(self.url)
        db_name = parsed.path.strip("/") or "admin"
        return self.url, db_name
    
    def connect(self) -> None:
        """Establish MongoDB connection."""
        url, db_name = self._parse_url()
        self._client = MongoClient(url)
        self._db = self._client[db_name]
    
    def disconnect(self) -> None:
        """Close MongoDB connection."""
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
    
    def test_connection(self) -> dict[str, Any]:
        """Test connection and return database info."""
        url, db_name = self._parse_url()
        
        with MongoClient(url) as client:
            info = client.server_info()
            parsed = urlparse(url)
            
            return {
                "version": info.get("version", "Unknown"),
                "database": db_name,
                "host": parsed.hostname or "localhost",
                "port": parsed.port or 27017,
                "type": "mongodb",
            }
    
    def execute(self, query: str, params: tuple | None = None) -> QueryResult:
        """Execute a MongoDB query (as JSON/dict command)."""
        if not self._client:
            self.connect()
        
        start = time.perf_counter()
        
        try:
            import json
            cmd = json.loads(query)
            result = self._db.command(cmd)
            
            return QueryResult(
                columns=list(result.keys()) if isinstance(result, dict) else [],
                rows=[(result,)] if result else [],
                row_count=1,
                execution_time_ms=(time.perf_counter() - start) * 1000,
            )
        except Exception as e:
            return QueryResult(error=str(e))
    
    def execute_read_only(self, query: str, params: tuple | None = None) -> QueryResult:
        """Execute a read-only MongoDB query."""
        return self.execute(query, params)
    
    def find(self, collection: str, filter: dict = None, limit: int = 100) -> QueryResult:
        """Execute a find query on a collection."""
        if not self._client:
            self.connect()
        
        start = time.perf_counter()
        
        try:
            cursor = self._db[collection].find(filter or {}).limit(limit)
            docs = list(cursor)
            
            columns = []
            if docs:
                columns = list(docs[0].keys())
            
            return QueryResult(
                columns=columns,
                rows=[tuple(doc.values()) for doc in docs],
                row_count=len(docs),
                execution_time_ms=(time.perf_counter() - start) * 1000,
            )
        except Exception as e:
            return QueryResult(error=str(e))
    
    def get_schema(self) -> dict[str, Any]:
        """Get database schema (collections and sample fields)."""
        if not self._client:
            self.connect()
        
        schema = {"collections": {}}
        
        for coll_name in self._db.list_collection_names():
            # Sample a document to infer fields
            sample = self._db[coll_name].find_one()
            fields = []
            
            if sample:
                for key, value in sample.items():
                    fields.append({
                        "name": key,
                        "type": type(value).__name__,
                    })
            
            schema["collections"][coll_name] = {
                "fields": fields,
            }
        
        return schema
    
    def get_table_info(self, table: str) -> dict[str, Any]:
        """Get collection information."""
        if not self._client:
            self.connect()
        
        coll = self._db[table]
        stats = self._db.command("collStats", table)
        
        # Get indexes
        indexes = []
        for idx in coll.list_indexes():
            indexes.append({
                "name": idx["name"],
                "keys": list(idx["key"].keys()),
            })
        
        # Sample fields
        sample = coll.find_one()
        fields = [{"name": k, "type": type(v).__name__} for k, v in (sample or {}).items()]
        
        return {
            "collection": table,
            "fields": fields,
            "indexes": indexes,
            "size": stats.get("size", 0),
            "document_count": stats.get("count", 0),
            "avg_doc_size": stats.get("avgObjSize", 0),
        }
    
    def explain_query(self, query: str) -> ExplainPlan:
        """Get execution plan for a query."""
        # MongoDB explain requires parsing the query
        return ExplainPlan(raw_plan="Use db.collection.find().explain() in MongoDB shell")
    
    def get_slow_queries(self, limit: int = 10) -> list[dict[str, Any]]:
        """Get slow queries from profiler."""
        if not self._client:
            self.connect()
        
        try:
            cursor = self._db.system.profile.find(
                {"millis": {"$gt": 100}}
            ).sort("millis", -1).limit(limit)
            
            return [
                {
                    "operation": doc.get("op"),
                    "collection": doc.get("ns"),
                    "duration_ms": doc.get("millis"),
                    "query": doc.get("query"),
                }
                for doc in cursor
            ]
        except Exception:
            return []
    
    def get_index_stats(self) -> list[dict[str, Any]]:
        """Get index usage statistics."""
        if not self._client:
            self.connect()
        
        stats = []
        
        for coll_name in self._db.list_collection_names():
            try:
                index_stats = self._db[coll_name].aggregate([{"$indexStats": {}}])
                for stat in index_stats:
                    stats.append({
                        "collection": coll_name,
                        "index": stat["name"],
                        "accesses": stat["accesses"]["ops"],
                    })
            except Exception:
                pass
        
        return stats
    
    def get_health_metrics(self) -> dict[str, Any]:
        """Get MongoDB health metrics."""
        if not self._client:
            self.connect()
        
        server_status = self._db.command("serverStatus")
        
        return {
            "active_connections": server_status.get("connections", {}).get("current", 0),
            "available_connections": server_status.get("connections", {}).get("available", 0),
            "memory_resident_mb": server_status.get("mem", {}).get("resident", 0),
            "operations": server_status.get("opcounters", {}),
            "uptime_seconds": server_status.get("uptime", 0),
        }
