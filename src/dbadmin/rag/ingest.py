"""Documentation ingestion for RAG system."""

import os
from pathlib import Path
from typing import Any

from dbadmin.rag.retriever import DocumentRetriever


# Core database documentation to include
CORE_DOCS = {
    "postgresql": """
PostgreSQL Query Optimization Guide

INDEXES:
- B-tree indexes are default, good for equality and range queries
- Use CREATE INDEX CONCURRENTLY to avoid blocking writes
- Partial indexes: CREATE INDEX idx ON table (col) WHERE condition
- Covering indexes: Include additional columns with INCLUDE clause
- Check unused indexes: SELECT * FROM pg_stat_user_indexes WHERE idx_scan = 0

EXPLAIN ANALYZE:
- Always use EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) for real execution stats
- Look for Seq Scan on large tables - usually means missing index
- High "Rows Removed by Filter" indicates missing index on WHERE columns
- Nested Loop with high row counts can be slow - consider Hash Join

VACUUM:
- VACUUM reclaims dead tuple space, VACUUM FULL compacts but locks table
- autovacuum should handle most cases - check pg_stat_user_tables
- Monitor n_dead_tup for tables that need vacuuming
- VACUUM ANALYZE updates statistics for query planner

CONNECTION POOLING:
- Use PgBouncer for connection pooling
- Transaction pooling mode is most efficient
- Default max_connections=100, rarely need more with pooler

COMMON ANTI-PATTERNS:
- SELECT * - only select columns you need
- Missing LIMIT on large tables
- N+1 queries - use JOINs or batch queries
- Implicit type casts in WHERE clauses
""",
    
    "mysql": """
MySQL/MariaDB Optimization Guide

INDEXES:
- InnoDB uses clustered index on primary key
- Secondary indexes include primary key
- Use EXPLAIN to check index usage
- Covering index avoids table lookup (Using index in EXPLAIN)
- Index hints: USE INDEX, FORCE INDEX, IGNORE INDEX

QUERY CACHE:
- Deprecated in MySQL 8.0
- For MariaDB: query_cache_type, query_cache_size
- Invalidated on any table modification

INNODB BUFFER POOL:
- Set innodb_buffer_pool_size to 70-80% of available RAM
- Monitor with SHOW ENGINE INNODB STATUS
- Buffer pool hit ratio should be >99%

SLOW QUERY LOG:
- Enable: SET GLOBAL slow_query_log = 'ON'
- Set threshold: SET GLOBAL long_query_time = 1
- Find slow queries in slow_query_log file
- Use mysqldumpslow to analyze

COMMON ISSUES:
- Full table scans (type=ALL in EXPLAIN)
- Using filesort - consider index for ORDER BY
- Using temporary - optimize GROUP BY
- Missing index on JOIN columns
""",
    
    "mongodb": """
MongoDB Optimization Guide

INDEXES:
- Create indexes with db.collection.createIndex()
- Compound indexes: order matters, equality-sort-range
- Sparse indexes for optional fields
- TTL indexes for automatic document expiration
- Text indexes for full-text search
- Check usage with db.collection.aggregate([{$indexStats:{}}])

EXPLAIN:
- db.collection.find().explain("executionStats")
- Look for COLLSCAN (collection scan) - needs index
- Check totalDocsExamined vs nReturned ratio
- executionTimeMillis for query duration

AGGREGATION PIPELINE:
- Put $match and $sort early in pipeline (can use indexes)
- Use $project to reduce document size
- $lookup is like SQL JOIN - can be slow
- $unwind on large arrays is expensive

PROFILER:
- Enable: db.setProfilingLevel(1, {slowms: 100})
- Query: db.system.profile.find()
- Shows all slow operations

SHARDING:
- Choose shard key carefully (cardinality, write distribution)
- Avoid scatter-gather queries
- Use zone sharding for data locality

COMMON ISSUES:
- Large documents (>16MB limit)
- Unbounded array growth
- Missing indexes on query fields
- Querying large arrays without projection
""",
    
    "redis": """
Redis Optimization Guide

MEMORY:
- Use INFO memory to check usage
- maxmemory sets limit, maxmemory-policy sets eviction
- Eviction policies: volatile-lru, allkeys-lru, volatile-ttl
- MEMORY USAGE key for specific key size

KEY PATTERNS:
- Use : as separator (user:1234:profile)
- Set TTL on session data
- Use SCAN instead of KEYS in production
- Avoid large keys (>1MB is large)

DATA STRUCTURES:
- Strings: simple values, counters (INCR)
- Hashes: object storage, memory efficient
- Lists: queues (LPUSH/RPOP), capped with LTRIM
- Sets: unique items, intersections
- Sorted Sets: leaderboards, ranges

PERSISTENCE:
- RDB: snapshots, faster restart
- AOF: append-only log, more durable
- RDB+AOF: recommended for most cases

SLOW LOG:
- CONFIG SET slowlog-log-slower-than 10000
- SLOWLOG GET 10 to see slow commands
- Common slow: KEYS, SMEMBERS on large sets

PIPELINING:
- Batch commands to reduce round trips
- Redis-py: pipe = r.pipeline()

CLUSTER:
- 16384 hash slots distributed across nodes
- Use hash tags {user}:1234 for key affinity
- -MOVED and -ASK redirections
""",
}


def ingest_core_documentation() -> dict[str, Any]:
    """Ingest core database documentation into RAG system.
    
    Returns:
        Statistics about ingested documentation
    """
    retriever = DocumentRetriever()
    stats = {"total_chunks": 0, "databases": {}}
    
    for db_type, content in CORE_DOCS.items():
        chunk_count = retriever.add_documentation(
            content=content,
            source=f"{db_type.title()} Optimization Guide",
            db_type=db_type,
            chunk_size=400,
        )
        stats["databases"][db_type] = chunk_count
        stats["total_chunks"] += chunk_count
    
    return stats


def ingest_file(
    file_path: Path,
    db_type: str,
    source_name: str = None,
) -> int:
    """Ingest a documentation file into RAG system.
    
    Args:
        file_path: Path to documentation file (.md, .txt)
        db_type: Database type this doc relates to
        source_name: Optional source name
        
    Returns:
        Number of chunks ingested
    """
    content = file_path.read_text(encoding="utf-8")
    source = source_name or file_path.name
    
    retriever = DocumentRetriever()
    return retriever.add_documentation(
        content=content,
        source=source,
        db_type=db_type,
    )


def ingest_directory(docs_dir: Path, db_type: str) -> dict[str, int]:
    """Ingest all documentation files from a directory.
    
    Args:
        docs_dir: Directory containing documentation files
        db_type: Database type for all files
        
    Returns:
        Dict mapping filename to chunk count
    """
    stats = {}
    for file_path in docs_dir.glob("**/*.md"):
        try:
            chunks = ingest_file(file_path, db_type)
            stats[file_path.name] = chunks
        except Exception as e:
            stats[file_path.name] = f"Error: {e}"
    
    return stats
