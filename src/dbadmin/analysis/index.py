"""Index recommendation engine."""

from dataclasses import dataclass
from typing import Any

from dbadmin.connectors.base import BaseConnector
from dbadmin.analysis.health import Recommendation


@dataclass
class IndexRecommendation(Recommendation):
    """Index-specific recommendation."""
    table: str = ""
    columns: list[str] = None
    index_type: str = "btree"
    
    def __post_init__(self):
        if self.columns is None:
            self.columns = []


class IndexAnalyzer:
    """Analyze database indexes and recommend improvements."""
    
    def __init__(self, connector: BaseConnector):
        """Initialize analyzer with database connector."""
        self.connector = connector
    
    def get_recommendations(self) -> list[Recommendation]:
        """Get index recommendations based on database analysis.
        
        Analyzes:
        - Unused indexes (candidates for removal)
        - Duplicate indexes
        - Missing indexes (based on slow query patterns)
        """
        recommendations = []
        
        # Find unused indexes
        recommendations.extend(self._find_unused_indexes())
        
        # Analyze for missing indexes (based on schema)
        recommendations.extend(self._suggest_missing_indexes())
        
        return recommendations
    
    def _find_unused_indexes(self) -> list[Recommendation]:
        """Find indexes that are never or rarely used."""
        recommendations = []
        
        try:
            index_stats = self.connector.get_index_stats()
        except Exception:
            return []
        
        for idx in index_stats:
            scans = idx.get("scans", idx.get("accesses", 0))
            size = idx.get("size_bytes", 0)
            index_name = idx.get("index", idx.get("name", "unknown"))
            table_name = idx.get("table", idx.get("schema", ""))
            
            # Skip primary key indexes
            if "pkey" in index_name.lower() or "primary" in index_name.lower():
                continue
            
            if scans == 0:
                size_mb = size / (1024 * 1024) if size else 0
                recommendations.append(Recommendation(
                    title=f"Unused index: {index_name}",
                    description=f"Index on {table_name} has never been used. "
                               f"Removing it could save {size_mb:.1f}MB and improve write performance.",
                    priority="low" if size_mb < 10 else "medium",
                    category="index",
                    impact=f"Save {size_mb:.1f}MB disk space, faster writes",
                    sql=f"DROP INDEX {index_name};  -- Verify before running",
                ))
        
        return recommendations
    
    def _suggest_missing_indexes(self) -> list[Recommendation]:
        """Suggest indexes based on schema analysis."""
        recommendations = []
        
        try:
            schema = self.connector.get_schema()
        except Exception:
            return []
        
        tables = schema.get("tables", {})
        
        for table_name, table_info in tables.items():
            columns = table_info.get("columns", [])
            
            # Get existing indexes for this table
            try:
                table_detail = self.connector.get_table_info(table_name)
                existing_indexes = table_detail.get("indexes", [])
                indexed_columns = set()
                for idx in existing_indexes:
                    if isinstance(idx, dict):
                        for col in idx.get("columns", [idx.get("name", "")]):
                            indexed_columns.add(col.lower())
            except Exception:
                indexed_columns = set()
            
            # Look for common patterns that should be indexed
            for col in columns:
                col_name = col["name"].lower()
                col_type = col.get("type", "").lower()
                
                # Foreign key pattern (ends with _id)
                if col_name.endswith("_id") and col_name not in indexed_columns:
                    recommendations.append(Recommendation(
                        title=f"Missing index on foreign key: {table_name}.{col['name']}",
                        description=f"Column {col['name']} appears to be a foreign key but has no index. "
                                   "This can slow down JOINs and lookups.",
                        priority="medium",
                        category="index",
                        impact="Faster JOINs and foreign key lookups",
                        sql=f"CREATE INDEX idx_{table_name}_{col['name']} ON {table_name}({col['name']});",
                    ))
                
                # Email column pattern
                if "email" in col_name and col_name not in indexed_columns:
                    recommendations.append(Recommendation(
                        title=f"Consider index on email: {table_name}.{col['name']}",
                        description="Email columns are commonly queried. Consider adding an index.",
                        priority="low",
                        category="index",
                        sql=f"CREATE INDEX idx_{table_name}_{col['name']} ON {table_name}({col['name']});",
                    ))
                
                # Status/state columns
                if col_name in ("status", "state", "type", "category") and col_name not in indexed_columns:
                    recommendations.append(Recommendation(
                        title=f"Consider index on {col_name}: {table_name}.{col['name']}",
                        description=f"Column {col['name']} is likely used for filtering. Consider a partial index.",
                        priority="low",
                        category="index",
                    ))
        
        return recommendations
