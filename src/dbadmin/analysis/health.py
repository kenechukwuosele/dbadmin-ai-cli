"""Health analysis for databases."""

from dataclasses import dataclass, field
from typing import Any

from dbadmin.connectors.base import BaseConnector


@dataclass
class HealthMetric:
    """Individual health metric."""
    name: str
    value: Any
    is_healthy: bool
    threshold: Any = None
    description: str = ""


@dataclass
class Recommendation:
    """Optimization recommendation."""
    title: str
    description: str
    priority: str  # critical, high, medium, low
    category: str  # index, query, maintenance, config
    impact: str = ""
    sql: str = ""
    
    def to_dict(self) -> dict:
        return {
            "title": self.title,
            "description": self.description,
            "priority": self.priority,
            "category": self.category,
            "impact": self.impact,
            "sql": self.sql,
        }


@dataclass
class HealthReport:
    """Database health report."""
    database_name: str
    score: int  # 0-100
    metrics: list[HealthMetric] = field(default_factory=list)
    recommendations: list[Recommendation] = field(default_factory=list)
    critical_issues: list[str] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        return {
            "database_name": self.database_name,
            "score": self.score,
            "metrics": [
                {"name": m.name, "value": m.value, "is_healthy": m.is_healthy}
                for m in self.metrics
            ],
            "recommendations": [r.to_dict() for r in self.recommendations],
            "critical_issues": self.critical_issues,
        }


class HealthAnalyzer:
    """Analyze database health and generate reports.
    
    Health score algorithm (0-100):
    - Query performance (40% weight)
    - Resource utilization (25% weight)
    - Connection pool health (15% weight)
    - Index coverage (10% weight)
    - Replication lag (10% weight)
    """
    
    # Weights for health score calculation
    WEIGHTS = {
        "query_performance": 0.40,
        "resource_utilization": 0.25,
        "connection_health": 0.15,
        "index_coverage": 0.10,
        "replication": 0.10,
    }
    
    def __init__(self, connector: BaseConnector):
        """Initialize analyzer with database connector."""
        self.connector = connector
    
    def analyze(self) -> HealthReport:
        """Perform full health analysis.
        
        Returns:
            HealthReport with score, metrics, and recommendations
        """
        # Get raw metrics from database
        raw_metrics = self.connector.get_health_metrics()
        
        # Calculate component scores
        scores = {}
        metrics = []
        recommendations = []
        critical_issues = []
        
        # Connection health
        conn_score, conn_metrics, conn_recs = self._analyze_connections(raw_metrics)
        scores["connection_health"] = conn_score
        metrics.extend(conn_metrics)
        recommendations.extend(conn_recs)
        
        # Cache/buffer health
        cache_score, cache_metrics, cache_recs = self._analyze_cache(raw_metrics)
        scores["resource_utilization"] = cache_score
        metrics.extend(cache_metrics)
        recommendations.extend(cache_recs)
        
        # Query performance (from slow queries if available)
        query_score, query_metrics, query_recs = self._analyze_queries()
        scores["query_performance"] = query_score
        metrics.extend(query_metrics)
        recommendations.extend(query_recs)
        
        # Index coverage
        index_score, index_metrics, index_recs = self._analyze_indexes()
        scores["index_coverage"] = index_score
        metrics.extend(index_metrics)
        recommendations.extend(index_recs)
        
        # Default replication score (would need specific implementation)
        scores["replication"] = 100
        
        # Calculate weighted overall score
        overall_score = int(sum(
            scores.get(key, 100) * weight
            for key, weight in self.WEIGHTS.items()
        ))
        
        # Identify critical issues
        if overall_score < 50:
            critical_issues.append("Database health is critically low")
        for rec in recommendations:
            if rec.priority == "critical":
                critical_issues.append(rec.title)
        
        # Get database name
        try:
            info = self.connector.test_connection()
            db_name = info.get("database", "Unknown")
        except Exception:
            db_name = "Unknown"
        
        return HealthReport(
            database_name=db_name,
            score=overall_score,
            metrics=metrics,
            recommendations=sorted(recommendations, key=lambda r: 
                {"critical": 0, "high": 1, "medium": 2, "low": 3}.get(r.priority, 4)),
            critical_issues=critical_issues,
        )
    
    def _analyze_connections(self, metrics: dict) -> tuple[int, list, list]:
        """Analyze connection pool health."""
        health_metrics = []
        recommendations = []
        
        active = metrics.get("active_connections", 0)
        available = metrics.get("available_connections", 100)
        
        # Calculate connection usage percentage
        if available > 0:
            usage_pct = (active / (active + available)) * 100
        else:
            usage_pct = active * 10  # Estimate
        
        health_metrics.append(HealthMetric(
            name="Active Connections",
            value=active,
            is_healthy=usage_pct < 80,
            description=f"{usage_pct:.1f}% connection utilization",
        ))
        
        # Score based on usage
        if usage_pct > 90:
            score = 20
            recommendations.append(Recommendation(
                title="Connection pool exhaustion risk",
                description="Connection usage is above 90%. Consider increasing pool size or using connection pooling.",
                priority="critical",
                category="config",
                impact="Prevents connection failures",
            ))
        elif usage_pct > 80:
            score = 60
            recommendations.append(Recommendation(
                title="High connection usage",
                description="Connection usage is above 80%. Monitor closely.",
                priority="medium",
                category="config",
            ))
        elif usage_pct > 60:
            score = 80
        else:
            score = 100
        
        return score, health_metrics, recommendations
    
    def _analyze_cache(self, metrics: dict) -> tuple[int, list, list]:
        """Analyze cache/buffer pool health."""
        health_metrics = []
        recommendations = []
        
        # Try different cache hit ratio field names
        hit_ratio = metrics.get("cache_hit_ratio") or metrics.get("buffer_pool_hit_ratio") or metrics.get("hit_rate", 0)
        
        health_metrics.append(HealthMetric(
            name="Cache Hit Ratio",
            value=f"{hit_ratio:.1f}%",
            is_healthy=hit_ratio > 90,
        ))
        
        if hit_ratio < 80:
            score = 40
            recommendations.append(Recommendation(
                title="Low cache hit ratio",
                description=f"Cache hit ratio is {hit_ratio:.1f}%. Consider increasing buffer/cache size.",
                priority="high",
                category="config",
                impact="Improved query performance",
            ))
        elif hit_ratio < 90:
            score = 70
            recommendations.append(Recommendation(
                title="Cache hit ratio below optimal",
                description=f"Cache hit ratio is {hit_ratio:.1f}%. Target is 95%+.",
                priority="medium",
                category="config",
            ))
        elif hit_ratio < 95:
            score = 85
        else:
            score = 100
        
        return score, health_metrics, recommendations
    
    def _analyze_queries(self) -> tuple[int, list, list]:
        """Analyze query performance."""
        health_metrics = []
        recommendations = []
        
        try:
            slow_queries = self.connector.get_slow_queries(limit=5)
        except Exception:
            slow_queries = []
        
        health_metrics.append(HealthMetric(
            name="Slow Queries",
            value=len(slow_queries),
            is_healthy=len(slow_queries) < 3,
        ))
        
        if len(slow_queries) > 5:
            score = 50
            recommendations.append(Recommendation(
                title="Multiple slow queries detected",
                description=f"Found {len(slow_queries)} slow queries. Review and optimize.",
                priority="high",
                category="query",
            ))
        elif len(slow_queries) > 0:
            score = 75
            recommendations.append(Recommendation(
                title="Slow queries present",
                description=f"Found {len(slow_queries)} slow queries to review.",
                priority="medium",
                category="query",
            ))
        else:
            score = 100
        
        return score, health_metrics, recommendations
    
    def _analyze_indexes(self) -> tuple[int, list, list]:
        """Analyze index usage and coverage."""
        health_metrics = []
        recommendations = []
        
        try:
            index_stats = self.connector.get_index_stats()
        except Exception:
            index_stats = []
        
        # Find unused indexes
        unused = [idx for idx in index_stats if idx.get("scans", idx.get("accesses", 1)) == 0]
        
        health_metrics.append(HealthMetric(
            name="Unused Indexes",
            value=len(unused),
            is_healthy=len(unused) < 3,
        ))
        
        if len(unused) > 5:
            score = 70
            recommendations.append(Recommendation(
                title="Multiple unused indexes",
                description=f"Found {len(unused)} unused indexes. Consider removing to improve write performance.",
                priority="medium",
                category="index",
            ))
        elif len(unused) > 0:
            score = 85
        else:
            score = 100
        
        return score, health_metrics, recommendations
