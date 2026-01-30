"""Query analysis and optimization."""

from dataclasses import dataclass, field
from typing import Any

from dbadmin.connectors.base import BaseConnector, ExplainPlan


@dataclass
class QuerySuggestion:
    """Query optimization suggestion."""
    title: str
    description: str
    improved_query: str = ""
    expected_improvement: str = ""


@dataclass 
class QueryAnalysisResult:
    """Result of query analysis."""
    original_query: str
    performance_score: int  # 0-100
    explain_plan: dict = field(default_factory=dict)
    issues: list[str] = field(default_factory=list)
    suggestions: list[QuerySuggestion] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        return {
            "original_query": self.original_query,
            "performance_score": self.performance_score,
            "explain_plan": self.explain_plan,
            "issues": self.issues,
            "suggestions": [
                {"title": s.title, "description": s.description, "improved_query": s.improved_query}
                for s in self.suggestions
            ],
        }


class QueryAnalyzer:
    """Analyze SQL queries for performance issues."""
    
    def __init__(self, connector: BaseConnector = None):
        """Initialize analyzer with optional database connector."""
        self.connector = connector
    
    def analyze(self, query: str, run_explain: bool = True) -> QueryAnalysisResult:
        """Analyze a SQL query for performance issues.
        
        Args:
            query: SQL query to analyze
            run_explain: Whether to run EXPLAIN on the database
            
        Returns:
            QueryAnalysisResult with score, issues, and suggestions
        """
        issues = []
        suggestions = []
        explain_data = {}
        
        # Static analysis
        static_issues, static_suggestions = self._static_analysis(query)
        issues.extend(static_issues)
        suggestions.extend(static_suggestions)
        
        # Run EXPLAIN if connector available
        if run_explain and self.connector:
            try:
                explain_plan = self.connector.explain_query(query)
                explain_data = explain_plan.to_dict()
                
                # Analyze explain plan
                plan_issues, plan_suggestions = self._analyze_explain(explain_plan)
                issues.extend(plan_issues)
                suggestions.extend(plan_suggestions)
            except Exception as e:
                issues.append(f"Could not get explain plan: {e}")
        
        # Calculate performance score
        score = self._calculate_score(issues, explain_data)
        
        return QueryAnalysisResult(
            original_query=query,
            performance_score=score,
            explain_plan=explain_data,
            issues=issues,
            suggestions=suggestions,
        )
    
    def _static_analysis(self, query: str) -> tuple[list[str], list[QuerySuggestion]]:
        """Perform static analysis on query text."""
        issues = []
        suggestions = []
        query_upper = query.upper()
        
        # Check for SELECT *
        if "SELECT *" in query_upper:
            issues.append("Using SELECT * - retrieves all columns")
            suggestions.append(QuerySuggestion(
                title="Avoid SELECT *",
                description="Specify only the columns you need to reduce data transfer and potentially use covering indexes.",
            ))
        
        # Check for missing LIMIT
        if "SELECT" in query_upper and "LIMIT" not in query_upper and "COUNT" not in query_upper:
            if "WHERE" not in query_upper or "=" not in query:
                issues.append("No LIMIT clause on potentially large result set")
                suggestions.append(QuerySuggestion(
                    title="Add LIMIT clause",
                    description="Add LIMIT to prevent accidentally fetching too many rows.",
                ))
        
        # Check for LIKE with leading wildcard
        if "LIKE '%'" in query or "LIKE '%" in query:
            issues.append("LIKE with leading wildcard cannot use index")
            suggestions.append(QuerySuggestion(
                title="Avoid leading wildcard in LIKE",
                description="LIKE '%value' cannot use an index. Consider full-text search if needed.",
            ))
        
        # Check for OR conditions that might prevent index use
        if " OR " in query_upper and "WHERE" in query_upper:
            issues.append("OR conditions may prevent optimal index usage")
            suggestions.append(QuerySuggestion(
                title="Consider UNION for OR conditions",
                description="Sometimes UNION of two queries performs better than OR in WHERE clause.",
            ))
        
        # Check for functions on indexed columns
        import re
        func_pattern = r'WHERE\s+\w+\s*\([^)]*\)\s*='
        if re.search(func_pattern, query_upper):
            issues.append("Function applied to column in WHERE clause may prevent index use")
        
        return issues, suggestions
    
    def _analyze_explain(self, plan: ExplainPlan) -> tuple[list[str], list[QuerySuggestion]]:
        """Analyze execution plan for issues."""
        issues = []
        suggestions = []
        
        # Check for sequential scan
        if plan.scan_type in ("Seq Scan", "ALL", "COLLSCAN"):
            issues.append(f"Full table/collection scan detected ({plan.scan_type})")
            suggestions.append(QuerySuggestion(
                title="Add index to avoid full scan",
                description="Create an index on the columns used in WHERE or JOIN conditions.",
            ))
        
        # Check for high row estimates
        if plan.rows > 10000:
            issues.append(f"Query may process {plan.rows:,} rows")
        
        # Check warnings from plan
        for warning in plan.warnings:
            issues.append(warning)
        
        return issues, suggestions
    
    def _calculate_score(self, issues: list[str], explain_data: dict) -> int:
        """Calculate performance score based on analysis."""
        score = 100
        
        # Deduct for issues
        critical_keywords = ["full scan", "seq scan", "collscan", "all"]
        for issue in issues:
            issue_lower = issue.lower()
            if any(kw in issue_lower for kw in critical_keywords):
                score -= 25
            else:
                score -= 10
        
        # Deduct for high row count
        rows = explain_data.get("rows", 0)
        if rows > 100000:
            score -= 20
        elif rows > 10000:
            score -= 10
        
        return max(0, min(100, score))
