"""Recommendation commands for database optimization."""

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from dbadmin.connectors import get_connector
from dbadmin.analysis.index import IndexAnalyzer
from dbadmin.analysis.health import HealthAnalyzer

app = typer.Typer(help="Get optimization recommendations")
console = Console()


@app.callback(invoke_without_command=True)
def recommend(
    ctx: typer.Context,
    database: str = typer.Argument(
        ...,
        help="Database connection URL or configured name",
    ),
    rec_type: str = typer.Option(
        "all",
        "--type", "-t",
        help="Recommendation type: index, query, maintenance, or all",
    ),
    limit: int = typer.Option(
        10,
        "--limit", "-n",
        help="Maximum number of recommendations",
    ),
    apply: bool = typer.Option(
        False,
        "--apply",
        help="Interactively apply recommendations",
    ),
    format: str = typer.Option(
        "rich",
        "--format", "-f",
        help="Output format: rich, json, or sql",
    ),
) -> None:
    """Get optimization recommendations for a database.
    
    Analyzes the database and provides actionable recommendations
    for indexes, queries, and maintenance tasks.
    
    Examples:
        dbadmin recommend my-postgres-db
        dbadmin recommend postgresql://localhost/mydb --type index
        dbadmin recommend my-db --apply
    """
    try:
        connector = get_connector(database)
        
        with console.status("Analyzing database..."):
            recommendations = _gather_recommendations(connector, rec_type, limit)
        
        if not recommendations:
            console.print("[green]✅ No recommendations - your database looks healthy![/green]")
            return
        
        if format == "json":
            import json
            console.print(json.dumps([r.to_dict() for r in recommendations], indent=2))
        elif format == "sql":
            _display_sql_only(recommendations)
        else:
            _display_recommendations(recommendations)
        
        if apply:
            _interactive_apply(connector, recommendations)
            
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)


def _gather_recommendations(connector, rec_type: str, limit: int):
    """Gather recommendations based on type."""
    recommendations = []
    
    if rec_type in ("all", "index"):
        index_analyzer = IndexAnalyzer(connector)
        recommendations.extend(index_analyzer.get_recommendations())
    
    if rec_type in ("all", "maintenance"):
        health_analyzer = HealthAnalyzer(connector)
        health_report = health_analyzer.analyze()
        recommendations.extend(health_report.recommendations)
    
    # Sort by priority and limit
    priority_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    recommendations.sort(key=lambda r: priority_order.get(r.priority, 4))
    
    return recommendations[:limit]


def _display_recommendations(recommendations) -> None:
    """Display recommendations in rich format."""
    console.print(Panel(
        f"Found [bold]{len(recommendations)}[/bold] optimization recommendations",
        title="📋 Recommendations",
        border_style="cyan",
    ))
    
    for i, rec in enumerate(recommendations, 1):
        priority_colors = {
            "critical": "red",
            "high": "yellow",
            "medium": "blue",
            "low": "dim",
        }
        color = priority_colors.get(rec.priority, "white")
        
        console.print(f"\n[bold]{i}. {rec.title}[/bold]")
        console.print(f"   [{color}][{rec.priority.upper()}][/{color}] {rec.category}")
        console.print(f"   [dim]{rec.description}[/dim]")
        
        if rec.impact:
            console.print(f"   [green]Expected Impact:[/green] {rec.impact}")
        
        if rec.sql:
            from rich.syntax import Syntax
            console.print(Syntax(rec.sql, "sql", theme="monokai", padding=1))


def _display_sql_only(recommendations) -> None:
    """Display only SQL commands from recommendations."""
    for rec in recommendations:
        if rec.sql:
            console.print(f"-- {rec.title}")
            console.print(rec.sql)
            console.print()


def _interactive_apply(connector, recommendations) -> None:
    """Interactively apply recommendations."""
    applicable = [r for r in recommendations if r.sql]
    
    if not applicable:
        console.print("[yellow]No applicable SQL commands in recommendations[/yellow]")
        return
    
    console.print(f"\n[bold]Apply {len(applicable)} recommendations?[/bold]")
    
    for rec in applicable:
        if typer.confirm(f"Apply: {rec.title}?"):
            try:
                connector.execute(rec.sql)
                console.print(f"[green]✅ Applied: {rec.title}[/green]")
            except Exception as e:
                console.print(f"[red]❌ Failed: {e}[/red]")
