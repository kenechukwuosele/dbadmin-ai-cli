"""Health check command for database monitoring."""

import typer
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from dbadmin.connectors import get_connector
from dbadmin.analysis.health import HealthAnalyzer

app = typer.Typer(help="Database health monitoring commands")
console = Console()


@app.callback(invoke_without_command=True)
def health(
    ctx: typer.Context,
    database: str = typer.Argument(
        ...,
        help="Database connection URL or configured database name",
    ),
    format: str = typer.Option(
        "table",
        "--format", "-f",
        help="Output format: table, json, or brief",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose", "-v",
        help="Show detailed metrics",
    ),
) -> None:
    """Check the health status of a database.
    
    Analyzes database health and provides a score (0-100) along with
    actionable recommendations.
    
    Examples:
        dbadmin health postgresql://localhost:5432/mydb
        dbadmin health my-postgres-db
    """
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
        transient=True,
    ) as progress:
        progress.add_task("Connecting to database...", total=None)
        
        try:
            # Get appropriate connector
            connector = get_connector(database)
            
            progress.add_task("Analyzing health metrics...", total=None)
            
            # Run health analysis
            analyzer = HealthAnalyzer(connector)
            health_report = analyzer.analyze()
            
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
            raise typer.Exit(1)
    
    # Display results based on format
    if format == "json":
        import json
        console.print(json.dumps(health_report.to_dict(), indent=2))
    elif format == "brief":
        _display_brief(health_report)
    else:
        _display_table(health_report, verbose)


def _display_brief(report) -> None:
    """Display brief health summary."""
    score = report.score
    status_color = "green" if score >= 80 else "yellow" if score >= 60 else "red"
    console.print(f"Health Score: [{status_color}]{score}/100[/{status_color}]")
    
    if report.critical_issues:
        console.print(f"[red]Critical Issues:[/red] {len(report.critical_issues)}")


def _display_table(report, verbose: bool) -> None:
    """Display detailed health table."""
    # Score panel
    score = report.score
    status_color = "green" if score >= 80 else "yellow" if score >= 60 else "red"
    status_emoji = "✅" if score >= 80 else "⚠️" if score >= 60 else "❌"
    
    console.print(
        Panel(
            f"[bold {status_color}]{status_emoji} Health Score: {score}/100[/bold {status_color}]\n"
            f"[dim]Database: {report.database_name}[/dim]",
            title="Database Health",
            border_style=status_color,
        )
    )
    
    # Metrics table
    if verbose and report.metrics:
        table = Table(title="Health Metrics", show_header=True)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", justify="right")
        table.add_column("Status", justify="center")
        
        for metric in report.metrics:
            status = "🟢" if metric.is_healthy else "🔴"
            table.add_row(metric.name, str(metric.value), status)
        
        console.print(table)
    
    # Recommendations
    if report.recommendations:
        console.print("\n[bold]📋 Recommendations:[/bold]")
        for i, rec in enumerate(report.recommendations[:5], 1):
            priority_color = {
                "critical": "red",
                "high": "yellow", 
                "medium": "blue",
                "low": "dim",
            }.get(rec.priority, "white")
            console.print(f"  {i}. [{priority_color}][{rec.priority.upper()}][/{priority_color}] {rec.message}")
