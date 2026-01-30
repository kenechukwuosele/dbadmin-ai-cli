"""Query analysis command for optimization suggestions."""

import typer
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from dbadmin.connectors import get_connector
from dbadmin.analysis.query import QueryAnalyzer

app = typer.Typer(help="Query analysis and optimization")
console = Console()


@app.callback(invoke_without_command=True)
def analyze(
    ctx: typer.Context,
    query: str = typer.Argument(
        ...,
        help="SQL query to analyze (or path to .sql file)",
    ),
    database: str = typer.Option(
        None,
        "--database", "-d",
        help="Database connection URL or name",
    ),
    explain: bool = typer.Option(
        True,
        "--explain/--no-explain",
        help="Run EXPLAIN ANALYZE on the query",
    ),
    suggest: bool = typer.Option(
        True,
        "--suggest/--no-suggest",
        help="Get AI-powered optimization suggestions",
    ),
    format: str = typer.Option(
        "rich",
        "--format", "-f",
        help="Output format: rich, json, or plain",
    ),
) -> None:
    """Analyze a SQL query for performance issues.
    
    Provides execution plan analysis, identifies bottlenecks,
    and suggests optimizations.
    
    Examples:
        dbadmin analyze "SELECT * FROM users WHERE email = 'test@example.com'"
        dbadmin analyze query.sql --database my-postgres
        dbadmin analyze "SELECT * FROM orders" -d postgresql://localhost/shop
    """
    # Check if query is a file path
    if query.endswith(".sql"):
        try:
            with open(query) as f:
                query = f.read()
        except FileNotFoundError:
            console.print(f"[red]Error:[/red] File not found: {query}")
            raise typer.Exit(1)
    
    console.print(Panel(
        Syntax(query, "sql", theme="monokai", line_numbers=False),
        title="Query",
        border_style="blue",
    ))
    
    try:
        # Get connector if database specified
        connector = get_connector(database) if database else None
        
        # Analyze query
        analyzer = QueryAnalyzer(connector)
        result = analyzer.analyze(query, run_explain=explain)
        
        # Display results
        if format == "json":
            import json
            console.print(json.dumps(result.to_dict(), indent=2))
        else:
            _display_analysis(result, suggest)
            
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)


def _display_analysis(result, show_suggestions: bool) -> None:
    """Display query analysis results."""
    # Performance score
    score = result.performance_score
    score_color = "green" if score >= 80 else "yellow" if score >= 60 else "red"
    
    console.print(f"\n[bold]Performance Score:[/bold] [{score_color}]{score}/100[/{score_color}]")
    
    # Issues found
    if result.issues:
        console.print("\n[bold red]⚠️ Issues Found:[/bold red]")
        for issue in result.issues:
            console.print(f"  • {issue}")
    
    # Execution plan summary
    if result.explain_plan:
        console.print("\n[bold]📊 Execution Plan Summary:[/bold]")
        console.print(f"  • Estimated Cost: {result.explain_plan.get('cost', 'N/A')}")
        console.print(f"  • Rows Estimated: {result.explain_plan.get('rows', 'N/A')}")
        console.print(f"  • Scan Type: {result.explain_plan.get('scan_type', 'N/A')}")
    
    # Optimization suggestions
    if show_suggestions and result.suggestions:
        console.print("\n[bold green]💡 Optimization Suggestions:[/bold green]")
        for i, suggestion in enumerate(result.suggestions, 1):
            console.print(f"\n  {i}. [cyan]{suggestion.title}[/cyan]")
            console.print(f"     {suggestion.description}")
            if suggestion.improved_query:
                console.print("\n     [dim]Suggested rewrite:[/dim]")
                console.print(Syntax(
                    suggestion.improved_query,
                    "sql",
                    theme="monokai",
                    line_numbers=False,
                    padding=1,
                ))
