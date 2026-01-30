"""Direct natural language query command."""

import typer
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from dbadmin.ai.chat import ChatSession
from dbadmin.config import get_settings

app = typer.Typer(help="Execute natural language database queries")
console = Console()


@app.callback(invoke_without_command=True)
def query(
    ctx: typer.Context,
    question: str = typer.Argument(
        ...,
        help="Natural language question or command",
    ),
    database: str = typer.Option(
        ...,
        "--database", "-d",
        help="Database connection URL or name (required)",
    ),
    provider: str = typer.Option(
        None,
        "--provider", "-p",
        help="LLM provider: openrouter, groq, openai, anthropic, etc.",
    ),
    model: str = typer.Option(
        None,
        "--model", "-m",
        help="Model name (e.g., gpt-4o, llama-3.1-70b)",
    ),
    execute: bool = typer.Option(
        True,
        "--execute/--no-execute",
        help="Execute the generated query",
    ),
    format: str = typer.Option(
        "table",
        "--format", "-f",
        help="Output format: table, json, csv",
    ),
) -> None:
    """Execute a natural language query against a database.
    
    Converts your question to SQL and optionally executes it.
    
    Examples:
        dbadmin query "show me top 10 users" -d postgresql://localhost/mydb
        dbadmin query "count orders by status" -d my-postgres -p groq
        dbadmin query "find users without orders" -d my-db --no-execute
    """
    # Check for API key
    import os
    has_key = any([
        os.getenv("OPENROUTER_API_KEY"),
        os.getenv("GROQ_API_KEY"),
        os.getenv("OPENAI_API_KEY"),
    ])
    
    if not has_key:
        console.print(Panel(
            "[yellow]No LLM API key found![/yellow]\n\n"
            "[bold]Recommended (free):[/bold]\n"
            "1. Get a free OpenRouter key at: [cyan]https://openrouter.ai/keys[/cyan]\n"
            "2. Set: [green]OPENROUTER_API_KEY=sk-or-...[/green]\n\n"
            "[dim]Alternatives: GROQ_API_KEY, OPENAI_API_KEY[/dim]",
            title="Setup Required",
            border_style="yellow",
        ))
        raise typer.Exit(1)
    
    # Initialize chat session with database (smart routing always on)
    try:
        session = ChatSession(
            database=database, 
            provider=provider, 
            model=model, 
            use_rag=False,
            smart_routing=True,  # Always enabled
        )
    except Exception as e:
        console.print(f"[red]Failed to connect to database:[/red] {e}")
        raise typer.Exit(1)
    
    # Convert and optionally execute (with critic verification)
    with console.status("Converting to SQL..."):
        response = session.execute_nl_query(question, use_critic=True)  # Always verify
    
    # Display SQL
    if response.query_executed:
        console.print(Panel(
            Syntax(response.query_executed, "sql", theme="monokai"),
            title="Generated SQL",
            border_style="blue",
        ))
    
    # Display results
    if response.query_result:
        if response.query_result.get("error"):
            console.print(f"[red]Error:[/red] {response.query_result['error']}")
        elif execute:
            _display_results(response.query_result, format)
    else:
        console.print(response.content)


def _display_results(result: dict, format: str) -> None:
    """Display query results in specified format."""
    columns = result.get("columns", [])
    rows = result.get("rows", [])
    row_count = result.get("row_count", len(rows))
    exec_time = result.get("execution_time_ms", 0)
    
    if not rows:
        console.print("[dim]No results returned.[/dim]")
        return
    
    if format == "json":
        import json
        data = [dict(zip(columns, row)) for row in rows]
        console.print(json.dumps(data, indent=2, default=str))
    
    elif format == "csv":
        import csv
        import io
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(columns)
        writer.writerows(rows)
        console.print(output.getvalue())
    
    else:  # table
        table = Table(title=f"Results ({row_count} rows, {exec_time:.1f}ms)")
        
        for col in columns:
            table.add_column(str(col), overflow="fold")
        
        for row in rows[:50]:  # Limit display
            table.add_row(*[str(v)[:50] for v in row])
        
        if len(rows) > 50:
            table.add_row(*["..." for _ in columns])
        
        console.print(table)
