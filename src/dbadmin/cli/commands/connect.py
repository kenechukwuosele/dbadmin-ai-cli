"""Database connection management commands."""

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from dbadmin.connectors import get_connector, detect_db_type
from dbadmin.config import get_settings

app = typer.Typer(help="Database connection management")
console = Console()


@app.callback(invoke_without_command=True)
def connect(
    ctx: typer.Context,
    url: str = typer.Argument(
        None,
        help="Database connection URL to test",
    ),
    list_all: bool = typer.Option(
        False,
        "--list", "-l",
        help="List all configured databases",
    ),
    save: str = typer.Option(
        None,
        "--save", "-s",
        help="Save connection with this name",
    ),
) -> None:
    """Test database connection or manage saved connections.
    
    Examples:
        dbadmin connect postgresql://user:pass@localhost:5432/mydb
        dbadmin connect mongodb://localhost:27017/admin
        dbadmin connect --list
        dbadmin connect postgresql://... --save my-prod-db
    """
    if list_all:
        _list_connections()
        return
    
    if not url:
        console.print("[yellow]Provide a database URL or use --list to see configured databases[/yellow]")
        raise typer.Exit(1)
    
    # Detect database type
    db_type = detect_db_type(url)
    console.print(f"[dim]Detected database type: {db_type}[/dim]")
    
    # Test connection
    try:
        connector = get_connector(url)
        
        with console.status("Testing connection..."):
            info = connector.test_connection()
        
        console.print(Panel(
            f"[green]✅ Connection successful![/green]\n\n"
            f"[bold]Database:[/bold] {info.get('database', 'N/A')}\n"
            f"[bold]Version:[/bold] {info.get('version', 'N/A')}\n"
            f"[bold]Host:[/bold] {info.get('host', 'N/A')}\n"
            f"[bold]Type:[/bold] {db_type}",
            title="Connection Test",
            border_style="green",
        ))
        
        if save:
            _save_connection(save, url, db_type)
            console.print(f"\n[green]Saved as '{save}'[/green]")
            
    except Exception as e:
        console.print(Panel(
            f"[red]❌ Connection failed![/red]\n\n"
            f"[bold]Error:[/bold] {e}",
            title="Connection Test",
            border_style="red",
        ))
        raise typer.Exit(1)


def _list_connections() -> None:
    """List all configured database connections."""
    settings = get_settings()
    dbs = settings.get_configured_databases()
    
    if not dbs:
        console.print("[yellow]No databases configured. Set environment variables or use 'dbadmin connect <url> --save <name>'[/yellow]")
        return
    
    table = Table(title="Configured Databases", show_header=True)
    table.add_column("Name", style="cyan")
    table.add_column("Type", style="magenta")
    table.add_column("URL", style="dim")
    
    for db_type, url in dbs.items():
        # Mask password in URL
        masked_url = _mask_password(url)
        table.add_row(db_type, db_type, masked_url)
    
    console.print(table)


def _mask_password(url: str) -> str:
    """Mask password in database URL for display."""
    import re
    return re.sub(r'://([^:]+):([^@]+)@', r'://\1:****@', url)


def _save_connection(name: str, url: str, db_type: str) -> None:
    """Save connection to local config file."""
    import json
    from pathlib import Path
    
    config_file = Path.home() / ".dbadmin" / "connections.json"
    config_file.parent.mkdir(parents=True, exist_ok=True)
    
    connections = {}
    if config_file.exists():
        connections = json.loads(config_file.read_text())
    
    connections[name] = {
        "url": url,
        "type": db_type,
    }
    
    config_file.write_text(json.dumps(connections, indent=2))
