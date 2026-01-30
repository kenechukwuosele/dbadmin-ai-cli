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
    """Save connection with encrypted credential storage.
    
    Uses the system keyring for secure password storage.
    Only connection metadata is stored in JSON; credentials go to keyring.
    """
    import json
    from pathlib import Path
    from urllib.parse import urlparse, urlunparse
    
    # Try to use keyring for secure storage
    try:
        import keyring
        use_keyring = True
    except ImportError:
        use_keyring = False
        console.print("[yellow]Warning: keyring not installed. Credentials will be stored in plain text.[/yellow]")
        console.print("[dim]Install with: pip install keyring[/dim]")
    
    config_file = Path.home() / ".dbadmin" / "connections.json"
    config_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Set restrictive permissions on the config directory (Unix-like systems)
    try:
        import os
        os.chmod(config_file.parent, 0o700)
    except (OSError, AttributeError):
        pass  # Windows or permission error
    
    connections = {}
    if config_file.exists():
        connections = json.loads(config_file.read_text())
    
    # Parse URL to extract and secure the password
    parsed = urlparse(url)
    
    if use_keyring and parsed.password:
        # Store password in system keyring
        keyring.set_password("dbadmin", f"{name}_password", parsed.password)
        
        # Store URL without password in config file
        safe_url = urlunparse((
            parsed.scheme,
            f"{parsed.username}@{parsed.hostname}" + (f":{parsed.port}" if parsed.port else ""),
            parsed.path,
            parsed.params,
            parsed.query,
            parsed.fragment
        ))
        
        connections[name] = {
            "url": safe_url,
            "type": db_type,
            "has_keyring_password": True,
        }
    else:
        # Fallback: store full URL (less secure)
        connections[name] = {
            "url": url,
            "type": db_type,
            "has_keyring_password": False,
        }
    
    config_file.write_text(json.dumps(connections, indent=2))
    
    # Set restrictive permissions on the config file
    try:
        import os
        os.chmod(config_file, 0o600)
    except (OSError, AttributeError):
        pass


def _load_connection(name: str) -> str | None:
    """Load a saved connection, retrieving password from keyring if needed."""
    import json
    from pathlib import Path
    from urllib.parse import urlparse, urlunparse
    
    config_file = Path.home() / ".dbadmin" / "connections.json"
    if not config_file.exists():
        return None
    
    connections = json.loads(config_file.read_text())
    conn = connections.get(name)
    if not conn:
        return None
    
    url = conn["url"]
    
    # Retrieve password from keyring if stored there
    if conn.get("has_keyring_password"):
        try:
            import keyring
            password = keyring.get_password("dbadmin", f"{name}_password")
            if password:
                parsed = urlparse(url)
                # Reconstruct URL with password
                netloc = f"{parsed.username}:{password}@{parsed.hostname}"
                if parsed.port:
                    netloc += f":{parsed.port}"
                url = urlunparse((
                    parsed.scheme,
                    netloc,
                    parsed.path,
                    parsed.params,
                    parsed.query,
                    parsed.fragment
                ))
        except ImportError:
            console.print("[red]Error: keyring required but not installed[/red]")
            return None
    
    return url
