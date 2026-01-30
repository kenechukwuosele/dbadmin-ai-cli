"""Main CLI entry point for DbAdmin AI."""

import typer
from rich.console import Console
from rich.panel import Panel

from dbadmin import __version__
from dbadmin.cli.commands import analyze, chat, connect, health, recommend, query

# Initialize Rich console for beautiful output
console = Console()

# Create main Typer app
app = typer.Typer(
    name="dbadmin",
    help="🤖 AI-powered database administration CLI tool",
    add_completion=True,
    rich_markup_mode="rich",
    no_args_is_help=True,
)

# Register command groups
app.add_typer(health.app, name="health", help="Database health monitoring")
app.add_typer(analyze.app, name="analyze", help="Query analysis and optimization")
app.add_typer(recommend.app, name="recommend", help="Get optimization recommendations")
app.add_typer(connect.app, name="connect", help="Database connection management")
app.add_typer(chat.app, name="chat", help="Interactive AI chat interface")
app.add_typer(query.app, name="query", help="Natural language database queries")


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    version: bool = typer.Option(
        False, "--version", "-v", help="Show version and exit"
    ),
) -> None:
    """DbAdmin AI - Your intelligent database administration assistant."""
    if version:
        console.print(
            Panel(
                f"[bold cyan]DbAdmin AI[/bold cyan] v{__version__}\n"
                "[dim]AI-powered database administration CLI[/dim]",
                border_style="cyan",
            )
        )
        raise typer.Exit()
    
    # If no command provided, show help
    if ctx.invoked_subcommand is None:
        console.print(
            Panel(
                "[bold cyan]DbAdmin AI[/bold cyan] - Your intelligent database assistant\n\n"
                "Use [green]dbadmin --help[/green] to see available commands.\n"
                "Use [green]dbadmin chat[/green] to start an interactive session.",
                title="Welcome",
                border_style="cyan",
            )
        )


if __name__ == "__main__":
    app()
