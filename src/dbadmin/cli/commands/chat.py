"""Interactive AI chat command for database assistance."""

import os
import typer
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.prompt import Prompt

from dbadmin.ai.chat import ChatSession
from dbadmin.ai.llm import PROVIDERS

app = typer.Typer(help="Interactive AI chat interface")
console = Console()


@app.callback(invoke_without_command=True)
def chat(
    ctx: typer.Context,
    database: str = typer.Option(
        None,
        "--database", "-d",
        help="Database to use as context for questions",
    ),
    provider: str = typer.Option(
        None,
        "--provider", "-p",
        help="LLM provider: openrouter, groq, openai, anthropic, ollama, etc.",
    ),
    model: str = typer.Option(
        None,
        "--model", "-m",
        help="Model name (e.g., gpt-4o, claude-3.5-sonnet, llama-3.1-70b)",
    ),
    smart: bool = typer.Option(
        True,
        "--smart/--no-smart", "-s",
        help="Smart routing: auto-selects best model per task (default: on)",
    ),
    verify: bool = typer.Option(
        True,
        "--verify/--no-verify", "-v",
        help="Critic pattern: second model reviews SQL (default: on)",
    ),
    no_rag: bool = typer.Option(
        False,
        "--no-rag",
        help="Disable RAG documentation lookup",
    ),
) -> None:
    """Start an interactive AI chat session for database help.
    
    Ask questions in natural language about database optimization,
    troubleshooting, and best practices.
    
    Examples:
        dbadmin chat
        dbadmin chat -d postgresql://localhost/mydb
        dbadmin chat -p openai -m gpt-4o
        dbadmin chat --smart  # Auto-route to best model per task
        dbadmin chat --verify # Use critic to verify SQL
    """
    # Check for any API key
    has_key = any([
        os.getenv(config["env_key"]) 
        for config in PROVIDERS.values() 
        if config.get("env_key")
    ]) or os.getenv("LLM_API_KEY")
    
    # Also check if Ollama is running
    if not has_key:
        try:
            import httpx
            if httpx.get("http://localhost:11434/api/tags", timeout=1).status_code == 200:
                has_key = True
        except Exception:
            pass
    
    if not has_key:
        _show_setup_help()
        raise typer.Exit(1)
    
    # Initialize chat session
    try:
        session = ChatSession(
            database=database,
            provider=provider,
            model=model,
            use_rag=not no_rag,
            smart_routing=smart,
        )
    except Exception as e:
        console.print(f"[red]Error initializing chat:[/red] {e}")
        raise typer.Exit(1)
    
    # Welcome message
    if smart:
        mode_info = "[dim]Mode: Smart routing (auto-selects best model per task)[/dim]"
    elif session._llm:
        mode_info = f"[dim]Provider: {session._llm.provider} | Model: {session._llm.model}[/dim]"
    else:
        mode_info = "[dim]Mode: Default[/dim]"
    
    if verify:
        mode_info += "\n[dim]Verification: Enabled (critic reviews SQL)[/dim]"
    
    console.print(Panel(
        "[bold cyan]DbAdmin AI Chat[/bold cyan]\n\n"
        "Ask me anything about database administration!\n"
        "I can help with query optimization, troubleshooting,\n"
        "performance tuning, and best practices.\n\n"
        f"{mode_info}\n\n"
        "[dim]Commands: exit, clear, models, help[/dim]",
        title="🤖 Welcome",
        border_style="cyan",
    ))
    
    if database:
        console.print(f"[dim]Connected to: {database}[/dim]\n")
    
    # Chat loop
    while True:
        try:
            user_input = Prompt.ask("\n[bold blue]You[/bold blue]")
            
            if not user_input.strip():
                continue
            
            # Handle commands
            cmd = user_input.lower().strip()
            if cmd in ("exit", "quit", "q"):
                console.print("[dim]Goodbye! 👋[/dim]")
                break
            
            if cmd == "clear":
                session.clear_history()
                console.print("[dim]Conversation cleared.[/dim]")
                continue
            
            if cmd == "models":
                _show_models()
                continue
            
            if cmd == "help":
                _show_help()
                continue
            
            # Get AI response
            with console.status("Thinking...", spinner="dots"):
                response = session.send_message(user_input, use_critic=verify)
            
            # Display response
            console.print("\n[bold green]Assistant[/bold green]")
            console.print(Markdown(response.content))
            
            # Show metadata
            if response.model_used:
                info = f"[dim]Model: {response.model_used}"
                if response.was_reviewed:
                    info += " ✓ Verified"
                info += "[/dim]"
                console.print(info)
            
            if response.sources:
                console.print("\n[dim]📚 Sources:[/dim]")
                for source in response.sources[:3]:
                    console.print(f"  [dim]• {source}[/dim]")
                    
        except KeyboardInterrupt:
            console.print("\n[dim]Goodbye! 👋[/dim]")
            break
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")


def _show_setup_help() -> None:
    """Show setup instructions."""
    console.print(Panel(
        "[yellow]No LLM API key found![/yellow]\n\n"
        "[bold]Pick any provider:[/bold]\n\n"
        "🆓 [cyan]OpenRouter[/cyan] (100+ models, many free)\n"
        "   Get key: https://openrouter.ai/keys\n"
        "   Set: OPENROUTER_API_KEY=sk-or-...\n\n"
        "🆓 [cyan]Groq[/cyan] (fast, free tier)\n"
        "   Get key: https://console.groq.com\n"
        "   Set: GROQ_API_KEY=gsk_...\n\n"
        "🆓 [cyan]Ollama[/cyan] (local, free)\n"
        "   Run: ollama serve\n\n"
        "💰 [cyan]OpenAI[/cyan]: OPENAI_API_KEY=sk-...\n"
        "💰 [cyan]Anthropic[/cyan]: ANTHROPIC_API_KEY=sk-ant-...\n\n"
        "[dim]Or any OpenAI-compatible API via LLM_BASE_URL + LLM_API_KEY[/dim]",
        title="Setup Required",
        border_style="yellow",
    ))


def _show_models() -> None:
    """Show available models per provider."""
    from rich.table import Table
    table = Table(title="Available Models")
    table.add_column("Provider", style="cyan")
    table.add_column("Models", style="white")
    
    for name, config in PROVIDERS.items():
        models = ", ".join(config["models"][:3])
        if len(config["models"]) > 3:
            models += f" (+{len(config['models']) - 3} more)"
        table.add_row(name, models)
    
    console.print(table)


def _show_help() -> None:
    """Show help for chat commands."""
    console.print(Panel(
        "[bold]Commands:[/bold]\n"
        "• [cyan]exit[/cyan] - End session\n"
        "• [cyan]clear[/cyan] - Clear history\n"
        "• [cyan]models[/cyan] - List available models\n"
        "• [cyan]help[/cyan] - Show this help\n\n"
        "[bold]Example Questions:[/bold]\n"
        "• Why is my query slow?\n"
        "• What indexes should I add?\n"
        "• How do I optimize a JOIN?",
        title="💡 Help",
        border_style="blue",
    ))
