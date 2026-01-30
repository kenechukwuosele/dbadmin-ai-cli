"""Chat session for interactive database conversations with natural language."""

from dataclasses import dataclass, field
from typing import Any

from dbadmin.ai.llm import LLMClient, get_llm_client
from dbadmin.ai.prompts import SYSTEM_PROMPT, NL_TO_SQL_PROMPT, format_schema_for_prompt
from dbadmin.connectors import get_connector, detect_db_type
from dbadmin.connectors.base import BaseConnector


@dataclass
class ChatMessage:
    """A message in the chat history."""
    role: str  # 'user', 'assistant', 'system'
    content: str


@dataclass
class ChatResponse:
    """Response from chat session."""
    content: str
    query_executed: str = ""
    query_result: dict = field(default_factory=dict)
    sources: list[str] = field(default_factory=list)
    model_used: str = ""
    was_reviewed: bool = False


class ChatSession:
    """Interactive chat session for database operations using natural language.
    
    Features:
    - Natural language to SQL conversion
    - Smart task routing (mini models for simple, smart for complex)
    - Critic pattern for SQL verification (optional)
    """
    
    def __init__(
        self,
        database: str = None,
        provider: str = None,
        model: str = None,
        use_rag: bool = True,
        smart_routing: bool = False,
    ):
        """Initialize chat session.
        
        Args:
            database: Database URL or name to connect to
            provider: LLM provider (openrouter, groq, openai, etc.)
            model: LLM model to use (auto-detected if not specified)
            use_rag: Whether to use RAG for documentation lookup
            smart_routing: Enable task-based model routing (saves costs)
        """
        self.database = database
        self.use_rag = use_rag
        self.smart_routing = smart_routing
        self._history: list[ChatMessage] = []
        self._connector: BaseConnector | None = None
        self._schema: dict = {}
        self._db_type: str = ""
        
        # Initialize LLM client or router
        if smart_routing:
            from dbadmin.ai.router import TaskRouter
            self._router = TaskRouter()
            self._llm = None  # Will be set per-request
        else:
            self._router = None
            self._llm = get_llm_client(provider=provider, model=model)
        
        self._provider = provider
        self._model = model
        
        # Connect to database if provided
        if database:
            self._connect_database(database)
        
        # Initialize RAG if enabled
        self._rag = None
        if use_rag:
            self._init_rag()
    
    def _connect_database(self, database: str) -> None:
        """Connect to database and fetch schema."""
        try:
            self._connector = get_connector(database)
            self._connector.connect()
            self._db_type = detect_db_type(database) if "://" in database else "postgresql"
            self._schema = self._connector.get_schema()
        except Exception as e:
            print(f"Warning: Could not connect to database: {e}")
    
    def _init_rag(self) -> None:
        """Initialize RAG system for documentation lookup."""
        try:
            from dbadmin.rag.retriever import DocumentRetriever
            self._rag = DocumentRetriever()
        except Exception:
            pass
    
    def _get_llm_for_task(self, user_input: str) -> LLMClient:
        """Get appropriate LLM based on task complexity."""
        if self._router:
            from dbadmin.ai.router import TaskType
            task_type = self._router.classify_task(user_input)
            return self._router.get_client_for_task(task_type)
        return self._llm
    
    def send_message(self, user_input: str, use_critic: bool = False) -> ChatResponse:
        """Process user message and return response.
        
        Args:
            user_input: User's message
            use_critic: Use critic pattern for SQL verification
        """
        self._history.append(ChatMessage(role="user", content=user_input))
        
        # Get appropriate LLM for this task
        llm = self._get_llm_for_task(user_input)
        model_used = f"{llm.provider}/{llm.model}"
        
        # Build context
        schema_str = format_schema_for_prompt(self._schema, self._db_type) if self._schema else "No database connected"
        
        # Get RAG context
        rag_context = ""
        sources = []
        if self._rag and self.use_rag:
            try:
                rag_results = self._rag.retrieve(user_input, k=3)
                rag_context = "\n\nRelevant documentation:\n" + "\n".join(
                    [r["content"] for r in rag_results]
                )
                sources = [r["source"] for r in rag_results]
            except Exception:
                pass
        
        # Build system prompt
        system_prompt = SYSTEM_PROMPT.format(
            database_context=f"Connected to: {self.database or 'None'}\nType: {self._db_type or 'Unknown'}",
            schema_info=schema_str,
        ) + rag_context
        
        # Build messages
        messages = [{"role": "system", "content": system_prompt}]
        messages.extend([{"role": m.role, "content": m.content} for m in self._history[-10:]])
        
        # Get response (with optional critic review for SQL)
        was_reviewed = False
        if use_critic and self._should_use_critic(user_input):
            assistant_content, was_reviewed = self._generate_with_critic(user_input, schema_str)
        else:
            response = llm.complete(messages, temperature=0.3)
            assistant_content = response.content
        
        # Execute SQL if present
        query_executed = ""
        query_result = {}
        
        if self._connector and self._should_execute_query(assistant_content, user_input):
            sql = self._extract_sql(assistant_content)
            if sql:
                query_executed, query_result = self._try_execute_query(sql)
                if query_result.get("rows"):
                    assistant_content += f"\n\n**Query Results:**\n```\n{self._format_results(query_result)}\n```"
        
        self._history.append(ChatMessage(role="assistant", content=assistant_content))
        
        return ChatResponse(
            content=assistant_content,
            query_executed=query_executed,
            query_result=query_result,
            sources=sources,
            model_used=model_used,
            was_reviewed=was_reviewed,
        )
    
    def _should_use_critic(self, user_input: str) -> bool:
        """Determine if critic pattern should be used."""
        # Use critic for SQL generation, schema changes, dangerous ops
        keywords = ["create", "alter", "drop", "delete", "update", "insert", "migrate"]
        return any(kw in user_input.lower() for kw in keywords)
    
    def _generate_with_critic(self, user_input: str, schema_str: str) -> tuple[str, bool]:
        """Generate response with critic review."""
        from dbadmin.ai.router import CriticPattern, TaskType
        
        critic = CriticPattern(max_iterations=2)
        result = critic.generate_with_review(
            prompt=f"User request: {user_input}\n\nSchema:\n{schema_str}",
            task_type=TaskType.SQL_GENERATION,
            context={"schema": schema_str, "db_type": self._db_type},
        )
        
        return result["content"], result["reviewed"]
    
    def _should_execute_query(self, response: str, user_input: str) -> bool:
        """Determine if we should execute a query in the response."""
        query_keywords = ["show", "list", "find", "get", "count", "select", "display", "tell me"]
        has_query_intent = any(kw in user_input.lower() for kw in query_keywords)
        has_sql = "```sql" in response.lower() or "SELECT" in response
        return has_query_intent and has_sql
    
    def _extract_sql(self, response: str) -> str:
        """Extract SQL query from response."""
        import re
        sql_match = re.search(r'```sql\s*(.*?)\s*```', response, re.DOTALL | re.IGNORECASE)
        if sql_match:
            return sql_match.group(1).strip()
        select_match = re.search(r'(SELECT\s+.*?;)', response, re.DOTALL | re.IGNORECASE)
        if select_match:
            return select_match.group(1).strip()
        return ""
    
    def _try_execute_query(self, sql: str) -> tuple[str, dict]:
        """Safely execute a query and return results."""
        try:
            result = self._connector.execute_read_only(sql)
            if result.error:
                return sql, {"error": result.error}
            return sql, {
                "columns": result.columns,
                "rows": result.rows[:50],
                "row_count": result.row_count,
                "execution_time_ms": result.execution_time_ms,
            }
        except Exception as e:
            return sql, {"error": str(e)}
    
    def _format_results(self, result: dict) -> str:
        """Format query results for display."""
        if result.get("error"):
            return f"Error: {result['error']}"
        
        columns = result.get("columns", [])
        rows = result.get("rows", [])
        
        if not rows:
            return "No results returned."
        
        lines = []
        lines.append(" | ".join(str(c) for c in columns))
        lines.append("-" * len(lines[0]))
        
        for row in rows[:20]:
            lines.append(" | ".join(str(v)[:30] for v in row))
        
        if len(rows) > 20:
            lines.append(f"... and {len(rows) - 20} more rows")
        
        lines.append(f"\n({result.get('row_count', len(rows))} rows, {result.get('execution_time_ms', 0):.1f}ms)")
        return "\n".join(lines)
    
    def execute_nl_query(self, question: str, use_critic: bool = False) -> ChatResponse:
        """Execute a natural language query directly."""
        if not self._connector:
            return ChatResponse(content="No database connected.")
        
        schema_str = format_schema_for_prompt(self._schema, self._db_type)
        llm = self._get_llm_for_task(question)
        
        if use_critic:
            # Use critic pattern for important queries
            from dbadmin.ai.router import CriticPattern, TaskType
            critic = CriticPattern()
            result = critic.generate_with_review(
                prompt=NL_TO_SQL_PROMPT.format(
                    db_type=self._db_type,
                    schema=schema_str,
                    question=question,
                ),
                task_type=TaskType.SQL_GENERATION,
            )
            sql = result["content"].strip()
            was_reviewed = result["reviewed"]
        else:
            prompt = NL_TO_SQL_PROMPT.format(
                db_type=self._db_type,
                schema=schema_str,
                question=question,
            )
            messages = [{"role": "user", "content": prompt}]
            response = llm.complete(messages, temperature=0.1)
            sql = response.content.strip()
            was_reviewed = False
        
        query_executed, query_result = self._try_execute_query(sql)
        
        if query_result.get("error"):
            content = f"Generated SQL:\n```sql\n{sql}\n```\n\nError: {query_result['error']}"
        else:
            content = f"Generated SQL:\n```sql\n{sql}\n```\n\n**Results:**\n```\n{self._format_results(query_result)}\n```"
        
        return ChatResponse(
            content=content,
            query_executed=query_executed,
            query_result=query_result,
            model_used=f"{llm.provider}/{llm.model}",
            was_reviewed=was_reviewed,
        )
    
    def clear_history(self) -> None:
        """Clear conversation history."""
        self._history.clear()
    
    def get_history(self) -> list[ChatMessage]:
        """Get conversation history."""
        return self._history.copy()
