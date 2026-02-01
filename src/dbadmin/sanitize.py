"""Input sanitization utilities for DbAdmin AI.

Provides functions to sanitize user input before including in prompts
to help mitigate prompt injection attacks.
"""

import re
from typing import Any, Optional


# Patterns that might indicate prompt injection attempts
INJECTION_PATTERNS = [
    r"ignore\s+(previous|above|all)\s+(instructions?|prompts?)",
    r"disregard\s+(previous|above|all)",
    r"forget\s+(everything|all|previous)",
    r"new\s+instructions?:",
    r"system\s*:",
    r"</?(system|user|assistant)>",
    r"<\|.*?\|>",  # Special control tokens
]

# Compile patterns for efficiency
_INJECTION_RE = re.compile("|".join(INJECTION_PATTERNS), re.IGNORECASE)


def detect_injection_attempt(text: str) -> bool:
    """Detect potential prompt injection attempts.
    
    Args:
        text: User input to check
        
    Returns:
        True if potential injection detected
    """
    return bool(_INJECTION_RE.search(text))


def sanitize_for_prompt(text: str, max_length: int = 10000) -> str:
    """Sanitize user input for inclusion in LLM prompts.
    
    Args:
        text: Raw user input
        max_length: Maximum allowed length
        
    Returns:
        Sanitized text safe for prompt inclusion
    """
    if not text:
        return ""
    
    # Truncate to prevent token exhaustion
    if len(text) > max_length:
        text = text[:max_length] + "...[truncated]"
    
    # Escape any XML/HTML-like tags that might be interpreted as control tokens
    text = re.sub(r"<([/]?)(system|user|assistant|s|im_start|im_end)", r"&lt;\1\2", text, flags=re.IGNORECASE)
    
    # Escape potential instruction separators
    text = text.replace("```", "'''")
    
    return text


def sanitize_sql_for_display(sql: str) -> str:
    """Sanitize SQL for safe display/logging.
    
    Args:
        sql: SQL query to sanitize
        
    Returns:
        SQL with sensitive patterns masked
    """
    if not sql:
        return ""
    
    # Mask potential password literals
    sql = re.sub(
        r"(password\s*=\s*['\"])([^'\"]+)(['\"])",
        r"\1***\3",
        sql,
        flags=re.IGNORECASE
    )
    
    return sql


def wrap_user_input(text: str, label: str = "USER_INPUT") -> str:
    """Wrap user input with clear delimiters for the LLM.
    
    This helps the model distinguish between instructions and user data.
    
    Args:
        text: User input to wrap
        label: Label for the input
        
    Returns:
        Wrapped input with clear delimiters
    """
    sanitized = sanitize_for_prompt(text)
    return f"--- START {label} ---\n{sanitized}\n--- END {label} ---"


def log_if_suspicious(text: str, logger: Optional[object] = None) -> bool:
    """Log and return True if input looks suspicious.
    
    Args:
        text: Input to check
        logger: Optional logger instance
        
    Returns:
        True if suspicious patterns detected
    """
    if detect_injection_attempt(text):
        if logger:
            try:
                logger.warning(f"Potential prompt injection detected: {text[:100]}...")
            except Exception:
                pass
        return True
    return False


# ===========================================================
# SQL Injection Prevention
# ===========================================================

# Dangerous SQL patterns that might indicate injection attempts
SQL_DANGEROUS_PATTERNS = [
    r";\s*(DROP|DELETE|TRUNCATE|ALTER|UPDATE|INSERT)",  # Multi-statement attacks
    r"--\s*$",  # SQL comment at end of line
    r"--\s+",  # SQL comment with content
    r"/\*.*\*/",  # Block comments
    r"UNION\s+(ALL\s+)?SELECT",  # UNION-based injection
    r"OR\s+['\"]?\d+['\"]?\s*=\s*['\"]?\d+['\"]?",  # OR 1=1 style
    r"AND\s+['\"]?\d+['\"]?\s*=\s*['\"]?\d+['\"]?",  # AND 1=1 style
    r"'\s*OR\s+'",  # String-based OR injection
    r";\s*EXEC\s",  # EXEC statements
    r"xp_cmdshell",  # SQL Server command execution
    r"BENCHMARK\s*\(",  # MySQL timing attacks
    r"SLEEP\s*\(",  # MySQL/PostgreSQL timing attacks
    r"WAITFOR\s+DELAY",  # SQL Server timing attacks
    r"pg_sleep",  # PostgreSQL timing attacks
    r"INTO\s+OUTFILE",  # MySQL file writes
    r"LOAD_FILE\s*\(",  # MySQL file reads
    r"CHAR\s*\(\s*\d+",  # Character encoding bypass
]

# Compile SQL patterns for efficiency
_SQL_INJECTION_RE = re.compile("|".join(SQL_DANGEROUS_PATTERNS), re.IGNORECASE)

# Valid SQL identifier pattern (conservative)
_SQL_IDENTIFIER = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')


def detect_sql_injection(input_text: str) -> bool:
    """Detect potential SQL injection in user input.
    
    This is a heuristic-based check that catches common attack patterns.
    It should be used in addition to (not instead of) parameterized queries.
    
    Args:
        input_text: User input to check
        
    Returns:
        True if potential injection detected
    """
    if not input_text:
        return False
    return bool(_SQL_INJECTION_RE.search(input_text))


def validate_sql_identifier(name: str, db_type: str = "generic") -> str:
    """Validate a SQL identifier (table name, column name, etc).
    
    This function validates that an identifier is safe to use in SQL queries.
    It should be used for dynamic table/column names that cannot be parameterized.
    
    Args:
        name: The identifier to validate
        db_type: Database type for length limits ('postgresql', 'mysql', 'generic')
        
    Returns:
        The validated identifier
        
    Raises:
        ValueError: If the identifier is invalid or potentially dangerous
    """
    if not name:
        raise ValueError("Identifier cannot be empty")
    
    if not _SQL_IDENTIFIER.match(name):
        raise ValueError(
            f"Invalid identifier: {name!r}. "
            "Only alphanumeric characters and underscores allowed, "
            "must start with letter or underscore."
        )
    
    # Check length limits by database type
    max_lengths = {
        "postgresql": 63,
        "mysql": 64,
        "generic": 63,
    }
    max_len = max_lengths.get(db_type, 63)
    
    if len(name) > max_len:
        raise ValueError(f"Identifier too long: {len(name)} > {max_len} characters")
    
    # Check for reserved words that might cause issues
    dangerous_names = {"dual", "all", "null", "default", "true", "false"}
    if name.lower() in dangerous_names:
        raise ValueError(f"Identifier is a reserved word: {name!r}")
    
    return name


def sanitize_order_direction(direction: str) -> str:
    """Sanitize ORDER BY direction.
    
    Args:
        direction: User-provided sort direction
        
    Returns:
        Safe direction string ('ASC' or 'DESC')
        
    Raises:
        ValueError: If direction is invalid
    """
    direction = direction.strip().upper()
    if direction not in ("ASC", "DESC"):
        raise ValueError(f"Invalid sort direction: {direction!r}")
    return direction


def sanitize_limit(limit: Any, max_limit: int = 10000) -> int:
    """Sanitize LIMIT value.
    
    Args:
        limit: User-provided limit value
        max_limit: Maximum allowed limit
        
    Returns:
        Safe integer limit value
        
    Raises:
        ValueError: If limit is invalid
    """
    try:
        limit = int(limit)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid limit value: {limit!r}")
    
    if limit < 0:
        raise ValueError("Limit cannot be negative")
    
    if limit > max_limit:
        raise ValueError(f"Limit too large: {limit} > {max_limit}")
    
    return limit


def log_suspicious_sql(query: str, logger: Optional[object] = None) -> bool:
    """Log and return True if SQL looks suspicious.
    
    Args:
        query: SQL query or fragment to check
        logger: Optional logger instance
        
    Returns:
        True if suspicious patterns detected
    """
    if detect_sql_injection(query):
        if logger:
            try:
                logger.warning(f"Potential SQL injection detected: {query[:200]}...")
            except Exception:
                pass
        return True
    return False

