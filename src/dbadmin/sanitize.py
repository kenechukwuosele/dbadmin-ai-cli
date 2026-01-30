"""Input sanitization utilities for DbAdmin AI.

Provides functions to sanitize user input before including in prompts
to help mitigate prompt injection attacks.
"""

import re
from typing import Optional


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
