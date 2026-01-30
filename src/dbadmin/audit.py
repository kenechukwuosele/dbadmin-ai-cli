"""Audit logging for DbAdmin AI.

Provides structured logging for security-relevant operations like:
- Database connections
- SQL queries executed
- LLM prompts and responses
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Optional


# Configure audit logger
_audit_logger = None


def _get_audit_logger() -> logging.Logger:
    """Get or create the audit logger."""
    global _audit_logger
    if _audit_logger is not None:
        return _audit_logger
    
    _audit_logger = logging.getLogger("dbadmin.audit")
    _audit_logger.setLevel(logging.INFO)
    
    # Don't propagate to root logger
    _audit_logger.propagate = False
    
    # Create audit log directory
    log_dir = Path.home() / ".dbadmin" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Set restrictive permissions
    try:
        os.chmod(log_dir, 0o700)
    except (OSError, AttributeError):
        pass
    
    # File handler with JSON format
    log_file = log_dir / "audit.log"
    handler = logging.FileHandler(log_file)
    handler.setFormatter(logging.Formatter("%(message)s"))
    _audit_logger.addHandler(handler)
    
    # Set restrictive permissions on log file
    try:
        os.chmod(log_file, 0o600)
    except (OSError, AttributeError):
        pass
    
    return _audit_logger


def _mask_sensitive(data: dict) -> dict:
    """Mask sensitive fields in log data."""
    masked = data.copy()
    sensitive_keys = {"password", "api_key", "secret", "token", "credential"}
    
    for key, value in masked.items():
        if any(s in key.lower() for s in sensitive_keys):
            masked[key] = "***REDACTED***"
        elif isinstance(value, str) and len(value) > 100:
            masked[key] = value[:100] + "...[truncated]"
    
    return masked


def log_event(
    event_type: str,
    details: dict[str, Any],
    user: Optional[str] = None,
    severity: str = "INFO",
) -> None:
    """Log a security-relevant event.
    
    Args:
        event_type: Type of event (e.g., "db_query", "connection", "llm_call")
        details: Event details (sensitive data will be masked)
        user: Optional user identifier
        severity: Log severity (INFO, WARNING, ERROR)
    """
    logger = _get_audit_logger()
    
    entry = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "event_type": event_type,
        "severity": severity,
        "user": user or os.getenv("USER", os.getenv("USERNAME", "unknown")),
        "details": _mask_sensitive(details),
    }
    
    log_line = json.dumps(entry, default=str)
    
    if severity == "ERROR":
        logger.error(log_line)
    elif severity == "WARNING":
        logger.warning(log_line)
    else:
        logger.info(log_line)


def log_db_connection(db_type: str, host: str, database: str, success: bool) -> None:
    """Log a database connection attempt."""
    log_event(
        "db_connection",
        {
            "db_type": db_type,
            "host": host,
            "database": database,
            "success": success,
        },
        severity="INFO" if success else "WARNING",
    )


def log_query_execution(
    query: str,
    db_type: str,
    execution_time_ms: float,
    row_count: int,
    error: Optional[str] = None,
) -> None:
    """Log a SQL query execution."""
    log_event(
        "query_execution",
        {
            "query": query[:500] if len(query) > 500 else query,  # Truncate long queries
            "db_type": db_type,
            "execution_time_ms": execution_time_ms,
            "row_count": row_count,
            "error": error,
        },
        severity="ERROR" if error else "INFO",
    )


def log_llm_call(
    provider: str,
    model: str,
    prompt_length: int,
    response_length: int,
    tokens_used: int,
) -> None:
    """Log an LLM API call (without content for privacy)."""
    log_event(
        "llm_call",
        {
            "provider": provider,
            "model": model,
            "prompt_length": prompt_length,
            "response_length": response_length,
            "tokens_used": tokens_used,
        },
    )
