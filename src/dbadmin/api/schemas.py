"""Pydantic schemas for API request/response models."""

from pydantic import BaseModel, Field
from typing import Any
from datetime import datetime


# ============ Chat Schemas ============

class ChatRequest(BaseModel):
    """Chat message request."""
    message: str = Field(..., description="User message")
    database: str | None = Field(None, description="Database connection URL or name")
    session_id: str | None = Field(None, description="Chat session ID for history")


class ChatResponse(BaseModel):
    """Chat message response."""
    content: str
    sql_query: str | None = None
    query_result: dict | None = None
    sources: list[str] = []
    model_used: str = ""
    was_reviewed: bool = False
    session_id: str = ""


# ============ Query Schemas ============

class QueryRequest(BaseModel):
    """Natural language query request."""
    question: str = Field(..., description="Natural language question")
    database: str = Field(..., description="Database connection URL or name")
    execute: bool = Field(True, description="Execute the generated SQL")


class QueryResponse(BaseModel):
    """Query response with results."""
    sql: str
    columns: list[str] = []
    rows: list[list[Any]] = []
    row_count: int = 0
    execution_time_ms: float = 0
    error: str | None = None
    model_used: str = ""


# ============ Database Schemas ============

class DatabaseConnection(BaseModel):
    """Database connection info."""
    id: str
    name: str
    url: str
    db_type: str
    is_connected: bool = False


class DatabaseConnectRequest(BaseModel):
    """Request to connect/save a database."""
    url: str = Field(..., description="Database connection URL")
    name: str | None = Field(None, description="Friendly name for the connection")
    save: bool = Field(False, description="Save connection for later")


class DatabaseConnectResponse(BaseModel):
    """Response from connection attempt."""
    success: bool
    message: str
    connection: DatabaseConnection | None = None


# ============ Health Schemas ============

class HealthMetric(BaseModel):
    """Single health metric."""
    name: str
    value: float | str
    unit: str = ""
    status: str = "ok"  # ok, warning, critical


class HealthReport(BaseModel):
    """Database health report."""
    database: str
    db_type: str
    overall_score: float = Field(..., ge=0, le=100)
    status: str  # healthy, warning, critical
    metrics: list[HealthMetric] = []
    recommendations: list[str] = []
    checked_at: datetime


# ============ Schema Schemas ============

class TableInfo(BaseModel):
    """Table information."""
    name: str
    db_schema: str = "public"
    row_count: int | None = None
    columns: list[dict] = []
    indexes: list[dict] = []


class SchemaResponse(BaseModel):
    """Database schema response."""
    database: str
    db_type: str
    tables: list[TableInfo] = []
