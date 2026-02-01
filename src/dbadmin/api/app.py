"""FastAPI backend for DbAdmin AI web dashboard."""

import uuid
from datetime import datetime
from typing import Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from dbadmin.api.schemas import (
    ChatRequest, ChatResponse,
    QueryRequest, QueryResponse,
    DatabaseConnectRequest, DatabaseConnectResponse, DatabaseConnection,
    HealthReport, HealthMetric,
    SchemaResponse, TableInfo,
)
from dbadmin.ai.chat import ChatSession
from dbadmin.connectors import get_connector, detect_db_type
from dbadmin.analysis.health import HealthAnalyzer


# Store active sessions and connections
_sessions: dict[str, ChatSession] = {}
_connections: dict[str, DatabaseConnection] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """App lifespan events."""
    # Startup
    yield
    # Shutdown - cleanup sessions
    _sessions.clear()
    _connections.clear()


app = FastAPI(
    title="DbAdmin AI",
    description="AI-powered database administration API",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============ Chat Endpoints ============

@app.post("/api/chat", response_model=ChatResponse)
async def chat(request: ChatRequest) -> ChatResponse:
    """Send a message and get AI response."""
    session_id = request.session_id or str(uuid.uuid4())
    
    # Get or create session
    if session_id not in _sessions:
        _sessions[session_id] = ChatSession(
            database=request.database,
            smart_routing=True,
        )
    
    session = _sessions[session_id]
    
    # Get response
    response = session.send_message(request.message, use_critic=True)
    
    return ChatResponse(
        content=response.content,
        sql_query=response.query_executed or None,
        query_result=response.query_result or None,
        sources=response.sources,
        model_used=response.model_used,
        was_reviewed=response.was_reviewed,
        session_id=session_id,
    )


@app.delete("/api/chat/{session_id}")
async def clear_chat(session_id: str) -> dict:
    """Clear chat session."""
    if session_id in _sessions:
        _sessions[session_id].clear_history()
        del _sessions[session_id]
    return {"success": True}


# ============ Query Endpoints ============

@app.post("/api/query", response_model=QueryResponse)
async def query(request: QueryRequest) -> QueryResponse:
    """Convert natural language to SQL and execute."""
    try:
        session = ChatSession(
            database=request.database,
            smart_routing=True,
            use_rag=False,
        )
        
        response = session.execute_nl_query(request.question, use_critic=True)
        
        result = response.query_result or {}
        
        return QueryResponse(
            sql=response.query_executed or "",
            columns=result.get("columns", []),
            rows=result.get("rows", []),
            row_count=result.get("row_count", 0),
            execution_time_ms=result.get("execution_time_ms", 0),
            error=result.get("error"),
            model_used=response.model_used,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ============ Database Endpoints ============

@app.get("/api/databases", response_model=list[DatabaseConnection])
async def list_databases() -> list[DatabaseConnection]:
    """List saved database connections."""
    return list(_connections.values())


@app.post("/api/databases/connect", response_model=DatabaseConnectResponse)
async def connect_database(request: DatabaseConnectRequest) -> DatabaseConnectResponse:
    """Test and optionally save a database connection."""
    try:
        connector = get_connector(request.url)
        connector.connect()
        
        db_type = detect_db_type(request.url)
        conn_id = str(uuid.uuid4())[:8]
        name = request.name or f"{db_type}-{conn_id}"
        
        connection = DatabaseConnection(
            id=conn_id,
            name=name,
            url=request.url,
            db_type=db_type,
            is_connected=True,
        )
        
        if request.save:
            _connections[conn_id] = connection
        
        connector.disconnect()
        
        return DatabaseConnectResponse(
            success=True,
            message=f"Connected to {db_type} database",
            connection=connection,
        )
    except Exception as e:
        return DatabaseConnectResponse(
            success=False,
            message=str(e),
            connection=None,
        )


@app.delete("/api/databases/{db_id}")
async def remove_database(db_id: str) -> dict:
    """Remove a saved connection."""
    if db_id in _connections:
        del _connections[db_id]
    return {"success": True}


# ============ Health Endpoints ============

@app.get("/api/databases/{db_id}/health", response_model=HealthReport)
async def get_health(db_id: str) -> HealthReport:
    """Get database health report."""
    if db_id not in _connections:
        raise HTTPException(status_code=404, detail="Database not found")
    
    connection = _connections[db_id]
    
    try:
        connector = get_connector(connection.url)
        connector.connect()
        
        analyzer = HealthAnalyzer(connector)
        report = analyzer.analyze()
        
        connector.disconnect()
        
        return HealthReport(
            database=connection.name,
            db_type=connection.db_type,
            overall_score=report.overall_score,
            status=report.status,
            metrics=[
                HealthMetric(
                    name=m.name,
                    value=m.value,
                    unit=m.unit,
                    status=m.status,
                )
                for m in report.metrics
            ],
            recommendations=[r.description for r in report.recommendations],
            checked_at=datetime.now(),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/health/check", response_model=HealthReport)
async def check_health_url(url: str) -> HealthReport:
    """Check health of database by URL."""
    try:
        connector = get_connector(url)
        connector.connect()
        
        db_type = detect_db_type(url)
        analyzer = HealthAnalyzer(connector)
        report = analyzer.analyze()
        
        connector.disconnect()
        
        return HealthReport(
            database=url.split("/")[-1] if "/" in url else "database",
            db_type=db_type,
            overall_score=report.overall_score,
            status=report.status,
            metrics=[
                HealthMetric(
                    name=m.name,
                    value=m.value,
                    unit=m.unit,
                    status=m.status,
                )
                for m in report.metrics
            ],
            recommendations=[r.description for r in report.recommendations],
            checked_at=datetime.now(),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============ Schema Endpoints ============

@app.get("/api/databases/{db_id}/schema", response_model=SchemaResponse)
async def get_schema(db_id: str) -> SchemaResponse:
    """Get database schema."""
    if db_id not in _connections:
        raise HTTPException(status_code=404, detail="Database not found")
    
    connection = _connections[db_id]
    
    try:
        connector = get_connector(connection.url)
        connector.connect()
        
        schema = connector.get_schema()
        
        tables = [
            TableInfo(
                name=table["name"],
                schema=table.get("schema", "public"),
                row_count=table.get("row_count"),
                columns=table.get("columns", []),
                indexes=table.get("indexes", []),
            )
            for table in schema.get("tables", [])
        ]
        
        connector.disconnect()
        
        return SchemaResponse(
            database=connection.name,
            db_type=connection.db_type,
            tables=tables,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============ WebSocket for Real-time Metrics ============

@app.websocket("/api/ws/metrics/{db_id}")
async def metrics_websocket(websocket: WebSocket, db_id: str):
    """WebSocket for real-time metrics streaming."""
    await websocket.accept()
    
    if db_id not in _connections:
        await websocket.close(code=4004, reason="Database not found")
        return
    
    connection = _connections[db_id]
    
    try:
        connector = get_connector(connection.url)
        connector.connect()
        
        import asyncio
        while True:
            # Get current metrics
            try:
                analyzer = HealthAnalyzer(connector)
                report = analyzer.analyze()
                
                await websocket.send_json({
                    "score": report.overall_score,
                    "status": report.status,
                    "metrics": [
                        {"name": m.name, "value": m.value, "status": m.status}
                        for m in report.metrics
                    ],
                    "timestamp": datetime.now().isoformat(),
                })
            except Exception:
                pass
            
            await asyncio.sleep(5)  # Update every 5 seconds
            
    except WebSocketDisconnect:
        pass
    finally:
        try:
            connector.disconnect()
        except Exception:
            pass


# ============ Health Check ============

@app.get("/api/health")
async def api_health() -> dict:
    """API health check."""
    return {
        "status": "ok",
        "version": "0.1.0",
        "sessions": len(_sessions),
        "connections": len(_connections),
    }
