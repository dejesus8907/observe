"""Health and readiness endpoints.

Health should answer two different questions:
- Is the process alive?
- Is the service actually ready to serve requests?

The old version lied about readiness by always returning "ready" without
checking startup state or database connectivity.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from netobserv.config.settings import get_settings
from netobserv.storage.database import get_db_session

router = APIRouter()


class HealthResponse(BaseModel):
    status: str
    version: str
    metrics_enabled: bool
    tracing_enabled: bool


class ReadinessResponse(BaseModel):
    status: str
    startup_complete: bool
    database_ok: bool
    startup_error: str | None = None


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Liveness probe for process-level health."""
    settings = get_settings()
    return HealthResponse(
        status="ok",
        version=settings.app_version,
        metrics_enabled=settings.metrics_enabled,
        tracing_enabled=settings.tracing_enabled,
    )


@router.get("/ready", response_model=ReadinessResponse)
async def readiness_check(
    request: Request,
    db: AsyncSession = Depends(get_db_session),
) -> ReadinessResponse:
    """Readiness probe backed by startup state and DB connectivity."""
    startup_complete = bool(getattr(request.app.state, "startup_complete", False))
    startup_error = getattr(request.app.state, "startup_error", None)

    database_ok = False
    try:
        await db.execute(text("SELECT 1"))
        database_ok = True
    except Exception:
        database_ok = False

    status_value = "ready" if startup_complete and database_ok else "not_ready"
    return ReadinessResponse(
        status=status_value,
        startup_complete=startup_complete,
        database_ok=database_ok,
        startup_error=startup_error,
    )
