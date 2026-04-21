"""Replay API endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from netobserv.api.common import require_resource, serialize_datetime
from netobserv.replay.engine import ReplayEngine
from netobserv.storage.database import get_db_session
from netobserv.storage.models import ReplayRecord
from netobserv.storage.repository import SnapshotRepository, WorkflowRepository

router = APIRouter()


class ReplayResponse(BaseModel):
    replay_id: str
    source_workflow_id: str
    status: str
    passed: bool | None = None
    diff_summary: dict[str, Any] = Field(default_factory=dict)
    notes: str = ""


class ReplayDetailResponse(ReplayResponse):
    created_at: str | None = None
    completed_at: str | None = None


@router.post("/workflows/{workflow_id}/replay", response_model=ReplayResponse, status_code=status.HTTP_202_ACCEPTED)
async def replay_workflow(
    workflow_id: str,
    db: AsyncSession = Depends(get_db_session),
) -> ReplayResponse:
    """Run a replay against a persisted workflow."""
    workflow_repo = WorkflowRepository(db)
    require_resource(await workflow_repo.get(workflow_id), "Workflow not found")
    snap_repo = SnapshotRepository(db)

    class _ReplayRepo:
        def __init__(self, session: Any) -> None:
            self.session = session

    engine = ReplayEngine(
        workflow_repo=workflow_repo,
        snapshot_repo=snap_repo,
        replay_repo=_ReplayRepo(db),
    )
    result = await engine.replay(source_workflow_id=workflow_id)

    return ReplayResponse(
        replay_id=result.replay_id,
        source_workflow_id=result.source_workflow_id,
        status="completed",
        passed=result.passed,
        diff_summary=result.diff_summary,
        notes=result.notes,
    )


@router.get("/replays/{replay_id}", response_model=ReplayDetailResponse)
async def get_replay(
    replay_id: str,
    db: AsyncSession = Depends(get_db_session),
) -> ReplayDetailResponse:
    """Get persisted replay details."""
    result = await db.execute(select(ReplayRecord).where(ReplayRecord.id == replay_id))
    record = require_resource(result.scalar_one_or_none(), "Replay not found")

    return ReplayDetailResponse(
        replay_id=record.id,
        source_workflow_id=record.source_workflow_id,
        status=record.status,
        passed=record.passed,
        diff_summary=record.diff_summary or {},
        created_at=serialize_datetime(record.created_at),
        completed_at=serialize_datetime(record.completed_at),
        notes=record.notes or "",
    )
