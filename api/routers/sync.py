"""Sync API endpoints.

The sync API is deliberately cautious. Planning is supported, but execution
should not pretend to be a production-grade remote remediation fabric when this
process does not actually dispatch external workers.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Request, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from netobserv.api.common import require_resource, serialize_datetime
from netobserv.models.enums import SyncMode
from netobserv.storage.database import get_db_session
from netobserv.runtime.dispatcher import DispatchRequest
from netobserv.runtime.models import StageSpec
from netobserv.storage.repository import SyncRepository

router = APIRouter()


class SyncPlanRequest(BaseModel):
    snapshot_id: str | None = None
    workflow_id: str | None = None
    mode: SyncMode = SyncMode.DRY_RUN
    allow_create: bool = True
    allow_update: bool = True
    allow_delete: bool = False


class SyncPlanResponse(BaseModel):
    plan_id: str
    mode: str
    status: str
    created_at: str | None = None


@router.post("/plans", response_model=SyncPlanResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_sync_plan(
    request: SyncPlanRequest,
    db: AsyncSession = Depends(get_db_session),
) -> SyncPlanResponse:
    """Create and persist a sync plan record."""
    sync_repo = SyncRepository(db)
    policy = {
        "mode": request.mode.value,
        "allow_create": request.allow_create,
        "allow_update": request.allow_update,
        "allow_delete": request.allow_delete,
    }
    plan = await sync_repo.create_plan(
        workflow_id=request.workflow_id,
        snapshot_id=request.snapshot_id,
        mode=request.mode.value,
        policy=policy,
    )
    return SyncPlanResponse(
        plan_id=plan.id,
        mode=plan.mode,
        status=plan.status,
        created_at=serialize_datetime(plan.created_at),
    )


@router.get("/plans/{plan_id}")
async def get_sync_plan(
    plan_id: str,
    db: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    """Get sync plan details including all actions."""
    sync_repo = SyncRepository(db)
    plan = require_resource(await sync_repo.get_plan(plan_id), "Sync plan not found")

    return {
        "plan_id": plan.id,
        "mode": plan.mode,
        "status": plan.status,
        "action_count": len(plan.actions),
        "actions": [
            {
                "id": a.id,
                "object_type": a.object_type,
                "action_type": a.action_type,
                "object_ref": a.object_ref,
                "status": a.status,
                "payload": a.payload,
            }
            for a in plan.actions
        ],
        "created_at": serialize_datetime(plan.created_at),
        "policy": plan.policy,
    }


@router.post("/plans/{plan_id}/execute", status_code=status.HTTP_202_ACCEPTED)
async def execute_sync_plan(
    plan_id: str,
    http_request: Request,
    db: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    """Return an honest execution response for a sync plan."""
    sync_repo = SyncRepository(db)
    plan = require_resource(await sync_repo.get_plan(plan_id), "Sync plan not found")

    if plan.mode == SyncMode.DRY_RUN.value:
        return {
            "plan_id": plan_id,
            "status": "dry_run",
            "executed": False,
            "message": "Dry-run plan: no changes executed.",
        }

    dispatcher = getattr(http_request.app.state, "runtime_dispatcher", None)
    if dispatcher is None:
        return {
            "plan_id": plan_id,
            "status": "accepted",
            "executed": False,
            "message": "Runtime dispatcher is unavailable; sync execution remains pending.",
        }

    decision = dispatcher.submit(
        DispatchRequest(
            workflow_id=f"runtime-sync-{plan_id}",
            job_type="sync",
            stage_specs=[StageSpec(name="execute_sync", order=0)],
            payload={"plan_id": plan_id, "mode": plan.mode},
        )
    )
    return {
        "plan_id": plan_id,
        "runtime_job_id": decision.job.job_id,
        "status": decision.job.state.value,
        "executed": False,
        "message": "Sync runtime job created.",
    }
