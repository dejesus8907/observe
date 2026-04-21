"""Validation API endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Request, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from netobserv.api.common import require_resource, serialize_datetime
from netobserv.models.enums import WorkflowStatus
from netobserv.storage.database import get_db_session
from netobserv.runtime.dispatcher import DispatchRequest
from netobserv.runtime.models import StageSpec
from netobserv.storage.repository import DriftRepository, SnapshotRepository, WorkflowRepository
from netobserv.workflows.manager import WorkflowManager

router = APIRouter()


class ValidationRunRequest(BaseModel):
    snapshot_id: str | None = None
    intended_source: str = "netbox"
    intended_config: dict[str, Any] = Field(default_factory=dict)
    sync_policy: dict[str, Any] = Field(default_factory=dict)


class ValidationRunResponse(BaseModel):
    workflow_id: str
    status: str
    accepted: bool
    message: str


class DriftRecordView(BaseModel):
    id: str
    object_type: str
    mismatch_type: str
    field_name: str | None = None
    intended_value: Any = None
    actual_value: Any = None
    severity: str
    explanation: str | None = None
    remediation_hint: str | None = None
    confidence: float | None = None
    created_at: str | None = None


class ValidationDriftResponse(BaseModel):
    workflow_id: str
    snapshot_id: str | None = None
    drift: list[DriftRecordView] = Field(default_factory=list)
    total: int
    message: str | None = None


@router.post("/runs", response_model=ValidationRunResponse, status_code=status.HTTP_202_ACCEPTED)
async def start_validation_run(
    request: ValidationRunRequest,
    db: AsyncSession = Depends(get_db_session),
) -> ValidationRunResponse:
    """Create a validation workflow record without faking execution dispatch."""
    workflow_repo = WorkflowRepository(db)
    manager = WorkflowManager(workflow_repo=workflow_repo)
    workflow_id = await manager.start_workflow(
        "validation",
        {
            "snapshot_id": request.snapshot_id,
            "intended_source": request.intended_source,
            "intended_config": request.intended_config,
            "sync_policy": request.sync_policy,
        },
    )
    await manager.complete_workflow(workflow_id, WorkflowStatus.PENDING)

    return ValidationRunResponse(
        workflow_id=workflow_id,
        status=WorkflowStatus.PENDING.value,
        accepted=True,
        message="Validation workflow record created. Execution is not auto-dispatched by the API.",
    )


@router.get("/runs/{workflow_id}")
async def get_validation_run(
    workflow_id: str,
    db: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    """Get validation workflow status."""
    workflow_repo = WorkflowRepository(db)
    manager = WorkflowManager(workflow_repo=workflow_repo)

    result = await manager.get_status(workflow_id)
    if "error" in result:
        require_resource(None, f"Workflow {workflow_id} not found")
    return result


@router.get("/runs/{workflow_id}/drift", response_model=ValidationDriftResponse)
async def get_validation_drift(
    workflow_id: str,
    severity: str | None = None,
    db: AsyncSession = Depends(get_db_session),
) -> ValidationDriftResponse:
    """Get drift records associated with a validation workflow."""
    workflow_repo = WorkflowRepository(db)
    require_resource(await workflow_repo.get(workflow_id), "Workflow not found")

    snap_repo = SnapshotRepository(db)
    snapshots = await snap_repo.list_recent(limit=20)
    snapshot_id = next((s.id for s in snapshots if s.workflow_id == workflow_id), None)

    if not snapshot_id:
        return ValidationDriftResponse(
            workflow_id=workflow_id,
            snapshot_id=None,
            drift=[],
            total=0,
            message="No snapshot found for workflow.",
        )

    drift_repo = DriftRepository(db)
    records = await drift_repo.get_by_snapshot(snapshot_id, severity=severity)

    return ValidationDriftResponse(
        workflow_id=workflow_id,
        snapshot_id=snapshot_id,
        drift=[
            DriftRecordView(
                id=d.id,
                object_type=d.object_type,
                mismatch_type=d.mismatch_type,
                field_name=d.field_name,
                intended_value=d.intended_value,
                actual_value=d.actual_value,
                severity=d.severity,
                explanation=d.explanation,
                remediation_hint=d.remediation_hint,
                confidence=d.confidence,
                created_at=serialize_datetime(d.created_at),
            )
            for d in records
        ],
        total=len(records),
    )
