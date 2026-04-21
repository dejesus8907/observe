"""Discovery API endpoints.

The API should be honest about what it does. This router can create workflow
records and expose their status, but it does not launch an external worker
fabric. Responses therefore use accepted/queued semantics instead of pretending
that distributed execution already exists.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Request, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from netobserv.api.common import require_resource
from netobserv.models.enums import WorkflowStatus
from netobserv.storage.database import get_db_session
from netobserv.runtime.dispatcher import DispatchRequest
from netobserv.runtime.models import StageSpec
from netobserv.storage.repository import WorkflowRepository
from netobserv.workflows.manager import WorkflowManager

router = APIRouter()


class WorkflowStepSummary(BaseModel):
    stage: str
    status: str
    elapsed_seconds: float | None = None
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class WorkflowStatusResponse(BaseModel):
    workflow_id: str
    workflow_type: str
    status: str
    current_stage: str | None = None
    created_at: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
    error_message: str | None = None
    steps: list[WorkflowStepSummary] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class DiscoveryRunRequest(BaseModel):
    inventory_source: str = "static_file"
    inventory_config: dict[str, Any] = Field(default_factory=dict)
    scope: str = "global"
    concurrency: int = Field(default=20, ge=1, le=500)
    read_only: bool = True


class DiscoveryRunResponse(BaseModel):
    workflow_id: str
    status: str
    accepted: bool
    message: str


class WorkflowArtifactsResponse(BaseModel):
    workflow_id: str
    artifacts: dict[str, Any] = Field(default_factory=dict)


@router.post("/runs", response_model=DiscoveryRunResponse, status_code=status.HTTP_202_ACCEPTED)
async def start_discovery_run(
    request: DiscoveryRunRequest,
    db: AsyncSession = Depends(get_db_session),
) -> DiscoveryRunResponse:
    """Create a discovery workflow record and return an honest accepted status."""
    workflow_repo = WorkflowRepository(db)
    manager = WorkflowManager(workflow_repo=workflow_repo)

    workflow_id = await manager.start_workflow(
        "discovery",
        {
            "scope": request.scope,
            "inventory_source": request.inventory_source,
            "inventory_config": request.inventory_config,
            "concurrency": request.concurrency,
            "read_only": request.read_only,
        },
    )

    # The workflow manager creates the record immediately, but this API layer does
    # not yet own a durable background worker to advance the workflow.
    await manager.complete_workflow(workflow_id, WorkflowStatus.PENDING)

    return DiscoveryRunResponse(
        workflow_id=workflow_id,
        status=WorkflowStatus.PENDING.value,
        accepted=True,
        message=(
            "Discovery workflow record created. Execution is not automatically "
            "dispatched by this API process."
        ),
    )


@router.get("/runs/{workflow_id}", response_model=WorkflowStatusResponse)
async def get_discovery_run(
    workflow_id: str,
    db: AsyncSession = Depends(get_db_session),
) -> WorkflowStatusResponse:
    """Get persisted discovery workflow status."""
    workflow_repo = WorkflowRepository(db)
    manager = WorkflowManager(workflow_repo=workflow_repo)
    status_dict = await manager.get_status(workflow_id)
    if "error" in status_dict:
        require_resource(None, f"Workflow {workflow_id} not found")
    return WorkflowStatusResponse(**status_dict)


@router.get("/runs/{workflow_id}/artifacts", response_model=WorkflowArtifactsResponse)
async def get_discovery_artifacts(
    workflow_id: str,
    db: AsyncSession = Depends(get_db_session),
) -> WorkflowArtifactsResponse:
    """Return artifacts collected for each workflow stage."""
    workflow_repo = WorkflowRepository(db)
    record = require_resource(await workflow_repo.get(workflow_id), "Workflow not found")

    artifacts: dict[str, Any] = {}
    for step in record.steps:
        artifacts[step.stage_name] = step.artifacts

    return WorkflowArtifactsResponse(workflow_id=workflow_id, artifacts=artifacts)
