"""Runtime job management API endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, Field

from netobserv.runtime.models import JobState
from netobserv.runtime.repository import JobSearchFilter

router = APIRouter()


class RuntimeJobView(BaseModel):
    job_id: str
    workflow_id: str
    job_type: str
    queue_name: str
    state: str
    attempt_number: int
    current_stage_name: str | None = None
    requested_by: str | None = None
    cancellation_requested: bool
    error_message: str | None = None
    created_at: str
    available_at: str
    started_at: str | None = None
    completed_at: str | None = None


class RuntimeJobListResponse(BaseModel):
    jobs: list[RuntimeJobView] = Field(default_factory=list)
    total: int


class RuntimeCancelResponse(BaseModel):
    job_id: str
    state: str
    cancellation_requested: bool
    message: str


def _runtime_repo(request: Request):
    repo = getattr(request.app.state, "runtime_repository", None)
    if repo is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Runtime repository is not initialized")
    return repo


def _runtime_dispatcher(request: Request):
    dispatcher = getattr(request.app.state, "runtime_dispatcher", None)
    if dispatcher is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Runtime dispatcher is not initialized")
    return dispatcher


def _job_view(job: Any) -> RuntimeJobView:
    return RuntimeJobView(
        job_id=job.job_id,
        workflow_id=job.workflow_id,
        job_type=job.job_type,
        queue_name=job.queue_name,
        state=job.state.value if hasattr(job.state, "value") else str(job.state),
        attempt_number=job.attempt_number,
        current_stage_name=job.current_stage_name,
        requested_by=job.requested_by,
        cancellation_requested=job.cancellation_requested,
        error_message=job.error_message,
        created_at=job.created_at.isoformat(),
        available_at=job.available_at.isoformat(),
        started_at=job.started_at.isoformat() if job.started_at else None,
        completed_at=job.completed_at.isoformat() if job.completed_at else None,
    )


@router.get("/jobs", response_model=RuntimeJobListResponse)
def list_runtime_jobs(request: Request, state: str | None = None, limit: int = 50) -> RuntimeJobListResponse:
    repo = _runtime_repo(request)
    states = None
    if state:
        try:
            states = [JobState(state)]
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid runtime job state: {state}") from exc
    jobs = repo.list_jobs(JobSearchFilter(states=states, limit=limit))
    return RuntimeJobListResponse(jobs=[_job_view(job) for job in jobs], total=len(jobs))


@router.get("/jobs/{job_id}", response_model=RuntimeJobView)
def get_runtime_job(job_id: str, request: Request) -> RuntimeJobView:
    repo = _runtime_repo(request)
    job = repo.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Runtime job not found")
    return _job_view(job)


@router.post("/jobs/{job_id}/cancel", response_model=RuntimeCancelResponse, status_code=status.HTTP_202_ACCEPTED)
def cancel_runtime_job(job_id: str, request: Request) -> RuntimeCancelResponse:
    dispatcher = _runtime_dispatcher(request)
    decision = dispatcher.cancel(job_id)
    return RuntimeCancelResponse(
        job_id=decision.job.job_id,
        state=decision.job.state.value if hasattr(decision.job.state, "value") else str(decision.job.state),
        cancellation_requested=decision.job.cancellation_requested,
        message="Runtime job cancellation requested",
    )
