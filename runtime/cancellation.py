"""Cooperative cancellation helpers for the durable runtime.

Cancellation in this runtime is cooperative, not preemptive. That means the
system must provide a consistent way to:
- inspect cancellation requests
- raise structured cancellation flow-control errors
- prepare job and stage records for terminal cancellation persistence

Without a dedicated module, cancellation logic gets smeared across workers,
dispatchers, and recovery code, which leads to inconsistent behavior and
operator confusion.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from .errors import CancellationRequestedError, ErrorContext
from .models import JobRecord, JobState, StageRecord, StageState, utc_now


@dataclass(slots=True)
class CancellationStatus:
    """Structured cancellation inspection result."""

    cancellation_requested: bool
    requested_for_job_id: Optional[str] = None
    reason: Optional[str] = None

    @classmethod
    def not_requested(cls) -> "CancellationStatus":
        return cls(cancellation_requested=False)


def evaluate_job_cancellation(job: JobRecord) -> CancellationStatus:
    """Return whether cooperative cancellation has been requested."""

    if not job.cancellation_requested:
        return CancellationStatus.not_requested()

    reason = None
    if isinstance(job.metadata, dict):
        raw_reason = job.metadata.get("cancellation_reason")
        if raw_reason is not None:
            text = str(raw_reason).strip()
            if text:
                reason = text

    return CancellationStatus(
        cancellation_requested=True,
        requested_for_job_id=job.job_id,
        reason=reason,
    )


def require_job_not_cancelled(
    job: JobRecord,
    *,
    worker_id: Optional[str] = None,
    stage_name: Optional[str] = None,
    attempt_number: Optional[int] = None,
) -> None:
    """Raise a structured cancellation error if the job was cancelled."""

    status = evaluate_job_cancellation(job)
    if not status.cancellation_requested:
        return

    message = status.reason or f"job '{job.job_id}' cancellation requested"
    raise CancellationRequestedError(
        message,
        context=ErrorContext(
            workflow_id=job.workflow_id,
            job_id=job.job_id,
            stage_name=stage_name or job.current_stage_name,
            worker_id=worker_id,
            lease_token=job.lease.lease_token if job.lease else None,
            attempt_number=attempt_number or job.attempt_number,
        ),
    )


def mark_job_cancelled(
    job: JobRecord,
    *,
    now: Optional[datetime] = None,
    error_message: Optional[str] = None,
) -> JobRecord:
    """Return a job record with terminal cancelled state applied.

    This prepares the state transition but does not persist it.
    """

    now = now or utc_now()
    if job.is_terminal():
        return job.model_copy(update={"error_message": error_message or job.error_message})

    transitioned = job.transition(JobState.CANCELLED, now=now)
    return transitioned.model_copy(
        update={
            "error_message": error_message or transitioned.error_message,
            "lease": None,
        }
    )


def mark_stage_cancelled(
    stage: StageRecord,
    *,
    now: Optional[datetime] = None,
    error_message: Optional[str] = None,
) -> StageRecord:
    """Return a stage record with terminal cancelled state applied."""

    now = now or utc_now()
    if stage.state in StageState.terminal_states():
        return stage.model_copy(update={"error_message": error_message or stage.error_message})

    return stage.model_copy(
        update={
            "state": StageState.CANCELLED,
            "completed_at": now,
            "error_message": error_message or stage.error_message,
        }
    )


def cancellation_note(job: JobRecord) -> Optional[str]:
    """Return a small operator-facing note about cancellation state."""

    status = evaluate_job_cancellation(job)
    if not status.cancellation_requested:
        return None
    if status.reason:
        return f"cancellation requested: {status.reason}"
    return "cancellation requested"


def attach_cancellation_reason(job: JobRecord, reason: Optional[str]) -> JobRecord:
    """Return a job copy with normalized cancellation reason metadata attached."""

    normalized = None
    if reason is not None:
        text = str(reason).strip()
        if text:
            normalized = text

    metadata = dict(job.metadata)
    if normalized is None:
        metadata.pop("cancellation_reason", None)
    else:
        metadata["cancellation_reason"] = normalized

    return job.model_copy(update={"metadata": metadata})
