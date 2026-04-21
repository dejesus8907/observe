"""Timeout helpers for durable runtime jobs and stages.

Timeout handling should not be smeared across worker, recovery, and repository
code as ad hoc datetime comparisons. This module centralizes timeout semantics
so the runtime evaluates deadlines consistently and can persist timeout-driven
state transitions honestly.

This file provides:
- job deadline calculation
- stage deadline calculation
- timeout inspection helpers
- timeout transition helpers for jobs and stage records

It does not perform queue polling or lease recovery. It is pure timeout logic.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from .errors import TimeoutError
from .models import JobRecord, JobState, StageRecord, StageState, utc_now


@dataclass(slots=True)
class DeadlineStatus:
    """Structured deadline evaluation result."""

    is_timed_out: bool
    deadline: Optional[datetime]
    exceeded_by_seconds: float = 0.0

    @classmethod
    def not_applicable(cls) -> "DeadlineStatus":
        return cls(is_timed_out=False, deadline=None, exceeded_by_seconds=0.0)


def job_deadline(job: JobRecord) -> Optional[datetime]:
    """Return the absolute job deadline, if the job has a timeout."""

    if not job.timeout_seconds or not job.started_at:
        return None
    return job.started_at + timedelta(seconds=job.timeout_seconds)


def stage_deadline(stage: StageRecord, timeout_seconds: Optional[float]) -> Optional[datetime]:
    """Return the absolute deadline for a stage attempt, if configured."""

    if not timeout_seconds or not stage.started_at:
        return None
    return stage.started_at + timedelta(seconds=timeout_seconds)


def evaluate_deadline(deadline: Optional[datetime], *, now: Optional[datetime] = None) -> DeadlineStatus:
    """Evaluate whether a deadline has been exceeded."""

    if deadline is None:
        return DeadlineStatus.not_applicable()

    now = now or utc_now()
    if now < deadline:
        return DeadlineStatus(is_timed_out=False, deadline=deadline, exceeded_by_seconds=0.0)

    exceeded = (now - deadline).total_seconds()
    return DeadlineStatus(is_timed_out=True, deadline=deadline, exceeded_by_seconds=exceeded)


def evaluate_job_timeout(job: JobRecord, *, now: Optional[datetime] = None) -> DeadlineStatus:
    """Evaluate whether a job has exceeded its configured timeout."""

    return evaluate_deadline(job_deadline(job), now=now)


def evaluate_stage_timeout(
    stage: StageRecord,
    *,
    timeout_seconds: Optional[float],
    now: Optional[datetime] = None,
) -> DeadlineStatus:
    """Evaluate whether a stage attempt has exceeded its configured timeout."""

    return evaluate_deadline(stage_deadline(stage, timeout_seconds), now=now)


def require_job_not_timed_out(job: JobRecord, *, now: Optional[datetime] = None) -> None:
    """Raise a structured timeout error if the job deadline is exceeded."""

    status = evaluate_job_timeout(job, now=now)
    if status.is_timed_out:
        raise TimeoutError(
            f"job '{job.job_id}' exceeded its timeout by {status.exceeded_by_seconds:.3f} seconds"
        )


def require_stage_not_timed_out(
    stage: StageRecord,
    *,
    timeout_seconds: Optional[float],
    now: Optional[datetime] = None,
) -> None:
    """Raise a structured timeout error if the stage deadline is exceeded."""

    status = evaluate_stage_timeout(stage, timeout_seconds=timeout_seconds, now=now)
    if status.is_timed_out:
        raise TimeoutError(
            f"stage '{stage.stage_name}' attempt {stage.attempt_number} exceeded its timeout "
            f"by {status.exceeded_by_seconds:.3f} seconds"
        )


def mark_job_timed_out(
    job: JobRecord,
    *,
    now: Optional[datetime] = None,
    error_message: Optional[str] = None,
) -> JobRecord:
    """Return a timed-out job record with terminal state applied.

    This helper does not persist anything. It prepares the correct state
    transition so callers stop duplicating timeout transition code.
    """

    now = now or utc_now()
    if job.is_terminal():
        return job.model_copy(update={"error_message": error_message or job.error_message})

    transitioned = job.transition(JobState.TIMED_OUT, now=now)
    return transitioned.model_copy(
        update={
            "error_message": error_message or transitioned.error_message,
            "lease": None,
        }
    )


def mark_stage_timed_out(
    stage: StageRecord,
    *,
    now: Optional[datetime] = None,
    error_message: Optional[str] = None,
) -> StageRecord:
    """Return a stage record with timed-out terminal state applied."""

    now = now or utc_now()
    if stage.state in StageState.terminal_states():
        return stage.model_copy(update={"error_message": error_message or stage.error_message})

    return stage.model_copy(
        update={
            "state": StageState.TIMED_OUT,
            "completed_at": now,
            "error_message": error_message or stage.error_message,
        }
    )
