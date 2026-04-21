"""Runtime dispatcher for durable job submission and queue handoff.

This module sits between the control plane (API, CLI, workflow launcher) and
the worker runtime. Its job is to create valid durable execution records,
enforce queue-facing submission rules, and centralize handoff semantics so the
rest of the platform stops inventing its own partial enqueue logic.

A bad dispatcher usually looks like this:
- accept arbitrary dict payloads
- stuff them into storage
- let workers discover inconsistencies later

That is garbage. The dispatcher must validate the execution contract up front.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

from .errors import ConfigurationError, InputError
from .models import (
    ExecutionMode,
    JobRecord,
    JobState,
    RetryPolicy,
    StageSpec,
    utc_now,
)
from .repository import RuntimeRepository


@dataclass(slots=True)
class DispatchRequest:
    """Declarative request to submit a new runtime job.

    This is the control-plane input contract for durable submission. It
    intentionally separates request shaping from the persisted `JobRecord`
    so the dispatcher can enforce defaults and invariants in one place.
    """

    workflow_id: str
    job_type: str
    payload: Dict[str, Any] = field(default_factory=dict)
    stage_specs: List[StageSpec] = field(default_factory=list)
    queue_name: str = "default"
    requested_by: Optional[str] = None
    execution_mode: ExecutionMode = ExecutionMode.NORMAL
    priority: int = 100
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    timeout_seconds: Optional[float] = None
    available_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    job_id: Optional[str] = None

    def normalized_workflow_id(self) -> str:
        value = self.workflow_id.strip()
        if not value:
            raise InputError("workflow_id is required")
        return value

    def normalized_job_type(self) -> str:
        value = self.job_type.strip()
        if not value:
            raise InputError("job_type is required")
        return value

    def normalized_queue_name(self) -> str:
        value = self.queue_name.strip()
        if not value:
            raise InputError("queue_name is required")
        return value

    def normalized_requested_by(self) -> Optional[str]:
        if self.requested_by is None:
            return None
        value = self.requested_by.strip()
        return value or None


@dataclass(slots=True)
class DispatchDecision:
    """Structured dispatcher result returned to callers.

    The dispatcher should not make callers reverse-engineer queue state
    from raw records. This result explains what happened.
    """

    job: JobRecord
    accepted: bool
    enqueued: bool
    notes: List[str] = field(default_factory=list)


class RuntimeDispatcher:
    """Submit and manage durable runtime jobs.

    The dispatcher owns:
    - construction of valid `JobRecord`s
    - queue name and stage plan validation
    - cancellation requests
    - retry requeue transitions
    - explicit operator-facing notes about what actually happened

    It does *not* execute jobs. Workers do that.
    """

    def __init__(
        self,
        repository: RuntimeRepository,
        *,
        allowed_queues: Optional[List[str]] = None,
        default_queue: str = "default",
    ) -> None:
        self._repository = repository
        self._allowed_queues = {item.strip() for item in (allowed_queues or [default_queue]) if item.strip()}
        if not self._allowed_queues:
            raise ConfigurationError("dispatcher must have at least one allowed queue")
        self._default_queue = default_queue.strip()
        if not self._default_queue:
            raise ConfigurationError("default_queue is required")
        if self._default_queue not in self._allowed_queues:
            self._allowed_queues.add(self._default_queue)

    @property
    def allowed_queues(self) -> List[str]:
        """Return normalized allowed queue names."""

        return sorted(self._allowed_queues)

    def submit(self, request: DispatchRequest, *, now: Optional[datetime] = None) -> DispatchDecision:
        """Validate and durably enqueue a new job.

        This is the main control-plane entrypoint for runtime submission.
        The returned job is already persisted in durable storage.
        """

        now = now or utc_now()
        job = self._build_job_record(request, now=now)
        created = self._repository.create_job(job)
        return DispatchDecision(
            job=created,
            accepted=True,
            enqueued=created.state == JobState.QUEUED,
            notes=[
                f"job accepted into queue '{created.queue_name}'",
                "job is durable but not yet executing",
            ],
        )

    def cancel(self, job_id: str, *, now: Optional[datetime] = None) -> DispatchDecision:
        """Request cooperative cancellation for a durable job."""

        now = now or utc_now()
        job = self._repository.request_cancellation(job_id, now=now)
        notes = ["cancellation requested"]
        if job.is_terminal():
            notes.append("job was already terminal; cancellation flag is informational only")
        elif job.state == JobState.QUEUED:
            notes.append("worker has not started execution yet")
        else:
            notes.append("running worker must observe and honor cancellation cooperatively")
        return DispatchDecision(job=job, accepted=True, enqueued=job.state == JobState.QUEUED, notes=notes)

    def requeue_retry_wait_job(self, job_id: str, *, now: Optional[datetime] = None) -> DispatchDecision:
        """Move a retry-wait job back into the queued state when eligible.

        This is typically used by a scheduler or recovery loop once the
        `available_at` threshold has been reached.
        """

        now = now or utc_now()
        job = self._repository.require_job(job_id)
        if job.state != JobState.RETRY_WAIT:
            raise InputError(f"job '{job_id}' is not in retry_wait state")
        if job.available_at > now:
            raise InputError(
                f"job '{job_id}' is not eligible for requeue until {job.available_at.isoformat()}"
            )

        job.ensure_transition_allowed(JobState.QUEUED)
        updated = job.transition(JobState.QUEUED, now=now)
        updated = updated.model_copy(update={"error_message": None, "failure_category": None})
        persisted = self._repository.update_job(updated)
        return DispatchDecision(
            job=persisted,
            accepted=True,
            enqueued=True,
            notes=["retry wait satisfied", "job returned to queued state"],
        )

    def _build_job_record(self, request: DispatchRequest, *, now: datetime) -> JobRecord:
        """Construct a validated durable job record from a dispatch request."""

        workflow_id = request.normalized_workflow_id()
        job_type = request.normalized_job_type()
        queue_name = request.normalized_queue_name() or self._default_queue
        if queue_name not in self._allowed_queues:
            raise InputError(
                f"queue '{queue_name}' is not allowed; allowed queues: {', '.join(sorted(self._allowed_queues))}"
            )

        stage_specs = self._normalize_stage_specs(request.stage_specs)
        available_at = request.available_at or now
        if available_at < now:
            available_at = now

        job_id = (request.job_id or str(uuid4())).strip()
        if not job_id:
            raise InputError("job_id cannot be blank")

        metadata = dict(request.metadata)
        metadata.setdefault("submitted_at", now.isoformat())
        if request.requested_by:
            metadata.setdefault("requested_by", request.normalized_requested_by())

        return JobRecord(
            job_id=job_id,
            workflow_id=workflow_id,
            job_type=job_type,
            state=JobState.QUEUED,
            execution_mode=request.execution_mode,
            requested_by=request.normalized_requested_by(),
            queue_name=queue_name,
            priority=request.priority,
            payload=dict(request.payload),
            stage_specs=stage_specs,
            retry_policy=request.retry_policy,
            created_at=now,
            available_at=available_at,
            timeout_seconds=request.timeout_seconds,
            metadata=metadata,
        )

    def _normalize_stage_specs(self, stage_specs: List[StageSpec]) -> List[StageSpec]:
        """Return a validated, stably ordered stage plan."""

        if not stage_specs:
            raise InputError("at least one stage_spec is required")

        ordered = sorted(stage_specs, key=lambda item: (item.order, item.name))
        expected_orders = list(range(len(ordered)))
        actual_orders = [item.order for item in ordered]
        if actual_orders != expected_orders:
            raise InputError(
                "stage_spec order values must be contiguous starting at 0; "
                f"got {actual_orders}"
            )
        return [item.model_copy(deep=True) for item in ordered]
