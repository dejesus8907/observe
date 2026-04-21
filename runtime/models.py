"""Canonical runtime execution models.

This file defines the durable execution contract for the background runtime.
It intentionally comes first because every later component—repository,
dispatcher, worker loop, recovery, timeout handling—depends on these
semantics being explicit and stable.

The goal is to eliminate hand-wavy execution behavior. A runtime that does
not define lease ownership, retryability, idempotency, and terminal state
rules up front is not a runtime. It is wishful thinking.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator, model_validator


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""

    return datetime.now(timezone.utc)


class RuntimeModel(BaseModel):
    """Base model for runtime contracts.

    - `validate_assignment=True` keeps in-memory state honest during tests and
      orchestration code.
    - `extra='ignore'` prevents older workers from crashing when newer payloads
      contain additive fields.
    """

    model_config = {
        "validate_assignment": True,
        "extra": "ignore",
    }


class JobState(str, Enum):
    """Persistent lifecycle states for a job.

    The runtime persists these states in durable storage and exposes them to
    APIs, CLI tooling, and workers. Only the states here are valid.
    """

    QUEUED = "queued"
    LEASED = "leased"
    RUNNING = "running"
    RETRY_WAIT = "retry_wait"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"

    @classmethod
    def terminal_states(cls) -> set["JobState"]:
        return {
            cls.SUCCEEDED,
            cls.FAILED,
            cls.CANCELLED,
            cls.TIMED_OUT,
        }


class StageState(str, Enum):
    """Persistent lifecycle states for an individual stage within a job."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"
    SKIPPED = "skipped"

    @classmethod
    def terminal_states(cls) -> set["StageState"]:
        return {
            cls.SUCCEEDED,
            cls.FAILED,
            cls.CANCELLED,
            cls.TIMED_OUT,
            cls.SKIPPED,
        }


class FailureCategory(str, Enum):
    """Structured failure classes used by retry and recovery logic."""

    INPUT = "input"
    CONFIG = "config"
    AUTH = "auth"
    CONNECTOR = "connector"
    PARSE = "parse"
    NORMALIZATION = "normalization"
    PERSISTENCE = "persistence"
    TOPOLOGY = "topology"
    VALIDATION = "validation"
    TRANSIENT_SYSTEM = "transient_system"
    INTERNAL = "internal"


class ExecutionMode(str, Enum):
    """Requested job execution mode."""

    NORMAL = "normal"
    DRY_RUN = "dry_run"
    VALIDATE_ONLY = "validate_only"


class IdempotencyMode(str, Enum):
    """Declares whether a stage may be safely retried or resumed."""

    IDEMPOTENT = "idempotent"
    NON_IDEMPOTENT = "non_idempotent"


class ResumabilityMode(str, Enum):
    """Declares whether a stage may resume after interruption."""

    RESUMABLE = "resumable"
    RESTART_ONLY = "restart_only"
    NON_RESUMABLE = "non_resumable"


class RetryPolicy(RuntimeModel):
    """Retry policy attached to a job or stage.

    The runtime must never assume retryability. It must be declared.
    """

    max_attempts: int = Field(default=1, ge=1, le=100)
    base_delay_seconds: float = Field(default=5.0, ge=0.0, le=3600.0)
    max_delay_seconds: float = Field(default=300.0, ge=0.0, le=86400.0)
    backoff_multiplier: float = Field(default=2.0, ge=1.0, le=10.0)
    jitter_ratio: float = Field(default=0.1, ge=0.0, le=1.0)
    retryable_failures: List[FailureCategory] = Field(
        default_factory=lambda: [FailureCategory.TRANSIENT_SYSTEM]
    )

    @model_validator(mode="after")
    def validate_bounds(self) -> "RetryPolicy":
        if self.max_delay_seconds < self.base_delay_seconds:
            raise ValueError("max_delay_seconds must be >= base_delay_seconds")
        return self

    def allows_retry_for(self, category: FailureCategory, attempt_number: int) -> bool:
        """Return whether another attempt is allowed for this failure category."""

        return attempt_number < self.max_attempts and category in self.retryable_failures

    def nominal_delay_for_attempt(self, next_attempt_number: int) -> float:
        """Return exponential backoff delay without random jitter applied.

        Jitter belongs in the worker/executor layer. The contract here stays
        deterministic.
        """

        exponent = max(next_attempt_number - 2, 0)
        delay = self.base_delay_seconds * (self.backoff_multiplier ** exponent)
        return min(delay, self.max_delay_seconds)


class Lease(RuntimeModel):
    """Exclusive claim on a job by a worker."""

    worker_id: str = Field(min_length=1, max_length=256)
    lease_token: str = Field(default_factory=lambda: str(uuid4()))
    acquired_at: datetime = Field(default_factory=utc_now)
    expires_at: datetime
    heartbeat_at: datetime = Field(default_factory=utc_now)

    @field_validator("worker_id")
    @classmethod
    def normalize_worker_id(cls, value: str) -> str:
        return value.strip()

    @model_validator(mode="after")
    def validate_temporal_order(self) -> "Lease":
        if self.expires_at <= self.acquired_at:
            raise ValueError("expires_at must be later than acquired_at")
        if self.heartbeat_at < self.acquired_at:
            raise ValueError("heartbeat_at cannot be earlier than acquired_at")
        return self

    def is_expired(self, now: Optional[datetime] = None) -> bool:
        now = now or utc_now()
        return now >= self.expires_at

    def heartbeat(self, ttl_seconds: float) -> "Lease":
        """Return a renewed lease with an updated heartbeat and expiry."""

        now = utc_now()
        return self.model_copy(
            update={
                "heartbeat_at": now,
                "expires_at": now + timedelta(seconds=ttl_seconds),
            }
        )


class StageSpec(RuntimeModel):
    """Declarative contract for one executable stage inside a job."""

    name: str = Field(min_length=1, max_length=128)
    order: int = Field(ge=0, le=10000)
    idempotency: IdempotencyMode = IdempotencyMode.IDEMPOTENT
    resumability: ResumabilityMode = ResumabilityMode.RESTART_ONLY
    timeout_seconds: Optional[float] = Field(default=None, gt=0.0, le=86400.0)
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        return value.strip()


class StageRecord(RuntimeModel):
    """Persistent execution record for a stage attempt."""

    stage_name: str = Field(min_length=1, max_length=128)
    stage_order: int = Field(ge=0, le=10000)
    state: StageState = StageState.PENDING
    attempt_number: int = Field(default=1, ge=1, le=1000)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    worker_id: Optional[str] = None
    lease_token: Optional[str] = None
    failure_category: Optional[FailureCategory] = None
    error_message: Optional[str] = Field(default=None, max_length=4000)
    artifact_refs: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("stage_name")
    @classmethod
    def normalize_stage_name(cls, value: str) -> str:
        return value.strip()

    @model_validator(mode="after")
    def validate_stage_times(self) -> "StageRecord":
        if self.started_at and self.completed_at and self.completed_at < self.started_at:
            raise ValueError("completed_at cannot be earlier than started_at")
        return self

    def is_terminal(self) -> bool:
        return self.state in StageState.terminal_states()


class JobRecord(RuntimeModel):
    """Persistent execution record for a workflow job.

    A job wraps one execution request. It owns its stage plan, lease metadata,
    and retry lifecycle. Storage should treat this record as the source of
    truth, not any in-memory worker state.
    """

    job_id: str = Field(default_factory=lambda: str(uuid4()))
    workflow_id: str = Field(min_length=1, max_length=256)
    job_type: str = Field(min_length=1, max_length=128)
    state: JobState = JobState.QUEUED
    execution_mode: ExecutionMode = ExecutionMode.NORMAL
    requested_by: Optional[str] = Field(default=None, max_length=256)
    queue_name: str = Field(default="default", min_length=1, max_length=128)
    priority: int = Field(default=100, ge=0, le=100000)
    payload: Dict[str, Any] = Field(default_factory=dict)
    stage_specs: List[StageSpec] = Field(default_factory=list)
    stage_records: List[StageRecord] = Field(default_factory=list)
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)
    lease: Optional[Lease] = None
    current_stage_name: Optional[str] = Field(default=None, max_length=128)
    attempt_number: int = Field(default=1, ge=1, le=1000)
    created_at: datetime = Field(default_factory=utc_now)
    available_at: datetime = Field(default_factory=utc_now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    timeout_seconds: Optional[float] = Field(default=None, gt=0.0, le=604800.0)
    failure_category: Optional[FailureCategory] = None
    error_message: Optional[str] = Field(default=None, max_length=4000)
    cancellation_requested: bool = False
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("workflow_id", "job_type", "queue_name")
    @classmethod
    def normalize_required_text(cls, value: str) -> str:
        return value.strip()

    @field_validator("requested_by")
    @classmethod
    def normalize_optional_text(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = value.strip()
        return value or None

    @model_validator(mode="after")
    def validate_job_temporal_order(self) -> "JobRecord":
        if self.started_at and self.started_at < self.created_at:
            raise ValueError("started_at cannot be earlier than created_at")
        if self.completed_at:
            reference = self.started_at or self.created_at
            if self.completed_at < reference:
                raise ValueError("completed_at cannot be earlier than job start")
        return self

    @model_validator(mode="after")
    def validate_stage_plan(self) -> "JobRecord":
        names = [stage.name for stage in self.stage_specs]
        if len(names) != len(set(names)):
            raise ValueError("stage_specs must have unique stage names")
        orders = [stage.order for stage in self.stage_specs]
        if len(orders) != len(set(orders)):
            raise ValueError("stage_specs must have unique stage order values")
        return self

    def is_terminal(self) -> bool:
        return self.state in JobState.terminal_states()

    def requires_valid_lease(self) -> bool:
        return self.state in {JobState.LEASED, JobState.RUNNING}

    def next_stage_spec(self) -> Optional[StageSpec]:
        """Return the next uncompleted stage spec in execution order."""

        terminal = {
            record.stage_name
            for record in self.stage_records
            if record.state in {StageState.SUCCEEDED, StageState.SKIPPED}
        }
        for spec in sorted(self.stage_specs, key=lambda item: item.order):
            if spec.name not in terminal:
                return spec
        return None

    def ensure_transition_allowed(self, new_state: JobState) -> None:
        """Raise if a transition is invalid.

        This is intentionally strict. Loose transition semantics are how runtime
        state gets corrupted.
        """

        allowed = {
            JobState.QUEUED: {JobState.LEASED, JobState.CANCELLED},
            JobState.LEASED: {JobState.RUNNING, JobState.RETRY_WAIT, JobState.CANCELLED, JobState.TIMED_OUT},
            JobState.RUNNING: {
                JobState.SUCCEEDED,
                JobState.FAILED,
                JobState.RETRY_WAIT,
                JobState.CANCELLED,
                JobState.TIMED_OUT,
            },
            JobState.RETRY_WAIT: {JobState.QUEUED, JobState.CANCELLED, JobState.TIMED_OUT},
            JobState.SUCCEEDED: set(),
            JobState.FAILED: set(),
            JobState.CANCELLED: set(),
            JobState.TIMED_OUT: set(),
        }
        if new_state not in allowed[self.state]:
            raise ValueError(f"invalid job state transition: {self.state} -> {new_state}")

    def transition(self, new_state: JobState, *, now: Optional[datetime] = None) -> "JobRecord":
        """Return a copy with a validated state transition applied."""

        self.ensure_transition_allowed(new_state)
        now = now or utc_now()
        updates: Dict[str, Any] = {"state": new_state}
        if new_state in {JobState.RUNNING} and not self.started_at:
            updates["started_at"] = now
        if new_state in JobState.terminal_states():
            updates["completed_at"] = now
        if new_state in {JobState.QUEUED, JobState.RETRY_WAIT, JobState.CANCELLED, JobState.TIMED_OUT, JobState.FAILED, JobState.SUCCEEDED}:
            updates["lease"] = None
        return self.model_copy(update=updates)

    def request_cancellation(self) -> "JobRecord":
        """Mark the job for cooperative cancellation."""

        return self.model_copy(update={"cancellation_requested": True})

    def with_lease(self, lease: Lease) -> "JobRecord":
        """Attach a lease and move into the leased state."""

        self.ensure_transition_allowed(JobState.LEASED)
        return self.model_copy(update={"state": JobState.LEASED, "lease": lease})

    def eligible_for_retry(self) -> bool:
        if self.failure_category is None:
            return False
        return self.retry_policy.allows_retry_for(self.failure_category, self.attempt_number)

    def next_available_retry_time(self, now: Optional[datetime] = None) -> datetime:
        """Return the deterministic next retry time."""

        now = now or utc_now()
        delay = self.retry_policy.nominal_delay_for_attempt(self.attempt_number + 1)
        return now + timedelta(seconds=delay)
