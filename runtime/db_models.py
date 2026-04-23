"""SQLAlchemy ORM models for the durable runtime subsystem.

This file defines the database-backed persistence schema for runtime execution.
It is designed to support the semantics already established by the runtime
contracts:

- durable jobs
- durable stage attempt history
- durable lease ownership
- retry scheduling
- cooperative cancellation
- timeout / terminal transitions
- recovery of expired leases

Design principles
-----------------
1. One authoritative row per job (`RuntimeJobModel`)
2. Immutable-ish stage attempt history (`RuntimeStageAttemptModel`)
3. Lease state lives on the job row for atomic acquisition/reclaim behavior
4. Optimistic concurrency is supported via `version`
5. Search-heavy fields are indexed explicitly
6. JSON columns are used only for structured payload/metadata, not core state

Important honesty note
----------------------
This file defines the schema layer. It does *not* by itself provide atomic
lease acquisition or repository semantics. That belongs in `sql_repository.py`.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp for ORM defaults."""

    return datetime.now(timezone.utc)


class RuntimeBase(DeclarativeBase):
    """Declarative base for runtime persistence models."""

    pass


class RuntimeJobModel(RuntimeBase):
    """Durable execution job row.

    This row is the authoritative SQL representation of a runtime job. It owns:
    - queue visibility
    - current state
    - retry timing
    - active lease fields
    - timeout/cancellation flags
    - structured payload / metadata

    Lease fields deliberately live on this row so a SQL repository can do
    atomic "claim next job" operations with a single row update.
    """

    __tablename__ = "runtime_jobs"

    # Identity / routing
    job_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    workflow_id: Mapped[str] = mapped_column(String(256), index=True, nullable=False)
    job_type: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    queue_name: Mapped[str] = mapped_column(String(128), index=True, nullable=False, default="default")
    requested_by: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)

    # Execution state
    state: Mapped[str] = mapped_column(String(32), index=True, nullable=False, default="queued")
    execution_mode: Mapped[str] = mapped_column(String(32), nullable=False, default="normal")
    priority: Mapped[int] = mapped_column(Integer, index=True, nullable=False, default=100)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    current_stage_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)

    # Retry / failure
    failure_category: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Timing
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now, index=True)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now, index=True)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    timeout_seconds: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Cooperative cancellation
    cancellation_requested: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)

    # Lease ownership (lives on job row for atomic SQL claim/reclaim)
    lease_worker_id: Mapped[Optional[str]] = mapped_column(String(256), nullable=True, index=True)
    lease_token: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    lease_acquired_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_heartbeat_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)

    # Structured fields
    payload: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    metadata_json: Mapped[dict] = mapped_column("metadata", JSON, nullable=False, default=dict)
    retry_policy_json: Mapped[dict] = mapped_column("retry_policy", JSON, nullable=False, default=dict)
    stage_plan_json: Mapped[list] = mapped_column("stage_plan", JSON, nullable=False, default=list)

    # Optimistic concurrency
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    # Relationships
    stage_attempts: Mapped[list["RuntimeStageAttemptModel"]] = relationship(
        back_populates="job",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by="(RuntimeStageAttemptModel.stage_order, RuntimeStageAttemptModel.attempt_number, RuntimeStageAttemptModel.stage_attempt_id)",
    )

    __table_args__ = (
        Index(
            "ix_runtime_jobs_queue_poll",
            "queue_name",
            "state",
            "available_at",
            "priority",
            "created_at",
        ),
        Index(
            "ix_runtime_jobs_expired_leases",
            "queue_name",
            "lease_expires_at",
            "state",
        ),
        Index(
            "ix_runtime_jobs_workflow_lookup",
            "workflow_id",
            "job_type",
            "created_at",
        ),
        Index(
            "ix_runtime_jobs_cancellable",
            "state",
            "cancellation_requested",
            "queue_name",
        ),
    )


class RuntimeStageAttemptModel(RuntimeBase):
    """Durable stage attempt history row.

    One row per stage attempt. This table is append-heavy and query-light
    compared to `runtime_jobs`, but it is critical for:
    - auditability
    - recovery decisions
    - debugging retries / interruptions
    - later artifact replay / provenance work

    We intentionally keep stage attempt rows separate from the job row instead
    of mutating JSON blobs in-place. Mutable history stuffed into one JSON
    column is operational garbage.
    """

    __tablename__ = "runtime_stage_attempts"

    stage_attempt_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("runtime_jobs.job_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    stage_name: Mapped[str] = mapped_column(String(128), nullable=False)
    stage_order: Mapped[int] = mapped_column(Integer, nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)

    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    worker_id: Mapped[Optional[str]] = mapped_column(String(256), nullable=True, index=True)
    lease_token: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)

    failure_category: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    artifact_refs_json: Mapped[list] = mapped_column("artifact_refs", JSON, nullable=False, default=list)
    metadata_json: Mapped[dict] = mapped_column("metadata", JSON, nullable=False, default=dict)

    job: Mapped["RuntimeJobModel"] = relationship(back_populates="stage_attempts")

    __table_args__ = (
        UniqueConstraint(
            "job_id",
            "stage_name",
            "attempt_number",
            name="uq_runtime_stage_attempt_job_stage_attempt",
        ),
        Index(
            "ix_runtime_stage_attempts_job_stage_order",
            "job_id",
            "stage_order",
            "attempt_number",
        ),
        Index(
            "ix_runtime_stage_attempts_running",
            "state",
            "started_at",
        ),
        Index(
            "ix_runtime_stage_attempts_worker_lookup",
            "worker_id",
            "lease_token",
        ),
    )


class RuntimeRecoveryEventModel(RuntimeBase):
    """Optional recovery audit trail.

    This model records recovery actions such as:
    - expired lease reclaim
    - requeue after orphaned execution
    - forced fail due to non-resumable stage
    - cancellation during recovery

    This table is not strictly required for runtime correctness, but it is very
    useful operationally. Since the runtime is supposed to be auditable, we
    include it now instead of pretending audit can be bolted on later for free.
    """

    __tablename__ = "runtime_recovery_events"

    recovery_event_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("runtime_jobs.job_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    event_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    previous_state: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    new_state: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)

    lease_token: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    worker_id: Mapped[Optional[str]] = mapped_column(String(256), nullable=True, index=True)

    message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    details_json: Mapped[dict] = mapped_column("details", JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now, index=True)

    __table_args__ = (
        Index(
            "ix_runtime_recovery_events_job_created",
            "job_id",
            "created_at",
        ),
    )


def runtime_model_names() -> list[str]:
    """Return the runtime ORM model names for registration/inspection."""

    return [
        "RuntimeJobModel",
        "RuntimeStageAttemptModel",
        "RuntimeRecoveryEventModel",
    ]