"""SQLAlchemy-backed runtime repository.

This module turns the runtime from conceptually durable into actually durable.
It implements the `RuntimeRepository` contract against the ORM schema defined
in `db_models.py`.

Design goals
------------
- Atomic lease acquisition for the next eligible queued job
- Durable job state updates with optimistic concurrency support
- Durable stage attempt history
- Recovery-safe expired lease reclaim
- Honest retry / cancellation / timeout persistence

Important honesty note
----------------------
This implementation is written conservatively for correctness and clarity.
It does not pretend to solve every database-specific scaling problem. The
priority here is consistent durable semantics that can later be optimized.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from datetime import datetime, timedelta
from typing import Callable, List, Optional

from sqlalchemy import Select, and_, func, or_, select
from sqlalchemy.orm import Session, selectinload

from .db_models import (
    RuntimeJobModel,
    RuntimeRecoveryEventModel,
    RuntimeStageAttemptModel,
)
from .errors import InputError, PersistenceError
from .models import (
    FailureCategory,
    JobRecord,
    JobState,
    Lease,
    RetryPolicy,
    StageRecord,
    StageSpec,
    utc_now,
)
from .repository import (
    JobSearchFilter,
    LeaseSearchFilter,
    RuntimeRepository,
)


class SQLRuntimeRepository(RuntimeRepository):
    """Durable SQL-backed implementation of the runtime repository contract.

    Parameters
    ----------
    session_factory:
        Callable returning a context-managed SQLAlchemy `Session`.
        Example:
            lambda: Session(engine)
        or a scoped helper from the existing storage layer.

    Notes
    -----
    This implementation assumes the caller configures transaction isolation and
    engine behavior appropriately for the target database.
    """

    def __init__(self, session_factory: Callable[[], AbstractContextManager[Session]]) -> None:
        self._session_factory = session_factory

    # ------------------------------------------------------------------
    # Job lifecycle
    # ------------------------------------------------------------------

    def create_job(self, job: JobRecord) -> JobRecord:
        with self._session_factory() as session:
            existing = session.get(RuntimeJobModel, job.job_id)
            if existing is not None:
                raise PersistenceError(
                    f"job '{job.job_id}' already exists",
                    retryable=False,
                    permanent=True,
                )

            model = self._to_job_model(job)
            session.add(model)
            session.flush()

            for stage in job.stage_records:
                session.add(self._to_stage_model(stage, job.job_id))

            session.commit()
            loaded = self._load_job(session, job.job_id)
            return self._to_job_record(loaded)

    def get_job(self, job_id: str) -> Optional[JobRecord]:
        with self._session_factory() as session:
            model = self._load_job(session, job_id, required=False)
            if model is None:
                return None
            return self._to_job_record(model)

    def update_job(self, job: JobRecord) -> JobRecord:
        with self._session_factory() as session:
            model = self._load_job(session, job.job_id, required=True)
            self._apply_job_record_to_model(job, model)
            model.version = model.version + 1
            session.commit()
            loaded = self._load_job(session, job.job_id)
            return self._to_job_record(loaded)

    def list_jobs(self, search: Optional[JobSearchFilter] = None) -> List[JobRecord]:
        search = search or JobSearchFilter()
        with self._session_factory() as session:
            stmt = (
                select(RuntimeJobModel)
                .options(selectinload(RuntimeJobModel.stage_attempts))
            )
            stmt = self._apply_job_search(stmt, search)
            stmt = stmt.order_by(
                RuntimeJobModel.priority.asc(),
                RuntimeJobModel.available_at.asc(),
                RuntimeJobModel.created_at.asc(),
                RuntimeJobModel.job_id.asc(),
            ).limit(search.normalized_limit())

            rows = session.execute(stmt).scalars().unique().all()
            return [self._to_job_record(row) for row in rows]

    # ------------------------------------------------------------------
    # Queue / lease operations
    # ------------------------------------------------------------------

    def acquire_next_lease(
        self,
        *,
        worker_id: str,
        queue_name: str,
        lease: Lease,
        now: Optional[datetime] = None,
    ) -> Optional[JobRecord]:
        now = now or utc_now()
        queue_name = queue_name.strip()
        worker_id = worker_id.strip()

        if not queue_name:
            raise InputError("queue_name is required")
        if not worker_id:
            raise InputError("worker_id is required")

        with self._session_factory() as session:
            # Conservative approach: lock/select eligible queued rows in order,
            # then claim the first one that is still claimable.
            stmt = (
                select(RuntimeJobModel)
                .where(
                    RuntimeJobModel.queue_name == queue_name,
                    RuntimeJobModel.state == JobState.QUEUED.value,
                    RuntimeJobModel.available_at <= now,
                )
                .order_by(
                    RuntimeJobModel.priority.asc(),
                    RuntimeJobModel.available_at.asc(),
                    RuntimeJobModel.created_at.asc(),
                    RuntimeJobModel.job_id.asc(),
                )
                .limit(25)
                .with_for_update(skip_locked=True)
            )

            candidates = session.execute(stmt).scalars().all()
            if not candidates:
                return None

            chosen = candidates[0]
            chosen.state = JobState.LEASED.value
            chosen.lease_worker_id = lease.worker_id
            chosen.lease_token = lease.lease_token
            chosen.lease_acquired_at = lease.acquired_at
            chosen.lease_heartbeat_at = lease.heartbeat_at
            chosen.lease_expires_at = lease.expires_at
            chosen.version = chosen.version + 1

            session.commit()
            loaded = self._load_job(session, chosen.job_id)
            return self._to_job_record(loaded)

    def renew_lease(
        self,
        *,
        job_id: str,
        worker_id: str,
        lease_token: str,
        ttl_seconds: float,
        now: Optional[datetime] = None,
    ) -> JobRecord:
        now = now or utc_now()
        with self._session_factory() as session:
            model = self._load_job_for_update(session, job_id)
            self._assert_lease_owner(model, worker_id=worker_id, lease_token=lease_token)

            model.lease_heartbeat_at = now
            model.lease_expires_at = now + timedelta(seconds=ttl_seconds)
            model.version = model.version + 1

            session.commit()
            loaded = self._load_job(session, job_id)
            return self._to_job_record(loaded)

    def release_lease(
        self,
        *,
        job_id: str,
        worker_id: str,
        lease_token: str,
        now: Optional[datetime] = None,
    ) -> JobRecord:
        with self._session_factory() as session:
            model = self._load_job_for_update(session, job_id)
            self._assert_lease_owner(model, worker_id=worker_id, lease_token=lease_token)

            if model.state == JobState.LEASED.value:
                model.state = JobState.QUEUED.value

            self._clear_lease(model)
            model.version = model.version + 1

            session.commit()
            loaded = self._load_job(session, job_id)
            return self._to_job_record(loaded)

    def list_expired_leases(
        self, search: Optional[LeaseSearchFilter] = None
    ) -> List[JobRecord]:
        search = search or LeaseSearchFilter()
        reference = search.expires_before or utc_now()

        with self._session_factory() as session:
            stmt = (
                select(RuntimeJobModel)
                .options(selectinload(RuntimeJobModel.stage_attempts))
                .where(
                    RuntimeJobModel.lease_token.is_not(None),
                    RuntimeJobModel.lease_expires_at.is_not(None),
                    RuntimeJobModel.lease_expires_at <= reference,
                    RuntimeJobModel.state.in_([JobState.LEASED.value, JobState.RUNNING.value]),
                )
            )

            if search.queue_name:
                stmt = stmt.where(RuntimeJobModel.queue_name == search.queue_name)
            if search.worker_id:
                stmt = stmt.where(RuntimeJobModel.lease_worker_id == search.worker_id)

            stmt = stmt.order_by(
                RuntimeJobModel.lease_expires_at.asc(),
                RuntimeJobModel.job_id.asc(),
            ).limit(search.normalized_limit())

            rows = session.execute(stmt).scalars().unique().all()
            return [self._to_job_record(row) for row in rows]

    # ------------------------------------------------------------------
    # Stage persistence
    # ------------------------------------------------------------------

    def append_stage_record(self, job_id: str, stage: StageRecord) -> JobRecord:
        with self._session_factory() as session:
            model = self._load_job_for_update(session, job_id)
            session.add(self._to_stage_model(stage, job_id))
            model.version = model.version + 1
            session.commit()
            loaded = self._load_job(session, job_id)
            return self._to_job_record(loaded)

    def replace_stage_record(
        self,
        *,
        job_id: str,
        stage_name: str,
        attempt_number: int,
        record: StageRecord,
    ) -> JobRecord:
        with self._session_factory() as session:
            model = self._load_job_for_update(session, job_id)
            stmt = (
                select(RuntimeStageAttemptModel)
                .where(
                    RuntimeStageAttemptModel.job_id == job_id,
                    RuntimeStageAttemptModel.stage_name == stage_name,
                    RuntimeStageAttemptModel.attempt_number == attempt_number,
                )
                .with_for_update()
            )
            existing = session.execute(stmt).scalar_one_or_none()
            if existing is None:
                raise PersistenceError(
                    f"stage attempt '{stage_name}#{attempt_number}' does not exist",
                    retryable=False,
                    permanent=True,
                )

            self._apply_stage_record_to_model(record, existing)
            model.version = model.version + 1

            session.commit()
            loaded = self._load_job(session, job_id)
            return self._to_job_record(loaded)

    # ------------------------------------------------------------------
    # Retry / cancellation / recovery control
    # ------------------------------------------------------------------

    def schedule_retry(
        self,
        *,
        job_id: str,
        available_at: datetime,
        error_message: Optional[str],
        now: Optional[datetime] = None,
    ) -> JobRecord:
        now = now or utc_now()
        with self._session_factory() as session:
            model = self._load_job_for_update(session, job_id)
            if model.state not in {JobState.LEASED.value, JobState.RUNNING.value}:
                raise PersistenceError(
                    f"job '{job_id}' cannot be moved to retry_wait from state '{model.state}'",
                    retryable=False,
                    permanent=True,
                )

            model.state = JobState.RETRY_WAIT.value
            model.available_at = available_at
            model.attempt_number = model.attempt_number + 1
            model.error_message = error_message
            self._clear_lease(model)
            model.version = model.version + 1

            session.commit()
            loaded = self._load_job(session, job_id)
            return self._to_job_record(loaded)

    def request_cancellation(self, job_id: str, now: Optional[datetime] = None) -> JobRecord:
        with self._session_factory() as session:
            model = self._load_job_for_update(session, job_id)
            model.cancellation_requested = True
            model.version = model.version + 1
            session.commit()
            loaded = self._load_job(session, job_id)
            return self._to_job_record(loaded)

    def mark_timed_out(
        self,
        *,
        job_id: str,
        error_message: Optional[str],
        now: Optional[datetime] = None,
    ) -> JobRecord:
        now = now or utc_now()
        with self._session_factory() as session:
            model = self._load_job_for_update(session, job_id)
            model.state = JobState.TIMED_OUT.value
            model.completed_at = now
            model.error_message = error_message
            self._clear_lease(model)
            model.version = model.version + 1
            session.commit()
            loaded = self._load_job(session, job_id)
            return self._to_job_record(loaded)

    def reclaim_expired_lease(
        self,
        *,
        job_id: str,
        expected_lease_token: str,
        now: Optional[datetime] = None,
    ) -> JobRecord:
        now = now or utc_now()
        with self._session_factory() as session:
            model = self._load_job_for_update(session, job_id)

            if not model.lease_token:
                raise PersistenceError("job has no lease to reclaim", retryable=False, permanent=True)
            if model.lease_token != expected_lease_token:
                raise PersistenceError(
                    "lease token mismatch during reclaim",
                    retryable=False,
                    permanent=True,
                )
            if model.lease_expires_at is None or model.lease_expires_at > now:
                raise PersistenceError("lease is not expired", retryable=False, permanent=True)

            previous_state = model.state
            if model.cancellation_requested:
                model.state = JobState.CANCELLED.value
                model.completed_at = now
                message = "job cancelled during lease reclaim"
            else:
                model.state = JobState.QUEUED.value
                message = "expired lease reclaimed and job requeued"

            self._clear_lease(model)
            model.version = model.version + 1

            session.add(
                RuntimeRecoveryEventModel(
                    job_id=model.job_id,
                    event_type="expired_lease_reclaim",
                    previous_state=previous_state,
                    new_state=model.state,
                    lease_token=expected_lease_token,
                    worker_id=model.lease_worker_id,
                    message=message,
                    details_json={},
                    created_at=now,
                )
            )

            session.commit()
            loaded = self._load_job(session, job_id)
            return self._to_job_record(loaded)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _apply_job_search(
        self,
        stmt: Select,
        search: JobSearchFilter,
    ) -> Select:
        if search.states:
            stmt = stmt.where(RuntimeJobModel.state.in_([state.value for state in search.states]))
        if search.queue_name:
            stmt = stmt.where(RuntimeJobModel.queue_name == search.queue_name)
        if search.workflow_id:
            stmt = stmt.where(RuntimeJobModel.workflow_id == search.workflow_id)
        if search.job_type:
            stmt = stmt.where(RuntimeJobModel.job_type == search.job_type)
        if search.available_before:
            stmt = stmt.where(RuntimeJobModel.available_at <= search.available_before)
        if search.created_after:
            stmt = stmt.where(RuntimeJobModel.created_at >= search.created_after)
        if search.requested_by:
            stmt = stmt.where(RuntimeJobModel.requested_by == search.requested_by)
        if search.cancellation_requested is not None:
            stmt = stmt.where(RuntimeJobModel.cancellation_requested == search.cancellation_requested)
        return stmt

    def _load_job(
        self,
        session: Session,
        job_id: str,
        *,
        required: bool = True,
    ) -> Optional[RuntimeJobModel]:
        stmt = (
            select(RuntimeJobModel)
            .options(selectinload(RuntimeJobModel.stage_attempts))
            .where(RuntimeJobModel.job_id == job_id)
        )
        model = session.execute(stmt).scalar_one_or_none()
        if model is None and required:
            raise InputError(f"job '{job_id}' was not found")
        return model

    def _load_job_for_update(self, session: Session, job_id: str) -> RuntimeJobModel:
        stmt = (
            select(RuntimeJobModel)
            .where(RuntimeJobModel.job_id == job_id)
            .with_for_update()
        )
        model = session.execute(stmt).scalar_one_or_none()
        if model is None:
            raise InputError(f"job '{job_id}' was not found")
        return model

    def _assert_lease_owner(
        self,
        model: RuntimeJobModel,
        *,
        worker_id: str,
        lease_token: str,
    ) -> None:
        if not model.lease_token or not model.lease_worker_id:
            raise PersistenceError("job has no active lease", retryable=False, permanent=True)
        if model.lease_worker_id != worker_id or model.lease_token != lease_token:
            raise PersistenceError("lease ownership mismatch", retryable=False, permanent=True)

    def _clear_lease(self, model: RuntimeJobModel) -> None:
        model.lease_worker_id = None
        model.lease_token = None
        model.lease_acquired_at = None
        model.lease_heartbeat_at = None
        model.lease_expires_at = None

    # ------------------------------------------------------------------
    # Serialization / deserialization
    # ------------------------------------------------------------------

    def _to_job_model(self, job: JobRecord) -> RuntimeJobModel:
        lease = job.lease
        return RuntimeJobModel(
            job_id=job.job_id,
            workflow_id=job.workflow_id,
            job_type=job.job_type,
            queue_name=job.queue_name,
            requested_by=job.requested_by,
            state=job.state.value,
            execution_mode=job.execution_mode.value,
            priority=job.priority,
            attempt_number=job.attempt_number,
            current_stage_name=job.current_stage_name,
            failure_category=job.failure_category.value if job.failure_category else None,
            error_message=job.error_message,
            created_at=job.created_at,
            available_at=job.available_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
            timeout_seconds=job.timeout_seconds,
            cancellation_requested=job.cancellation_requested,
            lease_worker_id=lease.worker_id if lease else None,
            lease_token=lease.lease_token if lease else None,
            lease_acquired_at=lease.acquired_at if lease else None,
            lease_heartbeat_at=lease.heartbeat_at if lease else None,
            lease_expires_at=lease.expires_at if lease else None,
            payload=dict(job.payload),
            metadata_json=dict(job.metadata),
            retry_policy_json=job.retry_policy.model_dump(mode="json"),
            stage_plan_json=[spec.model_dump(mode="json") for spec in job.stage_specs],
            version=1,
        )

    def _to_stage_model(self, stage: StageRecord, job_id: str) -> RuntimeStageAttemptModel:
        return RuntimeStageAttemptModel(
            job_id=job_id,
            stage_name=stage.stage_name,
            stage_order=stage.stage_order,
            state=stage.state.value,
            attempt_number=stage.attempt_number,
            started_at=stage.started_at,
            completed_at=stage.completed_at,
            worker_id=stage.worker_id,
            lease_token=stage.lease_token,
            failure_category=stage.failure_category.value if stage.failure_category else None,
            error_message=stage.error_message,
            artifact_refs_json=list(stage.artifact_refs),
            metadata_json=dict(stage.metadata),
        )

    def _apply_job_record_to_model(self, job: JobRecord, model: RuntimeJobModel) -> None:
        lease = job.lease
        model.workflow_id = job.workflow_id
        model.job_type = job.job_type
        model.queue_name = job.queue_name
        model.requested_by = job.requested_by
        model.state = job.state.value
        model.execution_mode = job.execution_mode.value
        model.priority = job.priority
        model.attempt_number = job.attempt_number
        model.current_stage_name = job.current_stage_name
        model.failure_category = job.failure_category.value if job.failure_category else None
        model.error_message = job.error_message
        model.created_at = job.created_at
        model.available_at = job.available_at
        model.started_at = job.started_at
        model.completed_at = job.completed_at
        model.timeout_seconds = job.timeout_seconds
        model.cancellation_requested = job.cancellation_requested
        model.lease_worker_id = lease.worker_id if lease else None
        model.lease_token = lease.lease_token if lease else None
        model.lease_acquired_at = lease.acquired_at if lease else None
        model.lease_heartbeat_at = lease.heartbeat_at if lease else None
        model.lease_expires_at = lease.expires_at if lease else None
        model.payload = dict(job.payload)
        model.metadata_json = dict(job.metadata)
        model.retry_policy_json = job.retry_policy.model_dump(mode="json")
        model.stage_plan_json = [spec.model_dump(mode="json") for spec in job.stage_specs]

    def _apply_stage_record_to_model(
        self,
        record: StageRecord,
        model: RuntimeStageAttemptModel,
    ) -> None:
        model.stage_name = record.stage_name
        model.stage_order = record.stage_order
        model.state = record.state.value
        model.attempt_number = record.attempt_number
        model.started_at = record.started_at
        model.completed_at = record.completed_at
        model.worker_id = record.worker_id
        model.lease_token = record.lease_token
        model.failure_category = record.failure_category.value if record.failure_category else None
        model.error_message = record.error_message
        model.artifact_refs_json = list(record.artifact_refs)
        model.metadata_json = dict(record.metadata)

    def _to_job_record(self, model: RuntimeJobModel) -> JobRecord:
        lease = None
        if model.lease_token and model.lease_worker_id and model.lease_expires_at and model.lease_acquired_at:
            lease = Lease(
                worker_id=model.lease_worker_id,
                lease_token=model.lease_token,
                acquired_at=model.lease_acquired_at,
                heartbeat_at=model.lease_heartbeat_at or model.lease_acquired_at,
                expires_at=model.lease_expires_at,
            )

        failure_category = None
        if model.failure_category:
            try:
                failure_category = FailureCategory(model.failure_category)
            except ValueError:
                failure_category = None

        stage_specs = [
            StageSpec.model_validate(item)
            for item in (model.stage_plan_json or [])
        ]

        stage_records = [
            self._to_stage_record(stage_model)
            for stage_model in sorted(
                model.stage_attempts,
                key=lambda item: (item.stage_order, item.attempt_number, item.stage_attempt_id),
            )
        ]

        return JobRecord(
            job_id=model.job_id,
            workflow_id=model.workflow_id,
            job_type=model.job_type,
            state=JobState(model.state),
            execution_mode=model.execution_mode,
            requested_by=model.requested_by,
            queue_name=model.queue_name,
            priority=model.priority,
            payload=dict(model.payload or {}),
            stage_specs=stage_specs,
            stage_records=stage_records,
            retry_policy=RetryPolicy.model_validate(model.retry_policy_json or {}),
            lease=lease,
            current_stage_name=model.current_stage_name,
            attempt_number=model.attempt_number,
            created_at=model.created_at,
            available_at=model.available_at,
            started_at=model.started_at,
            completed_at=model.completed_at,
            timeout_seconds=model.timeout_seconds,
            failure_category=failure_category,
            error_message=model.error_message,
            cancellation_requested=model.cancellation_requested,
            metadata=dict(model.metadata_json or {}),
        )

    def _to_stage_record(self, model: RuntimeStageAttemptModel) -> StageRecord:
        failure_category = None
        if model.failure_category:
            try:
                failure_category = FailureCategory(model.failure_category)
            except ValueError:
                failure_category = None

        return StageRecord(
            stage_name=model.stage_name,
            stage_order=model.stage_order,
            state=model.state,
            attempt_number=model.attempt_number,
            started_at=model.started_at,
            completed_at=model.completed_at,
            worker_id=model.worker_id,
            lease_token=model.lease_token,
            failure_category=failure_category,
            error_message=model.error_message,
            artifact_refs=list(model.artifact_refs_json or []),
            metadata=dict(model.metadata_json or {}),
        )