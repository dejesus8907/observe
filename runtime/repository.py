"""Durable repository contract for the execution runtime.

This file defines the persistence boundary for the background runtime.
It is intentionally an abstract contract first, not a storage-specific
implementation. The worker, dispatcher, timeout handling, and recovery logic
must all depend on durable semantics, not on ad hoc SQL calls scattered
throughout the codebase.

A good runtime repository must support more than CRUD:
- atomic lease acquisition
- heartbeat renewal
- state transition persistence
- retry scheduling
- cancellation signaling
- expired lease discovery
- operator visibility

If the persistence contract is weak, every higher-level runtime component
becomes race-prone garbage.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Optional

from .errors import InputError, PersistenceError
from .models import JobRecord, JobState, Lease, StageRecord, StageState, utc_now


@dataclass(slots=True)
class JobSearchFilter:
    """Durable search filter for operator and worker queries."""

    states: Optional[List[JobState]] = None
    queue_name: Optional[str] = None
    workflow_id: Optional[str] = None
    job_type: Optional[str] = None
    available_before: Optional[datetime] = None
    created_after: Optional[datetime] = None
    requested_by: Optional[str] = None
    cancellation_requested: Optional[bool] = None
    limit: int = 100

    def normalized_limit(self) -> int:
        return max(1, min(self.limit, 1000))


@dataclass(slots=True)
class LeaseSearchFilter:
    """Search filter for lease and recovery operations."""

    queue_name: Optional[str] = None
    expires_before: Optional[datetime] = None
    worker_id: Optional[str] = None
    limit: int = 100

    def normalized_limit(self) -> int:
        return max(1, min(self.limit, 1000))


class RuntimeRepository(ABC):
    """Abstract durability contract for runtime jobs, stages, and leases.

    This interface is intentionally explicit about ownership and atomicity.
    Storage backends may differ (SQL, KV store, document DB), but these
    semantics must remain stable.
    """

    # ------------------------------------------------------------------
    # Job lifecycle
    # ------------------------------------------------------------------

    @abstractmethod
    def create_job(self, job: JobRecord) -> JobRecord:
        """Persist a newly submitted job.

        Implementations must reject duplicate job IDs and persist the full
        initial contract, including stage specs and retry policy.
        """

    @abstractmethod
    def get_job(self, job_id: str) -> Optional[JobRecord]:
        """Return the current durable job state, if present."""

    @abstractmethod
    def update_job(self, job: JobRecord) -> JobRecord:
        """Persist the current durable job state.

        Implementations should use optimistic locking or equivalent safeguards
        if supported by the backend.
        """

    @abstractmethod
    def list_jobs(self, search: Optional[JobSearchFilter] = None) -> List[JobRecord]:
        """List jobs matching a durable search filter."""

    # ------------------------------------------------------------------
    # Queue / lease operations
    # ------------------------------------------------------------------

    @abstractmethod
    def acquire_next_lease(
        self,
        *,
        worker_id: str,
        queue_name: str,
        lease: Lease,
        now: Optional[datetime] = None,
    ) -> Optional[JobRecord]:
        """Atomically lease the next eligible job for a worker.

        Eligibility rules:
        - queue matches
        - job state is `queued`
        - available_at <= now
        - no conflicting valid lease exists

        The returned job must already reflect the newly persisted lease.
        """

    @abstractmethod
    def renew_lease(
        self,
        *,
        job_id: str,
        worker_id: str,
        lease_token: str,
        ttl_seconds: float,
        now: Optional[datetime] = None,
    ) -> JobRecord:
        """Renew an existing lease heartbeat and expiry.

        Must fail if the worker/token pair no longer owns the lease.
        """

    @abstractmethod
    def release_lease(
        self,
        *,
        job_id: str,
        worker_id: str,
        lease_token: str,
        now: Optional[datetime] = None,
    ) -> JobRecord:
        """Release a lease without completing the job.

        This is primarily for safe shutdown or cooperative handoff.
        The repository should move the job back to `queued` or leave state
        handling to the caller according to implementation policy. The durable
        result returned here must reflect the persisted record.
        """

    @abstractmethod
    def list_expired_leases(
        self, search: Optional[LeaseSearchFilter] = None
    ) -> List[JobRecord]:
        """Return jobs whose active leases have expired.

        Used by recovery logic to reclaim orphaned execution.
        """

    # ------------------------------------------------------------------
    # Stage persistence
    # ------------------------------------------------------------------

    @abstractmethod
    def append_stage_record(self, job_id: str, stage: StageRecord) -> JobRecord:
        """Persist a new stage attempt record and return the updated job."""

    @abstractmethod
    def replace_stage_record(
        self,
        *,
        job_id: str,
        stage_name: str,
        attempt_number: int,
        record: StageRecord,
    ) -> JobRecord:
        """Replace one persisted stage attempt record.

        This must target a specific stage attempt, not 'whichever one matches
        vaguely'. Weak targeting here creates corrupted history.
        """

    # ------------------------------------------------------------------
    # Retry / cancellation / recovery control
    # ------------------------------------------------------------------

    @abstractmethod
    def schedule_retry(
        self,
        *,
        job_id: str,
        available_at: datetime,
        error_message: Optional[str],
        now: Optional[datetime] = None,
    ) -> JobRecord:
        """Persist retry-wait state and the next available execution time."""

    @abstractmethod
    def request_cancellation(self, job_id: str, now: Optional[datetime] = None) -> JobRecord:
        """Persist a cooperative cancellation request."""

    @abstractmethod
    def mark_timed_out(
        self,
        *,
        job_id: str,
        error_message: Optional[str],
        now: Optional[datetime] = None,
    ) -> JobRecord:
        """Persist terminal timeout state for a job."""

    @abstractmethod
    def reclaim_expired_lease(
        self,
        *,
        job_id: str,
        expected_lease_token: str,
        now: Optional[datetime] = None,
    ) -> JobRecord:
        """Atomically reclaim an expired lease for recovery handling.

        This operation must fail if the lease token no longer matches or the
        lease is no longer expired. Recovery code must not be allowed to steal
        healthy jobs.
        """

    # ------------------------------------------------------------------
    # Convenience default helpers
    # ------------------------------------------------------------------

    def require_job(self, job_id: str) -> JobRecord:
        """Return a durable job or raise a structured input error."""

        record = self.get_job(job_id)
        if record is None:
            raise InputError(f"job '{job_id}' was not found")
        return record


class InMemoryRuntimeRepository(RuntimeRepository):
    """Reference implementation for tests and early local development.

    This class is not a substitute for real durable storage. It exists so the
    dispatcher, worker, and recovery logic can be developed against the same
    repository contract before wiring in a database-backed implementation.

    Important truth:
    - it is process-local
    - it is not crash durable
    - it provides semantics, not production guarantees
    """

    def __init__(self) -> None:
        self._jobs: dict[str, JobRecord] = {}

    # ------------------------------------------------------------------
    # Job lifecycle
    # ------------------------------------------------------------------

    def create_job(self, job: JobRecord) -> JobRecord:
        if job.job_id in self._jobs:
            raise PersistenceError(f"job '{job.job_id}' already exists", retryable=False, permanent=True)
        self._jobs[job.job_id] = job.model_copy(deep=True)
        return self._jobs[job.job_id].model_copy(deep=True)

    def get_job(self, job_id: str) -> Optional[JobRecord]:
        job = self._jobs.get(job_id)
        return job.model_copy(deep=True) if job else None

    def update_job(self, job: JobRecord) -> JobRecord:
        if job.job_id not in self._jobs:
            raise PersistenceError(f"job '{job.job_id}' does not exist", retryable=False, permanent=True)
        self._jobs[job.job_id] = job.model_copy(deep=True)
        return self._jobs[job.job_id].model_copy(deep=True)

    def list_jobs(self, search: Optional[JobSearchFilter] = None) -> List[JobRecord]:
        search = search or JobSearchFilter()
        records = list(self._jobs.values())

        if search.states:
            allowed = set(search.states)
            records = [job for job in records if job.state in allowed]
        if search.queue_name:
            records = [job for job in records if job.queue_name == search.queue_name]
        if search.workflow_id:
            records = [job for job in records if job.workflow_id == search.workflow_id]
        if search.job_type:
            records = [job for job in records if job.job_type == search.job_type]
        if search.available_before:
            records = [job for job in records if job.available_at <= search.available_before]
        if search.created_after:
            records = [job for job in records if job.created_at >= search.created_after]
        if search.requested_by:
            records = [job for job in records if job.requested_by == search.requested_by]
        if search.cancellation_requested is not None:
            records = [
                job for job in records if job.cancellation_requested == search.cancellation_requested
            ]

        records.sort(key=lambda item: (item.priority, item.available_at, item.created_at, item.job_id))
        limit = search.normalized_limit()
        return [job.model_copy(deep=True) for job in records[:limit]]

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
        candidates = [
            job
            for job in self._jobs.values()
            if job.queue_name == queue_name
            and job.state == JobState.QUEUED
            and job.available_at <= now
        ]
        candidates.sort(key=lambda item: (item.priority, item.available_at, item.created_at, item.job_id))
        if not candidates:
            return None

        job = candidates[0]
        leased = job.with_lease(lease)
        self._jobs[job.job_id] = leased
        return leased.model_copy(deep=True)

    def renew_lease(
        self,
        *,
        job_id: str,
        worker_id: str,
        lease_token: str,
        ttl_seconds: float,
        now: Optional[datetime] = None,
    ) -> JobRecord:
        job = self.require_job(job_id)
        if job.lease is None:
            raise PersistenceError(f"job '{job_id}' has no active lease", retryable=False, permanent=True)
        if job.lease.worker_id != worker_id or job.lease.lease_token != lease_token:
            raise PersistenceError("lease ownership mismatch", retryable=False, permanent=True)
        renewed = job.model_copy(update={"lease": job.lease.heartbeat(ttl_seconds)})
        self._jobs[job_id] = renewed
        return renewed.model_copy(deep=True)

    def release_lease(
        self,
        *,
        job_id: str,
        worker_id: str,
        lease_token: str,
        now: Optional[datetime] = None,
    ) -> JobRecord:
        job = self.require_job(job_id)
        if job.lease is None:
            raise PersistenceError(f"job '{job_id}' has no active lease", retryable=False, permanent=True)
        if job.lease.worker_id != worker_id or job.lease.lease_token != lease_token:
            raise PersistenceError("lease ownership mismatch", retryable=False, permanent=True)

        updates = {"lease": None}
        if job.state == JobState.LEASED:
            updates["state"] = JobState.QUEUED
        released = job.model_copy(update=updates)
        self._jobs[job_id] = released
        return released.model_copy(deep=True)

    def list_expired_leases(
        self, search: Optional[LeaseSearchFilter] = None
    ) -> List[JobRecord]:
        search = search or LeaseSearchFilter()
        reference = search.expires_before or utc_now()
        jobs = [
            job
            for job in self._jobs.values()
            if job.lease is not None
            and job.lease.expires_at <= reference
        ]
        if search.queue_name:
            jobs = [job for job in jobs if job.queue_name == search.queue_name]
        if search.worker_id:
            jobs = [job for job in jobs if job.lease and job.lease.worker_id == search.worker_id]

        jobs.sort(key=lambda item: (item.lease.expires_at if item.lease else utc_now(), item.job_id))
        limit = search.normalized_limit()
        return [job.model_copy(deep=True) for job in jobs[:limit]]

    # ------------------------------------------------------------------
    # Stage persistence
    # ------------------------------------------------------------------

    def append_stage_record(self, job_id: str, stage: StageRecord) -> JobRecord:
        job = self.require_job(job_id)
        updated = job.model_copy(update={"stage_records": [*job.stage_records, stage.model_copy(deep=True)]})
        self._jobs[job_id] = updated
        return updated.model_copy(deep=True)

    def replace_stage_record(
        self,
        *,
        job_id: str,
        stage_name: str,
        attempt_number: int,
        record: StageRecord,
    ) -> JobRecord:
        job = self.require_job(job_id)
        replaced = False
        stage_records: List[StageRecord] = []
        for existing in job.stage_records:
            if existing.stage_name == stage_name and existing.attempt_number == attempt_number:
                stage_records.append(record.model_copy(deep=True))
                replaced = True
            else:
                stage_records.append(existing.model_copy(deep=True))

        if not replaced:
            raise PersistenceError(
                f"stage attempt '{stage_name}#{attempt_number}' does not exist",
                retryable=False,
                permanent=True,
            )

        updated = job.model_copy(update={"stage_records": stage_records})
        self._jobs[job_id] = updated
        return updated.model_copy(deep=True)

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
        job = self.require_job(job_id)
        job.ensure_transition_allowed(JobState.RETRY_WAIT)
        updated = job.model_copy(
            update={
                "state": JobState.RETRY_WAIT,
                "available_at": available_at,
                "attempt_number": job.attempt_number + 1,
                "error_message": error_message,
                "lease": None,
            }
        )
        self._jobs[job_id] = updated
        return updated.model_copy(deep=True)

    def request_cancellation(self, job_id: str, now: Optional[datetime] = None) -> JobRecord:
        job = self.require_job(job_id)
        updated = job.request_cancellation()
        self._jobs[job_id] = updated
        return updated.model_copy(deep=True)

    def mark_timed_out(
        self,
        *,
        job_id: str,
        error_message: Optional[str],
        now: Optional[datetime] = None,
    ) -> JobRecord:
        now = now or utc_now()
        job = self.require_job(job_id)
        if job.is_terminal():
            updated = job.model_copy(update={"error_message": error_message or job.error_message})
        else:
            updated = job.transition(JobState.TIMED_OUT, now=now).model_copy(
                update={"error_message": error_message}
            )
        self._jobs[job_id] = updated
        return updated.model_copy(deep=True)

    def reclaim_expired_lease(
        self,
        *,
        job_id: str,
        expected_lease_token: str,
        now: Optional[datetime] = None,
    ) -> JobRecord:
        now = now or utc_now()
        job = self.require_job(job_id)
        if job.lease is None:
            raise PersistenceError("job has no lease to reclaim", retryable=False, permanent=True)
        if job.lease.lease_token != expected_lease_token:
            raise PersistenceError("lease token mismatch during reclaim", retryable=False, permanent=True)
        if job.lease.expires_at > now:
            raise PersistenceError("lease is not expired", retryable=False, permanent=True)

        state = JobState.QUEUED if not job.cancellation_requested else JobState.CANCELLED
        reclaimed = job.model_copy(update={"state": state, "lease": None})
        self._jobs[job_id] = reclaimed
        return reclaimed.model_copy(deep=True)


def sort_stage_records(records: Iterable[StageRecord]) -> List[StageRecord]:
    """Return stage records in a stable execution-history order."""

    return sorted(
        records,
        key=lambda item: (item.stage_order, item.attempt_number, item.started_at or utc_now()),
    )


def latest_stage_record(records: Iterable[StageRecord], stage_name: str) -> Optional[StageRecord]:
    """Return the most recent attempt for a given stage, if any."""

    matches = [record for record in records if record.stage_name == stage_name]
    if not matches:
        return None
    matches.sort(key=lambda item: (item.attempt_number, item.started_at or utc_now(), item.stage_order))
    return matches[-1]
