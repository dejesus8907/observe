"""Recovery logic for expired leases and orphaned runtime jobs.

A durable runtime is not real unless it can survive worker death, process
restarts, and lease expiry without duplicating unsafe work or quietly losing
jobs. This module centralizes recovery policy so callers do not invent their
own half-broken reclaim logic.

Recovery owns:
- discovery of expired leases
- safe reclaim of orphaned jobs
- policy decisions for requeue vs fail vs cancel
- conservative handling of interrupted stage state

It does *not* execute stages itself. It prepares jobs so workers can resume or
retry safely according to declared stage semantics.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from .errors import InternalRuntimeError, PersistenceError
from .models import IdempotencyMode, JobRecord, JobState, ResumabilityMode, StageState, utc_now
from .repository import LeaseSearchFilter, RuntimeRepository, latest_stage_record


@dataclass(slots=True)
class RecoveryPolicy:
    """Policy knobs for reclaiming expired jobs.

    The defaults are intentionally conservative. Recovery should bias toward
    correctness, not optimistic resumption theater.
    """

    queue_name: Optional[str] = None
    fail_non_resumable_running_stage: bool = True
    cancel_if_requested: bool = True
    requeue_leased_jobs: bool = True
    requeue_running_jobs_without_active_stage: bool = True
    limit: int = 100


@dataclass(slots=True)
class RecoveryResult:
    """Structured result for one recovery sweep."""

    scanned_jobs: int = 0
    requeued_jobs: int = 0
    failed_jobs: int = 0
    cancelled_jobs: int = 0
    untouched_jobs: int = 0
    job_ids: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)


class RuntimeRecovery:
    """Lease-expiry and orphaned-job recovery engine.

    This component does the ugly but necessary work of reclaiming jobs after
    worker failure. It intentionally avoids cleverness. Unsafe recovery is
    worse than delayed recovery.
    """

    def __init__(self, repository: RuntimeRepository) -> None:
        self._repository = repository

    def run_once(
        self,
        policy: Optional[RecoveryPolicy] = None,
        *,
        now: Optional[datetime] = None,
    ) -> RecoveryResult:
        """Run one expired-lease recovery sweep."""

        policy = policy or RecoveryPolicy()
        now = now or utc_now()
        result = RecoveryResult()

        expired = self._repository.list_expired_leases(
            LeaseSearchFilter(
                queue_name=policy.queue_name,
                expires_before=now,
                limit=policy.limit,
            )
        )

        result.scanned_jobs = len(expired)

        for job in expired:
            result.job_ids.append(job.job_id)
            recovered = self._recover_job(job, policy=policy, now=now)
            if recovered.state == JobState.QUEUED:
                result.requeued_jobs += 1
            elif recovered.state == JobState.FAILED:
                result.failed_jobs += 1
            elif recovered.state == JobState.CANCELLED:
                result.cancelled_jobs += 1
            else:
                result.untouched_jobs += 1

        if not expired:
            result.notes.append("no expired leases found")

        return result

    def _recover_job(
        self,
        job: JobRecord,
        *,
        policy: RecoveryPolicy,
        now: datetime,
    ) -> JobRecord:
        """Recover one expired leased/running job conservatively."""

        if job.lease is None:
            raise PersistenceError("cannot recover job without an active lease", retryable=False, permanent=True)

        reclaimed = self._repository.reclaim_expired_lease(
            job_id=job.job_id,
            expected_lease_token=job.lease.lease_token,
            now=now,
        )

        if reclaimed.state == JobState.CANCELLED:
            return reclaimed

        if reclaimed.cancellation_requested and policy.cancel_if_requested:
            if reclaimed.is_terminal():
                return reclaimed
            cancelled = reclaimed.transition(JobState.CANCELLED, now=now).model_copy(
                update={"error_message": "job cancelled during recovery"}
            )
            return self._repository.update_job(cancelled)

        if job.state == JobState.LEASED:
            if not policy.requeue_leased_jobs:
                failed = reclaimed.transition(JobState.FAILED, now=now).model_copy(
                    update={"error_message": "recovery policy forbids requeue of leased job"}
                )
                return self._repository.update_job(failed)
            return reclaimed

        if job.state != JobState.RUNNING:
            return reclaimed

        latest = self._latest_running_or_terminal_stage(job)
        if latest is None:
            if policy.requeue_running_jobs_without_active_stage:
                return reclaimed
            failed = reclaimed.transition(JobState.FAILED, now=now).model_copy(
                update={"error_message": "running job had no persisted active stage during recovery"}
            )
            return self._repository.update_job(failed)

        if latest.state != StageState.RUNNING:
            return reclaimed

        spec = self._get_stage_spec(job, latest.stage_name)
        if spec is None:
            raise InternalRuntimeError(
                f"job '{job.job_id}' has running stage '{latest.stage_name}' with no matching stage spec"
            )

        if policy.fail_non_resumable_running_stage and spec.resumability == ResumabilityMode.NON_RESUMABLE:
            failed = reclaimed.transition(JobState.FAILED, now=now).model_copy(
                update={
                    "error_message": (
                        f"non-resumable stage '{latest.stage_name}' was interrupted by lease expiry"
                    )
                }
            )
            return self._repository.update_job(failed)

        if spec.idempotency == IdempotencyMode.NON_IDEMPOTENT and spec.resumability != ResumabilityMode.RESUMABLE:
            failed = reclaimed.transition(JobState.FAILED, now=now).model_copy(
                update={
                    "error_message": (
                        f"non-idempotent stage '{latest.stage_name}' cannot be safely replayed after recovery"
                    )
                }
            )
            return self._repository.update_job(failed)

        return reclaimed

    def _latest_running_or_terminal_stage(self, job: JobRecord):
        """Return the most relevant last stage attempt for recovery decisions."""

        if not job.stage_records:
            return None
        ordered = sorted(
            job.stage_records,
            key=lambda item: (item.stage_order, item.attempt_number, item.started_at or utc_now()),
        )
        return ordered[-1]

    def _get_stage_spec(self, job: JobRecord, stage_name: str):
        for spec in job.stage_specs:
            if spec.name == stage_name:
                return spec
        return None
