"""Cadence-based scheduler for the durable runtime.

This module is the glue between passive durable state and active runtime
progress. Workers execute jobs, but they do not by themselves:
- requeue retry-wait jobs when their backoff expires
- sweep expired leases for recovery
- emit scheduler-scoped run summaries

That is the scheduler's job.

Design goals
------------
- Requeue eligible retry-wait jobs conservatively
- Run expired-lease recovery sweeps
- Avoid hidden execution semantics
- Produce structured summaries for operator visibility
- Be safe to run repeatedly

Important honesty note
----------------------
This scheduler does not execute jobs directly. It only prepares the runtime
state so workers can pick up eligible jobs safely.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from .dispatcher import RuntimeDispatcher
from .errors import InputError, PersistenceError
from .metrics import (
    record_recovery_action,
    record_scheduler_requeue,
    set_component_health,
    time_repository_operation,
)
from .models import JobState, utc_now
from .repository import JobSearchFilter
from .recovery import RecoveryPolicy, RecoveryResult, RuntimeRecovery
from .repository import RuntimeRepository


@dataclass(slots=True)
class SchedulerConfig:
    """Configuration for one scheduler sweep."""

    queue_name: Optional[str] = None
    retry_requeue_limit: int = 100
    recovery_limit: int = 100
    enable_retry_requeue: bool = True
    enable_recovery: bool = True
    fail_fast: bool = False

    def __post_init__(self) -> None:
        if self.queue_name is not None:
            self.queue_name = self.queue_name.strip() or None
        if self.retry_requeue_limit <= 0:
            raise ValueError("retry_requeue_limit must be > 0")
        if self.recovery_limit <= 0:
            raise ValueError("recovery_limit must be > 0")


@dataclass(slots=True)
class SchedulerSweepResult:
    """Structured summary of one scheduler sweep."""

    queue_name: Optional[str]
    started_at: datetime
    completed_at: Optional[datetime] = None

    scanned_retry_wait_jobs: int = 0
    requeued_jobs: int = 0
    failed_requeues: int = 0

    recovery_scanned_jobs: int = 0
    recovery_requeued_jobs: int = 0
    recovery_failed_jobs: int = 0
    recovery_cancelled_jobs: int = 0
    recovery_untouched_jobs: int = 0

    notes: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    requeued_job_ids: List[str] = field(default_factory=list)
    recovery_job_ids: List[str] = field(default_factory=list)

    def finish(self, when: Optional[datetime] = None) -> "SchedulerSweepResult":
        self.completed_at = when or utc_now()
        return self


class RuntimeScheduler:
    """Runtime scheduler for retry requeue and recovery sweeps.

    The scheduler is intentionally conservative. It changes durable state, but
    it does not execute stages or jobs itself.
    """

    def __init__(
        self,
        repository: RuntimeRepository,
        dispatcher: RuntimeDispatcher,
        recovery: Optional[RuntimeRecovery] = None,
    ) -> None:
        self._repository = repository
        self._dispatcher = dispatcher
        self._recovery = recovery or RuntimeRecovery(repository)

    def run_once(
        self,
        config: Optional[SchedulerConfig] = None,
        *,
        now: Optional[datetime] = None,
    ) -> SchedulerSweepResult:
        """Run one scheduler sweep.

        This may:
        - requeue eligible retry_wait jobs
        - reclaim expired leases via recovery
        """

        config = config or SchedulerConfig()
        now = now or utc_now()
        result = SchedulerSweepResult(queue_name=config.queue_name, started_at=now)

        set_component_health(component="runtime_scheduler", healthy=True)

        try:
            if config.enable_retry_requeue:
                self._run_retry_requeue(result, config=config, now=now)
            else:
                result.notes.append("retry requeue disabled")

            if config.enable_recovery:
                self._run_recovery(result, config=config, now=now)
            else:
                result.notes.append("recovery sweep disabled")

        except Exception as exc:
            set_component_health(component="runtime_scheduler", healthy=False)
            result.errors.append(str(exc))
            if config.fail_fast:
                raise
        finally:
            result.finish()

        return result

    def _run_retry_requeue(
        self,
        result: SchedulerSweepResult,
        *,
        config: SchedulerConfig,
        now: datetime,
    ) -> None:
        """Requeue retry-wait jobs whose backoff window has elapsed."""

        with time_repository_operation(operation="scheduler_list_retry_wait_jobs"):
            search = JobSearchFilter(
                states=[JobState.RETRY_WAIT],
                queue_name=config.queue_name,
                available_before=now,
                limit=config.retry_requeue_limit,
            )
            jobs = self._repository.list_jobs(search)

        result.scanned_retry_wait_jobs = len(jobs)

        if not jobs:
            result.notes.append("no eligible retry_wait jobs found")
            return

        for job in jobs:
            try:
                decision = self._dispatcher.requeue_retry_wait_job(job.job_id, now=now)
                result.requeued_jobs += 1
                result.requeued_job_ids.append(decision.job.job_id)
                record_scheduler_requeue(queue=decision.job.queue_name, job_type=decision.job.job_type)
            except Exception as exc:
                result.failed_requeues += 1
                result.errors.append(f"requeue failed for job {job.job_id}: {exc}")

    def _run_recovery(
        self,
        result: SchedulerSweepResult,
        *,
        config: SchedulerConfig,
        now: datetime,
    ) -> None:
        """Run one expired-lease recovery sweep."""

        policy = RecoveryPolicy(
            queue_name=config.queue_name,
            limit=config.recovery_limit,
        )

        recovery_result = self._recovery.run_once(policy=policy, now=now)

        result.recovery_scanned_jobs = recovery_result.scanned_jobs
        result.recovery_requeued_jobs = recovery_result.requeued_jobs
        result.recovery_failed_jobs = recovery_result.failed_jobs
        result.recovery_cancelled_jobs = recovery_result.cancelled_jobs
        result.recovery_untouched_jobs = recovery_result.untouched_jobs
        result.recovery_job_ids.extend(recovery_result.job_ids)
        result.notes.extend(recovery_result.notes)

        for _ in range(recovery_result.requeued_jobs):
            record_recovery_action(queue=config.queue_name or "default", action="requeued")
        for _ in range(recovery_result.failed_jobs):
            record_recovery_action(queue=config.queue_name or "default", action="failed")
        for _ in range(recovery_result.cancelled_jobs):
            record_recovery_action(queue=config.queue_name or "default", action="cancelled")
        for _ in range(recovery_result.untouched_jobs):
            record_recovery_action(queue=config.queue_name or "default", action="untouched")