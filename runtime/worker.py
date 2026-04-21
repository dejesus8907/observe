"""Worker execution loop for the durable runtime.

This module implements the actual runtime engine that consumes durably queued
jobs, acquires leases, executes stages, renews heartbeats, and persists
terminal outcomes. It is intentionally conservative: correctness comes before
throughput theater.

The worker owns:
- lease acquisition
- heartbeat renewal
- cooperative cancellation checks
- stage-by-stage execution persistence
- retry scheduling for retryable failures
- terminal success / failure / timeout transitions

What it does *not* do:
- distributed scheduling magic
- preemptive cancellation
- arbitrary parallel execution inside one job
- hiding broken stage semantics behind vague exception handling
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Protocol

from .errors import (
    CancellationRequestedError,
    ErrorContext,
    InternalRuntimeError,
    LeaseExpiredError,
    LeaseLostError,
    RuntimeExecutionError,
    TimeoutError,
    classify_exception,
)
from .models import (
    FailureCategory,
    IdempotencyMode,
    JobRecord,
    JobState,
    Lease,
    ResumabilityMode,
    StageRecord,
    StageState,
    StageSpec,
    utc_now,
)
from .repository import RuntimeRepository, latest_stage_record
from .metrics import (
    record_job_completed,
    record_job_retry,
    record_job_started,
    record_job_timeout,
    record_lease_acquired,
    record_lease_renewed,
    record_stage_attempt,
)


class StageExecutor(Protocol):
    """Callable contract for one runtime stage executor."""

    def __call__(self, job: JobRecord, stage_spec: StageSpec) -> Dict[str, Any]:
        """Execute one stage and return structured outputs / artifact refs."""


@dataclass(slots=True)
class WorkerConfig:
    """Execution and lease settings for a worker."""

    worker_id: str
    queue_name: str = "default"
    lease_ttl_seconds: float = 30.0
    heartbeat_interval_seconds: float = 10.0
    max_jobs_per_run: int = 1
    stop_on_empty_queue: bool = True

    def __post_init__(self) -> None:
        self.worker_id = self.worker_id.strip()
        self.queue_name = self.queue_name.strip()
        if not self.worker_id:
            raise ValueError("worker_id is required")
        if not self.queue_name:
            raise ValueError("queue_name is required")
        if self.lease_ttl_seconds <= 0:
            raise ValueError("lease_ttl_seconds must be > 0")
        if self.heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat_interval_seconds must be > 0")
        if self.heartbeat_interval_seconds >= self.lease_ttl_seconds:
            raise ValueError("heartbeat_interval_seconds must be less than lease_ttl_seconds")
        if self.max_jobs_per_run <= 0:
            raise ValueError("max_jobs_per_run must be > 0")


@dataclass(slots=True)
class WorkerResult:
    """Structured result for one worker polling run."""

    worker_id: str
    queue_name: str
    processed_jobs: int = 0
    succeeded_jobs: int = 0
    failed_jobs: int = 0
    retried_jobs: int = 0
    cancelled_jobs: int = 0
    timed_out_jobs: int = 0
    empty_polls: int = 0
    notes: List[str] = field(default_factory=list)
    job_ids: List[str] = field(default_factory=list)


class RuntimeWorker:
    """Durable job worker.

    This worker is deliberately single-job-at-a-time. You can scale out with
    more workers later. Starting with internal concurrency inside one worker
    before lease semantics are proven is how people build race conditions into
    their foundations.
    """

    def __init__(
        self,
        repository: RuntimeRepository,
        config: WorkerConfig,
        stage_executors: Dict[str, StageExecutor],
    ) -> None:
        self._repository = repository
        self._config = config
        self._stage_executors = dict(stage_executors)

    def run_once(self, *, now: Optional[datetime] = None) -> WorkerResult:
        """Poll the queue and process up to `max_jobs_per_run` jobs."""

        now = now or utc_now()
        result = WorkerResult(worker_id=self._config.worker_id, queue_name=self._config.queue_name)

        for _ in range(self._config.max_jobs_per_run):
            job = self._acquire_job(now=now)
            if job is None:
                result.empty_polls += 1
                result.notes.append("no eligible queued job found")
                if self._config.stop_on_empty_queue:
                    break
                continue

            result.processed_jobs += 1
            result.job_ids.append(job.job_id)

            final_job = self._process_leased_job(job, now=now)
            if final_job.state == JobState.SUCCEEDED:
                result.succeeded_jobs += 1
            elif final_job.state == JobState.FAILED:
                result.failed_jobs += 1
            elif final_job.state == JobState.RETRY_WAIT:
                result.retried_jobs += 1
            elif final_job.state == JobState.CANCELLED:
                result.cancelled_jobs += 1
            elif final_job.state == JobState.TIMED_OUT:
                result.timed_out_jobs += 1

        return result

    def _acquire_job(self, *, now: datetime) -> Optional[JobRecord]:
        lease = Lease(
            worker_id=self._config.worker_id,
            acquired_at=now,
            heartbeat_at=now,
            expires_at=now + timedelta(seconds=self._config.lease_ttl_seconds),
        )
        leased_job = self._repository.acquire_next_lease(
            worker_id=self._config.worker_id,
            queue_name=self._config.queue_name,
            lease=lease,
            now=now,
        )
        if leased_job is not None:
            record_lease_acquired(queue=self._config.queue_name, worker_id=self._config.worker_id)
        return leased_job

    def _process_leased_job(self, job: JobRecord, *, now: datetime) -> JobRecord:
        """Execute a leased job to a terminal state or retry-wait state."""

        self._assert_valid_lease(job, now=now)

        running = job.transition(JobState.RUNNING, now=now)
        job = self._repository.update_job(running)
        record_job_started(queue=job.queue_name, job_type=job.job_type, worker_id=self._config.worker_id)

        try:
            while True:
                self._assert_valid_lease(job, now=now)
                self._check_cancellation(job)
                self._check_job_timeout(job, now=now)

                stage = job.next_stage_spec()
                if stage is None:
                    completed = job.transition(JobState.SUCCEEDED, now=now)
                    persisted = self._repository.update_job(completed)
                    duration = None
                    if persisted.started_at and persisted.completed_at:
                        duration = (persisted.completed_at - persisted.started_at).total_seconds()
                    record_job_completed(queue=persisted.queue_name, job_type=persisted.job_type, outcome='succeeded', duration_seconds=duration)
                    return persisted

                job = self._execute_stage(job, stage, now=now)
                now = utc_now()
                job = self._maybe_heartbeat(job, now=now)

        except CancellationRequestedError as exc:
            cancelled = job.transition(JobState.CANCELLED, now=utc_now()).model_copy(
                update={
                    "failure_category": None,
                    "error_message": str(exc),
                }
            )
            persisted = self._repository.update_job(cancelled)
            duration = None
            if persisted.started_at and persisted.completed_at:
                duration = (persisted.completed_at - persisted.started_at).total_seconds()
            record_job_completed(queue=persisted.queue_name, job_type=persisted.job_type, outcome='cancelled', duration_seconds=duration)
            return persisted

        except TimeoutError as exc:
            timed_out = job.transition(JobState.TIMED_OUT, now=utc_now()).model_copy(
                update={
                    "failure_category": FailureCategory.TRANSIENT_SYSTEM,
                    "error_message": str(exc),
                }
            )
            persisted = self._repository.update_job(timed_out)
            duration = None
            if persisted.started_at and persisted.completed_at:
                duration = (persisted.completed_at - persisted.started_at).total_seconds()
            record_job_timeout(queue=persisted.queue_name, job_type=persisted.job_type)
            record_job_completed(queue=persisted.queue_name, job_type=persisted.job_type, outcome='timed_out', duration_seconds=duration)
            return persisted

        except RuntimeExecutionError as exc:
            return self._handle_execution_failure(job, exc, now=utc_now())

        except Exception as exc:  # pragma: no cover - safety net
            normalized = classify_exception(
                exc,
                default_message="unhandled worker exception",
                context=self._error_context(job),
            )
            return self._handle_execution_failure(job, normalized, now=utc_now())

    def _execute_stage(self, job: JobRecord, stage: StageSpec, *, now: datetime) -> JobRecord:
        """Execute one stage attempt and persist the result."""

        self._ensure_stage_executor(stage.name)
        current_attempt = self._next_stage_attempt_number(job, stage.name)

        prior = latest_stage_record(job.stage_records, stage.name)
        if prior and prior.state == StageState.SUCCEEDED:
            skipped = StageRecord(
                stage_name=stage.name,
                stage_order=stage.order,
                state=StageState.SKIPPED,
                attempt_number=current_attempt,
                started_at=now,
                completed_at=now,
                worker_id=self._config.worker_id,
                lease_token=job.lease.lease_token if job.lease else None,
                metadata={"reason": "already_succeeded"},
            )
            return self._repository.append_stage_record(job.job_id, skipped)

        if prior and stage.idempotency == IdempotencyMode.NON_IDEMPOTENT:
            raise InternalRuntimeError(
                f"non-idempotent stage '{stage.name}' cannot be retried blindly",
                context=self._error_context(job, stage_name=stage.name, attempt_number=current_attempt),
            )

        if prior and stage.resumability == ResumabilityMode.NON_RESUMABLE and prior.state == StageState.RUNNING:
            raise InternalRuntimeError(
                f"non-resumable stage '{stage.name}' was interrupted in running state",
                context=self._error_context(job, stage_name=stage.name, attempt_number=current_attempt),
            )

        started = StageRecord(
            stage_name=stage.name,
            stage_order=stage.order,
            state=StageState.RUNNING,
            attempt_number=current_attempt,
            started_at=now,
            worker_id=self._config.worker_id,
            lease_token=job.lease.lease_token if job.lease else None,
        )
        job = self._repository.append_stage_record(job.job_id, started)

        try:
            self._check_cancellation(job)
            self._check_job_timeout(job, now=now)
            executor = self._stage_executors[stage.name]
            outputs = executor(job, stage)
            finished = started.model_copy(
                update={
                    "state": StageState.SUCCEEDED,
                    "completed_at": utc_now(),
                    "artifact_refs": self._extract_artifact_refs(outputs),
                    "metadata": self._sanitize_stage_outputs(outputs),
                }
            )
            return self._repository.replace_stage_record(
                job_id=job.job_id,
                stage_name=stage.name,
                attempt_number=current_attempt,
                record=finished,
            )
            duration = None
            if finished.started_at and finished.completed_at:
                duration = (finished.completed_at - finished.started_at).total_seconds()
            record_stage_attempt(queue=job.queue_name, job_type=job.job_type, stage_name=stage.name, outcome='succeeded', duration_seconds=duration)
            return persisted

        except Exception as exc:
            normalized = classify_exception(
                exc,
                default_message=f"stage '{stage.name}' failed",
                context=self._error_context(job, stage_name=stage.name, attempt_number=current_attempt),
            )
            failed = started.model_copy(
                update={
                    "state": StageState.FAILED,
                    "completed_at": utc_now(),
                    "failure_category": normalized.category,
                    "error_message": normalized.message,
                    "metadata": {"error": normalized.to_record()},
                }
            )
            self._repository.replace_stage_record(
                job_id=job.job_id,
                stage_name=stage.name,
                attempt_number=current_attempt,
                record=failed,
            )
            duration = None
            if failed.started_at and failed.completed_at:
                duration = (failed.completed_at - failed.started_at).total_seconds()
            record_stage_attempt(queue=job.queue_name, job_type=job.job_type, stage_name=stage.name, outcome='failed', duration_seconds=duration)
            raise normalized

    def _handle_execution_failure(
        self,
        job: JobRecord,
        exc: RuntimeExecutionError,
        *,
        now: datetime,
    ) -> JobRecord:
        """Persist retry-wait or terminal failure state."""

        category = exc.category
        updated = job.model_copy(
            update={
                "failure_category": category,
                "error_message": exc.message,
            }
        )

        if updated.retry_policy.allows_retry_for(category, updated.attempt_number):
            available_at = updated.next_available_retry_time(now=now)
            return self._repository.schedule_retry(
                job_id=updated.job_id,
                available_at=available_at,
                error_message=exc.message,
                now=now,
            )
            record_job_retry(queue=persisted.queue_name, job_type=persisted.job_type, failure_category=category.value)
            return persisted

        failed = updated.transition(JobState.FAILED, now=now)
        persisted = self._repository.update_job(failed)
        duration = None
        if persisted.started_at and persisted.completed_at:
            duration = (persisted.completed_at - persisted.started_at).total_seconds()
        record_job_completed(queue=persisted.queue_name, job_type=persisted.job_type, outcome='failed', duration_seconds=duration)
        return persisted

    def _maybe_heartbeat(self, job: JobRecord, *, now: datetime) -> JobRecord:
        """Renew the lease when heartbeat is due."""

        if job.lease is None:
            raise LeaseLostError(
                "job lost its lease during execution",
                context=self._error_context(job),
            )

        elapsed = (now - job.lease.heartbeat_at).total_seconds()
        if elapsed < self._config.heartbeat_interval_seconds:
            return job

        renewed = self._repository.renew_lease(
            job_id=job.job_id,
            worker_id=self._config.worker_id,
            lease_token=job.lease.lease_token,
            ttl_seconds=self._config.lease_ttl_seconds,
            now=now,
        )
        record_lease_renewed(queue=renewed.queue_name, worker_id=self._config.worker_id)
        return renewed

    def _check_cancellation(self, job: JobRecord) -> None:
        current = self._repository.require_job(job.job_id)
        if current.cancellation_requested:
            raise CancellationRequestedError(context=self._error_context(current))

    def _check_job_timeout(self, job: JobRecord, *, now: datetime) -> None:
        if not job.timeout_seconds or not job.started_at:
            return
        deadline = job.started_at + timedelta(seconds=job.timeout_seconds)
        if now >= deadline:
            raise TimeoutError(
                f"job '{job.job_id}' exceeded timeout of {job.timeout_seconds} seconds",
                context=self._error_context(job),
            )

    def _assert_valid_lease(self, job: JobRecord, *, now: datetime) -> None:
        if job.lease is None:
            raise LeaseLostError("job has no active lease", context=self._error_context(job))
        if job.lease.worker_id != self._config.worker_id:
            raise LeaseLostError("worker does not own this lease", context=self._error_context(job))
        if job.lease.is_expired(now):
            raise LeaseExpiredError("job lease has expired", context=self._error_context(job))

    def _ensure_stage_executor(self, stage_name: str) -> None:
        if stage_name not in self._stage_executors:
            raise InternalRuntimeError(f"no executor registered for stage '{stage_name}'")

    def _next_stage_attempt_number(self, job: JobRecord, stage_name: str) -> int:
        attempts = [record.attempt_number for record in job.stage_records if record.stage_name == stage_name]
        return (max(attempts) + 1) if attempts else 1

    def _extract_artifact_refs(self, outputs: Dict[str, Any]) -> List[str]:
        refs = outputs.get("artifact_refs", []) if isinstance(outputs, dict) else []
        if not isinstance(refs, list):
            return []
        return [str(item).strip() for item in refs if str(item).strip()]

    def _sanitize_stage_outputs(self, outputs: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(outputs, dict):
            return {}
        sanitized = dict(outputs)
        sanitized.pop("artifact_refs", None)
        return sanitized

    def _error_context(
        self,
        job: JobRecord,
        *,
        stage_name: Optional[str] = None,
        attempt_number: Optional[int] = None,
    ) -> ErrorContext:
        return ErrorContext(
            workflow_id=job.workflow_id,
            job_id=job.job_id,
            stage_name=stage_name or job.current_stage_name,
            worker_id=self._config.worker_id,
            lease_token=job.lease.lease_token if job.lease else None,
            attempt_number=attempt_number or job.attempt_number,
        )
