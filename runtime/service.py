"""Long-running runtime service wrapper.

This module wraps the runtime worker and scheduler into an operator-facing
service loop. It is intentionally conservative and explicit about cadence,
shutdown, and error accounting.

The service owns:
- worker polling cadence
- scheduler sweep cadence
- cooperative shutdown
- service-level run summaries
- health state emission

It does *not* replace the worker or scheduler. It coordinates them.

Important honesty note
----------------------
This service is synchronous and process-local by design. It does not pretend to
be a distributed supervisor. It gives the runtime a clean operational loop that
can later be hosted by a CLI command, a dedicated process, or a service manager.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from time import monotonic, sleep, time
from typing import Callable, List, Optional

from .metrics import record_worker_heartbeat, set_component_health
from .models import utc_now
from .scheduler import RuntimeScheduler, SchedulerConfig, SchedulerSweepResult
from .worker import RuntimeWorker, WorkerResult


@dataclass(slots=True)
class RuntimeServiceConfig:
    """Configuration for the runtime service loop."""

    worker_interval_seconds: float = 1.0
    scheduler_interval_seconds: float = 15.0
    idle_sleep_seconds: float = 1.0
    max_iterations: Optional[int] = None
    fail_fast: bool = False
    emit_heartbeat: bool = True
    run_scheduler_on_start: bool = True
    run_worker_on_start: bool = True

    def __post_init__(self) -> None:
        if self.worker_interval_seconds <= 0:
            raise ValueError("worker_interval_seconds must be > 0")
        if self.scheduler_interval_seconds <= 0:
            raise ValueError("scheduler_interval_seconds must be > 0")
        if self.idle_sleep_seconds < 0:
            raise ValueError("idle_sleep_seconds must be >= 0")
        if self.max_iterations is not None and self.max_iterations <= 0:
            raise ValueError("max_iterations must be > 0 when provided")


@dataclass(slots=True)
class RuntimeServiceState:
    """Mutable state for the service loop."""

    started_at: datetime
    stopped_at: Optional[datetime] = None
    shutdown_requested: bool = False
    iteration_count: int = 0
    last_worker_run_at: Optional[datetime] = None
    last_scheduler_run_at: Optional[datetime] = None
    last_error: Optional[str] = None

    def request_shutdown(self) -> None:
        self.shutdown_requested = True

    def finish(self, when: Optional[datetime] = None) -> None:
        self.stopped_at = when or utc_now()


@dataclass(slots=True)
class RuntimeServiceResult:
    """Structured result for a bounded service run."""

    started_at: datetime
    completed_at: Optional[datetime] = None
    iterations: int = 0
    worker_runs: int = 0
    scheduler_runs: int = 0
    worker_processed_jobs: int = 0
    worker_succeeded_jobs: int = 0
    worker_failed_jobs: int = 0
    worker_retried_jobs: int = 0
    worker_cancelled_jobs: int = 0
    worker_timed_out_jobs: int = 0
    scheduler_requeued_jobs: int = 0
    scheduler_failed_requeues: int = 0
    recovery_requeued_jobs: int = 0
    recovery_failed_jobs: int = 0
    recovery_cancelled_jobs: int = 0
    errors: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    def finish(self, when: Optional[datetime] = None) -> "RuntimeServiceResult":
        self.completed_at = when or utc_now()
        return self


class RuntimeService:
    """Service wrapper coordinating worker and scheduler cadences.

    The service is safe to use in:
    - a foreground CLI process
    - a dedicated daemon process
    - a test harness running bounded iterations

    It intentionally avoids hidden threads or signal magic. Shutdown is
    cooperative and explicit.
    """

    def __init__(
        self,
        worker: RuntimeWorker,
        scheduler: RuntimeScheduler,
        *,
        worker_id: str,
        queue_name: str = "default",
        now_factory: Callable[[], datetime] = utc_now,
    ) -> None:
        self._worker = worker
        self._scheduler = scheduler
        self._worker_id = worker_id.strip()
        self._queue_name = queue_name.strip() or "default"
        self._now_factory = now_factory
        if not self._worker_id:
            raise ValueError("worker_id is required")

    def run(
        self,
        config: Optional[RuntimeServiceConfig] = None,
        *,
        state: Optional[RuntimeServiceState] = None,
        scheduler_config: Optional[SchedulerConfig] = None,
    ) -> RuntimeServiceResult:
        """Run the service loop for a bounded or unbounded duration.

        When `max_iterations` is set, this method returns after that many loop
        iterations. Otherwise it runs until `state.request_shutdown()` is called
        from the outside.
        """

        config = config or RuntimeServiceConfig()
        state = state or RuntimeServiceState(started_at=self._now_factory())
        result = RuntimeServiceResult(started_at=state.started_at)

        last_worker_tick = 0.0
        last_scheduler_tick = 0.0
        first_iteration = True

        set_component_health(component="runtime_service", healthy=True)

        try:
            while not state.shutdown_requested:
                now_dt = self._now_factory()
                now_mono = monotonic()

                should_run_worker = (
                    (first_iteration and config.run_worker_on_start)
                    or (now_mono - last_worker_tick >= config.worker_interval_seconds)
                )
                should_run_scheduler = (
                    (first_iteration and config.run_scheduler_on_start)
                    or (now_mono - last_scheduler_tick >= config.scheduler_interval_seconds)
                )

                did_work = False

                if should_run_scheduler:
                    sweep = self._run_scheduler(now_dt, config, state, result, scheduler_config=scheduler_config)
                    last_scheduler_tick = now_mono
                    result.scheduler_runs += 1
                    did_work = True
                    self._accumulate_scheduler_result(result, sweep)

                if should_run_worker:
                    worker_result = self._run_worker(now_dt, config, state, result)
                    last_worker_tick = now_mono
                    result.worker_runs += 1
                    did_work = True
                    self._accumulate_worker_result(result, worker_result)

                if config.emit_heartbeat:
                    self._emit_heartbeat()

                state.iteration_count += 1
                result.iterations = state.iteration_count
                first_iteration = False

                if config.max_iterations is not None and state.iteration_count >= config.max_iterations:
                    result.notes.append("max_iterations reached")
                    break

                if not did_work and config.idle_sleep_seconds > 0:
                    sleep(config.idle_sleep_seconds)

        except Exception as exc:
            set_component_health(component="runtime_service", healthy=False)
            state.last_error = str(exc)
            result.errors.append(str(exc))
            if config.fail_fast:
                raise
        finally:
            state.finish(self._now_factory())
            result.finish(state.stopped_at)
            if state.shutdown_requested:
                result.notes.append("shutdown requested")
            set_component_health(component="runtime_service", healthy=not bool(result.errors))

        return result

    def request_shutdown(self, state: RuntimeServiceState) -> None:
        """Cooperatively request service shutdown."""

        state.request_shutdown()

    def run_once(
        self,
        *,
        run_worker: bool = True,
        run_scheduler: bool = True,
        scheduler_config: Optional[SchedulerConfig] = None,
    ) -> RuntimeServiceResult:
        """Run a single bounded iteration for testing or operator control."""

        config = RuntimeServiceConfig(
            max_iterations=1,
            run_worker_on_start=run_worker,
            run_scheduler_on_start=run_scheduler,
            emit_heartbeat=True,
        )
        state = RuntimeServiceState(started_at=self._now_factory())
        return self.run(config=config, state=state, scheduler_config=scheduler_config)

    def _run_worker(
        self,
        now_dt: datetime,
        config: RuntimeServiceConfig,
        state: RuntimeServiceState,
        result: RuntimeServiceResult,
    ) -> WorkerResult:
        try:
            worker_result = self._worker.run_once(now=now_dt)
            state.last_worker_run_at = now_dt
            return worker_result
        except Exception as exc:
            state.last_error = str(exc)
            result.errors.append(f"worker run failed: {exc}")
            if config.fail_fast:
                raise
            return WorkerResult(worker_id=self._worker_id, queue_name=self._queue_name, notes=["worker failure"])

    def _run_scheduler(
        self,
        now_dt: datetime,
        config: RuntimeServiceConfig,
        state: RuntimeServiceState,
        result: RuntimeServiceResult,
        *,
        scheduler_config: Optional[SchedulerConfig] = None,
    ) -> SchedulerSweepResult:
        try:
            effective_scheduler_config = scheduler_config or SchedulerConfig(queue_name=self._queue_name)
            if effective_scheduler_config.queue_name is None:
                effective_scheduler_config = SchedulerConfig(
                    queue_name=self._queue_name,
                    retry_requeue_limit=effective_scheduler_config.retry_requeue_limit,
                    recovery_limit=effective_scheduler_config.recovery_limit,
                    enable_retry_requeue=effective_scheduler_config.enable_retry_requeue,
                    enable_recovery=effective_scheduler_config.enable_recovery,
                    fail_fast=effective_scheduler_config.fail_fast,
                )
            sweep = self._scheduler.run_once(
                config=effective_scheduler_config,
                now=now_dt,
            )
            state.last_scheduler_run_at = now_dt
            return sweep
        except Exception as exc:
            state.last_error = str(exc)
            result.errors.append(f"scheduler run failed: {exc}")
            if config.fail_fast:
                raise
            failed = SchedulerSweepResult(queue_name=self._queue_name, started_at=now_dt)
            failed.errors.append(str(exc))
            failed.finish(self._now_factory())
            return failed

    def _emit_heartbeat(self) -> None:
        record_worker_heartbeat(
            worker_id=self._worker_id,
            queue=self._queue_name,
            unix_time=time(),
        )

    def _accumulate_worker_result(self, result: RuntimeServiceResult, worker_result: WorkerResult) -> None:
        result.worker_processed_jobs += worker_result.processed_jobs
        result.worker_succeeded_jobs += worker_result.succeeded_jobs
        result.worker_failed_jobs += worker_result.failed_jobs
        result.worker_retried_jobs += worker_result.retried_jobs
        result.worker_cancelled_jobs += worker_result.cancelled_jobs
        result.worker_timed_out_jobs += worker_result.timed_out_jobs
        result.notes.extend(worker_result.notes)

    def _accumulate_scheduler_result(
        self,
        result: RuntimeServiceResult,
        sweep: SchedulerSweepResult,
    ) -> None:
        result.scheduler_requeued_jobs += sweep.requeued_jobs
        result.scheduler_failed_requeues += sweep.failed_requeues
        result.recovery_requeued_jobs += sweep.recovery_requeued_jobs
        result.recovery_failed_jobs += sweep.recovery_failed_jobs
        result.recovery_cancelled_jobs += sweep.recovery_cancelled_jobs
        result.notes.extend(sweep.notes)
        result.errors.extend(sweep.errors)\n