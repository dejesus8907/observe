"""Runtime-specific observability helpers.

This module instruments the durable runtime subsystem with metrics that reflect
actual execution behavior rather than generic application noise.

Goals
-----
- Expose queue pressure and worker activity
- Track lease acquisition / renewal / reclaim behavior
- Track retries, cancellations, timeouts, and terminal outcomes
- Measure stage and job execution durations
- Keep labels low-cardinality so metrics stay usable in production

Important honesty note
----------------------
This file provides runtime-scoped instrumentation primitives. It does *not*
automatically wire every runtime path. The worker, scheduler, recovery, and
repository layers must actually call these helpers.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from time import perf_counter
from typing import Iterator, Optional

from prometheus_client import Counter, Gauge, Histogram


def _norm(value: Optional[str], default: str = "unknown") -> str:
    """Normalize labels to low-cardinality strings.

    Metrics are not a trash can for arbitrary free-form values.
    """

    if value is None:
        return default
    text = str(value).strip().lower().replace(" ", "_")
    return text or default


# ----------------------------------------------------------------------
# Queue / inventory of work
# ----------------------------------------------------------------------

runtime_queue_depth = Gauge(
    "netobserv_runtime_queue_depth",
    "Current number of runtime jobs by queue and state.",
    labelnames=("queue", "state"),
)

runtime_active_leases = Gauge(
    "netobserv_runtime_active_leases",
    "Current number of active runtime leases by queue.",
    labelnames=("queue",),
)

runtime_worker_heartbeat_timestamp = Gauge(
    "netobserv_runtime_worker_heartbeat_unixtime",
    "Unix timestamp of the last observed worker heartbeat.",
    labelnames=("worker_id", "queue"),
)

runtime_component_health = Gauge(
    "netobserv_runtime_component_health",
    "Runtime component health state where 1=healthy and 0=unhealthy.",
    labelnames=("component",),
)

# ----------------------------------------------------------------------
# Job lifecycle / outcomes
# ----------------------------------------------------------------------

runtime_jobs_submitted_total = Counter(
    "netobserv_runtime_jobs_submitted_total",
    "Total runtime jobs submitted.",
    labelnames=("queue", "job_type", "mode"),
)

runtime_jobs_started_total = Counter(
    "netobserv_runtime_jobs_started_total",
    "Total runtime jobs started by workers.",
    labelnames=("queue", "job_type", "worker_id"),
)

runtime_jobs_completed_total = Counter(
    "netobserv_runtime_jobs_completed_total",
    "Total runtime jobs completed by terminal outcome.",
    labelnames=("queue", "job_type", "outcome"),
)

runtime_jobs_retried_total = Counter(
    "netobserv_runtime_jobs_retried_total",
    "Total runtime jobs moved into retry_wait.",
    labelnames=("queue", "job_type", "failure_category"),
)

runtime_jobs_cancel_requested_total = Counter(
    "netobserv_runtime_jobs_cancel_requested_total",
    "Total runtime cancellation requests.",
    labelnames=("queue", "job_type"),
)

runtime_jobs_timed_out_total = Counter(
    "netobserv_runtime_jobs_timed_out_total",
    "Total runtime jobs timed out.",
    labelnames=("queue", "job_type"),
)

runtime_job_duration_seconds = Histogram(
    "netobserv_runtime_job_duration_seconds",
    "Runtime job execution duration in seconds.",
    labelnames=("queue", "job_type", "outcome"),
    buckets=(0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 900, 1800, 3600),
)

# ----------------------------------------------------------------------
# Stage lifecycle / outcomes
# ----------------------------------------------------------------------

runtime_stage_attempts_total = Counter(
    "netobserv_runtime_stage_attempts_total",
    "Total runtime stage attempts by stage and outcome.",
    labelnames=("queue", "job_type", "stage_name", "outcome"),
)

runtime_stage_duration_seconds = Histogram(
    "netobserv_runtime_stage_duration_seconds",
    "Runtime stage execution duration in seconds.",
    labelnames=("queue", "job_type", "stage_name", "outcome"),
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300),
)

# ----------------------------------------------------------------------
# Lease / recovery / scheduler behavior
# ----------------------------------------------------------------------

runtime_lease_acquired_total = Counter(
    "netobserv_runtime_lease_acquired_total",
    "Total runtime lease acquisitions.",
    labelnames=("queue", "worker_id"),
)

runtime_lease_renewed_total = Counter(
    "netobserv_runtime_lease_renewed_total",
    "Total runtime lease renewals.",
    labelnames=("queue", "worker_id"),
)

runtime_lease_released_total = Counter(
    "netobserv_runtime_lease_released_total",
    "Total runtime lease releases.",
    labelnames=("queue", "worker_id"),
)

runtime_lease_reclaim_total = Counter(
    "netobserv_runtime_lease_reclaim_total",
    "Total expired lease reclaim actions by resulting outcome.",
    labelnames=("queue", "outcome"),
)

runtime_scheduler_requeue_total = Counter(
    "netobserv_runtime_scheduler_requeue_total",
    "Total retry_wait jobs requeued by the runtime scheduler.",
    labelnames=("queue", "job_type"),
)

runtime_recovery_actions_total = Counter(
    "netobserv_runtime_recovery_actions_total",
    "Total runtime recovery actions.",
    labelnames=("queue", "action"),
)

runtime_repository_operations_total = Counter(
    "netobserv_runtime_repository_operations_total",
    "Total runtime repository operations by name and outcome.",
    labelnames=("operation", "outcome"),
)

runtime_repository_operation_duration_seconds = Histogram(
    "netobserv_runtime_repository_operation_duration_seconds",
    "Duration of runtime repository operations in seconds.",
    labelnames=("operation", "outcome"),
    buckets=(0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10),
)


@dataclass(slots=True)
class RuntimeMetricLabels:
    """Normalized labels commonly reused across runtime instrumentation."""

    queue: str = "default"
    job_type: str = "unknown"
    stage_name: str = "unknown"
    worker_id: str = "unknown"
    mode: str = "normal"
    outcome: str = "unknown"
    failure_category: str = "unknown"
    component: str = "runtime"

    @classmethod
    def from_values(
        cls,
        *,
        queue: Optional[str] = None,
        job_type: Optional[str] = None,
        stage_name: Optional[str] = None,
        worker_id: Optional[str] = None,
        mode: Optional[str] = None,
        outcome: Optional[str] = None,
        failure_category: Optional[str] = None,
        component: Optional[str] = None,
    ) -> "RuntimeMetricLabels":
        return cls(
            queue=_norm(queue, "default"),
            job_type=_norm(job_type),
            stage_name=_norm(stage_name),
            worker_id=_norm(worker_id),
            mode=_norm(mode, "normal"),
            outcome=_norm(outcome),
            failure_category=_norm(failure_category),
            component=_norm(component, "runtime"),
        )


# ----------------------------------------------------------------------
# Direct recording helpers
# ----------------------------------------------------------------------

def record_queue_depth(*, queue: str, state: str, count: int) -> None:
    runtime_queue_depth.labels(queue=_norm(queue, "default"), state=_norm(state)).set(max(0, int(count)))


def record_active_leases(*, queue: str, count: int) -> None:
    runtime_active_leases.labels(queue=_norm(queue, "default")).set(max(0, int(count)))


def record_worker_heartbeat(*, worker_id: str, queue: str, unix_time: float) -> None:
    runtime_worker_heartbeat_timestamp.labels(
        worker_id=_norm(worker_id), queue=_norm(queue, "default")
    ).set(float(unix_time))


def set_component_health(*, component: str, healthy: bool) -> None:
    runtime_component_health.labels(component=_norm(component)).set(1 if healthy else 0)


def record_job_submitted(*, queue: str, job_type: str, mode: str) -> None:
    runtime_jobs_submitted_total.labels(
        queue=_norm(queue, "default"),
        job_type=_norm(job_type),
        mode=_norm(mode, "normal"),
    ).inc()


def record_job_started(*, queue: str, job_type: str, worker_id: str) -> None:
    runtime_jobs_started_total.labels(
        queue=_norm(queue, "default"),
        job_type=_norm(job_type),
        worker_id=_norm(worker_id),
    ).inc()


def record_job_completed(*, queue: str, job_type: str, outcome: str, duration_seconds: Optional[float] = None) -> None:
    labels = dict(queue=_norm(queue, "default"), job_type=_norm(job_type), outcome=_norm(outcome))
    runtime_jobs_completed_total.labels(**labels).inc()
    if duration_seconds is not None and duration_seconds >= 0:
        runtime_job_duration_seconds.labels(**labels).observe(duration_seconds)


def record_job_retry(*, queue: str, job_type: str, failure_category: str) -> None:
    runtime_jobs_retried_total.labels(
        queue=_norm(queue, "default"),
        job_type=_norm(job_type),
        failure_category=_norm(failure_category),
    ).inc()


def record_job_cancel_requested(*, queue: str, job_type: str) -> None:
    runtime_jobs_cancel_requested_total.labels(
        queue=_norm(queue, "default"),
        job_type=_norm(job_type),
    ).inc()


def record_job_timeout(*, queue: str, job_type: str) -> None:
    runtime_jobs_timed_out_total.labels(
        queue=_norm(queue, "default"),
        job_type=_norm(job_type),
    ).inc()


def record_stage_attempt(
    *,
    queue: str,
    job_type: str,
    stage_name: str,
    outcome: str,
    duration_seconds: Optional[float] = None,
) -> None:
    labels = dict(
        queue=_norm(queue, "default"),
        job_type=_norm(job_type),
        stage_name=_norm(stage_name),
        outcome=_norm(outcome),
    )
    runtime_stage_attempts_total.labels(**labels).inc()
    if duration_seconds is not None and duration_seconds >= 0:
        runtime_stage_duration_seconds.labels(**labels).observe(duration_seconds)


def record_lease_acquired(*, queue: str, worker_id: str) -> None:
    runtime_lease_acquired_total.labels(queue=_norm(queue, "default"), worker_id=_norm(worker_id)).inc()


def record_lease_renewed(*, queue: str, worker_id: str) -> None:
    runtime_lease_renewed_total.labels(queue=_norm(queue, "default"), worker_id=_norm(worker_id)).inc()


def record_lease_released(*, queue: str, worker_id: str) -> None:
    runtime_lease_released_total.labels(queue=_norm(queue, "default"), worker_id=_norm(worker_id)).inc()


def record_lease_reclaim(*, queue: str, outcome: str) -> None:
    runtime_lease_reclaim_total.labels(queue=_norm(queue, "default"), outcome=_norm(outcome)).inc()


def record_scheduler_requeue(*, queue: str, job_type: str) -> None:
    runtime_scheduler_requeue_total.labels(queue=_norm(queue, "default"), job_type=_norm(job_type)).inc()


def record_recovery_action(*, queue: str, action: str) -> None:
    runtime_recovery_actions_total.labels(queue=_norm(queue, "default"), action=_norm(action)).inc()


def record_repository_operation(*, operation: str, outcome: str, duration_seconds: Optional[float] = None) -> None:
    labels = dict(operation=_norm(operation), outcome=_norm(outcome))
    runtime_repository_operations_total.labels(**labels).inc()
    if duration_seconds is not None and duration_seconds >= 0:
        runtime_repository_operation_duration_seconds.labels(**labels).observe(duration_seconds)


# ----------------------------------------------------------------------
# Timing context managers
# ----------------------------------------------------------------------

@contextmanager
def time_job(*, queue: str, job_type: str, outcome_on_success: str = "succeeded") -> Iterator[None]:
    """Measure a job duration and emit the final completion metric.

    Exceptions are re-raised after recording a failed outcome.
    """

    start = perf_counter()
    try:
        yield
    except Exception:
        record_job_completed(
            queue=queue,
            job_type=job_type,
            outcome="failed",
            duration_seconds=perf_counter() - start,
        )
        raise
    else:
        record_job_completed(
            queue=queue,
            job_type=job_type,
            outcome=outcome_on_success,
            duration_seconds=perf_counter() - start,
        )


@contextmanager
def time_stage(*, queue: str, job_type: str, stage_name: str, outcome_on_success: str = "succeeded") -> Iterator[None]:
    """Measure a stage duration and emit the final stage metric."""

    start = perf_counter()
    try:
        yield
    except Exception:
        record_stage_attempt(
            queue=queue,
            job_type=job_type,
            stage_name=stage_name,
            outcome="failed",
            duration_seconds=perf_counter() - start,
        )
        raise
    else:
        record_stage_attempt(
            queue=queue,
            job_type=job_type,
            stage_name=stage_name,
            outcome=outcome_on_success,
            duration_seconds=perf_counter() - start,
        )


@contextmanager
def time_repository_operation(*, operation: str) -> Iterator[None]:
    """Measure a runtime repository operation."""

    start = perf_counter()
    try:
        yield
    except Exception:
        record_repository_operation(
            operation=operation,
            outcome="failed",
            duration_seconds=perf_counter() - start,
        )
        raise
    else:
        record_repository_operation(
            operation=operation,
            outcome="succeeded",
            duration_seconds=perf_counter() - start,
        )\n