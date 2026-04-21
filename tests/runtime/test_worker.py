from datetime import timedelta

import pytest

from netobserv.runtime.dispatcher import DispatchRequest, RuntimeDispatcher
from netobserv.runtime.errors import TransientSystemError
from netobserv.runtime.models import FailureCategory, JobState, RetryPolicy, StageSpec, utc_now
from netobserv.runtime.repository import InMemoryRuntimeRepository
from netobserv.runtime.worker import RuntimeWorker, WorkerConfig


def stage_specs(retry_policy=None):
    return [
        StageSpec(name="collect", order=0, retry_policy=retry_policy or RetryPolicy()),
        StageSpec(name="normalize", order=1, retry_policy=retry_policy or RetryPolicy()),
    ]


def make_worker(repo, executors):
    return RuntimeWorker(
        repo,
        WorkerConfig(worker_id="worker-1", queue_name="default", lease_ttl_seconds=30, heartbeat_interval_seconds=10),
        executors,
    )


def test_worker_executes_job_to_success():
    repo = InMemoryRuntimeRepository()
    dispatcher = RuntimeDispatcher(repo)
    dispatcher.submit(DispatchRequest(workflow_id="wf-1", job_type="discovery", stage_specs=stage_specs()))

    worker = make_worker(
        repo,
        {
            "collect": lambda job, stage: {"artifact_refs": ["a1"], "records": 10},
            "normalize": lambda job, stage: {"artifact_refs": ["a2"], "records": 5},
        },
    )
    result = worker.run_once()
    assert result.processed_jobs == 1
    assert result.succeeded_jobs == 1

    job = repo.list_jobs()[0]
    assert job.state == JobState.SUCCEEDED
    assert len(job.stage_records) == 2
    assert all(r.state.value == "succeeded" for r in job.stage_records)


def test_worker_moves_retryable_failure_to_retry_wait():
    repo = InMemoryRuntimeRepository()
    dispatcher = RuntimeDispatcher(repo)
    retry_policy = RetryPolicy(
        max_attempts=3,
        retryable_failures=[FailureCategory.TRANSIENT_SYSTEM],
    )
    dispatcher.submit(
        DispatchRequest(workflow_id="wf-1", job_type="discovery", stage_specs=stage_specs(retry_policy=retry_policy))
    )

    worker = make_worker(
        repo,
        {
            "collect": lambda job, stage: (_ for _ in ()).throw(TransientSystemError("temporary failure")),
            "normalize": lambda job, stage: {"ok": True},
        },
    )
    result = worker.run_once()
    assert result.retried_jobs == 1
    job = repo.list_jobs()[0]
    assert job.state == JobState.RETRY_WAIT
    assert job.attempt_number == 2


def test_worker_marks_cancelled_when_cancellation_requested_before_stage_execution():
    repo = InMemoryRuntimeRepository()
    dispatcher = RuntimeDispatcher(repo)
    decision = dispatcher.submit(
        DispatchRequest(workflow_id="wf-1", job_type="discovery", stage_specs=stage_specs())
    )
    repo.request_cancellation(decision.job.job_id)

    worker = make_worker(
        repo,
        {
            "collect": lambda job, stage: {"ok": True},
            "normalize": lambda job, stage: {"ok": True},
        },
    )
    result = worker.run_once()
    assert result.cancelled_jobs == 1
    job = repo.list_jobs()[0]
    assert job.state == JobState.CANCELLED


def test_worker_marks_failed_for_non_retryable_error():
    repo = InMemoryRuntimeRepository()
    dispatcher = RuntimeDispatcher(repo)
    dispatcher.submit(
        DispatchRequest(workflow_id="wf-1", job_type="discovery", stage_specs=stage_specs())
    )

    worker = make_worker(
        repo,
        {
            "collect": lambda job, stage: (_ for _ in ()).throw(ValueError("bad input")),
            "normalize": lambda job, stage: {"ok": True},
        },
    )
    result = worker.run_once()
    assert result.failed_jobs == 1
    job = repo.list_jobs()[0]
    assert job.state == JobState.FAILED
