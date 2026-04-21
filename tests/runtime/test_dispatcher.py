import pytest

from netobserv.runtime.dispatcher import DispatchRequest, RuntimeDispatcher
from netobserv.runtime.errors import InputError
from netobserv.runtime.models import ExecutionMode, JobState, StageSpec
from netobserv.runtime.repository import InMemoryRuntimeRepository


def stage_specs():
    return [StageSpec(name="collect", order=0), StageSpec(name="normalize", order=1)]


def test_submit_creates_durable_queued_job():
    repo = InMemoryRuntimeRepository()
    dispatcher = RuntimeDispatcher(repo, allowed_queues=["default", "priority"])
    decision = dispatcher.submit(
        DispatchRequest(
            workflow_id="wf-1",
            job_type="discovery",
            stage_specs=stage_specs(),
            execution_mode=ExecutionMode.DRY_RUN,
        )
    )
    assert decision.accepted is True
    assert decision.enqueued is True
    assert decision.job.state == JobState.QUEUED
    assert repo.get_job(decision.job.job_id) is not None


def test_submit_rejects_disallowed_queue():
    repo = InMemoryRuntimeRepository()
    dispatcher = RuntimeDispatcher(repo, allowed_queues=["default"])
    with pytest.raises(InputError):
        dispatcher.submit(
            DispatchRequest(
                workflow_id="wf-1",
                job_type="discovery",
                stage_specs=stage_specs(),
                queue_name="forbidden",
            )
        )


def test_submit_requires_contiguous_stage_orders():
    repo = InMemoryRuntimeRepository()
    dispatcher = RuntimeDispatcher(repo)
    with pytest.raises(InputError):
        dispatcher.submit(
            DispatchRequest(
                workflow_id="wf-1",
                job_type="discovery",
                stage_specs=[StageSpec(name="collect", order=0), StageSpec(name="normalize", order=2)],
            )
        )


def test_cancel_marks_job_for_cooperative_cancellation():
    repo = InMemoryRuntimeRepository()
    dispatcher = RuntimeDispatcher(repo)
    decision = dispatcher.submit(
        DispatchRequest(workflow_id="wf-1", job_type="discovery", stage_specs=stage_specs())
    )
    cancel = dispatcher.cancel(decision.job.job_id)
    assert cancel.job.cancellation_requested is True


def test_requeue_retry_wait_job_returns_job_to_queue():
    repo = InMemoryRuntimeRepository()
    dispatcher = RuntimeDispatcher(repo)
    decision = dispatcher.submit(
        DispatchRequest(workflow_id="wf-1", job_type="discovery", stage_specs=stage_specs())
    )
    job = repo.require_job(decision.job.job_id)
    leased = job.with_lease(
        __import__("netobserv.runtime.models", fromlist=["Lease"]).Lease(
            worker_id="worker-1",
            expires_at=job.available_at.replace() + __import__("datetime").timedelta(seconds=30),
        )
    )
    repo.update_job(leased)
    repo.schedule_retry(job_id=job.job_id, available_at=job.available_at, error_message="boom")
    requeued = dispatcher.requeue_retry_wait_job(job.job_id)
    assert requeued.job.state == JobState.QUEUED
