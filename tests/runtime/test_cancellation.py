import pytest

from netobserv.runtime.cancellation import (
    attach_cancellation_reason,
    cancellation_note,
    evaluate_job_cancellation,
    mark_job_cancelled,
    mark_stage_cancelled,
    require_job_not_cancelled,
)
from netobserv.runtime.models import JobRecord, JobState, StageRecord, StageState, StageSpec


def make_job():
    return JobRecord(
        workflow_id="wf-1",
        job_type="discovery",
        stage_specs=[StageSpec(name="collect", order=0)],
    )


def test_evaluate_job_cancellation_detects_flag_and_reason():
    job = attach_cancellation_reason(make_job().request_cancellation(), "operator requested")
    status = evaluate_job_cancellation(job)
    assert status.cancellation_requested is True
    assert status.reason == "operator requested"


def test_require_job_not_cancelled_raises():
    job = make_job().request_cancellation()
    with pytest.raises(Exception):
        require_job_not_cancelled(job, worker_id="worker-1")


def test_mark_job_cancelled_sets_terminal_state():
    job = make_job()
    updated = mark_job_cancelled(job, error_message="cancelled")
    assert updated.state == JobState.CANCELLED
    assert updated.error_message == "cancelled"


def test_mark_stage_cancelled_sets_terminal_state():
    stage = StageRecord(stage_name="collect", stage_order=0, state=StageState.RUNNING, attempt_number=1)
    updated = mark_stage_cancelled(stage, error_message="cancelled")
    assert updated.state == StageState.CANCELLED
    assert updated.error_message == "cancelled"


def test_cancellation_note_returns_human_summary():
    job = attach_cancellation_reason(make_job().request_cancellation(), "manual stop")
    assert cancellation_note(job) == "cancellation requested: manual stop"
