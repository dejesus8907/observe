from datetime import timedelta

import pytest

from netobserv.runtime.models import JobRecord, JobState, StageRecord, StageState, StageSpec, utc_now
from netobserv.runtime.timeouts import (
    evaluate_job_timeout,
    evaluate_stage_timeout,
    mark_job_timed_out,
    mark_stage_timed_out,
    require_job_not_timed_out,
    require_stage_not_timed_out,
)


def make_job():
    now = utc_now()
    return JobRecord(
        workflow_id="wf-1",
        job_type="discovery",
        stage_specs=[StageSpec(name="collect", order=0)],
        started_at=now,
        timeout_seconds=5,
    )


def test_evaluate_job_timeout_detects_expiry():
    job = make_job()
    status = evaluate_job_timeout(job, now=job.started_at + timedelta(seconds=6))
    assert status.is_timed_out is True
    assert status.exceeded_by_seconds >= 1


def test_require_job_not_timed_out_raises():
    job = make_job()
    with pytest.raises(Exception):
        require_job_not_timed_out(job, now=job.started_at + timedelta(seconds=10))


def test_mark_job_timed_out_sets_terminal_state():
    job = make_job()
    updated = mark_job_timed_out(job, now=job.started_at + timedelta(seconds=6), error_message="timeout")
    assert updated.state == JobState.TIMED_OUT
    assert updated.completed_at is not None
    assert updated.error_message == "timeout"


def test_stage_timeout_helpers():
    now = utc_now()
    stage = StageRecord(
        stage_name="collect",
        stage_order=0,
        state=StageState.RUNNING,
        attempt_number=1,
        started_at=now,
    )
    status = evaluate_stage_timeout(stage, timeout_seconds=2, now=now + timedelta(seconds=3))
    assert status.is_timed_out is True

    updated = mark_stage_timed_out(stage, now=now + timedelta(seconds=3), error_message="stage timeout")
    assert updated.state == StageState.TIMED_OUT
    assert updated.error_message == "stage timeout"

    with pytest.raises(Exception):
        require_stage_not_timed_out(stage, timeout_seconds=2, now=now + timedelta(seconds=3))
