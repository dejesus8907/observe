import pytest
from datetime import timedelta

from netobserv.runtime.models import (
    FailureCategory,
    IdempotencyMode,
    JobRecord,
    JobState,
    Lease,
    RetryPolicy,
    StageRecord,
    StageSpec,
    StageState,
    utc_now,
)


def make_stage_specs():
    return [
        StageSpec(name="collect", order=0),
        StageSpec(name="normalize", order=1),
    ]


def test_job_transition_to_running_sets_started_at():
    now = utc_now()
    job = JobRecord(workflow_id="wf-1", job_type="discovery", stage_specs=make_stage_specs())
    updated = job.transition(JobState.LEASED, now=now).transition(JobState.RUNNING, now=now)
    assert updated.state == JobState.RUNNING
    assert updated.started_at == now


def test_terminal_transition_sets_completed_at_and_clears_lease():
    now = utc_now()
    lease = Lease(worker_id="worker-1", expires_at=now + timedelta(seconds=30))
    job = JobRecord(
        workflow_id="wf-1",
        job_type="discovery",
        stage_specs=make_stage_specs(),
        state=JobState.LEASED,
        lease=lease,
    )
    updated = job.transition(JobState.TIMED_OUT, now=now)
    assert updated.state == JobState.TIMED_OUT
    assert updated.completed_at == now
    assert updated.lease is None


def test_invalid_job_transition_raises():
    job = JobRecord(workflow_id="wf-1", job_type="discovery", stage_specs=make_stage_specs())
    with pytest.raises(ValueError):
        job.transition(JobState.SUCCEEDED)


def test_retry_policy_delay_progression_and_cap():
    policy = RetryPolicy(max_attempts=5, base_delay_seconds=5, max_delay_seconds=20, backoff_multiplier=2.0)
    assert policy.nominal_delay_for_attempt(2) == 5
    assert policy.nominal_delay_for_attempt(3) == 10
    assert policy.nominal_delay_for_attempt(4) == 20
    assert policy.nominal_delay_for_attempt(5) == 20


def test_retry_policy_allows_specific_failure_categories():
    policy = RetryPolicy(
        max_attempts=3,
        retryable_failures=[FailureCategory.TRANSIENT_SYSTEM, FailureCategory.PERSISTENCE],
    )
    assert policy.allows_retry_for(FailureCategory.TRANSIENT_SYSTEM, 1) is True
    assert policy.allows_retry_for(FailureCategory.PERSISTENCE, 2) is True
    assert policy.allows_retry_for(FailureCategory.INPUT, 1) is False
    assert policy.allows_retry_for(FailureCategory.TRANSIENT_SYSTEM, 3) is False


def test_next_stage_spec_skips_succeeded_and_skipped():
    specs = make_stage_specs()
    job = JobRecord(
        workflow_id="wf-1",
        job_type="discovery",
        stage_specs=specs,
        stage_records=[
            StageRecord(stage_name="collect", stage_order=0, state=StageState.SUCCEEDED, attempt_number=1),
        ],
    )
    next_stage = job.next_stage_spec()
    assert next_stage is not None
    assert next_stage.name == "normalize"


def test_stage_record_temporal_validation():
    now = utc_now()
    with pytest.raises(ValueError):
        StageRecord(
            stage_name="collect",
            stage_order=0,
            state=StageState.FAILED,
            attempt_number=1,
            started_at=now,
            completed_at=now - timedelta(seconds=1),
        )


def test_job_requires_valid_unique_stage_spec_order_and_name():
    with pytest.raises(ValueError):
        JobRecord(
            workflow_id="wf-1",
            job_type="discovery",
            stage_specs=[StageSpec(name="a", order=0), StageSpec(name="a", order=1)],
        )
    with pytest.raises(ValueError):
        JobRecord(
            workflow_id="wf-1",
            job_type="discovery",
            stage_specs=[StageSpec(name="a", order=0), StageSpec(name="b", order=0)],
        )
