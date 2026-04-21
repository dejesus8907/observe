from datetime import timedelta

from netobserv.runtime.dispatcher import DispatchRequest, RuntimeDispatcher
from netobserv.runtime.models import JobState, Lease, StageRecord, StageSpec, StageState, utc_now, ResumabilityMode, IdempotencyMode
from netobserv.runtime.recovery import RecoveryPolicy, RuntimeRecovery
from netobserv.runtime.repository import InMemoryRuntimeRepository


def stage_specs(resumability=ResumabilityMode.RESTART_ONLY, idempotency=IdempotencyMode.IDEMPOTENT):
    return [StageSpec(name="collect", order=0, resumability=resumability, idempotency=idempotency)]


def test_recovery_requeues_expired_leased_job():
    repo = InMemoryRuntimeRepository()
    dispatcher = RuntimeDispatcher(repo)
    decision = dispatcher.submit(DispatchRequest(workflow_id="wf-1", job_type="discovery", stage_specs=stage_specs()))
    job = repo.require_job(decision.job.job_id)
    now = utc_now()
    leased = job.with_lease(Lease(worker_id="worker-1", acquired_at=now, heartbeat_at=now, expires_at=now))
    repo.update_job(leased)

    recovery = RuntimeRecovery(repo)
    result = recovery.run_once(RecoveryPolicy(), now=now)
    assert result.requeued_jobs == 1
    assert repo.require_job(job.job_id).state == JobState.QUEUED


def test_recovery_fails_non_resumable_interrupted_running_stage():
    repo = InMemoryRuntimeRepository()
    dispatcher = RuntimeDispatcher(repo)
    decision = dispatcher.submit(
        DispatchRequest(
            workflow_id="wf-1",
            job_type="discovery",
            stage_specs=stage_specs(resumability=ResumabilityMode.NON_RESUMABLE),
        )
    )
    job = repo.require_job(decision.job.job_id)
    now = utc_now()
    leased = job.with_lease(Lease(worker_id="worker-1", acquired_at=now, heartbeat_at=now, expires_at=now))
    running = repo.update_job(leased).transition(JobState.RUNNING, now=now)
    running = running.model_copy(
        update={
            "stage_records": [
                StageRecord(
                    stage_name="collect",
                    stage_order=0,
                    state=StageState.RUNNING,
                    attempt_number=1,
                    started_at=now,
                    worker_id="worker-1",
                    lease_token=running.lease.lease_token if running.lease else None,
                )
            ]
        }
    )
    repo.update_job(running)

    recovery = RuntimeRecovery(repo)
    result = recovery.run_once(RecoveryPolicy(), now=now)
    assert result.failed_jobs == 1
    assert repo.require_job(job.job_id).state == JobState.FAILED
