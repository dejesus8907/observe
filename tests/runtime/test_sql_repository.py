from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from netobserv.runtime.db_models import RuntimeBase
from netobserv.runtime.dispatcher import DispatchRequest, RuntimeDispatcher
from netobserv.runtime.models import JobState, Lease, StageRecord, StageSpec, StageState, utc_now
from netobserv.runtime.sql_repository import SQLRuntimeRepository


@contextmanager
def session_factory(engine):
    session = Session(engine)
    try:
        yield session
    finally:
        session.close()


def make_repo():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    RuntimeBase.metadata.create_all(engine)
    return SQLRuntimeRepository(lambda: session_factory(engine))


def test_create_and_get_job_round_trip():
    repo = make_repo()
    dispatcher = RuntimeDispatcher(repo)
    decision = dispatcher.submit(
        DispatchRequest(
            workflow_id="wf-1",
            job_type="discovery",
            stage_specs=[StageSpec(name="collect", order=0)],
        )
    )
    job = repo.get_job(decision.job.job_id)
    assert job is not None
    assert job.job_id == decision.job.job_id
    assert job.state == JobState.QUEUED


def test_acquire_and_renew_lease():
    repo = make_repo()
    dispatcher = RuntimeDispatcher(repo)
    decision = dispatcher.submit(
        DispatchRequest(
            workflow_id="wf-1",
            job_type="discovery",
            stage_specs=[StageSpec(name="collect", order=0)],
        )
    )
    now = utc_now()
    lease = Lease(worker_id="worker-1", acquired_at=now, heartbeat_at=now, expires_at=now.replace() + __import__("datetime").timedelta(seconds=30))
    leased = repo.acquire_next_lease(worker_id="worker-1", queue_name="default", lease=lease, now=now)
    assert leased is not None
    assert leased.state == JobState.LEASED

    renewed = repo.renew_lease(
        job_id=leased.job_id,
        worker_id="worker-1",
        lease_token=leased.lease.lease_token,
        ttl_seconds=30,
        now=now,
    )
    assert renewed.lease is not None
    assert renewed.lease.worker_id == "worker-1"


def test_append_and_replace_stage_record():
    repo = make_repo()
    dispatcher = RuntimeDispatcher(repo)
    decision = dispatcher.submit(
        DispatchRequest(
            workflow_id="wf-1",
            job_type="discovery",
            stage_specs=[StageSpec(name="collect", order=0)],
        )
    )
    stage = StageRecord(stage_name="collect", stage_order=0, state=StageState.RUNNING, attempt_number=1)
    job = repo.append_stage_record(decision.job.job_id, stage)
    assert len(job.stage_records) == 1

    finished = stage.model_copy(update={"state": StageState.SUCCEEDED})
    job = repo.replace_stage_record(
        job_id=decision.job.job_id,
        stage_name="collect",
        attempt_number=1,
        record=finished,
    )
    assert job.stage_records[0].state == StageState.SUCCEEDED
