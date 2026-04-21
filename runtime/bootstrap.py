"""Runtime bootstrap helpers."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator

from sqlalchemy.orm import Session

from netobserv.config.settings import Settings
from netobserv.runtime.dispatcher import RuntimeDispatcher
from netobserv.runtime.recovery import RuntimeRecovery
from netobserv.runtime.scheduler import RuntimeScheduler
from netobserv.runtime.service import RuntimeService
from netobserv.runtime.sql_repository import SQLRuntimeRepository
from netobserv.runtime.worker import RuntimeWorker, WorkerConfig
from netobserv.storage.database import RuntimeSessionLocal


@contextmanager
def runtime_session_scope() -> Iterator[Session]:
    if RuntimeSessionLocal is None:
        raise RuntimeError("RuntimeSessionLocal is not configured")
    session = RuntimeSessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@dataclass(slots=True)
class RuntimeComponents:
    repository: SQLRuntimeRepository
    dispatcher: RuntimeDispatcher
    recovery: RuntimeRecovery
    scheduler: RuntimeScheduler
    worker: RuntimeWorker
    service: RuntimeService


def build_runtime_components(settings: Settings) -> RuntimeComponents:
    repository = SQLRuntimeRepository(runtime_session_scope)
    dispatcher = RuntimeDispatcher(
        repository,
        allowed_queues=[settings.runtime_default_queue],
        default_queue=settings.runtime_default_queue,
    )
    recovery = RuntimeRecovery(repository)
    scheduler = RuntimeScheduler(repository, dispatcher, recovery)
    worker = RuntimeWorker(
        repository,
        WorkerConfig(
            worker_id=settings.runtime_worker_id,
            queue_name=settings.runtime_default_queue,
            lease_ttl_seconds=settings.runtime_lease_ttl_seconds,
            heartbeat_interval_seconds=settings.runtime_heartbeat_interval_seconds,
            max_jobs_per_run=settings.runtime_max_jobs_per_run,
        ),
        stage_executors={},
    )
    service = RuntimeService(
        worker,
        scheduler,
        worker_id=settings.runtime_worker_id,
        queue_name=settings.runtime_default_queue,
    )
    return RuntimeComponents(
        repository=repository,
        dispatcher=dispatcher,
        recovery=recovery,
        scheduler=scheduler,
        worker=worker,
        service=service,
    )
