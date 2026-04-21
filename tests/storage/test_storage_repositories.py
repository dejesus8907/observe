"""Focused tests for the enhanced storage layer."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from netobserv.storage.database import close_db, configure_storage, init_db, session_scope
from netobserv.storage.models import SnapshotRecord
from netobserv.storage.repository import ReplayRepository, SnapshotRepository, WorkflowRepository


@pytest.fixture
async def configured_storage(tmp_path):
    db_path = tmp_path / "storage_test.db"
    settings = SimpleNamespace(
        database_url=f"sqlite+aiosqlite:///{db_path}",
        database_echo=False,
        database_pool_size=5,
        database_max_overflow=10,
    )
    configure_storage(settings)
    await init_db()
    try:
        yield db_path
    finally:
        await close_db()


@pytest.mark.asyncio
async def test_workflow_repository_respects_explicit_workflow_id(configured_storage):
    async with session_scope() as session:
        repo = WorkflowRepository(session)
        record = await repo.create(
            "discovery",
            {"scope": "lab"},
            workflow_id="wf-123",
            status="running",
        )
        await session.flush()
        assert record.id == "wf-123"
        assert record.status == "running"
        assert record.started_at is not None


@pytest.mark.asyncio
async def test_snapshot_repository_supports_absolute_counts_and_failure(configured_storage):
    async with session_scope() as session:
        repo = SnapshotRepository(session)
        snapshot = await repo.create(workflow_id=None, scope="global")
        await repo.update_counts(snapshot.id, devices=2, interfaces=5)
        await repo.set_counts(snapshot.id, devices=10, edges=7, errors=1)
        updated = await repo.get(snapshot.id)
        assert updated is not None
        assert updated.device_count == 10
        assert updated.interface_count == 5
        assert updated.edge_count == 7
        assert updated.collection_errors == 1

        await repo.fail(snapshot.id, notes="connector timeout")
        failed = await repo.get(snapshot.id)
        assert failed is not None
        assert failed.status == "failed"
        assert failed.completed_at is not None
        assert failed.notes == "connector timeout"


@pytest.mark.asyncio
async def test_replay_repository_persists_and_completes_runs(configured_storage):
    async with session_scope() as session:
        repo = ReplayRepository(session)
        replay = await repo.create("wf-source", notes="smoke")
        assert replay.status == "pending"

        await repo.complete(
            replay.id,
            status="completed",
            diff_summary={"changed": 0},
            passed=True,
        )
        completed = await repo.get(replay.id)
        assert completed is not None
        assert completed.status == "completed"
        assert completed.passed is True
        assert completed.diff_summary == {"changed": 0}
        assert completed.completed_at is not None
