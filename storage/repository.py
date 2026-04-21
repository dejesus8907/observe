"""Repository pattern — higher-level query/persistence helpers wrapping SQLAlchemy."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Sequence

from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from netobserv.storage.models import (
    SnapshotRecord,
    DeviceRecord,
    TopologyEdgeRecord,
    TopologyConflictRecord,
    DriftRecord,
    WorkflowRecord,
    WorkflowStepRecord,
    SyncPlanRecord,
    SyncActionRecord,
    SyncAuditRecord,
    ReplayRecord,
)


class BaseRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session


class WorkflowRepository(BaseRepository):
    async def create(self, workflow_type: str, metadata: dict[str, Any]) -> WorkflowRecord:
        record = WorkflowRecord(
            workflow_type=workflow_type,
            status="running",
            metadata_=metadata,
            started_at=datetime.utcnow(),
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def get(self, workflow_id: str) -> WorkflowRecord | None:
        result = await self.session.execute(
            select(WorkflowRecord)
            .options(selectinload(WorkflowRecord.steps))
            .where(WorkflowRecord.id == workflow_id)
        )
        return result.scalar_one_or_none()

    async def update_status(
        self, workflow_id: str, status: str, error: str | None = None
    ) -> None:
        record = await self.get(workflow_id)
        if record:
            record.status = status
            if status in ("completed", "failed", "cancelled"):
                record.completed_at = datetime.utcnow()
            if error:
                record.error_message = error
            await self.session.flush()

    async def add_step(
        self,
        workflow_id: str,
        stage_name: str,
        status: str,
        artifacts: dict[str, Any] | None = None,
        errors: list[Any] | None = None,
        warnings: list[Any] | None = None,
        elapsed_seconds: float | None = None,
    ) -> WorkflowStepRecord:
        step = WorkflowStepRecord(
            workflow_id=workflow_id,
            stage_name=stage_name,
            status=status,
            artifacts=artifacts or {},
            errors=errors or [],
            warnings=warnings or [],
            elapsed_seconds=elapsed_seconds,
            completed_at=datetime.utcnow(),
        )
        self.session.add(step)
        await self.session.flush()
        return step

    async def list_recent(self, limit: int = 50) -> Sequence[WorkflowRecord]:
        result = await self.session.execute(
            select(WorkflowRecord)
            .order_by(WorkflowRecord.created_at.desc())
            .limit(limit)
        )
        return result.scalars().all()


class SnapshotRepository(BaseRepository):
    async def create(self, workflow_id: str, scope: str = "global") -> SnapshotRecord:
        record = SnapshotRecord(workflow_id=workflow_id, scope=scope)
        self.session.add(record)
        await self.session.flush()
        return record

    async def get(self, snapshot_id: str) -> SnapshotRecord | None:
        result = await self.session.execute(
            select(SnapshotRecord).where(SnapshotRecord.id == snapshot_id)
        )
        return result.scalar_one_or_none()

    async def complete(self, snapshot_id: str) -> None:
        record = await self.get(snapshot_id)
        if record:
            record.status = "completed"
            record.completed_at = datetime.utcnow()
            await self.session.flush()

    async def update_counts(
        self,
        snapshot_id: str,
        *,
        devices: int = 0,
        interfaces: int = 0,
        edges: int = 0,
        drift: int = 0,
        errors: int = 0,
    ) -> None:
        record = await self.get(snapshot_id)
        if record:
            record.device_count += devices
            record.interface_count += interfaces
            record.edge_count += edges
            record.drift_count += drift
            record.collection_errors += errors
            await self.session.flush()

    async def list_recent(
        self,
        limit: int = 20,
        scope: str | None = None,
        since: datetime | None = None,
    ) -> Sequence[SnapshotRecord]:
        q = select(SnapshotRecord).order_by(SnapshotRecord.started_at.desc())
        if scope:
            q = q.where(SnapshotRecord.scope == scope)
        if since:
            q = q.where(SnapshotRecord.started_at >= since)
        result = await self.session.execute(q.limit(limit))
        return result.scalars().all()


class DeviceRepository(BaseRepository):
    async def upsert_bulk(self, devices: list[DeviceRecord]) -> None:
        for d in devices:
            self.session.add(d)
        await self.session.flush()

    async def get_by_snapshot(self, snapshot_id: str) -> Sequence[DeviceRecord]:
        result = await self.session.execute(
            select(DeviceRecord).where(DeviceRecord.snapshot_id == snapshot_id)
        )
        return result.scalars().all()

    async def get_by_hostname(
        self, hostname: str, snapshot_id: str | None = None
    ) -> Sequence[DeviceRecord]:
        q = select(DeviceRecord).where(DeviceRecord.hostname == hostname)
        if snapshot_id:
            q = q.where(DeviceRecord.snapshot_id == snapshot_id)
        result = await self.session.execute(q)
        return result.scalars().all()


class TopologyRepository(BaseRepository):
    async def save_edges(self, edges: list[TopologyEdgeRecord]) -> None:
        for e in edges:
            self.session.add(e)
        await self.session.flush()

    async def get_edges_for_snapshot(
        self, snapshot_id: str, edge_type: str | None = None
    ) -> Sequence[TopologyEdgeRecord]:
        q = select(TopologyEdgeRecord).where(
            TopologyEdgeRecord.snapshot_id == snapshot_id
        )
        if edge_type:
            q = q.where(TopologyEdgeRecord.edge_type == edge_type)
        result = await self.session.execute(q)
        return result.scalars().all()

    async def get_neighbors(
        self, device_id: str, snapshot_id: str | None = None
    ) -> Sequence[TopologyEdgeRecord]:
        q = select(TopologyEdgeRecord).where(
            (TopologyEdgeRecord.source_node_id == device_id)
            | (TopologyEdgeRecord.target_node_id == device_id)
        )
        if snapshot_id:
            q = q.where(TopologyEdgeRecord.snapshot_id == snapshot_id)
        result = await self.session.execute(q)
        return result.scalars().all()

    async def save_conflicts(self, conflicts: list[TopologyConflictRecord]) -> None:
        for c in conflicts:
            self.session.add(c)
        await self.session.flush()

    async def get_conflicts(
        self, snapshot_id: str
    ) -> Sequence[TopologyConflictRecord]:
        result = await self.session.execute(
            select(TopologyConflictRecord).where(
                TopologyConflictRecord.snapshot_id == snapshot_id
            )
        )
        return result.scalars().all()


class DriftRepository(BaseRepository):
    async def save_bulk(self, records: list[DriftRecord]) -> None:
        for r in records:
            self.session.add(r)
        await self.session.flush()

    async def get_by_snapshot(
        self, snapshot_id: str, severity: str | None = None
    ) -> Sequence[DriftRecord]:
        q = select(DriftRecord).where(DriftRecord.snapshot_id == snapshot_id)
        if severity:
            q = q.where(DriftRecord.severity == severity)
        result = await self.session.execute(
            q.order_by(DriftRecord.created_at.desc())
        )
        return result.scalars().all()

    async def count_by_type(self, snapshot_id: str) -> dict[str, int]:
        result = await self.session.execute(
            select(DriftRecord.mismatch_type, func.count())
            .where(DriftRecord.snapshot_id == snapshot_id)
            .group_by(DriftRecord.mismatch_type)
        )
        return dict(result.all())  # type: ignore[arg-type]


class SyncRepository(BaseRepository):
    async def create_plan(
        self,
        workflow_id: str | None,
        snapshot_id: str | None,
        mode: str,
        policy: dict[str, Any],
    ) -> SyncPlanRecord:
        plan = SyncPlanRecord(
            workflow_id=workflow_id,
            snapshot_id=snapshot_id,
            mode=mode,
            policy=policy,
        )
        self.session.add(plan)
        await self.session.flush()
        return plan

    async def get_plan(self, plan_id: str) -> SyncPlanRecord | None:
        result = await self.session.execute(
            select(SyncPlanRecord)
            .options(selectinload(SyncPlanRecord.actions))
            .where(SyncPlanRecord.id == plan_id)
        )
        return result.scalar_one_or_none()

    async def save_actions(self, actions: list[SyncActionRecord]) -> None:
        for a in actions:
            self.session.add(a)
        await self.session.flush()

    async def log_audit(self, audit: SyncAuditRecord) -> None:
        self.session.add(audit)
        await self.session.flush()
