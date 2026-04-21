"""Snapshot API endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from netobserv.api.common import clamp_limit, require_resource, serialize_datetime
from netobserv.storage.database import get_db_session
from netobserv.storage.repository import DeviceRepository, SnapshotRepository, TopologyRepository

router = APIRouter()


class SnapshotSummary(BaseModel):
    id: str
    workflow_id: str
    scope: str
    status: str
    started_at: str | None = None
    completed_at: str | None = None
    device_count: int
    interface_count: int
    edge_count: int
    drift_count: int


class SnapshotListResponse(BaseModel):
    snapshots: list[SnapshotSummary] = Field(default_factory=list)
    total: int


class SnapshotDetailResponse(SnapshotSummary):
    collection_errors: int
    notes: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class DeviceSummary(BaseModel):
    id: str
    hostname: str
    management_ip: str | None = None
    vendor: str | None = None
    model: str | None = None
    device_role: str | None = None
    site: str | None = None
    status: str | None = None


class SnapshotDevicesResponse(BaseModel):
    snapshot_id: str
    devices: list[DeviceSummary] = Field(default_factory=list)
    total: int


class SnapshotTopologyEdge(BaseModel):
    id: str
    source_node_id: str
    target_node_id: str
    source_interface: str | None = None
    target_interface: str | None = None
    edge_type: str
    confidence: str | None = None
    confidence_score: float
    evidence_source: str | None = None


class SnapshotTopologyResponse(BaseModel):
    snapshot_id: str
    edges: list[SnapshotTopologyEdge] = Field(default_factory=list)
    total: int


@router.get("", response_model=SnapshotListResponse)
async def list_snapshots(
    limit: int = 20,
    scope: str | None = None,
    db: AsyncSession = Depends(get_db_session),
) -> SnapshotListResponse:
    """List recent snapshots with bounded limits."""
    repo = SnapshotRepository(db)
    snapshots = await repo.list_recent(limit=clamp_limit(limit), scope=scope)
    return SnapshotListResponse(
        snapshots=[
            SnapshotSummary(
                id=s.id,
                workflow_id=s.workflow_id,
                scope=s.scope,
                status=s.status,
                started_at=serialize_datetime(s.started_at),
                completed_at=serialize_datetime(s.completed_at),
                device_count=s.device_count,
                interface_count=s.interface_count,
                edge_count=s.edge_count,
                drift_count=s.drift_count,
            )
            for s in snapshots
        ],
        total=len(snapshots),
    )


@router.get("/{snapshot_id}", response_model=SnapshotDetailResponse)
async def get_snapshot(
    snapshot_id: str,
    db: AsyncSession = Depends(get_db_session),
) -> SnapshotDetailResponse:
    """Get full snapshot details."""
    repo = SnapshotRepository(db)
    snapshot = require_resource(await repo.get(snapshot_id), "Snapshot not found")
    return SnapshotDetailResponse(
        id=snapshot.id,
        workflow_id=snapshot.workflow_id,
        scope=snapshot.scope,
        status=snapshot.status,
        started_at=serialize_datetime(snapshot.started_at),
        completed_at=serialize_datetime(snapshot.completed_at),
        device_count=snapshot.device_count,
        interface_count=snapshot.interface_count,
        edge_count=snapshot.edge_count,
        drift_count=snapshot.drift_count,
        collection_errors=snapshot.collection_errors,
        notes=snapshot.notes,
        metadata=snapshot.snapshot_metadata or {},
    )


@router.get("/{snapshot_id}/devices", response_model=SnapshotDevicesResponse)
async def get_snapshot_devices(
    snapshot_id: str,
    db: AsyncSession = Depends(get_db_session),
) -> SnapshotDevicesResponse:
    """List devices stored in a snapshot."""
    snap_repo = SnapshotRepository(db)
    require_resource(await snap_repo.get(snapshot_id), "Snapshot not found")

    repo = DeviceRepository(db)
    devices = await repo.get_by_snapshot(snapshot_id)
    return SnapshotDevicesResponse(
        snapshot_id=snapshot_id,
        devices=[
            DeviceSummary(
                id=d.id,
                hostname=d.hostname,
                management_ip=d.management_ip,
                vendor=d.vendor,
                model=d.model,
                device_role=d.device_role,
                site=d.site,
                status=d.status,
            )
            for d in devices
        ],
        total=len(devices),
    )


@router.get("/{snapshot_id}/topology", response_model=SnapshotTopologyResponse)
async def get_snapshot_topology(
    snapshot_id: str,
    db: AsyncSession = Depends(get_db_session),
) -> SnapshotTopologyResponse:
    """Get topology edges associated with a snapshot."""
    snap_repo = SnapshotRepository(db)
    require_resource(await snap_repo.get(snapshot_id), "Snapshot not found")

    repo = TopologyRepository(db)
    edges = await repo.get_edges_for_snapshot(snapshot_id)
    return SnapshotTopologyResponse(
        snapshot_id=snapshot_id,
        edges=[
            SnapshotTopologyEdge(
                id=e.id,
                source_node_id=e.source_node_id,
                target_node_id=e.target_node_id,
                source_interface=e.source_interface,
                target_interface=e.target_interface,
                edge_type=e.edge_type,
                confidence=e.confidence,
                confidence_score=e.confidence_score,
                evidence_source=e.evidence_source,
            )
            for e in edges
        ],
        total=len(edges),
    )
