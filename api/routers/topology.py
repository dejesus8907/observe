"""Topology API endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from netobserv.api.common import require_resource, resolve_latest_snapshot_id
from netobserv.storage.database import get_db_session
from netobserv.storage.repository import DeviceRepository, SnapshotRepository, TopologyRepository

router = APIRouter()


class EdgeView(BaseModel):
    id: str
    source: str
    target: str
    source_interface: str | None = None
    target_interface: str | None = None
    type: str
    confidence: float


class TopologyResponse(BaseModel):
    snapshot_id: str
    edges: list[EdgeView] = Field(default_factory=list)
    total_edges: int


class ConflictView(BaseModel):
    id: str
    type: str
    description: str
    severity: str
    evidence: list[dict[str, Any]] | dict[str, Any] | None = None


class TopologyConflictsResponse(BaseModel):
    snapshot_id: str
    conflicts: list[ConflictView] = Field(default_factory=list)
    total: int


class NeighborView(BaseModel):
    edge_id: str
    peer_id: str
    local_interface: str | None = None
    peer_interface: str | None = None
    edge_type: str
    confidence_score: float


class NeighborsResponse(BaseModel):
    device_id: str
    neighbors: list[NeighborView] = Field(default_factory=list)
    total: int


class PathResponse(BaseModel):
    source: str
    target: str
    path: list[str] | None = None
    hops: int | None = None
    found: bool


@router.get("", response_model=TopologyResponse)
async def get_topology(
    snapshot_id: str | None = Query(None, description="Snapshot ID; uses latest if not provided"),
    db: AsyncSession = Depends(get_db_session),
) -> TopologyResponse:
    """Get the latest topology or a topology for a specific snapshot."""
    snap_repo = SnapshotRepository(db)
    topo_repo = TopologyRepository(db)

    snapshot_id = snapshot_id or await resolve_latest_snapshot_id(snap_repo)
    if not snapshot_id:
        raise HTTPException(status_code=404, detail="No snapshots found")

    edges = await topo_repo.get_edges_for_snapshot(snapshot_id)
    return TopologyResponse(
        snapshot_id=snapshot_id,
        edges=[
            EdgeView(
                id=e.id,
                source=e.source_node_id,
                target=e.target_node_id,
                source_interface=e.source_interface,
                target_interface=e.target_interface,
                type=e.edge_type,
                confidence=e.confidence_score,
            )
            for e in edges
        ],
        total_edges=len(edges),
    )


@router.get("/edges")
async def list_topology_edges(
    snapshot_id: str | None = Query(None),
    edge_type: str | None = Query(None),
    db: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    """List topology edges, optionally filtered by type."""
    snap_repo = SnapshotRepository(db)
    topo_repo = TopologyRepository(db)

    snapshot_id = snapshot_id or await resolve_latest_snapshot_id(snap_repo)
    if not snapshot_id:
        return {"edges": [], "total": 0}

    edges = await topo_repo.get_edges_for_snapshot(snapshot_id, edge_type=edge_type)
    return {
        "snapshot_id": snapshot_id,
        "edges": [
            {
                "id": e.id,
                "source_node_id": e.source_node_id,
                "target_node_id": e.target_node_id,
                "edge_type": e.edge_type,
                "confidence_score": e.confidence_score,
                "is_direct": e.is_direct,
                "evidence_source": e.evidence_source,
                "conflict_flags": e.conflict_flags,
            }
            for e in edges
        ],
        "total": len(edges),
    }


@router.get("/conflicts", response_model=TopologyConflictsResponse)
async def list_topology_conflicts(
    snapshot_id: str | None = Query(None),
    db: AsyncSession = Depends(get_db_session),
) -> TopologyConflictsResponse:
    """List detected topology conflicts."""
    snap_repo = SnapshotRepository(db)
    topo_repo = TopologyRepository(db)

    snapshot_id = snapshot_id or await resolve_latest_snapshot_id(snap_repo)
    if not snapshot_id:
        return TopologyConflictsResponse(snapshot_id="", conflicts=[], total=0)

    conflicts = await topo_repo.get_conflicts(snapshot_id)
    return TopologyConflictsResponse(
        snapshot_id=snapshot_id,
        conflicts=[
            ConflictView(
                id=c.id,
                type=c.conflict_type,
                description=c.description,
                severity=c.severity,
                evidence=c.evidence,
            )
            for c in conflicts
        ],
        total=len(conflicts),
    )


@router.get("/neighbors/{device_id}", response_model=NeighborsResponse)
async def get_device_neighbors(
    device_id: str,
    snapshot_id: str | None = Query(None),
    db: AsyncSession = Depends(get_db_session),
) -> NeighborsResponse:
    """Get topology neighbors for a specific device."""
    topo_repo = TopologyRepository(db)
    edges = await topo_repo.get_neighbors(device_id, snapshot_id=snapshot_id)
    return NeighborsResponse(
        device_id=device_id,
        neighbors=[
            NeighborView(
                edge_id=e.id,
                peer_id=e.target_node_id if e.source_node_id == device_id else e.source_node_id,
                local_interface=e.source_interface if e.source_node_id == device_id else e.target_interface,
                peer_interface=e.target_interface if e.source_node_id == device_id else e.source_interface,
                edge_type=e.edge_type,
                confidence_score=e.confidence_score,
            )
            for e in edges
        ],
        total=len(edges),
    )


@router.get("/path", response_model=PathResponse)
async def find_topology_path(
    source: str = Query(...),
    target: str = Query(...),
    snapshot_id: str | None = Query(None),
    db: AsyncSession = Depends(get_db_session),
) -> PathResponse:
    """Find the shortest path between two devices."""
    snap_repo = SnapshotRepository(db)
    topo_repo = TopologyRepository(db)
    device_repo = DeviceRepository(db)

    snapshot_id = snapshot_id or await resolve_latest_snapshot_id(snap_repo)
    if not snapshot_id:
        raise HTTPException(status_code=404, detail="No snapshots available")

    from netobserv.models.canonical import CanonicalDevice, CanonicalTopologyEdge
    from netobserv.topology.graph import TopologyGraph

    devices = await device_repo.get_by_snapshot(snapshot_id)
    canonical_devices = [
        CanonicalDevice(
            device_id=d.id,
            hostname=d.hostname,
            site=d.site,
            status=d.status,  # type: ignore[arg-type]
        )
        for d in devices
    ]

    edge_records = await topo_repo.get_edges_for_snapshot(snapshot_id)
    canonical_edges = [
        CanonicalTopologyEdge(
            edge_id=e.id,
            source_node_id=e.source_node_id,
            target_node_id=e.target_node_id,
            edge_type=e.edge_type,  # type: ignore[arg-type]
            confidence_score=e.confidence_score,
        )
        for e in edge_records
    ]

    graph = TopologyGraph(canonical_devices, canonical_edges)
    path = graph.find_path(source, target)

    return PathResponse(
        source=source,
        target=target,
        path=path,
        hops=len(path) - 1 if path else None,
        found=path is not None,
    )
