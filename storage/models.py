"""SQLAlchemy ORM models — the persistence layer for all NetObserv data."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    JSON,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from netobserv.storage.database import Base


def _uuid() -> str:
    return str(uuid.uuid4())


def _now() -> datetime:
    return datetime.utcnow()


# ---------------------------------------------------------------------------
# Workflow
# ---------------------------------------------------------------------------


class WorkflowRecord(Base):
    __tablename__ = "workflows"
    __table_args__ = (
        Index("ix_workflows_workflow_type", "workflow_type"),
        Index("ix_workflows_status", "status"),
        Index("ix_workflows_created_at", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    workflow_type: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(32), default="pending")
    metadata_: Mapped[dict[str, Any]] = mapped_column("metadata", JSON, default=dict)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    steps: Mapped[list[WorkflowStepRecord]] = relationship(
        "WorkflowStepRecord", back_populates="workflow", cascade="all, delete-orphan"
    )
    snapshots: Mapped[list[SnapshotRecord]] = relationship(
        "SnapshotRecord", back_populates="workflow"
    )


class WorkflowStepRecord(Base):
    __tablename__ = "workflow_steps"
    __table_args__ = (Index("ix_workflow_steps_workflow_id", "workflow_id"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    workflow_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("workflows.id", ondelete="CASCADE")
    )
    stage_name: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(32), default="pending")
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    elapsed_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    artifacts: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    errors: Mapped[list[Any]] = mapped_column(JSON, default=list)
    warnings: Mapped[list[Any]] = mapped_column(JSON, default=list)

    workflow: Mapped[WorkflowRecord] = relationship(
        "WorkflowRecord", back_populates="steps"
    )


# ---------------------------------------------------------------------------
# Snapshot
# ---------------------------------------------------------------------------


class SnapshotRecord(Base):
    __tablename__ = "snapshots"
    __table_args__ = (
        Index("ix_snapshots_workflow_id", "workflow_id"),
        Index("ix_snapshots_started_at", "started_at"),
        Index("ix_snapshots_scope", "scope"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    workflow_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("workflows.id", ondelete="SET NULL"), nullable=True
    )
    scope: Mapped[str] = mapped_column(String(256), default="global")
    status: Mapped[str] = mapped_column(String(32), default="in_progress")
    started_at: Mapped[datetime] = mapped_column(DateTime, default=_now)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    device_count: Mapped[int] = mapped_column(Integer, default=0)
    interface_count: Mapped[int] = mapped_column(Integer, default=0)
    edge_count: Mapped[int] = mapped_column(Integer, default=0)
    drift_count: Mapped[int] = mapped_column(Integer, default=0)
    collection_errors: Mapped[int] = mapped_column(Integer, default=0)
    notes: Mapped[str] = mapped_column(Text, default="")
    snapshot_metadata: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)

    workflow: Mapped[WorkflowRecord | None] = relationship(
        "WorkflowRecord", back_populates="snapshots"
    )
    devices: Mapped[list[DeviceRecord]] = relationship(
        "DeviceRecord", back_populates="snapshot", cascade="all, delete-orphan"
    )
    topology_edges: Mapped[list[TopologyEdgeRecord]] = relationship(
        "TopologyEdgeRecord", back_populates="snapshot", cascade="all, delete-orphan"
    )
    drift_records: Mapped[list[DriftRecord]] = relationship(
        "DriftRecord", back_populates="snapshot", cascade="all, delete-orphan"
    )


# ---------------------------------------------------------------------------
# Device
# ---------------------------------------------------------------------------


class DeviceRecord(Base):
    __tablename__ = "devices"
    __table_args__ = (
        Index("ix_devices_snapshot_id", "snapshot_id"),
        Index("ix_devices_hostname", "hostname"),
        Index("ix_devices_management_ip", "management_ip"),
        Index("ix_devices_site", "site"),
        Index("ix_devices_vendor", "vendor"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    snapshot_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("snapshots.id", ondelete="CASCADE"), nullable=True
    )
    hostname: Mapped[str] = mapped_column(String(256))
    management_ip: Mapped[str | None] = mapped_column(String(64), nullable=True)
    vendor: Mapped[str | None] = mapped_column(String(128), nullable=True)
    model: Mapped[str | None] = mapped_column(String(128), nullable=True)
    serial_number: Mapped[str | None] = mapped_column(String(128), nullable=True)
    software_version: Mapped[str | None] = mapped_column(String(256), nullable=True)
    device_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    device_role: Mapped[str | None] = mapped_column(String(128), nullable=True)
    site: Mapped[str | None] = mapped_column(String(256), nullable=True)
    region: Mapped[str | None] = mapped_column(String(256), nullable=True)
    rack: Mapped[str | None] = mapped_column(String(256), nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="unknown")
    namespace: Mapped[str | None] = mapped_column(String(128), nullable=True)
    platform: Mapped[str | None] = mapped_column(String(128), nullable=True)
    tags: Mapped[list[Any]] = mapped_column(JSON, default=list)
    custom_fields: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    source_system: Mapped[str] = mapped_column(String(128), default="unknown")
    source_timestamp: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    quality: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)

    snapshot: Mapped[SnapshotRecord | None] = relationship(
        "SnapshotRecord", back_populates="devices"
    )
    interfaces: Mapped[list[InterfaceRecord]] = relationship(
        "InterfaceRecord", back_populates="device", cascade="all, delete-orphan"
    )


# ---------------------------------------------------------------------------
# Interface
# ---------------------------------------------------------------------------


class InterfaceRecord(Base):
    __tablename__ = "interfaces"
    __table_args__ = (
        Index("ix_interfaces_device_id", "device_id"),
        Index("ix_interfaces_snapshot_id", "snapshot_id"),
        Index("ix_interfaces_name", "name"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    device_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("devices.id", ondelete="CASCADE")
    )
    snapshot_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    name: Mapped[str] = mapped_column(String(256))
    normalized_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    type: Mapped[str] = mapped_column(String(64), default="unknown")
    admin_state: Mapped[str] = mapped_column(String(32), default="unknown")
    oper_state: Mapped[str] = mapped_column(String(32), default="unknown")
    mtu: Mapped[int | None] = mapped_column(Integer, nullable=True)
    speed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    mac_address: Mapped[str | None] = mapped_column(String(64), nullable=True)
    ip_addresses: Mapped[list[Any]] = mapped_column(JSON, default=list)
    vrf: Mapped[str | None] = mapped_column(String(128), nullable=True)
    vlan_mode: Mapped[str | None] = mapped_column(String(32), nullable=True)
    access_vlan: Mapped[int | None] = mapped_column(Integer, nullable=True)
    trunk_vlans: Mapped[list[Any]] = mapped_column(JSON, default=list)
    lag_parent: Mapped[str | None] = mapped_column(String(256), nullable=True)
    lag_members: Mapped[list[Any]] = mapped_column(JSON, default=list)
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    duplex: Mapped[str | None] = mapped_column(String(32), nullable=True)
    source_timestamp: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    quality: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)

    device: Mapped[DeviceRecord] = relationship(
        "DeviceRecord", back_populates="interfaces"
    )


# ---------------------------------------------------------------------------
# IP Address
# ---------------------------------------------------------------------------


class IPAddressRecord(Base):
    __tablename__ = "ip_addresses"
    __table_args__ = (
        Index("ix_ip_addresses_device_id", "device_id"),
        Index("ix_ip_addresses_address", "address"),
        Index("ix_ip_addresses_snapshot_id", "snapshot_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    snapshot_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    address: Mapped[str] = mapped_column(String(64))
    device_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    interface_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    vrf: Mapped[str | None] = mapped_column(String(128), nullable=True)
    dns_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    is_primary: Mapped[bool] = mapped_column(Boolean, default=False)
    status: Mapped[str] = mapped_column(String(32), default="active")
    family: Mapped[int] = mapped_column(Integer, default=4)
    quality: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


# ---------------------------------------------------------------------------
# VLAN
# ---------------------------------------------------------------------------


class VLANRecord(Base):
    __tablename__ = "vlans"
    __table_args__ = (
        Index("ix_vlans_snapshot_id", "snapshot_id"),
        Index("ix_vlans_vid", "vid"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    snapshot_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    vid: Mapped[int] = mapped_column(Integer)
    name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    site: Mapped[str | None] = mapped_column(String(256), nullable=True)
    namespace: Mapped[str | None] = mapped_column(String(128), nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="unknown")
    attached_devices: Mapped[list[Any]] = mapped_column(JSON, default=list)
    attached_interfaces: Mapped[list[Any]] = mapped_column(JSON, default=list)
    quality: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


# ---------------------------------------------------------------------------
# VRF
# ---------------------------------------------------------------------------


class VRFRecord(Base):
    __tablename__ = "vrfs"
    __table_args__ = (Index("ix_vrfs_snapshot_id", "snapshot_id"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    snapshot_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    name: Mapped[str] = mapped_column(String(256))
    rd: Mapped[str | None] = mapped_column(String(64), nullable=True)
    device_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    import_targets: Mapped[list[Any]] = mapped_column(JSON, default=list)
    export_targets: Mapped[list[Any]] = mapped_column(JSON, default=list)
    quality: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------


class RouteRecord(Base):
    __tablename__ = "routes"
    __table_args__ = (
        Index("ix_routes_device_id", "device_id"),
        Index("ix_routes_snapshot_id", "snapshot_id"),
        Index("ix_routes_prefix", "prefix"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    snapshot_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    device_id: Mapped[str] = mapped_column(String(36))
    prefix: Mapped[str] = mapped_column(String(64))
    next_hop: Mapped[str | None] = mapped_column(String(64), nullable=True)
    next_hop_interface: Mapped[str | None] = mapped_column(String(256), nullable=True)
    protocol: Mapped[str] = mapped_column(String(32), default="unknown")
    route_type: Mapped[str] = mapped_column(String(16), default="ipv4")
    metric: Mapped[int | None] = mapped_column(Integer, nullable=True)
    preference: Mapped[int | None] = mapped_column(Integer, nullable=True)
    vrf: Mapped[str | None] = mapped_column(String(128), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    age: Mapped[int | None] = mapped_column(Integer, nullable=True)
    quality: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


# ---------------------------------------------------------------------------
# LLDP/CDP Neighbor
# ---------------------------------------------------------------------------


class NeighborRecord(Base):
    __tablename__ = "neighbors"
    __table_args__ = (
        Index("ix_neighbors_local_device_id", "local_device_id"),
        Index("ix_neighbors_snapshot_id", "snapshot_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    snapshot_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    local_device_id: Mapped[str] = mapped_column(String(36))
    local_interface: Mapped[str] = mapped_column(String(256))
    remote_device_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    remote_hostname: Mapped[str | None] = mapped_column(String(256), nullable=True)
    remote_interface: Mapped[str | None] = mapped_column(String(256), nullable=True)
    remote_management_ip: Mapped[str | None] = mapped_column(String(64), nullable=True)
    remote_platform: Mapped[str | None] = mapped_column(String(128), nullable=True)
    remote_vendor: Mapped[str | None] = mapped_column(String(128), nullable=True)
    protocol: Mapped[str] = mapped_column(String(16), default="lldp")
    ttl: Mapped[int | None] = mapped_column(Integer, nullable=True)
    quality: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


# ---------------------------------------------------------------------------
# Topology Edge
# ---------------------------------------------------------------------------


class TopologyEdgeRecord(Base):
    __tablename__ = "topology_edges"
    __table_args__ = (
        Index("ix_topology_edges_snapshot_id", "snapshot_id"),
        Index("ix_topology_edges_source_node_id", "source_node_id"),
        Index("ix_topology_edges_target_node_id", "target_node_id"),
        Index("ix_topology_edges_edge_type", "edge_type"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    snapshot_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("snapshots.id", ondelete="CASCADE"), nullable=True
    )
    source_node_id: Mapped[str] = mapped_column(String(36))
    target_node_id: Mapped[str] = mapped_column(String(36))
    source_interface: Mapped[str | None] = mapped_column(String(256), nullable=True)
    target_interface: Mapped[str | None] = mapped_column(String(256), nullable=True)
    edge_type: Mapped[str] = mapped_column(String(64), default="physical")
    evidence_source: Mapped[list[Any]] = mapped_column(JSON, default=list)
    evidence_count: Mapped[int] = mapped_column(Integer, default=0)
    confidence: Mapped[str] = mapped_column(String(32), default="low")
    confidence_score: Mapped[float] = mapped_column(Float, default=0.0)
    is_direct: Mapped[bool] = mapped_column(Boolean, default=False)
    conflict_flags: Mapped[list[Any]] = mapped_column(JSON, default=list)
    inference_method: Mapped[str | None] = mapped_column(String(128), nullable=True)
    edge_metadata: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)

    snapshot: Mapped[SnapshotRecord | None] = relationship(
        "SnapshotRecord", back_populates="topology_edges"
    )


# ---------------------------------------------------------------------------
# Topology Conflict
# ---------------------------------------------------------------------------


class TopologyConflictRecord(Base):
    __tablename__ = "topology_conflicts"
    __table_args__ = (Index("ix_topology_conflicts_snapshot_id", "snapshot_id"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    snapshot_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    conflict_type: Mapped[str] = mapped_column(String(64))
    involved_edge_ids: Mapped[list[Any]] = mapped_column(JSON, default=list)
    description: Mapped[str] = mapped_column(Text, default="")
    severity: Mapped[str] = mapped_column(String(32), default="medium")
    evidence: Mapped[list[Any]] = mapped_column(JSON, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)


# ---------------------------------------------------------------------------
# Drift Record
# ---------------------------------------------------------------------------


class DriftRecord(Base):
    __tablename__ = "drift_records"
    __table_args__ = (
        Index("ix_drift_records_snapshot_id", "snapshot_id"),
        Index("ix_drift_records_workflow_id", "workflow_id"),
        Index("ix_drift_records_object_type", "object_type"),
        Index("ix_drift_records_mismatch_type", "mismatch_type"),
        Index("ix_drift_records_severity", "severity"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    snapshot_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("snapshots.id", ondelete="CASCADE"), nullable=True
    )
    workflow_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    intended_ref: Mapped[str | None] = mapped_column(String(512), nullable=True)
    actual_ref: Mapped[str | None] = mapped_column(String(512), nullable=True)
    object_type: Mapped[str] = mapped_column(String(64))
    mismatch_type: Mapped[str] = mapped_column(String(64))
    field_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    intended_value: Mapped[Any] = mapped_column(JSON, nullable=True)
    actual_value: Mapped[Any] = mapped_column(JSON, nullable=True)
    severity: Mapped[str] = mapped_column(String(32), default="medium")
    explanation: Mapped[str] = mapped_column(Text, default="")
    remediation_hint: Mapped[str] = mapped_column(Text, default="")
    confidence: Mapped[float] = mapped_column(Float, default=1.0)
    evidence: Mapped[list[Any]] = mapped_column(JSON, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)
    drift_metadata: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)

    snapshot: Mapped[SnapshotRecord | None] = relationship(
        "SnapshotRecord", back_populates="drift_records"
    )


# ---------------------------------------------------------------------------
# Sync Plan
# ---------------------------------------------------------------------------


class SyncPlanRecord(Base):
    __tablename__ = "sync_plans"
    __table_args__ = (
        Index("ix_sync_plans_workflow_id", "workflow_id"),
        Index("ix_sync_plans_status", "status"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    workflow_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    snapshot_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    mode: Mapped[str] = mapped_column(String(32), default="dry_run")
    status: Mapped[str] = mapped_column(String(32), default="pending")
    action_count: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)
    executed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    policy: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    notes: Mapped[str] = mapped_column(Text, default="")

    actions: Mapped[list[SyncActionRecord]] = relationship(
        "SyncActionRecord", back_populates="plan", cascade="all, delete-orphan"
    )


# ---------------------------------------------------------------------------
# Sync Action
# ---------------------------------------------------------------------------


class SyncActionRecord(Base):
    __tablename__ = "sync_actions"
    __table_args__ = (
        Index("ix_sync_actions_plan_id", "plan_id"),
        Index("ix_sync_actions_action_type", "action_type"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    plan_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("sync_plans.id", ondelete="CASCADE")
    )
    drift_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    object_type: Mapped[str] = mapped_column(String(64))
    action_type: Mapped[str] = mapped_column(String(32))
    object_ref: Mapped[str | None] = mapped_column(String(512), nullable=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(String(32), default="pending")
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    executed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    plan: Mapped[SyncPlanRecord] = relationship(
        "SyncPlanRecord", back_populates="actions"
    )


# ---------------------------------------------------------------------------
# Sync Audit
# ---------------------------------------------------------------------------


class SyncAuditRecord(Base):
    __tablename__ = "sync_audits"
    __table_args__ = (
        Index("ix_sync_audits_plan_id", "plan_id"),
        Index("ix_sync_audits_created_at", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    plan_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    action_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    actor: Mapped[str] = mapped_column(String(256), default="system")
    event_type: Mapped[str] = mapped_column(String(64))
    object_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    object_ref: Mapped[str | None] = mapped_column(String(512), nullable=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    outcome: Mapped[str] = mapped_column(String(32), default="success")
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)


# ---------------------------------------------------------------------------
# Replay Run
# ---------------------------------------------------------------------------


class ReplayRecord(Base):
    __tablename__ = "replay_runs"
    __table_args__ = (
        Index("ix_replay_runs_source_workflow_id", "source_workflow_id"),
        Index("ix_replay_runs_created_at", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    source_workflow_id: Mapped[str] = mapped_column(String(36))
    status: Mapped[str] = mapped_column(String(32), default="pending")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    diff_summary: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    passed: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    notes: Mapped[str] = mapped_column(Text, default="")


# ---------------------------------------------------------------------------
# BGP Session
# ---------------------------------------------------------------------------


class BGPSessionRecord(Base):
    __tablename__ = "bgp_sessions"
    __table_args__ = (
        Index("ix_bgp_sessions_device_id", "device_id"),
        Index("ix_bgp_sessions_snapshot_id", "snapshot_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    snapshot_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    device_id: Mapped[str] = mapped_column(String(36))
    local_address: Mapped[str | None] = mapped_column(String(64), nullable=True)
    remote_address: Mapped[str] = mapped_column(String(64))
    local_asn: Mapped[int | None] = mapped_column(Integer, nullable=True)
    remote_asn: Mapped[int | None] = mapped_column(Integer, nullable=True)
    state: Mapped[str] = mapped_column(String(32), default="unknown")
    vrf: Mapped[str | None] = mapped_column(String(128), nullable=True)
    peer_group: Mapped[str | None] = mapped_column(String(128), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    uptime: Mapped[int | None] = mapped_column(Integer, nullable=True)
    prefixes_received: Mapped[int | None] = mapped_column(Integer, nullable=True)
    prefixes_sent: Mapped[int | None] = mapped_column(Integer, nullable=True)
    quality: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


# ---------------------------------------------------------------------------
# ARP Record
# ---------------------------------------------------------------------------


class ARPRecord(Base):
    __tablename__ = "arp_entries"
    __table_args__ = (
        Index("ix_arp_entries_device_id", "device_id"),
        Index("ix_arp_entries_snapshot_id", "snapshot_id"),
        Index("ix_arp_entries_ip_address", "ip_address"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    snapshot_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    device_id: Mapped[str] = mapped_column(String(36))
    interface: Mapped[str | None] = mapped_column(String(256), nullable=True)
    ip_address: Mapped[str] = mapped_column(String(64))
    mac_address: Mapped[str | None] = mapped_column(String(64), nullable=True)
    vrf: Mapped[str | None] = mapped_column(String(128), nullable=True)
    state: Mapped[str] = mapped_column(String(32), default="reachable")
    age: Mapped[int | None] = mapped_column(Integer, nullable=True)
    family: Mapped[int] = mapped_column(Integer, default=4)
    quality: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


# ---------------------------------------------------------------------------
# MAC Record
# ---------------------------------------------------------------------------


class MACRecord(Base):
    __tablename__ = "mac_entries"
    __table_args__ = (
        Index("ix_mac_entries_device_id", "device_id"),
        Index("ix_mac_entries_snapshot_id", "snapshot_id"),
        Index("ix_mac_entries_mac_address", "mac_address"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    snapshot_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    device_id: Mapped[str] = mapped_column(String(36))
    mac_address: Mapped[str] = mapped_column(String(64))
    vlan_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    interface: Mapped[str | None] = mapped_column(String(256), nullable=True)
    type: Mapped[str] = mapped_column(String(32), default="dynamic")
    age: Mapped[int | None] = mapped_column(Integer, nullable=True)
    quality: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
