"""SQLAlchemy ORM models for streaming event and topology delta persistence.

Every ChangeEvent and TopologyDelta is persisted. This is not optional for
DoD networks — operators need an immutable record of what the system saw,
when it saw it, and what it did with that information.

The streaming persistence schema is kept separate from the main NetObserv schema
(runtime_jobs, snapshots, etc.) via table prefixes. This allows the streaming
subsystem to be deployed independently or migrated separately.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    JSON, Boolean, DateTime, Float, ForeignKey, Index,
    Integer, String, Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class StreamingBase(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# ChangeEvent persistence
# ---------------------------------------------------------------------------


class StreamingEventModel(StreamingBase):
    """Persisted ChangeEvent row.

    Every event that passes dedup and enters the dispatch loop is written here.
    This table is the audit trail for the streaming subsystem.

    Partition strategy for large deployments: partition by month on detected_at.
    Retention policy: configurable, default 90 days matching snapshot retention.
    """

    __tablename__ = "streaming_events"

    __table_args__ = (
        Index("ix_streaming_events_device_id", "device_id"),
        Index("ix_streaming_events_kind", "kind"),
        Index("ix_streaming_events_severity", "severity"),
        Index("ix_streaming_events_detected_at", "detected_at"),
        Index("ix_streaming_events_device_kind", "device_id", "kind"),
        Index("ix_streaming_events_dedup_key", "dedup_key"),
    )

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    kind: Mapped[str] = mapped_column(String(64), nullable=False)
    severity: Mapped[str] = mapped_column(String(32), nullable=False)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    device_id: Mapped[str] = mapped_column(String(256), nullable=False)
    device_hostname: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    interface_name: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utc_now)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utc_now)

    previous_value: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    current_value: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    field_path: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)

    neighbor_device_id: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    neighbor_interface: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    vlan_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    vrf: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    prefix: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    mac_address: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    is_confirmed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    dedup_key: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)

    topology_patch_applied: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    topology_delta_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    human_summary: Mapped[str] = mapped_column(Text, nullable=False, default="")
    raw_payload: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)


# ---------------------------------------------------------------------------
# TopologyDelta persistence
# ---------------------------------------------------------------------------


class TopologyDeltaModel(StreamingBase):
    """Persisted TopologyDelta row.

    Each delta captures exactly one topology edge mutation caused by one event.
    The delta_id links back to StreamingEventModel.event_id via change_event_id.

    This table is the time-series topology change log. Querying it by device_id
    and time range gives a complete audit of how that device's topology evolved.
    """

    __tablename__ = "streaming_topology_deltas"

    __table_args__ = (
        Index("ix_topology_deltas_source", "source_node_id"),
        Index("ix_topology_deltas_target", "target_node_id"),
        Index("ix_topology_deltas_created_at", "created_at"),
        Index("ix_topology_deltas_change_kind", "change_kind"),
        Index("ix_topology_deltas_event_id", "change_event_id"),
    )

    delta_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    change_event_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    change_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    edge_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    source_node_id: Mapped[str] = mapped_column(String(256), nullable=False)
    target_node_id: Mapped[str] = mapped_column(String(256), nullable=False)
    source_interface: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    target_interface: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    edge_type: Mapped[str] = mapped_column(String(64), nullable=False, default="physical")
    previous_state: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    new_state: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utc_now)
    human_summary: Mapped[str] = mapped_column(Text, nullable=False, default="")


# ---------------------------------------------------------------------------
# Streaming session health
# ---------------------------------------------------------------------------


class StreamingSessionModel(StreamingBase):
    """Tracks health state of each active streaming session per device.

    Updated on every TELEMETRY_SESSION_LOST / RESTORED event.
    Used by the /api/streaming/sessions endpoint to give operators visibility
    into which devices have active telemetry coverage.
    """

    __tablename__ = "streaming_sessions"

    __table_args__ = (
        Index("ix_streaming_sessions_device_id", "device_id"),
        Index("ix_streaming_sessions_session_type", "session_type"),
    )

    session_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    device_id: Mapped[str] = mapped_column(String(256), nullable=False)
    device_hostname: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    session_type: Mapped[str] = mapped_column(String(32), nullable=False)  # gnmi / snmp_trap / syslog / ssh_poll
    state: Mapped[str] = mapped_column(String(32), nullable=False, default="unknown")  # connected / lost / reconnecting
    connected_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_event_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    reconnect_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utc_now)

# ---------------------------------------------------------------------------
# Resolved assertion persistence
# ---------------------------------------------------------------------------


class ResolvedAssertionModel(StreamingBase):
    """Persisted truth-arbitration result for contested streaming state.

    This table records what the conflict engine concluded after evaluating
    multiple pieces of evidence about the same field on the same subject.
    """

    __tablename__ = "streaming_resolved_assertions"

    __table_args__ = (
        Index("ix_streaming_resolved_subject", "subject_type", "subject_id", "field_name"),
        Index("ix_streaming_resolved_observed_at", "observed_at"),
        Index("ix_streaming_resolved_state", "resolution_state"),
        Index("ix_streaming_resolved_last_source", "last_authoritative_source"),
        Index("ix_streaming_resolved_cluster", "cluster_id"),
    )

    assertion_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    subject_type: Mapped[str] = mapped_column(String(64), nullable=False)
    subject_id: Mapped[str] = mapped_column(String(512), nullable=False)
    field_name: Mapped[str] = mapped_column(String(128), nullable=False)

    resolved_value: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    resolution_state: Mapped[str] = mapped_column(String(64), nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    conflict_type: Mapped[str] = mapped_column(String(64), nullable=False)

    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utc_now)
    stale_after: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_authoritative_source: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    cluster_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)

    contributing_evidence_ids: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    rejected_evidence_ids: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    disputed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    explanation: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
