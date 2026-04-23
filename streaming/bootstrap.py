"""Streaming bootstrap helpers."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select, desc

from netobserv.config.settings import Settings
from netobserv.storage.database import RuntimeSessionLocal
from netobserv.storage.models import SnapshotRecord, TopologyEdgeRecord
from netobserv.streaming.correlation_repository import CorrelationRepository
from netobserv.streaming.engine import StreamingEngine, StreamingEngineConfig
from netobserv.streaming.repository import StreamingRepository
from netobserv.streaming.service import StreamingService, StreamingServiceConfig


@dataclass(slots=True)
class StreamingComponents:
    repository: StreamingRepository
    correlation_repository: CorrelationRepository
    engine: StreamingEngine
    service: StreamingService


def _load_latest_snapshot_edges() -> list[dict]:
    if RuntimeSessionLocal is None:
        return []
    session = RuntimeSessionLocal()
    try:
        latest = session.execute(
            select(SnapshotRecord)
            .where(SnapshotRecord.status == "completed")
            .order_by(desc(SnapshotRecord.completed_at), desc(SnapshotRecord.started_at))
            .limit(1)
        ).scalar_one_or_none()
        if latest is None:
            return []
        edges = session.execute(
            select(TopologyEdgeRecord).where(TopologyEdgeRecord.snapshot_id == latest.id)
        ).scalars().all()
        return [
            {
                "id": e.id,
                "source_node_id": e.source_node_id,
                "target_node_id": e.target_node_id,
                "source_interface": e.source_interface,
                "target_interface": e.target_interface,
                "edge_type": e.edge_type,
                "confidence_score": e.confidence_score,
            }
            for e in edges
        ]
    finally:
        session.close()


def build_streaming_components(settings: Settings) -> StreamingComponents:
    repository = StreamingRepository()
    config = StreamingEngineConfig(
        enabled=settings.streaming_enabled,
        gnmi_enabled=settings.streaming_gnmi_enabled,
        ssh_poll_enabled=settings.streaming_ssh_poll_enabled,
        snmp_trap_enabled=settings.streaming_snmp_trap_enabled,
        syslog_enabled=settings.streaming_syslog_enabled,
        event_bus_queue_size=settings.streaming_event_bus_queue_size,
        event_bus_dedup_window_seconds=settings.streaming_event_bus_dedup_window_seconds,
        subscriber_timeout_seconds=settings.streaming_subscriber_timeout_seconds,
        topology_delta_queue_size=settings.streaming_topology_delta_queue_size,
        snmp_trap_host=settings.streaming_snmp_trap_host,
        snmp_trap_port=settings.streaming_snmp_trap_port,
        snmp_community=settings.streaming_snmp_trap_community,
        syslog_host=settings.streaming_syslog_host,
        syslog_udp_port=settings.streaming_syslog_udp_port,
        syslog_tcp_port=settings.streaming_syslog_tcp_port,
        event_bus_subscriber_timeout_seconds=settings.streaming_subscriber_timeout_seconds,
        metrics_update_interval_seconds=settings.streaming_metrics_update_interval_seconds,
    )
    engine = StreamingEngine(config, repository=repository)
    # wire persistence callbacks
    engine._event_bus.set_persist(repository.persist_event)  # noqa: SLF001 - controlled bootstrap wiring
    engine._event_bus.subscribe_delta(repository.persist_delta)  # noqa: SLF001
    edges = _load_latest_snapshot_edges()
    if edges:
        engine.seed_topology(edges)
    service = StreamingService(
        engine,
        StreamingServiceConfig(
            heartbeat_interval_seconds=settings.streaming_service_heartbeat_interval_seconds,
            emit_health_log=settings.streaming_service_emit_health_log,
        ),
    )
    return StreamingComponents(
        repository=repository,
        correlation_repository=engine.correlation_repository,
        engine=engine,
        service=service,
    )
