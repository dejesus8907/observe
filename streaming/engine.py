"""StreamingEngine — the top-level orchestrator for the real-time change detection system.

The engine wires together all streaming subsystems:
  - PersistentSessionPool (SSH poll diffs)
  - GnmiCollector (gNMI ON_CHANGE subscriptions)
  - SnmpTrapReceiver (SNMP trap ingest)
  - SyslogReceiver (syslog ingest)
  - EventBus (dedup, routing, dispatch)
  - TopologyPatcher (incremental edge mutations)

Lifecycle: start() / stop(). Thread-safe. Can be started as part of FastAPI
lifespan or as a standalone asyncio application.

Configuration-driven architecture: which subsystems are enabled is determined
by StreamingEngineConfig — an operator can enable gNMI for devices that support
it and SSH poll diff for everything else.

Seeding: on startup, the engine loads the latest completed snapshot from the
database and seeds the topology patcher with it. This ensures the live graph
starts from a known baseline rather than empty.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from netobserv.streaming.event_bus import EventBus
from netobserv.streaming.events import ChangeEvent, ChangeKind, ChangeSeverity, EventSource
from netobserv.streaming.gnmi_collector import GnmiCollector, GnmiTargetConfig
from netobserv.streaming.metrics import (
    record_event_dispatched,
    record_ingest_latency,
    record_ingest_latency_budget_exceeded,
    record_event_received,
    record_session_active,
    record_session_lost,
    record_topology_summary,
    update_bus_stats,
)
from netobserv.streaming.correlation_repository import CorrelationRepository
from netobserv.streaming.repository import StreamingRepository
from netobserv.streaming.session_pool import PersistentSessionPool, SessionConfig
from netobserv.streaming.snmp_trap_receiver import SnmpTrapReceiver, SnmpTrapReceiverConfig
from netobserv.streaming.syslog_receiver import SyslogReceiver, SyslogReceiverConfig
from netobserv.streaming.topology_patcher import TopologyDelta, TopologyPatcher

logger = logging.getLogger("streaming.engine")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Engine configuration
# ---------------------------------------------------------------------------


@dataclass
class StreamingEngineConfig:
    """Top-level configuration for the streaming engine."""

    enabled: bool = True

    # Which ingest sources to enable
    gnmi_enabled: bool = True
    ssh_poll_enabled: bool = True
    snmp_trap_enabled: bool = True
    syslog_enabled: bool = True

    # Event bus
    event_bus_queue_size: int = 10_000
    event_bus_dedup_window_seconds: float = 5.0
    event_bus_subscriber_timeout_seconds: float = 2.0

    # Topology delta queue size
    topology_delta_queue_size: int = 5_000

    # SNMP trap receiver
    snmp_trap_host: str = "0.0.0.0"
    snmp_trap_port: int = 1162
    snmp_community: str = "public"

    # Syslog receiver
    syslog_host: str = "0.0.0.0"
    syslog_udp_port: int = 1514
    syslog_tcp_port: int | None = 1514

    # Metrics update interval
    metrics_update_interval_seconds: float = 15.0

    # Real-time ingest budgets (seconds)
    ingest_latency_budget_seconds_gnmi: float = 2.0
    ingest_latency_budget_seconds_snmp_trap: float = 5.0
    ingest_latency_budget_seconds_syslog: float = 5.0
    ingest_latency_budget_seconds_ssh_poll_diff: float = 45.0
    ingest_latency_budget_seconds_netconf: float = 10.0
    ingest_latency_budget_seconds_unknown: float = 30.0
    ingest_hard_budget_multiplier: float = 5.0
    drop_events_over_hard_budget: bool = False

    # IP → device_id mapping (shared across all receivers)
    ip_to_device_id: dict[str, str] = field(default_factory=dict)
    allowed_trap_sources: list[str] = field(default_factory=list)
    allowed_syslog_sources: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# StreamingEngine
# ---------------------------------------------------------------------------


class StreamingEngine:
    """Wires and manages all streaming subsystems.

    Instantiate once per process. Pass via app.state in FastAPI.
    """

    def __init__(
        self,
        config: StreamingEngineConfig,
        repository: StreamingRepository | None = None,
        correlation_repository: CorrelationRepository | None = None,
    ) -> None:
        self._config = config
        self._running = False
        self._repository = repository or StreamingRepository()
        self._correlation_repository = correlation_repository or CorrelationRepository()

        # Shared queues
        self._event_queue: asyncio.Queue[ChangeEvent] = asyncio.Queue(
            maxsize=config.event_bus_queue_size
        )
        self._delta_queue: asyncio.Queue[TopologyDelta] = asyncio.Queue(
            maxsize=config.topology_delta_queue_size
        )

        # Core components
        self._event_bus = EventBus(
            dedup_window_seconds=config.event_bus_dedup_window_seconds,
            subscriber_timeout_seconds=config.event_bus_subscriber_timeout_seconds,
            queue_max_size=config.event_bus_queue_size,
        )
        self._patcher = TopologyPatcher(self._delta_queue)
        self._event_bus.set_patcher(self._patcher)

        # Ingest sources (initialized but not started)
        self._ssh_pool = PersistentSessionPool(self._event_queue)
        self._gnmi_collector = GnmiCollector(self._event_bus)
        self._snmp_receiver = SnmpTrapReceiver(
            SnmpTrapReceiverConfig(
                listen_host=config.snmp_trap_host,
                listen_port=config.snmp_trap_port,
                community=config.snmp_community,
                allowed_sources=config.allowed_trap_sources,
                source_ip_to_device_id=config.ip_to_device_id,
            ),
            self._event_bus,
        )
        self._syslog_receiver = SyslogReceiver(
            SyslogReceiverConfig(
                listen_host=config.syslog_host,
                udp_port=config.syslog_udp_port,
                tcp_port=config.syslog_tcp_port,
                allowed_sources=config.allowed_syslog_sources,
                source_ip_to_device_id=config.ip_to_device_id,
            ),
            self._event_bus,
        )

        # Background tasks
        self._ingest_task: asyncio.Task[None] | None = None
        self._metrics_task: asyncio.Task[None] | None = None

        # WebSocket broadcast registry
        self._ws_subscribers: dict[str, asyncio.Queue[dict[str, Any]]] = {}

    # ------------------------------------------------------------------
    # Device registration
    # ------------------------------------------------------------------

    def register_device_ssh(self, config: SessionConfig) -> None:
        """Add a device to the SSH poll pool."""
        self._ssh_pool.add_device(config)

    def register_device_gnmi(self, config: GnmiTargetConfig) -> None:
        """Add a device to the gNMI collector."""
        self._gnmi_collector.add_target(config)

    def register_ip_mapping(self, ip: str, device_id: str) -> None:
        """Map a source IP to a canonical device_id for trap/syslog classification."""
        self._config.ip_to_device_id[ip] = device_id
        self._snmp_receiver._config.source_ip_to_device_id[ip] = device_id
        self._syslog_receiver._config.source_ip_to_device_id[ip] = device_id

    def seed_topology(self, edges: list[dict[str, Any]]) -> None:
        """Seed the topology patcher from a snapshot's edge list."""
        self._patcher.seed_from_snapshot(edges)
        logger.info("Topology patcher seeded with %d edges", len(edges))

    # ------------------------------------------------------------------
    # WebSocket subscription
    # ------------------------------------------------------------------

    def subscribe_websocket(self, client_id: str) -> asyncio.Queue[dict[str, Any]]:
        """Register a WebSocket client and return its event queue."""
        q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=500)
        self._ws_subscribers[client_id] = q
        return q

    def unsubscribe_websocket(self, client_id: str) -> None:
        self._ws_subscribers.pop(client_id, None)

    async def _broadcast_to_websockets(self, event: ChangeEvent) -> None:
        """Push a serialized ChangeEvent to all connected WebSocket clients."""
        payload = {
            "type": "change_event",
            "event_id": event.event_id,
            "kind": event.kind.value,
            "severity": event.severity.value,
            "source": event.source.value,
            "device_id": event.device_id,
            "device_hostname": event.device_hostname,
            "interface_name": event.interface_name,
            "detected_at": event.detected_at.isoformat(),
            "human_summary": event.human_summary,
            "neighbor_device_id": event.neighbor_device_id,
            "current_value": event.current_value,
            "previous_value": event.previous_value,
        }
        dead: list[str] = []
        for client_id, q in self._ws_subscribers.items():
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                dead.append(client_id)
        for client_id in dead:
            self.unsubscribe_websocket(client_id)

    async def _broadcast_delta_to_websockets(self, delta: TopologyDelta) -> None:
        """Push a serialized TopologyDelta to all connected WebSocket clients."""
        payload = {
            "type": "topology_delta",
            "delta_id": delta.delta_id,
            "change_kind": delta.change_kind.value,
            "edge_id": delta.edge_id,
            "source_node_id": delta.source_node_id,
            "target_node_id": delta.target_node_id,
            "source_interface": delta.source_interface,
            "target_interface": delta.target_interface,
            "edge_type": delta.edge_type,
            "new_state": delta.new_state,
            "created_at": delta.created_at.isoformat(),
            "human_summary": delta.human_summary,
        }
        dead: list[str] = []
        for client_id, q in self._ws_subscribers.items():
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                dead.append(client_id)
        for client_id in dead:
            self.unsubscribe_websocket(client_id)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        if self._running:
            return

        if not self._config.enabled:
            logger.info("Streaming engine disabled by configuration")
            return

        self._running = True
        logger.info("Starting streaming engine")

        # Register broadcast and persistence callbacks on the event bus
        self._event_bus.set_broadcast(self._broadcast_to_websockets)
        self._event_bus.set_persist(self._repository.persist_event)
        self._event_bus.set_evidence_persist(self._repository.persist_evidence_record)
        self._event_bus.set_resolution_persist(self._repository.persist_resolved_assertion)
        self._event_bus.set_correlation_persist(self._persist_correlation_state)
        self._event_bus.subscribe_delta(self._broadcast_delta_to_websockets)
        self._event_bus.subscribe_delta(self._repository.persist_delta)

        # Start event bus
        await self._event_bus.start()

        # Start ingest loop (drains self._event_queue into self._event_bus)
        self._ingest_task = asyncio.create_task(
            self._ingest_loop(), name="streaming-ingest-loop"
        )

        # Start ingest sources
        if self._config.ssh_poll_enabled:
            await self._ssh_pool.start()

        if self._config.gnmi_enabled:
            await self._gnmi_collector.start()

        if self._config.snmp_trap_enabled:
            try:
                await self._snmp_receiver.start()
            except Exception as exc:
                logger.error("SNMP trap receiver failed to start: %s", exc)

        if self._config.syslog_enabled:
            try:
                await self._syslog_receiver.start()
            except Exception as exc:
                logger.error("Syslog receiver failed to start: %s", exc)

        # Start metrics updater
        self._metrics_task = asyncio.create_task(
            self._metrics_loop(), name="streaming-metrics-loop"
        )

        logger.info(
            "Streaming engine started",
            extra={
                "gnmi": self._config.gnmi_enabled,
                "ssh_poll": self._config.ssh_poll_enabled,
                "snmp_trap": self._config.snmp_trap_enabled,
                "syslog": self._config.syslog_enabled,
            },
        )

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False

        # Stop ingest sources
        await self._ssh_pool.stop()
        await self._gnmi_collector.stop()
        await self._snmp_receiver.stop()
        await self._syslog_receiver.stop()

        # Stop background tasks
        for task in [self._ingest_task, self._metrics_task]:
            if task:
                task.cancel()
        await asyncio.gather(
            *(t for t in [self._ingest_task, self._metrics_task] if t),
            return_exceptions=True,
        )

        await self._event_bus.stop()
        logger.info("Streaming engine stopped")

    # ------------------------------------------------------------------
    # Ingest loop: drains the raw event queue into the event bus
    # ------------------------------------------------------------------

    async def _ingest_loop(self) -> None:
        """Drain events from all ingest sources into the event bus."""
        while self._running:
            try:
                event = await asyncio.wait_for(
                    self._event_queue.get(), timeout=1.0
                )
                now = _utc_now()
                latency_seconds = max(0.0, (now - event.detected_at).total_seconds())
                source_key = event.source.value
                kind_key = event.kind.value
                record_ingest_latency(source_key, kind_key, latency_seconds)

                budget_seconds = self._ingest_budget_for_source(source_key)
                if latency_seconds > budget_seconds:
                    record_ingest_latency_budget_exceeded(source_key, kind_key)
                    logger.warning(
                        "Ingest latency budget exceeded",
                        extra={
                            "source": source_key,
                            "kind": kind_key,
                            "latency_seconds": latency_seconds,
                            "budget_seconds": budget_seconds,
                        },
                    )
                    event = event.model_copy(
                        update={
                            "raw_payload": {
                                **dict(event.raw_payload or {}),
                                "ingest_latency_seconds": latency_seconds,
                                "ingest_latency_budget_seconds": budget_seconds,
                                "latency_budget_exceeded": True,
                            }
                        }
                    )
                    hard_limit = budget_seconds * max(1.0, self._config.ingest_hard_budget_multiplier)
                    if self._config.drop_events_over_hard_budget and latency_seconds > hard_limit:
                        logger.error(
                            "Dropping stale event that exceeded hard ingest latency budget",
                            extra={
                                "source": source_key,
                                "kind": kind_key,
                                "latency_seconds": latency_seconds,
                                "hard_limit_seconds": hard_limit,
                                "event_id": event.event_id,
                            },
                        )
                        self._event_queue.task_done()
                        continue

                record_event_received(event.source.value, event.kind.value)
                await self._event_bus.publish(event)
                self._event_queue.task_done()
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("Ingest loop error: %s", exc)

    def _ingest_budget_for_source(self, source: str) -> float:
        if source == EventSource.GNMI.value:
            return self._config.ingest_latency_budget_seconds_gnmi
        if source == EventSource.SNMP_TRAP.value:
            return self._config.ingest_latency_budget_seconds_snmp_trap
        if source == EventSource.SYSLOG.value:
            return self._config.ingest_latency_budget_seconds_syslog
        if source == EventSource.SSH_POLL_DIFF.value:
            return self._config.ingest_latency_budget_seconds_ssh_poll_diff
        if source == EventSource.NETCONF.value:
            return self._config.ingest_latency_budget_seconds_netconf
        return self._config.ingest_latency_budget_seconds_unknown

    # ------------------------------------------------------------------
    # Metrics loop
    # ------------------------------------------------------------------

    async def _metrics_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._config.metrics_update_interval_seconds)
            try:
                summary = self._patcher.summary()
                record_topology_summary(
                    summary["degraded_edges"],
                    summary["conflicted_edges"],
                )
                record_session_active("ssh_poll", self._ssh_pool.active_session_count)
                record_session_active("gnmi", self._gnmi_collector.active_stream_count)
                bus_stats = self._event_bus.stats()
                update_bus_stats(bus_stats["queue_size"], bus_stats["dedup_cache_size"])
            except Exception as exc:
                logger.debug("Metrics update error: %s", exc)

    # ------------------------------------------------------------------
    # Query interface (used by API router)
    # ------------------------------------------------------------------

    def get_live_topology_summary(self) -> dict[str, Any]:
        return self._patcher.summary()

    def get_live_degraded_edges(self) -> list[dict[str, Any]]:
        return [
            {
                "edge_id": e.edge_id,
                "source_node_id": e.source_node_id,
                "target_node_id": e.target_node_id,
                "source_interface": e.source_interface,
                "target_interface": e.target_interface,
                "edge_type": e.edge_type,
                "is_degraded": e.is_degraded,
                "conflict_flags": e.conflict_flags,
                "last_seen": e.last_seen.isoformat(),
            }
            for e in self._patcher.get_degraded_edges()
        ]

    def get_live_neighbors(self, device_id: str) -> list[dict[str, Any]]:
        return [
            {
                "edge_id": e.edge_id,
                "source_node_id": e.source_node_id,
                "target_node_id": e.target_node_id,
                "source_interface": e.source_interface,
                "target_interface": e.target_interface,
                "edge_type": e.edge_type,
                "confidence_score": e.confidence_score,
                "is_degraded": e.is_degraded,
            }
            for e in self._patcher.get_neighbors(device_id)
        ]

    def get_bus_stats(self) -> dict[str, Any]:
        return self._event_bus.stats

    def get_pool_stats(self) -> dict[str, Any]:
        return self._ssh_pool.stats

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def ws_client_count(self) -> int:
        return len(self._ws_subscribers)

    @property
    def correlation_repository(self) -> CorrelationRepository:
        return self._correlation_repository

    async def _persist_correlation_state(self, decision: Any, cluster_id: str) -> None:
        cluster = self._event_bus._correlation.facade.correlator._clusters.get(cluster_id)
        if cluster is None:
            return
        await self._correlation_repository.persist_cluster_state(
            cluster=cluster,
            decision=decision,
            assertion_id=None,
        )
