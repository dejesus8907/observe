"""Prometheus instrumentation for the streaming engine.

Every significant streaming behavior gets a metric. The goal is that an
operator can answer all operational questions about the streaming engine
from a Grafana dashboard without touching logs:

- How many events per second per source and per kind?
- How many duplicates are being collapsed?
- How many sessions are live vs lost?
- How many topology patches are happening per minute?
- What is the p99 latency from event detected to topology patch applied?
- How full is the event bus queue?
- How many syslog messages are being rate-limited?
"""

from __future__ import annotations

from contextlib import contextmanager
from time import perf_counter
from typing import Iterator

from prometheus_client import Counter, Gauge, Histogram, Summary


_LATENCY_BUCKETS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)

# ---------------------------------------------------------------------------
# Ingest source counters
# ---------------------------------------------------------------------------

streaming_events_received_total = Counter(
    "netobserv_streaming_events_received_total",
    "Total ChangeEvents received from all ingest sources.",
    ["source", "kind"],
)

streaming_events_deduplicated_total = Counter(
    "netobserv_streaming_events_deduplicated_total",
    "Total ChangeEvents dropped as duplicates within the dedup window.",
    ["kind"],
)

streaming_events_dispatched_total = Counter(
    "netobserv_streaming_events_dispatched_total",
    "Total ChangeEvents dispatched through the event bus.",
    ["kind", "severity"],
)

streaming_events_dropped_total = Counter(
    "netobserv_streaming_events_dropped_total",
    "Total ChangeEvents dropped due to queue full or other errors.",
    ["source", "reason"],
)

# ---------------------------------------------------------------------------
# Session health gauges
# ---------------------------------------------------------------------------

streaming_sessions_active = Gauge(
    "netobserv_streaming_sessions_active",
    "Current number of active streaming sessions.",
    ["session_type"],
)

streaming_sessions_lost = Gauge(
    "netobserv_streaming_sessions_lost",
    "Current number of streaming sessions in lost/reconnecting state.",
    ["session_type"],
)

streaming_sessions_reconnect_attempts_total = Counter(
    "netobserv_streaming_sessions_reconnect_attempts_total",
    "Total streaming session reconnection attempts.",
    ["session_type", "device_id"],
)

# ---------------------------------------------------------------------------
# Topology patcher metrics
# ---------------------------------------------------------------------------

streaming_topology_deltas_total = Counter(
    "netobserv_streaming_topology_deltas_total",
    "Total topology deltas applied by the patcher.",
    ["change_kind", "edge_type"],
)

streaming_topology_degraded_edges = Gauge(
    "netobserv_streaming_topology_degraded_edges",
    "Current number of edges in degraded state.",
)

streaming_topology_conflicted_edges = Gauge(
    "netobserv_streaming_topology_conflicted_edges",
    "Current number of edges with conflict flags.",
)

# ---------------------------------------------------------------------------
# Event bus internals
# ---------------------------------------------------------------------------

streaming_event_bus_queue_size = Gauge(
    "netobserv_streaming_event_bus_queue_size",
    "Current number of events waiting in the event bus queue.",
)

streaming_event_bus_dedup_cache_size = Gauge(
    "netobserv_streaming_event_bus_dedup_cache_size",
    "Current size of the event bus deduplication cache.",
)

# ---------------------------------------------------------------------------
# Latency distributions
# ---------------------------------------------------------------------------

streaming_event_end_to_end_latency_seconds = Histogram(
    "netobserv_streaming_event_end_to_end_latency_seconds",
    "Latency from event detected_at to topology patch applied.",
    ["kind"],
    buckets=_LATENCY_BUCKETS,
)

streaming_ingest_latency_seconds = Histogram(
    "netobserv_streaming_ingest_latency_seconds",
    "Latency from event detected_at to event bus ingest.",
    ["source", "kind"],
    buckets=_LATENCY_BUCKETS,
)

streaming_ingest_latency_budget_exceeded_total = Counter(
    "netobserv_streaming_ingest_latency_budget_exceeded_total",
    "Events that exceeded configured per-source ingest latency budget.",
    ["source", "kind"],
)

streaming_topology_patch_duration_seconds = Histogram(
    "netobserv_streaming_topology_patch_duration_seconds",
    "Duration of a single topology patcher apply() call.",
    buckets=_LATENCY_BUCKETS,
)

streaming_subscriber_dispatch_duration_seconds = Histogram(
    "netobserv_streaming_subscriber_dispatch_duration_seconds",
    "Duration of one event dispatch cycle (all subscribers).",
    buckets=_LATENCY_BUCKETS,
)

# ---------------------------------------------------------------------------
# Source-specific counters
# ---------------------------------------------------------------------------

streaming_gnmi_notifications_total = Counter(
    "netobserv_streaming_gnmi_notifications_total",
    "Total raw gNMI notifications received.",
    ["device_id"],
)

streaming_snmp_traps_total = Counter(
    "netobserv_streaming_snmp_traps_total",
    "Total SNMP traps received.",
    ["classified"],
)

streaming_syslog_messages_total = Counter(
    "netobserv_streaming_syslog_messages_total",
    "Total syslog messages received.",
    ["protocol"],  # udp / tcp
)

streaming_syslog_rate_limited_total = Counter(
    "netobserv_streaming_syslog_rate_limited_total",
    "Total syslog messages dropped due to per-source rate limiting.",
)

streaming_ssh_poll_cycles_total = Counter(
    "netobserv_streaming_ssh_poll_cycles_total",
    "Total SSH poll cycles completed.",
    ["device_id"],
)

streaming_ssh_poll_events_produced_total = Counter(
    "netobserv_streaming_ssh_poll_events_produced_total",
    "Total ChangeEvents produced by SSH poll diffs.",
    ["device_id", "kind"],
)

# ---------------------------------------------------------------------------
# Recording helpers
# ---------------------------------------------------------------------------


def record_event_received(source: str, kind: str) -> None:
    streaming_events_received_total.labels(source=source, kind=kind).inc()


def record_event_deduplicated(kind: str) -> None:
    streaming_events_deduplicated_total.labels(kind=kind).inc()


def record_event_dispatched(kind: str, severity: str) -> None:
    streaming_events_dispatched_total.labels(kind=kind, severity=severity).inc()


def record_event_dropped(source: str, reason: str) -> None:
    streaming_events_dropped_total.labels(source=source, reason=reason).inc()


def record_topology_delta(change_kind: str, edge_type: str) -> None:
    streaming_topology_deltas_total.labels(change_kind=change_kind, edge_type=edge_type).inc()


def record_topology_summary(degraded: int, conflicted: int) -> None:
    streaming_topology_degraded_edges.set(max(0, degraded))
    streaming_topology_conflicted_edges.set(max(0, conflicted))


def record_session_active(session_type: str, count: int) -> None:
    streaming_sessions_active.labels(session_type=session_type).set(max(0, count))


def record_session_lost(session_type: str, count: int) -> None:
    streaming_sessions_lost.labels(session_type=session_type).set(max(0, count))


def record_session_reconnect(session_type: str, device_id: str) -> None:
    streaming_sessions_reconnect_attempts_total.labels(
        session_type=session_type, device_id=device_id
    ).inc()


def record_gnmi_notification(device_id: str) -> None:
    streaming_gnmi_notifications_total.labels(device_id=device_id).inc()


def record_snmp_trap(classified: bool) -> None:
    streaming_snmp_traps_total.labels(classified="yes" if classified else "no").inc()


def record_syslog_message(protocol: str) -> None:
    streaming_syslog_messages_total.labels(protocol=protocol).inc()


def record_syslog_rate_limited() -> None:
    streaming_syslog_rate_limited_total.inc()


def record_ssh_poll_cycle(device_id: str) -> None:
    streaming_ssh_poll_cycles_total.labels(device_id=device_id).inc()


def record_ssh_poll_event(device_id: str, kind: str) -> None:
    streaming_ssh_poll_events_produced_total.labels(device_id=device_id, kind=kind).inc()


def record_e2e_latency(kind: str, latency_seconds: float) -> None:
    if latency_seconds >= 0:
        streaming_event_end_to_end_latency_seconds.labels(kind=kind).observe(latency_seconds)


def record_ingest_latency(source: str, kind: str, latency_seconds: float) -> None:
    if latency_seconds >= 0:
        streaming_ingest_latency_seconds.labels(source=source, kind=kind).observe(latency_seconds)


def record_ingest_latency_budget_exceeded(source: str, kind: str) -> None:
    streaming_ingest_latency_budget_exceeded_total.labels(source=source, kind=kind).inc()


def update_bus_stats(queue_size: int, dedup_cache_size: int) -> None:
    streaming_event_bus_queue_size.set(max(0, queue_size))
    streaming_event_bus_dedup_cache_size.set(max(0, dedup_cache_size))


@contextmanager
def time_topology_patch() -> Iterator[None]:
    start = perf_counter()
    try:
        yield
    finally:
        streaming_topology_patch_duration_seconds.observe(perf_counter() - start)


@contextmanager
def time_dispatch_cycle() -> Iterator[None]:
    start = perf_counter()
    try:
        yield
    finally:
        streaming_subscriber_dispatch_duration_seconds.observe(perf_counter() - start)
