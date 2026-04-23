"""Trace-to-network correlator.

This is where the value lives: when a trace shows high latency or errors,
this module checks whether the network path between the affected services
has degraded links, interface flaps, BGP session drops, or route changes.

The correlation flow:
1. Identify the service edge(s) in a trace that are slow/failing
2. Map those edges to network paths via CrossLayerMapper
3. Check path segments for degradation
4. Produce a correlation result linking trace symptoms to network state

This is not a replacement for the existing network-layer correlation engine.
It's a bridge that lets app-layer symptoms point at network-layer causes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from netobserv.graph.cross_layer_mapper import (
    CrossLayerMapper,
    NetworkPathSegment,
    ServiceNetworkMapping,
)
from netobserv.graph.service_graph import ServiceGraph
from netobserv.telemetry.envelope import TelemetryEnvelope
from netobserv.telemetry.enums import SignalType, SpanStatusCode
from netobserv.telemetry.signals import SpanRecord
from netobserv.telemetry.store.trace_store import TraceStore, TraceView


@dataclass(slots=True)
class TraceNetworkCorrelation:
    """Result of correlating a trace to network state."""
    trace_id: str
    has_network_issues: bool = False
    affected_service_edges: list[str] = field(default_factory=list)
    degraded_segments: list[NetworkPathSegment] = field(default_factory=list)
    affected_devices: list[str] = field(default_factory=list)
    affected_interfaces: list[str] = field(default_factory=list)
    explanation: str = ""
    confidence: float = 0.0

    @property
    def summary(self) -> str:
        if not self.has_network_issues:
            return f"Trace {self.trace_id}: no network-layer issues detected"
        return (
            f"Trace {self.trace_id}: {len(self.degraded_segments)} degraded "
            f"network segment(s) on path(s) for "
            f"{len(self.affected_service_edges)} service edge(s)"
        )


@dataclass(slots=True)
class ServiceEdgeHealth:
    """Health of a service edge with network context."""
    source_service: str
    target_service: str
    trace_error_rate: float = 0.0
    trace_avg_latency_ms: float = 0.0
    network_path_degraded: bool = False
    degraded_segments: list[NetworkPathSegment] = field(default_factory=list)
    confidence: float = 0.0


class TraceNetworkCorrelator:
    """Correlates trace-layer symptoms to network-layer state.

    Usage:
        correlator = TraceNetworkCorrelator(
            trace_store=trace_store,
            service_graph=service_graph,
            cross_layer_mapper=mapper,
        )
        # Check if a slow trace has network issues
        result = correlator.correlate_trace("trace-abc")

        # Check if a service edge has network path degradation
        health = correlator.check_service_edge_health("api", "payment")

        # When a network event occurs, find affected traces
        affected = correlator.traces_affected_by_device("leaf-3")
    """

    def __init__(
        self,
        trace_store: TraceStore,
        service_graph: ServiceGraph,
        cross_layer_mapper: CrossLayerMapper,
    ) -> None:
        self._traces = trace_store
        self._graph = service_graph
        self._mapper = cross_layer_mapper

    def correlate_trace(self, trace_id: str) -> TraceNetworkCorrelation:
        """Check if a trace has network-layer issues on its service path.

        1. Get the trace from store
        2. Extract service edges from span parent/child
        3. Map each edge to network path
        4. Check for degradation
        """
        result = TraceNetworkCorrelation(trace_id=trace_id)
        trace = self._traces.get_trace(trace_id)
        if not trace:
            result.explanation = "Trace not found in store"
            return result

        # Extract service edges from the trace
        service_edges = self._extract_service_edges(trace)
        if not service_edges:
            result.explanation = "No cross-service edges in trace"
            return result

        # Check each edge for network degradation
        all_degraded: list[NetworkPathSegment] = []
        affected_edges: list[str] = []
        affected_devices: set[str] = set()
        affected_ifaces: set[str] = set()

        for src, tgt in service_edges:
            mapping = self._mapper.map_service_edge(src, tgt)
            if mapping.has_degraded_path:
                affected_edges.append(f"{src}→{tgt}")
                for seg in mapping.degraded_segments:
                    all_degraded.append(seg)
                    affected_devices.add(seg.device_id)
                    if seg.interface:
                        affected_ifaces.add(f"{seg.device_id}:{seg.interface}")

        result.affected_service_edges = affected_edges
        result.degraded_segments = all_degraded
        result.affected_devices = list(affected_devices)
        result.affected_interfaces = list(affected_ifaces)
        result.has_network_issues = len(all_degraded) > 0

        if result.has_network_issues:
            result.confidence = min(0.95, 0.5 + 0.15 * len(all_degraded))
            result.explanation = (
                f"Found {len(all_degraded)} degraded network segment(s) on "
                f"path(s) between services: {', '.join(affected_edges)}"
            )
        else:
            result.explanation = "All network paths between traced services are healthy"

        return result

    def check_service_edge_health(
        self, source: str, target: str
    ) -> ServiceEdgeHealth:
        """Check the health of a service edge including its network path."""
        health = ServiceEdgeHealth(
            source_service=source,
            target_service=target,
        )

        # Service-layer health from graph
        edge = self._graph.get_edge(source, target)
        if edge:
            health.trace_error_rate = edge.error_rate
            health.trace_avg_latency_ms = edge.avg_duration_ms

        # Network-layer health from mapper
        mapping = self._mapper.map_service_edge(source, target)
        health.network_path_degraded = mapping.has_degraded_path
        health.degraded_segments = mapping.degraded_segments

        # Confidence based on data available
        if edge and mapping.network_path:
            health.confidence = 0.8
        elif edge:
            health.confidence = 0.5
        else:
            health.confidence = 0.2

        return health

    def traces_affected_by_device(self, device_id: str) -> list[str]:
        """Find trace_ids for services whose traffic crosses a device."""
        services = self._mapper.get_services_affected_by_device(device_id)
        trace_ids: set[str] = set()
        for svc in services:
            traces = self._traces.get_traces_for_service(svc, limit=50)
            for t in traces:
                trace_ids.add(t.trace_id)
        return list(trace_ids)

    def traces_affected_by_link(self, device_id: str, interface: str) -> list[str]:
        """Find trace_ids for services whose traffic uses a specific link."""
        services = self._mapper.get_services_affected_by_link(device_id, interface)
        trace_ids: set[str] = set()
        for svc in services:
            traces = self._traces.get_traces_for_service(svc, limit=50)
            for t in traces:
                trace_ids.add(t.trace_id)
        return list(trace_ids)

    # --- Internal ---

    def _extract_service_edges(self, trace: TraceView) -> list[tuple[str, str]]:
        """Extract (source_service, target_service) pairs from a trace."""
        # Build span_id → service map
        span_services: dict[str, str] = {}
        for env in trace.spans:
            payload = env.payload
            if isinstance(payload, SpanRecord):
                svc = payload.service_name or env.target.service
                if svc:
                    span_services[payload.span_id] = svc

        # Find cross-service edges
        edges: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for env in trace.spans:
            payload = env.payload
            if isinstance(payload, SpanRecord) and payload.parent_span_id:
                child_svc = span_services.get(payload.span_id, "")
                parent_svc = span_services.get(payload.parent_span_id, "")
                if child_svc and parent_svc and child_svc != parent_svc:
                    edge = (parent_svc, child_svc)
                    if edge not in seen:
                        seen.add(edge)
                        edges.append(edge)
        return edges
