"""Telemetry query service — query interface for all signals.

Exposes the stores, correlators, and RCA orchestrator through a clean
query API. This is the layer that an HTTP router (FastAPI, Flask) or
CLI would call.

Query patterns:
- Traces: by trace_id, by service, by time window
- Metrics: by name, golden signals, exemplar lookup
- Logs: by trace_id, by service, text search, severity filter
- Profiles: by service, by trace_id, hotspots
- Correlation jumps: metric→trace, trace→logs, trace→profile, trace→network
- RCA: investigate service, investigate trace, investigate anomaly
- Topology: service graph, dependencies, impact radius
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from netobserv.telemetry.store.trace_store import TraceStore, TraceView
from netobserv.telemetry.store.metric_store import MetricStore, GoldenSignals
from netobserv.telemetry.store.log_store import LogStore
from netobserv.telemetry.store.profile_store import ProfileStore
from netobserv.telemetry.processing.span_processor import SpanProcessor, ServiceEdge
from netobserv.telemetry.processing.metric_anomaly_detector import MetricAnomalyDetector, MetricAnomaly
from netobserv.telemetry.processing.metric_trace_correlator import MetricTraceCorrelator
from netobserv.telemetry.processing.trace_log_correlator import TraceLogCorrelator
from netobserv.telemetry.processing.profile_trace_correlator import ProfileTraceCorrelator
from netobserv.telemetry.rca_orchestrator import RCAOrchestrator, RCAReport
from netobserv.graph.service_graph import ServiceGraph
from netobserv.graph.cross_layer_mapper import CrossLayerMapper
from netobserv.graph.trace_network_correlator import TraceNetworkCorrelator


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class QueryResult:
    """Generic query result wrapper."""
    success: bool = True
    data: Any = None
    error: str = ""
    count: int = 0
    query_time_ms: float = 0.0


class TelemetryQueryService:
    """Unified query service for all telemetry signals.

    This is the query API layer. It does NOT implement HTTP — it provides
    the methods that an HTTP router would call.

    Usage:
        query_svc = TelemetryQueryService(
            trace_store, metric_store, log_store, profile_store,
            span_processor, service_graph, cross_layer_mapper,
        )
        # Trace queries
        trace = query_svc.get_trace("trace-abc")
        traces = query_svc.traces_for_service("api")

        # Correlation jumps
        result = query_svc.jump_metric_to_trace("http_duration", service="api")
        result = query_svc.jump_trace_to_logs("trace-abc")

        # RCA
        report = query_svc.investigate_service("payment-svc")
    """

    def __init__(
        self,
        trace_store: TraceStore,
        metric_store: MetricStore,
        log_store: LogStore,
        profile_store: ProfileStore,
        span_processor: SpanProcessor | None = None,
        service_graph: ServiceGraph | None = None,
        cross_layer_mapper: CrossLayerMapper | None = None,
        anomaly_detector: MetricAnomalyDetector | None = None,
    ) -> None:
        self._traces = trace_store
        self._metrics = metric_store
        self._logs = log_store
        self._profiles = profile_store
        self._span_processor = span_processor
        self._graph = service_graph or ServiceGraph()
        self._mapper = cross_layer_mapper or CrossLayerMapper()
        self._detector = anomaly_detector

        # Build correlators
        self._metric_trace = MetricTraceCorrelator(metric_store, trace_store)
        self._trace_log = TraceLogCorrelator(trace_store, log_store)
        self._profile_trace = ProfileTraceCorrelator(profile_store, trace_store)
        self._trace_network = TraceNetworkCorrelator(
            trace_store, self._graph, self._mapper
        )
        self._rca = RCAOrchestrator(
            trace_store, metric_store, log_store, profile_store,
            self._graph, self._mapper, anomaly_detector,
        )

    # ================================================================
    # Trace queries
    # ================================================================

    def get_trace(self, trace_id: str) -> QueryResult:
        trace = self._traces.get_trace(trace_id)
        if not trace:
            return QueryResult(success=False, error=f"Trace {trace_id} not found")
        return QueryResult(data=self._serialize_trace(trace), count=1)

    def traces_for_service(self, service: str, limit: int = 20) -> QueryResult:
        traces = self._traces.get_traces_for_service(service, limit=limit)
        return QueryResult(
            data=[self._serialize_trace(t) for t in traces],
            count=len(traces),
        )

    def traces_in_window(self, start: datetime, end: datetime,
                         limit: int = 50) -> QueryResult:
        traces = self._traces.get_traces_in_window(start, end, limit=limit)
        return QueryResult(
            data=[self._serialize_trace(t) for t in traces],
            count=len(traces),
        )

    # ================================================================
    # Metric queries
    # ================================================================

    def golden_signals(self, service: str, window_seconds: float = 60.0) -> QueryResult:
        signals = self._metrics.golden_signals(service, window_seconds=window_seconds)
        return QueryResult(data={
            "service": signals.service,
            "request_rate": signals.request_rate,
            "error_rate": signals.error_rate,
            "latency_avg_ms": signals.latency_avg_ms,
            "latency_p50_ms": signals.latency_p50_ms,
            "latency_p99_ms": signals.latency_p99_ms,
            "saturation": signals.saturation,
        })

    def metric_series(self, metric_name: str, service: str = "") -> QueryResult:
        if service:
            series_list = [
                s for s in self._metrics.get_series_for_service(service)
                if s.metric_name == metric_name
            ]
        else:
            series_list = self._metrics.get_series_by_name(metric_name)
        return QueryResult(
            data=[{
                "metric_name": s.metric_name,
                "labels": s.labels,
                "sample_count": s.sample_count,
                "last_value": s.last_value,
                "exemplar_count": len(s.exemplars),
            } for s in series_list],
            count=len(series_list),
        )

    def detect_anomalies(self) -> QueryResult:
        if not self._detector:
            return QueryResult(success=False, error="No anomaly detector configured")
        anomalies = self._detector.evaluate()
        return QueryResult(
            data=[{
                "metric_name": a.metric_name,
                "anomaly_type": a.anomaly_type,
                "current_value": a.current_value,
                "threshold": a.threshold,
                "deviation_factor": a.deviation_factor,
                "severity": a.severity,
                "service": a.service,
                "exemplar_trace_ids": a.exemplar_trace_ids,
                "explanation": a.explanation,
            } for a in anomalies],
            count=len(anomalies),
        )

    # ================================================================
    # Log queries
    # ================================================================

    def logs_for_trace(self, trace_id: str) -> QueryResult:
        result = self._trace_log.logs_for_trace(trace_id)
        return QueryResult(
            data=[self._serialize_log(env) for env in result.logs],
            count=len(result.logs),
        )

    def search_logs(self, body_contains: str = "", service: str = "",
                    severity: str = "", limit: int = 50) -> QueryResult:
        from netobserv.telemetry.store.log_store import LogSearchQuery
        from netobserv.telemetry.enums import Severity
        sev = None
        if severity:
            try:
                sev = Severity(severity.lower())
            except ValueError:
                pass
        query = LogSearchQuery(
            body_contains=body_contains or None,
            service=service or None,
            min_severity=sev,
            limit=limit,
        )
        results = self._logs.search(query)
        return QueryResult(
            data=[self._serialize_log(env) for env in results],
            count=len(results),
        )

    # ================================================================
    # Profile queries
    # ================================================================

    def profiles_for_service(self, service: str, profile_type: str = "cpu",
                             limit: int = 10) -> QueryResult:
        profiles = self._profiles.get_profiles_for_service(
            service, profile_type=profile_type, limit=limit
        )
        return QueryResult(
            data=[self._serialize_profile(env) for env in profiles],
            count=len(profiles),
        )

    def hotspots(self, service: str, profile_type: str = "cpu") -> QueryResult:
        hotspots = self._profile_trace.hotspots_for_service(service, profile_type)
        return QueryResult(
            data=[{
                "function_name": h.function_name,
                "self_value": h.self_value,
                "total_value": h.total_value,
                "percentage": h.percentage,
            } for h in hotspots],
            count=len(hotspots),
        )

    # ================================================================
    # Correlation jumps
    # ================================================================

    def jump_metric_to_trace(self, metric_name: str, service: str = "",
                              limit: int = 5) -> QueryResult:
        traces = self._metric_trace.traces_for_metric(
            metric_name, service=service, limit=limit
        )
        return QueryResult(
            data=[self._serialize_trace(t) for t in traces],
            count=len(traces),
        )

    def jump_trace_to_logs(self, trace_id: str) -> QueryResult:
        return self.logs_for_trace(trace_id)

    def jump_trace_to_network(self, trace_id: str) -> QueryResult:
        result = self._trace_network.correlate_trace(trace_id)
        return QueryResult(data={
            "trace_id": result.trace_id,
            "has_network_issues": result.has_network_issues,
            "affected_service_edges": result.affected_service_edges,
            "affected_devices": result.affected_devices,
            "degraded_segments": [
                {"device": s.device_id, "interface": s.interface,
                 "peer_device": s.peer_device_id}
                for s in result.degraded_segments
            ],
            "confidence": result.confidence,
            "explanation": result.explanation,
        })

    def jump_trace_to_profile(self, trace_id: str,
                               profile_type: str = "cpu") -> QueryResult:
        result = self._profile_trace.profiles_for_trace(trace_id, profile_type)
        return QueryResult(
            data={
                "method": result.method,
                "confidence": result.confidence,
                "profiles": [self._serialize_profile(env) for env in result.profiles],
            },
            count=len(result.profiles),
        )

    # ================================================================
    # RCA
    # ================================================================

    def investigate_service(self, service: str) -> QueryResult:
        report = self._rca.investigate_service(service)
        return QueryResult(data=self._serialize_rca(report))

    def investigate_trace(self, trace_id: str) -> QueryResult:
        report = self._rca.investigate_trace(trace_id)
        return QueryResult(data=self._serialize_rca(report))

    # ================================================================
    # Topology
    # ================================================================

    def service_dependencies(self, service: str) -> QueryResult:
        deps = self._graph.dependencies(service)
        return QueryResult(
            data=[{
                "source": e.source_service,
                "target": e.target_service,
                "edge_type": e.edge_type,
                "call_count": e.call_count,
                "error_rate": e.error_rate,
                "retry_rate": e.retry_rate,
                "avg_duration_ms": e.avg_duration_ms,
            } for e in deps],
            count=len(deps),
        )

    def service_impact(self, service: str) -> QueryResult:
        affected = self._graph.impact_radius(service)
        return QueryResult(data=affected, count=len(affected))

    def all_services(self) -> QueryResult:
        services = self._graph.all_services()
        nodes = self._graph.get_node
        return QueryResult(
            data=[{
                "service": svc,
                "node_data": {
                    "span_count": n.node_data.span_count if n and n.node_data else 0,
                    "error_count": n.node_data.error_count if n and n.node_data else 0,
                } if (n := self._graph.get_node(svc)) else None,
            } for svc in services],
            count=len(services),
        )

    # ================================================================
    # Platform stats
    # ================================================================

    def platform_stats(self) -> QueryResult:
        return QueryResult(data={
            "traces": self._traces.stats,
            "metrics": self._metrics.stats,
            "logs": self._logs.stats,
            "profiles": self._profiles.stats,
            "service_graph": self._graph.stats,
            "cross_layer_mapper": self._mapper.stats,
        })

    # ================================================================
    # Serialization helpers
    # ================================================================

    def _serialize_trace(self, trace: TraceView) -> dict[str, Any]:
        return {
            "trace_id": trace.trace_id,
            "span_count": trace.span_count,
            "service_count": trace.service_count,
            "services": list(trace.services),
            "has_errors": trace.has_errors,
            "duration_ms": trace.duration_ms,
            "started_at": trace.started_at.isoformat() if trace.started_at else None,
        }

    def _serialize_log(self, env: Any) -> dict[str, Any]:
        from netobserv.telemetry.signals import LogRecord as LR
        payload = env.payload
        if isinstance(payload, LR):
            return {
                "body": payload.body[:500],
                "severity": payload.severity.value if hasattr(payload.severity, 'value') else str(payload.severity),
                "trace_id": payload.trace_id,
                "service": env.target.service,
                "observed_at": env.observed_at.isoformat() if env.observed_at else None,
            }
        return {"raw": str(env)}

    def _serialize_profile(self, env: Any) -> dict[str, Any]:
        from netobserv.telemetry.signals import ProfileRecord as PR
        payload = env.payload
        if isinstance(payload, PR):
            return {
                "profile_type": payload.profile_type,
                "sample_count": payload.sample_count,
                "duration_ms": payload.duration_ms,
                "trace_id": payload.trace_id,
                "top_functions": [
                    {"name": f.function_name, "percentage": f.percentage}
                    for f in payload.top_functions[:5]
                ],
                "service": env.target.service,
            }
        return {"raw": str(env)}

    def _serialize_rca(self, report: RCAReport) -> dict[str, Any]:
        return {
            "trigger": report.trigger,
            "investigated_at": report.investigated_at.isoformat(),
            "root_causes": [
                {
                    "category": c.category.value,
                    "description": c.description,
                    "confidence": c.confidence,
                    "is_primary": c.is_primary,
                    "caused_by": c.caused_by,
                    "evidence": c.evidence,
                    "affected_services": c.affected_services,
                    "affected_devices": c.affected_devices,
                    "related_trace_ids": c.related_trace_ids,
                    "hotspot_function": c.hotspot_function,
                    "event_time": c.event_time.isoformat() if c.event_time else None,
                }
                for c in report.root_causes
            ],
            "causal_chains": [
                {
                    "primary_cause": ch.primary_cause.description,
                    "depth": ch.depth,
                    "chain_confidence": ch.chain_confidence,
                    "symptoms": [s.description for s in ch.symptoms],
                    "all_services": sorted(ch.all_services),
                }
                for ch in report.causal_chains
            ],
            "primary_cause_count": len(report.primary_causes),
            "investigation_steps": report.investigation_steps,
            "traces_examined": report.traces_examined,
            "logs_examined": report.logs_examined,
            "network_paths_checked": report.network_paths_checked,
            "profiles_checked": report.profiles_checked,
        }
