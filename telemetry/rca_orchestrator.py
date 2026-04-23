"""Unified RCA orchestrator — automatic multi-signal root cause investigation.

v9.1: Causal aggregation replaces flat ranking.

Changes from v9:
1. RootCause carries event_time (when the underlying event happened, not
   when the finding was created). This enables temporal precedence scoring.
2. CausalAggregator builds a directed causal graph over findings:
   - Temporal precedence: cause must precede symptom
   - Service overlap: cause and symptom share affected services
   - Device-to-service linking: degraded device affects the symptom's services
   - Shared trace_ids: both findings reference the same trace
3. Root nodes (findings with no incoming causal edges) are identified as
   primary causes. Non-root findings are demoted to symptoms.
4. Cross-signal agreement boosts cause confidence when multiple independent
   signals corroborate it.
5. Recursive dependency investigation follows the service graph to find
   the deepest failing service in the chain.
6. No networkx dependency — the causal graph is a simple adjacency list.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

from netobserv.telemetry.enums import Severity, SignalType
from netobserv.telemetry.signals import LogRecord, ProfileRecord, SpanRecord
from netobserv.telemetry.store.trace_store import TraceStore, TraceView
from netobserv.telemetry.store.metric_store import MetricStore
from netobserv.telemetry.store.log_store import LogStore
from netobserv.telemetry.store.profile_store import ProfileStore
from netobserv.telemetry.processing.metric_anomaly_detector import MetricAnomaly, MetricAnomalyDetector
from netobserv.telemetry.processing.metric_trace_correlator import MetricTraceCorrelator
from netobserv.telemetry.processing.trace_log_correlator import TraceLogCorrelator
from netobserv.telemetry.processing.profile_trace_correlator import ProfileTraceCorrelator
from netobserv.graph.service_graph import ServiceGraph
from netobserv.graph.cross_layer_mapper import CrossLayerMapper
from netobserv.graph.trace_network_correlator import TraceNetworkCorrelator


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class CauseCategory(str, Enum):
    NETWORK_DEGRADATION = "network_degradation"
    SERVICE_ERROR = "service_error"
    LATENCY_SPIKE = "latency_spike"
    CPU_SATURATION = "cpu_saturation"
    MEMORY_PRESSURE = "memory_pressure"
    DEPENDENCY_FAILURE = "dependency_failure"
    ERROR_LOG_CLUSTER = "error_log_cluster"
    UNKNOWN = "unknown"


# Causal ontology: which category can cause which.
# Key = cause category, value = set of possible symptom categories.
_CAUSAL_ONTOLOGY: dict[CauseCategory, set[CauseCategory]] = {
    # Network degradation → retries, timeouts, errors, dependency failures
    CauseCategory.NETWORK_DEGRADATION: {
        CauseCategory.SERVICE_ERROR,
        CauseCategory.LATENCY_SPIKE,
        CauseCategory.DEPENDENCY_FAILURE,
        CauseCategory.ERROR_LOG_CLUSTER,
        CauseCategory.CPU_SATURATION,      # retry storms burn CPU
    },
    # CPU saturation → slow responses, timeouts
    CauseCategory.CPU_SATURATION: {
        CauseCategory.LATENCY_SPIKE,
        CauseCategory.SERVICE_ERROR,
        CauseCategory.ERROR_LOG_CLUSTER,
    },
    # Memory pressure → OOM, crashes, errors
    CauseCategory.MEMORY_PRESSURE: {
        CauseCategory.SERVICE_ERROR,
        CauseCategory.LATENCY_SPIKE,       # GC pauses
        CauseCategory.ERROR_LOG_CLUSTER,
        CauseCategory.CPU_SATURATION,      # GC thrashing
    },
    # Dependency failure → caller errors, latency, logs
    CauseCategory.DEPENDENCY_FAILURE: {
        CauseCategory.SERVICE_ERROR,
        CauseCategory.LATENCY_SPIKE,
        CauseCategory.ERROR_LOG_CLUSTER,
    },
    # Service error → error logs (nearly always)
    CauseCategory.SERVICE_ERROR: {
        CauseCategory.ERROR_LOG_CLUSTER,
    },
    # Latency spike → timeout errors, error logs
    CauseCategory.LATENCY_SPIKE: {
        CauseCategory.SERVICE_ERROR,       # timeouts become errors
        CauseCategory.ERROR_LOG_CLUSTER,
    },
    # Error logs are terminal symptoms — they don't cause other categories
    CauseCategory.ERROR_LOG_CLUSTER: set(),
    CauseCategory.UNKNOWN: set(),
}


@dataclass
class RootCause:
    """A single ranked probable cause."""
    category: CauseCategory
    description: str
    confidence: float = 0.0
    evidence: list[str] = field(default_factory=list)
    affected_services: list[str] = field(default_factory=list)
    affected_devices: list[str] = field(default_factory=list)
    related_trace_ids: list[str] = field(default_factory=list)
    hotspot_function: str = ""
    # Temporal context
    event_time: datetime | None = None
    # Causal role after aggregation
    is_primary: bool = False
    caused_by: str = ""
    symptom_of_index: int = -1


@dataclass
class CausalChain:
    """A chain of causally linked findings."""
    primary_cause: RootCause
    symptoms: list[RootCause] = field(default_factory=list)
    chain_confidence: float = 0.0

    @property
    def depth(self) -> int:
        return 1 + len(self.symptoms)

    @property
    def all_services(self) -> set[str]:
        services: set[str] = set(self.primary_cause.affected_services)
        for s in self.symptoms:
            services.update(s.affected_services)
        return services


@dataclass
class RCAReport:
    """Complete root cause analysis report."""
    trigger: str
    investigated_at: datetime = field(default_factory=_utc_now)
    root_causes: list[RootCause] = field(default_factory=list)
    causal_chains: list[CausalChain] = field(default_factory=list)
    services_examined: list[str] = field(default_factory=list)
    traces_examined: int = 0
    logs_examined: int = 0
    network_paths_checked: int = 0
    profiles_checked: int = 0
    investigation_steps: list[str] = field(default_factory=list)

    @property
    def top_cause(self) -> RootCause | None:
        return self.root_causes[0] if self.root_causes else None

    @property
    def has_network_cause(self) -> bool:
        return any(c.category == CauseCategory.NETWORK_DEGRADATION for c in self.root_causes)

    @property
    def has_app_cause(self) -> bool:
        return any(c.category in {
            CauseCategory.SERVICE_ERROR, CauseCategory.CPU_SATURATION,
            CauseCategory.DEPENDENCY_FAILURE
        } for c in self.root_causes)

    @property
    def primary_causes(self) -> list[RootCause]:
        return [c for c in self.root_causes if c.is_primary]


class CausalAggregator:
    """Builds a causal graph over RCA findings and identifies root causes."""

    def __init__(self, mapper: CrossLayerMapper) -> None:
        self._mapper = mapper

    def aggregate(self, findings: list[RootCause]) -> tuple[list[RootCause], list[CausalChain]]:
        if not findings:
            return [], []
        if len(findings) == 1:
            findings[0].is_primary = True
            return findings, [CausalChain(primary_cause=findings[0],
                                          chain_confidence=findings[0].confidence)]

        for f in findings:
            if f.event_time is None:
                f.event_time = _utc_now()
        sorted_findings = sorted(findings, key=lambda f: f.event_time)

        n = len(sorted_findings)
        edges: dict[int, list[int]] = {i: [] for i in range(n)}
        incoming: dict[int, list[int]] = {i: [] for i in range(n)}

        for i in range(n):
            for j in range(i + 1, n):
                if self._is_causally_linked(sorted_findings[i], sorted_findings[j]):
                    edges[i].append(j)
                    incoming[j].append(i)

        root_indices = [i for i in range(n) if not incoming[i]]
        if not root_indices:
            root_indices = [0]

        for i in root_indices:
            sorted_findings[i].is_primary = True

        for i in root_indices:
            symptom_count = len(edges[i])
            if symptom_count > 0:
                boost = min(0.25, 0.08 * symptom_count)
                sorted_findings[i].confidence = min(0.99, sorted_findings[i].confidence + boost)
                for j in edges[i]:
                    sorted_findings[j].caused_by = sorted_findings[i].description
                    sorted_findings[j].symptom_of_index = i

        self._apply_cross_signal_agreement(sorted_findings)
        chains = self._extract_chains(sorted_findings, edges, root_indices)
        sorted_findings.sort(key=lambda f: (not f.is_primary, -f.confidence))
        return sorted_findings, chains

    def _is_causally_linked(self, cause: RootCause, symptom: RootCause) -> bool:
        if cause.event_time and symptom.event_time:
            if cause.event_time > symptom.event_time:
                return False

        has_service_overlap = bool(
            set(cause.affected_services) & set(symptom.affected_services)
        )
        has_shared_traces = bool(
            set(cause.related_trace_ids) & set(symptom.related_trace_ids)
        ) if cause.related_trace_ids and symptom.related_trace_ids else False

        has_device_to_service = False
        if cause.affected_devices and symptom.affected_services:
            for device in cause.affected_devices:
                affected = self._mapper.get_services_affected_by_device(device)
                if set(affected) & set(symptom.affected_services):
                    has_device_to_service = True
                    break

        # Full causal ontology — which categories can cause which
        category_compatible = _CAUSAL_ONTOLOGY.get(cause.category, set())
        is_compatible = symptom.category in category_compatible

        structural_link = has_service_overlap or has_shared_traces or has_device_to_service
        return structural_link and (is_compatible or has_shared_traces)

    def _apply_cross_signal_agreement(self, findings: list[RootCause]) -> None:
        """Boost confidence when independent signal types corroborate.

        Requires:
        - Same service
        - Different categories (independent signals)
        - Temporal proximity (within 5 min of each other)
        - Diminishing returns: 1st agreement +0.08, 2nd +0.05, 3rd+ +0.03
        """
        if len(findings) < 2:
            return

        PROXIMITY_SECONDS = 300.0  # 5 minutes
        DIMINISHING_BONUSES = [0.08, 0.05, 0.03]  # 1st, 2nd, 3rd+ agreement

        # Group by affected service
        service_findings: dict[str, list[int]] = {}
        for i, f in enumerate(findings):
            for svc in f.affected_services:
                if svc not in service_findings:
                    service_findings[svc] = []
                service_findings[svc].append(i)

        # Track which findings have already been boosted (no double-dip)
        boosted: set[int] = set()

        for svc, indices in service_findings.items():
            if len(indices) < 2:
                continue

            # Filter to findings within temporal proximity of each other
            proximate: list[int] = []
            for i in indices:
                t_i = findings[i].event_time
                if t_i is None:
                    continue
                # Check if this finding is within PROXIMITY_SECONDS of any other
                for j in indices:
                    if i == j:
                        continue
                    t_j = findings[j].event_time
                    if t_j is None:
                        continue
                    if abs((t_i - t_j).total_seconds()) <= PROXIMITY_SECONDS:
                        proximate.append(i)
                        break

            if len(proximate) < 2:
                continue

            # Count distinct categories among proximate findings
            categories = {findings[i].category for i in proximate}
            if len(categories) < 2:
                continue

            # Apply diminishing bonus per finding
            agreements = len(categories) - 1  # number of corroborating signals
            for i in proximate:
                if i in boosted:
                    continue
                bonus_idx = min(agreements - 1, len(DIMINISHING_BONUSES) - 1)
                bonus = DIMINISHING_BONUSES[bonus_idx]
                findings[i].confidence = min(0.99, findings[i].confidence + bonus)
                boosted.add(i)

    def _extract_chains(self, findings, edges, root_indices):
        chains = []
        for root_idx in root_indices:
            chain = CausalChain(primary_cause=findings[root_idx],
                                chain_confidence=findings[root_idx].confidence)
            visited = {root_idx}
            queue = list(edges[root_idx])
            while queue:
                j = queue.pop(0)
                if j in visited:
                    continue
                visited.add(j)
                chain.symptoms.append(findings[j])
                queue.extend(edges.get(j, []))
            chains.append(chain)
        return chains


class RCAOrchestrator:
    """Automatic multi-signal root cause investigation."""

    def __init__(
        self,
        trace_store: TraceStore,
        metric_store: MetricStore,
        log_store: LogStore,
        profile_store: ProfileStore,
        service_graph: ServiceGraph,
        cross_layer_mapper: CrossLayerMapper,
        anomaly_detector: MetricAnomalyDetector | None = None,
        max_dependency_depth: int = 3,
    ) -> None:
        self._traces = trace_store
        self._metrics = metric_store
        self._logs = log_store
        self._profiles = profile_store
        self._graph = service_graph
        self._mapper = cross_layer_mapper
        self._detector = anomaly_detector
        self._max_depth = max_dependency_depth

        self._metric_trace = MetricTraceCorrelator(metric_store, trace_store)
        self._trace_log = TraceLogCorrelator(trace_store, log_store)
        self._profile_trace = ProfileTraceCorrelator(profile_store, trace_store)
        self._trace_network = TraceNetworkCorrelator(
            trace_store, service_graph, cross_layer_mapper
        )
        self._aggregator = CausalAggregator(cross_layer_mapper)

    def investigate_service(self, service: str, window_seconds: float = 300.0) -> RCAReport:
        report = RCAReport(trigger=f"service:{service}")
        report.services_examined.append(service)
        report.investigation_steps.append(f"Starting investigation of service '{service}'")

        anomalies = self._check_service_anomalies(service, report)
        traces = self._traces.get_traces_for_service(service, limit=20)
        report.traces_examined = len(traces)
        report.investigation_steps.append(f"Examined {len(traces)} recent traces")

        error_traces = [t for t in traces if t.has_errors]
        for trace in error_traces[:5]:
            self._check_trace_network(trace, report)

        self._check_error_logs(service, report)
        self._check_profiles(service, report)
        self._check_dependencies_recursive(service, report, visited=set(), depth=0)

        report.root_causes, report.causal_chains = self._aggregator.aggregate(report.root_causes)
        return report

    def investigate_anomaly(self, anomaly: MetricAnomaly) -> RCAReport:
        report = RCAReport(trigger=f"anomaly:{anomaly.metric_name}")
        service = anomaly.service
        if service:
            report.services_examined.append(service)

        report.investigation_steps.append(
            f"Anomaly: {anomaly.metric_name} = {anomaly.current_value:.3f} "
            f"({anomaly.anomaly_type})"
        )

        if anomaly.has_exemplars:
            correlation = self._metric_trace.correlate_anomaly(anomaly)
            if correlation.has_traces:
                report.investigation_steps.append(
                    f"Followed exemplar to {len(correlation.related_traces)} trace(s)"
                )
                for trace in correlation.related_traces[:3]:
                    report.traces_examined += 1
                    self._check_trace_network(trace, report)

        if service:
            self._check_error_logs(service, report)
            self._check_profiles(service, report)
            self._check_dependencies_recursive(service, report, visited=set(), depth=0)

        if not report.root_causes:
            report.root_causes.append(RootCause(
                category=CauseCategory.LATENCY_SPIKE if "duration" in anomaly.metric_name
                else CauseCategory.SERVICE_ERROR,
                description=anomaly.explanation,
                confidence=0.4,
                evidence=[f"{anomaly.anomaly_type}: {anomaly.metric_name}"],
                affected_services=[service] if service else [],
            ))

        report.root_causes, report.causal_chains = self._aggregator.aggregate(report.root_causes)
        return report

    def investigate_trace(self, trace_id: str) -> RCAReport:
        report = RCAReport(trigger=f"trace:{trace_id}")
        trace = self._traces.get_trace(trace_id)
        if not trace:
            report.investigation_steps.append(f"Trace {trace_id} not found")
            return report

        report.traces_examined = 1
        report.services_examined = list(trace.services)
        report.investigation_steps.append(
            f"Trace {trace_id}: {trace.span_count} spans, "
            f"{trace.service_count} services, errors={trace.has_errors}"
        )

        self._check_trace_network(trace, report)
        for service in trace.services:
            self._check_error_logs(service, report, trace_id=trace_id)

        profile_result = self._profile_trace.profiles_for_trace(trace_id, profile_type="cpu")
        if profile_result.has_profiles:
            report.profiles_checked += len(profile_result.profiles)
            payload = profile_result.profiles[0].payload
            if isinstance(payload, ProfileRecord) and payload.top_functions:
                top = payload.top_functions[0]
                report.root_causes.append(RootCause(
                    category=CauseCategory.CPU_SATURATION,
                    description=f"CPU hotspot: {top.function_name} ({top.percentage:.1f}% self time)",
                    confidence=0.6 if top.percentage > 50 else 0.35,
                    evidence=[f"Profile: {top.function_name} at {top.percentage}%"],
                    related_trace_ids=[trace_id],
                    hotspot_function=top.function_name,
                ))

        report.root_causes, report.causal_chains = self._aggregator.aggregate(report.root_causes)
        return report

    # --- Internal investigation steps ---

    def _check_service_anomalies(self, service, report):
        if not self._detector:
            return []
        anomalies = self._detector.evaluate()
        service_anomalies = [a for a in anomalies if a.service == service]
        for a in service_anomalies:
            report.investigation_steps.append(f"Anomaly detected: {a.metric_name} ({a.anomaly_type})")
            event_time = getattr(a, 'detected_at', _utc_now())
            if "duration" in a.metric_name or "latency" in a.metric_name:
                report.root_causes.append(RootCause(
                    category=CauseCategory.LATENCY_SPIKE,
                    description=f"Latency spike on {a.metric_name}: {a.current_value:.3f}",
                    confidence=0.6, evidence=[a.explanation],
                    affected_services=[service],
                    related_trace_ids=a.exemplar_trace_ids[:3],
                    event_time=event_time,
                ))
            elif "error" in a.metric_name:
                report.root_causes.append(RootCause(
                    category=CauseCategory.SERVICE_ERROR,
                    description=f"Error rate increase on {a.metric_name}",
                    confidence=0.65, evidence=[a.explanation],
                    affected_services=[service], event_time=event_time,
                ))
        return service_anomalies

    def _check_trace_network(self, trace, report):
        result = self._trace_network.correlate_trace(trace.trace_id)
        report.network_paths_checked += 1
        if result.has_network_issues:
            report.investigation_steps.append(
                f"Network issue on trace {trace.trace_id}: "
                f"{len(result.degraded_segments)} degraded segment(s)"
            )
            event_time = trace.started_at or _utc_now()
            if trace.started_at:
                event_time = trace.started_at - timedelta(seconds=1)
            report.root_causes.append(RootCause(
                category=CauseCategory.NETWORK_DEGRADATION,
                description=result.explanation,
                confidence=result.confidence,
                evidence=[f"Degraded: {s.device_id}:{s.interface}" for s in result.degraded_segments],
                affected_services=list(trace.services),
                affected_devices=result.affected_devices,
                related_trace_ids=[trace.trace_id],
                event_time=event_time,
            ))

    def _check_error_logs(self, service, report, trace_id=None):
        if trace_id:
            log_result = self._trace_log.logs_for_trace(trace_id)
            error_logs = [
                env for env in log_result.logs
                if isinstance(env.payload, LogRecord)
                and env.payload.severity in {Severity.ERROR, Severity.FATAL}
            ]
        else:
            error_logs = self._logs.get_error_logs(service, limit=10)

        report.logs_examined += len(error_logs)
        if error_logs:
            bodies = []
            earliest = None
            for env in error_logs[:5]:
                payload = env.payload
                if isinstance(payload, LogRecord):
                    bodies.append(payload.body[:100])
                if env.observed_at and (earliest is None or env.observed_at < earliest):
                    earliest = env.observed_at

            report.investigation_steps.append(f"Found {len(error_logs)} error log(s) for '{service}'")
            report.root_causes.append(RootCause(
                category=CauseCategory.ERROR_LOG_CLUSTER,
                description=f"{len(error_logs)} error logs in service '{service}'",
                confidence=0.45, evidence=bodies[:3],
                affected_services=[service],
                related_trace_ids=[trace_id] if trace_id else [],
                event_time=earliest,
            ))

    def _check_profiles(self, service, report):
        profiles = self._profiles.get_profiles_for_service(service, profile_type="cpu", limit=1)
        report.profiles_checked += len(profiles)
        if profiles:
            payload = profiles[0].payload
            if isinstance(payload, ProfileRecord) and payload.top_functions:
                top = payload.top_functions[0]
                if top.percentage > 40:
                    report.investigation_steps.append(
                        f"CPU hotspot in '{service}': {top.function_name} ({top.percentage:.1f}%)")
                    report.root_causes.append(RootCause(
                        category=CauseCategory.CPU_SATURATION,
                        description=f"CPU saturation in '{service}': {top.function_name} at {top.percentage:.1f}%",
                        confidence=0.55 if top.percentage > 60 else 0.35,
                        evidence=[f"{f.function_name}: {f.percentage:.1f}%" for f in payload.top_functions[:3]],
                        affected_services=[service],
                        hotspot_function=top.function_name,
                        event_time=profiles[0].observed_at,
                    ))

    def _check_dependencies(self, service, report):
        """Non-recursive dependency check (backward compatible)."""
        deps = self._graph.dependencies(service)
        for edge in deps:
            if edge.error_rate > 0.1:
                report.investigation_steps.append(
                    f"Dependency '{edge.target_service}' has {edge.error_rate:.0%} error rate")
                report.root_causes.append(RootCause(
                    category=CauseCategory.DEPENDENCY_FAILURE,
                    description=f"Downstream service '{edge.target_service}' error rate: {edge.error_rate:.0%} ({edge.error_count}/{edge.call_count} calls)",
                    confidence=min(0.85, 0.5 + edge.error_rate),
                    evidence=[f"{edge.edge_key}: {edge.error_rate:.0%} errors, avg latency {edge.avg_duration_ms:.0f}ms"],
                    affected_services=[service, edge.target_service],
                    event_time=edge.last_seen,
                ))

    def _check_dependencies_recursive(self, service, report, visited, depth):
        """Recursive dependency investigation — follows the chain.

        Deduplicates by target service: if a DEPENDENCY_FAILURE for the same
        target_service already exists, keeps the one with higher confidence.
        """
        if depth >= self._max_depth or service in visited:
            return
        visited.add(service)
        deps = self._graph.dependencies(service)
        for edge in deps:
            if edge.error_rate > 0.1:
                target = edge.target_service
                new_confidence = min(0.85, 0.5 + edge.error_rate)

                # Dedup: check if we already have a finding for this target
                existing_idx = None
                for idx, rc in enumerate(report.root_causes):
                    if (rc.category == CauseCategory.DEPENDENCY_FAILURE
                            and target in rc.affected_services
                            and target in rc.description):
                        existing_idx = idx
                        break

                if existing_idx is not None:
                    existing = report.root_causes[existing_idx]
                    if new_confidence > existing.confidence:
                        # Replace with higher-confidence finding
                        existing.confidence = new_confidence
                        existing.affected_services = list(set(existing.affected_services + [service]))
                        existing.evidence.append(
                            f"{edge.edge_key}: {edge.error_rate:.0%} errors (via {service})")
                    # Skip adding a new finding
                else:
                    report.investigation_steps.append(
                        f"{'  ' * depth}Dependency '{target}' has "
                        f"{edge.error_rate:.0%} error rate (depth={depth})")
                    report.root_causes.append(RootCause(
                        category=CauseCategory.DEPENDENCY_FAILURE,
                        description=f"Downstream service '{target}' error rate: {edge.error_rate:.0%} ({edge.error_count}/{edge.call_count} calls)",
                        confidence=new_confidence,
                        evidence=[f"{edge.edge_key}: {edge.error_rate:.0%} errors, avg latency {edge.avg_duration_ms:.0f}ms"],
                        affected_services=[service, target],
                        event_time=edge.last_seen,
                    ))

                if target not in report.services_examined:
                    report.services_examined.append(target)
                self._check_dependencies_recursive(target, report, visited, depth + 1)
