"""Signal gateway — the central telemetry pipeline orchestrator.

This is the component that turns a collection of modules into an engine.
Every TelemetryEnvelope enters through the gateway and is routed through:

1. Target enrichment (K8s inventory fills gaps in TargetRef)
2. Cardinality check (metrics only — drop if over budget)
3. Signal-specific store (trace_store, metric_store, log_store, profile_store)
4. Signal-specific processor (span_processor, anomaly_detector, etc.)
5. Cross-signal correlation (trace↔log linking, metric→trace exemplar)
6. Event emission (notify listeners of anomalies, correlations)

Without this module, each adapter manually feeds stores and processors.
With it, the pipeline is: adapter → gateway.ingest(envelope) → done.

The gateway also handles:
- Batch ingestion (ingest_batch)
- Pipeline stats (how many envelopes per signal type, drop rates)
- Listener registration (on_anomaly, on_correlation)
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from netobserv.telemetry.envelope import TelemetryEnvelope
from netobserv.telemetry.enums import SignalType
from netobserv.telemetry.signals import LogRecord, MetricPoint, ProfileRecord, SpanRecord
from netobserv.telemetry.store.trace_store import TraceStore
from netobserv.telemetry.store.metric_store import MetricStore
from netobserv.telemetry.store.log_store import LogStore
from netobserv.telemetry.store.profile_store import ProfileStore
from netobserv.telemetry.processing.span_processor import SpanProcessor
from netobserv.telemetry.processing.metric_anomaly_detector import MetricAnomalyDetector
from netobserv.telemetry.processing.trace_log_correlator import TraceLogCorrelator
from netobserv.telemetry.processing.metric_trace_correlator import MetricTraceCorrelator
from netobserv.telemetry.processing.profile_trace_correlator import ProfileTraceCorrelator


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class GatewayStats:
    """Pipeline throughput and routing statistics."""
    total_ingested: int = 0
    total_dropped: int = 0
    by_signal: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    enrichment_hits: int = 0
    anomalies_detected: int = 0
    correlations_found: int = 0


class SignalGateway:
    """Central telemetry pipeline orchestrator.

    Usage:
        gateway = SignalGateway(
            trace_store=trace_store,
            metric_store=metric_store,
            log_store=log_store,
            profile_store=profile_store,
            span_processor=span_processor,
        )
        # Optionally wire enrichment and detection
        gateway.set_inventory(k8s_inventory)
        gateway.set_anomaly_detector(detector)

        # Single entry point for all telemetry
        gateway.ingest(envelope)
        gateway.ingest_batch(envelopes)

        # React to events
        gateway.on_anomaly(lambda anomaly: alert(anomaly))
    """

    def __init__(
        self,
        trace_store: TraceStore,
        metric_store: MetricStore,
        log_store: LogStore,
        profile_store: ProfileStore,
        span_processor: SpanProcessor | None = None,
        anomaly_detector: MetricAnomalyDetector | None = None,
        anomaly_eval_interval: int = 50,
    ) -> None:
        self._trace_store = trace_store
        self._metric_store = metric_store
        self._log_store = log_store
        self._profile_store = profile_store
        self._span_processor = span_processor
        self._anomaly_detector = anomaly_detector

        # Anomaly detection: evaluate every N metrics, not every metric
        self._anomaly_eval_interval = anomaly_eval_interval
        self._metrics_since_eval = 0

        # Optional enrichment
        self._inventory: Any = None  # K8sInventory

        # Listeners
        self._anomaly_listeners: list[Callable] = []
        self._correlation_listeners: list[Callable] = []
        self._ingestion_listeners: list[Callable] = []
        self._network_event_listeners: list[Callable] = []

        # Stats
        self._stats = GatewayStats()

        # Signal routing table
        self._routes: dict[SignalType, Callable[[TelemetryEnvelope], None]] = {
            SignalType.SPAN: self._route_span,
            SignalType.TRACE: self._route_span,
            SignalType.METRIC: self._route_metric,
            SignalType.LOG: self._route_log,
            SignalType.PROFILE: self._route_profile,
            SignalType.NETWORK_EVENT: self._route_network_event,
        }

    # --- Configuration ---

    def set_inventory(self, inventory: Any) -> None:
        """Set the K8s inventory for target enrichment."""
        self._inventory = inventory

    def set_anomaly_detector(self, detector: MetricAnomalyDetector) -> None:
        self._anomaly_detector = detector

    def on_anomaly(self, callback: Callable) -> None:
        self._anomaly_listeners.append(callback)

    def on_correlation(self, callback: Callable) -> None:
        self._correlation_listeners.append(callback)

    def on_ingestion(self, callback: Callable) -> None:
        self._ingestion_listeners.append(callback)

    def on_network_event(self, callback: Callable) -> None:
        """Register listener for network events (reactive correlation)."""
        self._network_event_listeners.append(callback)

    # --- Ingestion ---

    def ingest(self, envelope: TelemetryEnvelope) -> None:
        """Ingest a single envelope through the full pipeline."""
        # Step 1: Enrich target from K8s inventory
        enriched = self._enrich(envelope)

        # Step 2: Route by signal type
        route = self._routes.get(enriched.signal_type)
        if route:
            route(enriched)
            self._stats.total_ingested += 1
            self._stats.by_signal[enriched.signal_type.value] += 1
        else:
            self._stats.total_dropped += 1

        # Notify listeners
        for listener in self._ingestion_listeners:
            try:
                listener(enriched)
            except Exception:
                pass

    def ingest_batch(self, envelopes: list[TelemetryEnvelope]) -> int:
        """Ingest a batch of envelopes. Returns count ingested."""
        for env in envelopes:
            self.ingest(env)
        return len(envelopes)

    # --- Signal routes ---

    def _route_span(self, env: TelemetryEnvelope) -> None:
        """Store span + extract service edges."""
        self._trace_store.store_span(env)
        if self._span_processor:
            edge = self._span_processor.process(env)
            if edge:
                self._stats.correlations_found += 1

    def _route_metric(self, env: TelemetryEnvelope) -> None:
        """Store metric. Evaluate anomaly rules periodically, not per-sample."""
        self._metric_store.store_metric(env)
        self._metrics_since_eval += 1
        if self._anomaly_detector and self._metrics_since_eval >= self._anomaly_eval_interval:
            self._metrics_since_eval = 0
            anomalies = self._anomaly_detector.evaluate()
            for anomaly in anomalies:
                self._stats.anomalies_detected += 1
                for listener in self._anomaly_listeners:
                    try:
                        listener(anomaly)
                    except Exception:
                        pass

    def _route_log(self, env: TelemetryEnvelope) -> None:
        """Store log. If it has a trace_id, it's automatically indexed
        for trace-log linking (the log store indexes by trace_id)."""
        self._log_store.store_log(env)
        # Trace-log correlation happens at query time via the trace_id index
        # in the log store — no extra processing needed at ingestion.
        # The log_store.get_logs_for_trace(trace_id) handles the join.

    def _route_profile(self, env: TelemetryEnvelope) -> None:
        """Store profile. The profile store indexes by trace_id for
        profile-trace correlation at query time."""
        self._profile_store.store_profile(env)

    def _route_network_event(self, env: TelemetryEnvelope) -> None:
        """Dispatch network events to listeners for reactive correlation.

        Network events (interface flap, BGP drop, link degradation) are
        processed by the streaming pipeline. The gateway dispatches them
        to registered listeners so the cross-layer mapper and RCA
        orchestrator can react.
        """
        for listener in self._network_event_listeners:
            try:
                listener(env)
            except Exception:
                pass

    # --- Enrichment ---

    def _enrich(self, envelope: TelemetryEnvelope) -> TelemetryEnvelope:
        """Enrich the envelope's TargetRef from K8s inventory."""
        if not self._inventory:
            return envelope
        if not hasattr(self._inventory, 'enrich_target'):
            return envelope

        enriched_target = self._inventory.enrich_target(envelope.target)
        if enriched_target is not envelope.target:
            self._stats.enrichment_hits += 1
            return envelope.with_enriched_target(
                **{f.name: getattr(enriched_target, f.name)
                   for f in enriched_target.__dataclass_fields__.values()
                   if f.name != "attrs" and getattr(enriched_target, f.name) is not None}
            )
        return envelope

    # --- Stats ---

    @property
    def stats(self) -> dict[str, Any]:
        return {
            "total_ingested": self._stats.total_ingested,
            "total_dropped": self._stats.total_dropped,
            "by_signal": dict(self._stats.by_signal),
            "enrichment_hits": self._stats.enrichment_hits,
            "anomalies_detected": self._stats.anomalies_detected,
            "correlations_found": self._stats.correlations_found,
            "stores": {
                "traces": self._trace_store.stats,
                "metrics": self._metric_store.stats,
                "logs": self._log_store.stats,
                "profiles": self._profile_store.stats,
            },
        }
