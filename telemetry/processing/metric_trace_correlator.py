"""Metric-to-trace correlator — the exemplar bridge.

When a metric anomaly is detected (latency spike, error rate increase),
this module follows the exemplar trace IDs to find the specific
request traces that caused the anomaly.

The correlation chain:
    metric anomaly detected
    → get exemplar trace_ids from the anomalous metric
    → look up those traces in the trace store
    → return the traces with their service graph context

If no exemplars are available, falls back to time-window + target
matching (weaker correlation).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from netobserv.telemetry.processing.metric_anomaly_detector import MetricAnomaly
from netobserv.telemetry.store.metric_store import MetricStore
from netobserv.telemetry.store.trace_store import TraceStore, TraceView


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class MetricTraceCorrelation:
    """Result of jumping from a metric anomaly to related traces."""
    anomaly: MetricAnomaly
    method: str                      # "exemplar", "time_window", "none"
    related_traces: list[TraceView] = field(default_factory=list)
    confidence: float = 0.0
    explanation: str = ""

    @property
    def has_traces(self) -> bool:
        return len(self.related_traces) > 0

    @property
    def trace_ids(self) -> list[str]:
        return [t.trace_id for t in self.related_traces]


class MetricTraceCorrelator:
    """Jumps from metric anomalies to related traces.

    Usage:
        correlator = MetricTraceCorrelator(metric_store, trace_store)

        # From a detected anomaly
        anomaly = detector.evaluate()[0]
        result = correlator.correlate_anomaly(anomaly)
        if result.has_traces:
            for trace in result.related_traces:
                print(f"Anomaly caused by trace {trace.trace_id}")

        # Direct exemplar lookup
        traces = correlator.traces_for_metric("http_duration", service="api")
    """

    def __init__(
        self,
        metric_store: MetricStore,
        trace_store: TraceStore,
        fallback_window_seconds: float = 60.0,
    ) -> None:
        self._metrics = metric_store
        self._traces = trace_store
        self._fallback_window = timedelta(seconds=fallback_window_seconds)

    def correlate_anomaly(self, anomaly: MetricAnomaly) -> MetricTraceCorrelation:
        """Follow an anomaly to related traces.

        Priority:
        1. Exemplar trace IDs (direct, high confidence)
        2. Time-window + service matching (indirect, lower confidence)
        """
        # Strategy 1: Exemplar-based (best)
        if anomaly.has_exemplars:
            traces = self._lookup_exemplar_traces(anomaly.exemplar_trace_ids)
            if traces:
                return MetricTraceCorrelation(
                    anomaly=anomaly,
                    method="exemplar",
                    related_traces=traces,
                    confidence=0.95,
                    explanation=(
                        f"Found {len(traces)} trace(s) via exemplars on "
                        f"{anomaly.metric_name}"
                    ),
                )

        # Strategy 2: Time-window + service matching (fallback)
        if anomaly.service:
            traces = self._lookup_by_service_window(
                anomaly.service, anomaly.detected_at
            )
            if traces:
                return MetricTraceCorrelation(
                    anomaly=anomaly,
                    method="time_window",
                    related_traces=traces,
                    confidence=0.45,
                    explanation=(
                        f"Found {len(traces)} trace(s) for service "
                        f"'{anomaly.service}' in time window (no exemplars)"
                    ),
                )

        return MetricTraceCorrelation(
            anomaly=anomaly,
            method="none",
            confidence=0.0,
            explanation="No related traces found",
        )

    def correlate_anomalies(
        self, anomalies: list[MetricAnomaly]
    ) -> list[MetricTraceCorrelation]:
        """Correlate multiple anomalies to traces."""
        return [self.correlate_anomaly(a) for a in anomalies]

    def traces_for_metric(
        self, metric_name: str, service: str = "", limit: int = 10
    ) -> list[TraceView]:
        """Find traces related to a metric via exemplars.

        Useful for ad-hoc exploration: "show me traces behind this metric."
        """
        series_list = self._metrics.get_series_by_name(metric_name)
        if service:
            series_list = [s for s in series_list if s.service == service]

        trace_ids: list[str] = []
        for series in series_list:
            trace_ids.extend(series.latest_exemplar_trace_ids(limit=limit))

        seen: set[str] = set()
        traces: list[TraceView] = []
        for tid in trace_ids:
            if tid in seen:
                continue
            seen.add(tid)
            trace = self._traces.get_trace(tid)
            if trace:
                traces.append(trace)
            if len(traces) >= limit:
                break
        return traces

    def metrics_for_trace(self, trace_id: str) -> list[tuple[str, float]]:
        """Reverse lookup: find metrics that have exemplars pointing at a trace.

        Returns list of (metric_name, exemplar_value).
        """
        results = self._metrics.get_exemplars_for_trace(trace_id)
        return [
            (self._metrics._series[skey].metric_name if skey in self._metrics._series else skey, ex.value)
            for skey, ex in results
        ]

    # --- Internal ---

    def _lookup_exemplar_traces(self, trace_ids: list[str]) -> list[TraceView]:
        traces: list[TraceView] = []
        for tid in trace_ids:
            trace = self._traces.get_trace(tid)
            if trace:
                traces.append(trace)
        return traces

    def _lookup_by_service_window(
        self, service: str, around: datetime
    ) -> list[TraceView]:
        start = around - self._fallback_window
        end = around + self._fallback_window / 2
        all_traces = self._traces.get_traces_for_service(service, limit=50)
        return [
            t for t in all_traces
            if t.started_at and start <= t.started_at <= end
        ]
