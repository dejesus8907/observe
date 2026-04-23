"""In-memory metric series storage with exemplar index.

Stores MetricPoint payloads in TelemetryEnvelopes, indexed by:
- metric name + label set (the series identity)
- service name (for golden signal queries)
- exemplar trace_id (the metric→trace bridge)

Not a production TSDB — this is bounded in-memory storage for the
correlation engine and anomaly detector to query against.
"""

from __future__ import annotations

import hashlib
import threading
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from netobserv.telemetry.envelope import TelemetryEnvelope
from netobserv.telemetry.enums import MetricType, SignalType
from netobserv.telemetry.signals import Exemplar, MetricPoint


from netobserv.telemetry.store.cardinality_limiter import CardinalityLimiter


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _series_key(metric_name: str, labels: dict[str, str]) -> str:
    """Deterministic series identity from name + sorted labels."""
    label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
    raw = f"{metric_name}{{{label_str}}}"
    return hashlib.sha256(raw.encode()).hexdigest()[:20]


@dataclass(slots=True)
class MetricSample:
    """A single timestamped value in a series."""
    value: float
    timestamp: datetime
    envelope_id: str = ""


@dataclass
class MetricSeries:
    """A time series: one metric name + one label set."""
    series_key: str
    metric_name: str
    metric_type: MetricType
    labels: dict[str, str] = field(default_factory=dict)
    service: str = ""
    samples: deque[MetricSample] = field(default_factory=lambda: deque(maxlen=1000))
    exemplars: list[Exemplar] = field(default_factory=list)
    last_value: float = 0.0
    last_seen: datetime | None = None

    def add_sample(self, value: float, timestamp: datetime, envelope_id: str = "") -> None:
        self.samples.append(MetricSample(value=value, timestamp=timestamp, envelope_id=envelope_id))
        self.last_value = value
        self.last_seen = timestamp

    def add_exemplar(self, exemplar: Exemplar) -> None:
        self.exemplars.append(exemplar)
        # Cap exemplars
        if len(self.exemplars) > 50:
            self.exemplars = self.exemplars[-50:]

    @property
    def sample_count(self) -> int:
        return len(self.samples)

    def values_in_window(self, start: datetime, end: datetime) -> list[float]:
        return [s.value for s in self.samples if start <= s.timestamp <= end]

    def latest_exemplar_trace_ids(self, limit: int = 5) -> list[str]:
        return [e.trace_id for e in reversed(self.exemplars[-limit:])]


@dataclass(slots=True)
class GoldenSignals:
    """Aggregated golden signals for a service from stored metrics."""
    service: str
    request_rate: float = 0.0          # requests/sec
    error_rate: float = 0.0            # errors/total
    latency_avg_ms: float = 0.0
    latency_p50_ms: float = 0.0
    latency_p99_ms: float = 0.0
    saturation: float = 0.0            # 0-1


class MetricStore:
    """In-memory metric storage with exemplar indexing.

    Usage:
        store = MetricStore()
        store.store_metric(envelope)  # envelope with MetricPoint payload
        series = store.get_series("http_request_duration_seconds", {"service": "api"})
        exemplars = store.get_exemplars_for_service("api")
        signals = store.golden_signals("api")
    """

    def __init__(self, max_series: int = 100_000, max_age_seconds: float = 900.0,
                 cardinality_limiter: CardinalityLimiter | None = None) -> None:
        self._max_series = max_series
        self._max_age = timedelta(seconds=max_age_seconds)
        self._lock = threading.Lock()
        self._cardinality = cardinality_limiter

        # series_key → MetricSeries
        self._series: dict[str, MetricSeries] = {}
        # service → set of series_keys
        self._service_index: dict[str, set[str]] = defaultdict(set)
        # exemplar trace_id → list of (series_key, Exemplar)
        self._exemplar_index: dict[str, list[tuple[str, Exemplar]]] = defaultdict(list)
        # metric_name → set of series_keys
        self._name_index: dict[str, set[str]] = defaultdict(set)

        self._total_stored = 0
        self._total_dropped = 0
        self._exemplars_dropped = 0

    def store_metric(self, envelope: TelemetryEnvelope) -> None:
        """Store a metric envelope."""
        if envelope.signal_type != SignalType.METRIC:
            return
        payload = envelope.payload
        if not isinstance(payload, MetricPoint):
            return

        service = envelope.target.service or ""
        labels = dict(payload.labels)
        if service and "service" not in labels:
            labels["service"] = service

        # Cardinality guard
        if self._cardinality:
            accepted, labels = self._cardinality.check(payload.metric_name, labels)
            if not accepted:
                self._total_dropped += 1
                return

        skey = _series_key(payload.metric_name, labels)

        with self._lock:
            series = self._series.get(skey)
            if not series:
                if len(self._series) >= self._max_series:
                    self._evict_oldest()
                series = MetricSeries(
                    series_key=skey,
                    metric_name=payload.metric_name,
                    metric_type=payload.metric_type,
                    labels=labels,
                    service=service,
                )
                self._series[skey] = series
                self._name_index[payload.metric_name].add(skey)
                if service:
                    self._service_index[service].add(skey)

            ts = envelope.observed_at or _utc_now()
            series.add_sample(payload.value, ts, envelope.envelope_id)
            self._total_stored += 1

            # Exemplar budget enforcement
            exemplar_limit = None
            if self._cardinality:
                exemplar_limit = self._cardinality.exemplar_limit_for(payload.metric_name)

            exemplars_to_store = payload.exemplars
            if exemplar_limit is not None and exemplar_limit > 0:
                if len(exemplars_to_store) > exemplar_limit:
                    self._exemplars_dropped += len(exemplars_to_store) - exemplar_limit
                    exemplars_to_store = exemplars_to_store[:exemplar_limit]

            for exemplar in exemplars_to_store:
                series.add_exemplar(exemplar)
                if exemplar.trace_id:
                    self._exemplar_index[exemplar.trace_id].append((skey, exemplar))

    def store_metrics(self, envelopes: list[TelemetryEnvelope]) -> None:
        for env in envelopes:
            self.store_metric(env)

    # --- Queries ---

    def get_series(self, metric_name: str, labels: dict[str, str] | None = None) -> MetricSeries | None:
        """Get a specific series by name + labels."""
        if labels is None:
            # Return the first series with this name
            keys = self._name_index.get(metric_name, set())
            if keys:
                return self._series.get(next(iter(keys)))
            return None
        skey = _series_key(metric_name, labels)
        return self._series.get(skey)

    def get_series_for_service(self, service: str) -> list[MetricSeries]:
        """Get all metric series for a service."""
        with self._lock:
            keys = self._service_index.get(service, set())
            return [self._series[k] for k in keys if k in self._series]

    def get_series_by_name(self, metric_name: str) -> list[MetricSeries]:
        """Get all series for a metric name (across labels/services)."""
        with self._lock:
            keys = self._name_index.get(metric_name, set())
            return [self._series[k] for k in keys if k in self._series]

    def get_exemplars_for_trace(self, trace_id: str) -> list[tuple[str, Exemplar]]:
        """Find which metrics have exemplars pointing at this trace.

        Returns list of (series_key, Exemplar).
        """
        with self._lock:
            return list(self._exemplar_index.get(trace_id, []))

    def get_exemplars_for_service(self, service: str, limit: int = 20) -> list[tuple[MetricSeries, Exemplar]]:
        """Get recent exemplars across all metrics for a service."""
        results: list[tuple[MetricSeries, Exemplar]] = []
        with self._lock:
            for skey in self._service_index.get(service, set()):
                series = self._series.get(skey)
                if series:
                    for ex in series.exemplars[-5:]:
                        results.append((series, ex))
                        if len(results) >= limit:
                            return results
        return results

    def golden_signals(self, service: str, window_seconds: float = 60.0) -> GoldenSignals:
        """Compute golden signals for a service from stored metrics."""
        signals = GoldenSignals(service=service)
        now = _utc_now()
        window_start = now - timedelta(seconds=window_seconds)

        with self._lock:
            for skey in self._service_index.get(service, set()):
                series = self._series.get(skey)
                if not series:
                    continue
                name = series.metric_name
                values = series.values_in_window(window_start, now)
                if not values:
                    continue

                # Request rate (from counters named *_total or *_count)
                if name.endswith("_total") or name.endswith("_count"):
                    if "error" in name.lower():
                        signals.error_rate = sum(values) / max(len(values), 1)
                    else:
                        signals.request_rate = sum(values) / max(window_seconds, 1)

                # Latency (from histograms/summaries named *_duration* or *_latency*)
                if "duration" in name or "latency" in name:
                    sorted_v = sorted(values)
                    signals.latency_avg_ms = (sum(sorted_v) / len(sorted_v)) * 1000
                    signals.latency_p50_ms = sorted_v[len(sorted_v) // 2] * 1000
                    signals.latency_p99_ms = sorted_v[int(len(sorted_v) * 0.99)] * 1000

                # Saturation (from gauges named *_utilization* or *_saturation*)
                if "utilization" in name or "saturation" in name:
                    signals.saturation = values[-1] if values else 0.0

        return signals

    def evict_expired(self, now: datetime | None = None) -> int:
        """Remove series with no samples newer than max_age."""
        now = now or _utc_now()
        cutoff = now - self._max_age
        evicted = 0
        with self._lock:
            stale = [
                skey for skey, series in self._series.items()
                if series.last_seen and series.last_seen < cutoff
            ]
            for skey in stale:
                self._remove_series(skey)
                evicted += 1
        return evicted

    @property
    def stats(self) -> dict[str, Any]:
        with self._lock:
            total_exemplars = sum(len(v) for v in self._exemplar_index.values())
            result: dict[str, Any] = {
                "series_count": len(self._series),
                "service_count": len(self._service_index),
                "metric_name_count": len(self._name_index),
                "exemplar_traces_indexed": len(self._exemplar_index),
                "total_exemplars": total_exemplars,
                "total_samples_stored": self._total_stored,
                "total_dropped": self._total_dropped,
                "exemplars_dropped": self._exemplars_dropped,
                "cardinality": self._cardinality.cardinality_report() if self._cardinality else None,
            }
            return result

    # --- Internal ---

    def _evict_oldest(self) -> None:
        if not self._series:
            return
        oldest_key = min(
            self._series,
            key=lambda k: self._series[k].last_seen or datetime.min.replace(tzinfo=timezone.utc),
        )
        self._remove_series(oldest_key)

    def _remove_series(self, skey: str) -> None:
        series = self._series.pop(skey, None)
        if not series:
            return
        self._name_index.get(series.metric_name, set()).discard(skey)
        if series.service:
            self._service_index.get(series.service, set()).discard(skey)
        # Clean exemplar index
        for ex in series.exemplars:
            if ex.trace_id and ex.trace_id in self._exemplar_index:
                self._exemplar_index[ex.trace_id] = [
                    (sk, e) for sk, e in self._exemplar_index[ex.trace_id] if sk != skey
                ]
                if not self._exemplar_index[ex.trace_id]:
                    del self._exemplar_index[ex.trace_id]
