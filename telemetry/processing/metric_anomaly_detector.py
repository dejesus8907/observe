"""Metric anomaly detection.

Detects anomalies in metric time series:
- Threshold violations (value exceeds static threshold)
- Spike detection (sudden increase relative to baseline)
- Rate change detection (counter acceleration/deceleration)
- Error rate alerts (error ratio exceeds threshold)

When an anomaly is detected, the detector returns exemplar trace IDs
if available, enabling the metric→trace jump.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from netobserv.telemetry.store.metric_store import MetricSeries, MetricStore


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class MetricAnomaly:
    """A detected anomaly on a metric series."""
    series_key: str
    metric_name: str
    anomaly_type: str           # threshold, spike, rate_change, error_rate
    current_value: float
    threshold: float = 0.0
    baseline_value: float = 0.0
    deviation_factor: float = 0.0
    service: str = ""
    labels: dict[str, str] = field(default_factory=dict)
    detected_at: datetime = field(default_factory=_utc_now)
    explanation: str = ""

    # Exemplar bridge: trace IDs from the anomalous metric
    exemplar_trace_ids: list[str] = field(default_factory=list)

    @property
    def has_exemplars(self) -> bool:
        return len(self.exemplar_trace_ids) > 0

    @property
    def severity(self) -> str:
        if self.deviation_factor >= 5.0:
            return "critical"
        if self.deviation_factor >= 3.0:
            return "high"
        if self.deviation_factor >= 2.0:
            return "medium"
        return "low"


@dataclass
class AnomalyRule:
    """Configuration for anomaly detection on a metric."""
    metric_name_pattern: str          # exact name or prefix*
    anomaly_type: str                 # threshold, spike, rate_change, error_rate
    threshold: float = 0.0
    spike_factor: float = 3.0        # current / baseline ratio for spike
    baseline_window_seconds: float = 300.0
    evaluation_window_seconds: float = 60.0
    min_samples: int = 3
    # Minimum baseline data before spike/rate_change rules activate.
    # Threshold rules always work; statistical rules need stable baseline.
    min_baseline_samples: int = 10


class MetricAnomalyDetector:
    """Detects anomalies in stored metric series.

    Two modes:
    - SLO/threshold: always active, no baseline required.
      "latency > 2s = violation" — deterministic, no false positives.
    - Statistical (spike, rate_change): requires min_baseline_samples
      of historical data before activating. Without sufficient baseline,
      these rules are silently skipped (not errored).

    Usage:
        detector = MetricAnomalyDetector(metric_store)
        detector.add_rule(AnomalyRule(
            metric_name_pattern="http_request_duration_seconds",
            anomaly_type="threshold", threshold=2.0,  # SLO: always active
        ))
        detector.add_rule(AnomalyRule(
            metric_name_pattern="http_request_duration_seconds",
            anomaly_type="spike", spike_factor=3.0,   # needs baseline
        ))
        anomalies = detector.evaluate()
    """

    def __init__(self, store: MetricStore) -> None:
        self._store = store
        self._rules: list[AnomalyRule] = []
        self._skipped_insufficient_baseline = 0

    def add_rule(self, rule: AnomalyRule) -> None:
        self._rules.append(rule)

    def add_default_rules(self) -> None:
        """Add standard anomaly detection rules for common metrics."""
        self._rules.extend([
            AnomalyRule("http_request_duration_seconds", "spike", spike_factor=3.0),
            AnomalyRule("http_request_duration_seconds", "threshold", threshold=2.0),
            AnomalyRule("http_requests_total", "rate_change", spike_factor=5.0),
            AnomalyRule("http_server_errors_total", "threshold", threshold=0.0),
            AnomalyRule("process_cpu_seconds_total", "spike", spike_factor=3.0),
        ])

    def evaluate(self, now: datetime | None = None) -> list[MetricAnomaly]:
        """Evaluate all rules against all matching series. Returns anomalies."""
        now = now or _utc_now()
        anomalies: list[MetricAnomaly] = []

        for rule in self._rules:
            matching_series = self._find_matching_series(rule.metric_name_pattern)
            for series in matching_series:
                anomaly = self._evaluate_rule(rule, series, now)
                if anomaly:
                    anomalies.append(anomaly)

        return anomalies

    def evaluate_series(self, series: MetricSeries, now: datetime | None = None) -> list[MetricAnomaly]:
        """Evaluate all matching rules against a single series."""
        now = now or _utc_now()
        anomalies: list[MetricAnomaly] = []
        for rule in self._rules:
            if self._name_matches(rule.metric_name_pattern, series.metric_name):
                anomaly = self._evaluate_rule(rule, series, now)
                if anomaly:
                    anomalies.append(anomaly)
        return anomalies

    def _evaluate_rule(self, rule: AnomalyRule, series: MetricSeries,
                       now: datetime) -> MetricAnomaly | None:
        eval_start = now - timedelta(seconds=rule.evaluation_window_seconds)
        baseline_start = now - timedelta(seconds=rule.baseline_window_seconds)

        eval_values = series.values_in_window(eval_start, now)
        if len(eval_values) < rule.min_samples:
            return None

        current = eval_values[-1]

        if rule.anomaly_type == "threshold" or rule.anomaly_type == "error_rate":
            # SLO/threshold: always active, no baseline needed
            return self._check_threshold(rule, series, current, now)
        elif rule.anomaly_type == "spike":
            baseline_values = series.values_in_window(baseline_start, eval_start)
            if len(baseline_values) < rule.min_baseline_samples:
                self._skipped_insufficient_baseline += 1
                return None
            return self._check_spike(rule, series, current, baseline_values, now)
        elif rule.anomaly_type == "rate_change":
            baseline_values = series.values_in_window(baseline_start, eval_start)
            if len(baseline_values) < rule.min_baseline_samples:
                self._skipped_insufficient_baseline += 1
                return None
            return self._check_rate_change(rule, series, eval_values, baseline_values, now)

        return None

    def _check_threshold(self, rule, series, current, now) -> MetricAnomaly | None:
        if current > rule.threshold:
            deviation = current / max(rule.threshold, 0.001)
            return self._make_anomaly(
                series, "threshold", current, now,
                threshold=rule.threshold,
                deviation_factor=deviation,
                explanation=f"{series.metric_name} = {current:.3f} exceeds threshold {rule.threshold:.3f}",
            )
        return None

    def _check_spike(self, rule, series, current, baseline_values, now) -> MetricAnomaly | None:
        if not baseline_values:
            return None
        baseline_avg = sum(baseline_values) / len(baseline_values)
        if baseline_avg <= 0:
            return None
        ratio = current / baseline_avg
        if ratio >= rule.spike_factor:
            return self._make_anomaly(
                series, "spike", current, now,
                baseline_value=baseline_avg,
                deviation_factor=ratio,
                explanation=(f"{series.metric_name} spiked: {current:.3f} is "
                           f"{ratio:.1f}x baseline {baseline_avg:.3f}"),
            )
        return None

    def _check_rate_change(self, rule, series, eval_values, baseline_values, now) -> MetricAnomaly | None:
        if len(eval_values) < 2 or not baseline_values:
            return None
        eval_rate = (eval_values[-1] - eval_values[0]) / max(len(eval_values), 1)
        baseline_rate = (baseline_values[-1] - baseline_values[0]) / max(len(baseline_values), 1) if len(baseline_values) >= 2 else 0
        if baseline_rate <= 0:
            return None
        ratio = abs(eval_rate) / abs(baseline_rate) if baseline_rate != 0 else 0
        if ratio >= rule.spike_factor:
            return self._make_anomaly(
                series, "rate_change", eval_values[-1], now,
                baseline_value=baseline_rate,
                deviation_factor=ratio,
                explanation=(f"{series.metric_name} rate changed: "
                           f"{eval_rate:.3f}/s vs baseline {baseline_rate:.3f}/s"),
            )
        return None

    def _make_anomaly(self, series, anomaly_type, current, now, **kwargs) -> MetricAnomaly:
        return MetricAnomaly(
            series_key=series.series_key,
            metric_name=series.metric_name,
            anomaly_type=anomaly_type,
            current_value=current,
            service=series.service,
            labels=dict(series.labels),
            detected_at=now,
            exemplar_trace_ids=series.latest_exemplar_trace_ids(limit=5),
            **kwargs,
        )

    def _find_matching_series(self, pattern: str) -> list[MetricSeries]:
        if pattern.endswith("*"):
            prefix = pattern[:-1]
            return [s for s in self._store.get_series_by_name(prefix)]

        return self._store.get_series_by_name(pattern)

    @staticmethod
    def _name_matches(pattern: str, name: str) -> bool:
        if pattern.endswith("*"):
            return name.startswith(pattern[:-1])
        return name == pattern
