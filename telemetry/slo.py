"""SLO / Error Budget framework.

Defines Service Level Indicators (SLIs), Service Level Objectives (SLOs),
and computes error budget burn rates over rolling windows.

Concepts:
- SLI: a measurement (e.g., "fraction of requests < 500ms")
- SLO: a target on an SLI (e.g., "99.9% over 30 days")
- Error budget: how much failure is allowed (1 - target = 0.1%)
- Burn rate: how fast the error budget is being consumed
  - burn_rate=1.0 means budget consumed exactly at the allowed pace
  - burn_rate>1.0 means budget will be exhausted before the window ends

Multi-window burn rate alerting (Google SRE book):
- Fast burn (5m window, 14.4x threshold): pages immediately
- Slow burn (1h window, 6x threshold): tickets within hours
- Budget exhaustion (30d window): tracks long-term health

Usage:
    slo_store = SLOStore()
    slo_store.define_slo(SLODefinition(
        slo_id="checkout-latency",
        service="checkout",
        sli_type=SLIType.LATENCY,
        threshold_ms=500.0,
        target=0.999,
        window_days=30,
    ))
    result = slo_store.evaluate("checkout-latency", metric_store)
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

from netobserv.telemetry.store.metric_store import MetricStore


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class SLIType(str, Enum):
    """Types of Service Level Indicators."""
    LATENCY = "latency"               # fraction of requests below threshold
    AVAILABILITY = "availability"     # fraction of non-error responses
    THROUGHPUT = "throughput"          # requests per second above threshold
    SATURATION = "saturation"         # resource utilization below threshold


class SLOStatus(str, Enum):
    """Current status of an SLO."""
    MET = "met"                       # within budget
    AT_RISK = "at_risk"               # burn rate elevated but budget remaining
    BREACHED = "breached"             # error budget exhausted
    NO_DATA = "no_data"               # insufficient data to evaluate


@dataclass
class SLODefinition:
    """A Service Level Objective definition."""
    slo_id: str
    service: str
    sli_type: SLIType
    target: float = 0.999             # 99.9%
    window_days: int = 30
    description: str = ""

    # SLI-specific thresholds
    threshold_ms: float = 500.0       # for latency: request must be below this
    error_status_codes: set[str] = field(default_factory=lambda: {"5xx"})

    # Metric names to evaluate against (auto-detected if empty)
    latency_metric: str = "http_request_duration_seconds"
    error_metric: str = "http_requests_total"
    total_metric: str = "http_requests_total"

    @property
    def error_budget_fraction(self) -> float:
        """Fraction of requests allowed to fail (1 - target)."""
        return 1.0 - self.target

    @property
    def window_seconds(self) -> float:
        return self.window_days * 86400


@dataclass
class BurnRateResult:
    """Burn rate computed over a specific window."""
    window_label: str                 # "5m", "1h", "30d"
    window_seconds: float
    burn_rate: float = 0.0            # 1.0 = normal, >1.0 = burning fast
    good_count: int = 0
    total_count: int = 0
    bad_count: int = 0
    observed_error_rate: float = 0.0
    sufficient_data: bool = False

    @property
    def budget_remaining_fraction(self) -> float:
        """How much of the error budget remains (0.0-1.0)."""
        if self.burn_rate <= 0:
            return 1.0
        consumed = min(self.burn_rate, 100.0) / 100.0
        return max(0.0, 1.0 - consumed)


@dataclass
class SLOEvaluation:
    """Complete SLO evaluation result."""
    slo_id: str
    service: str
    sli_type: SLIType
    target: float
    status: SLOStatus = SLOStatus.NO_DATA
    evaluated_at: datetime = field(default_factory=_utc_now)

    # Error budget
    error_budget_fraction: float = 0.0
    error_budget_remaining: float = 0.0
    error_budget_consumed_percent: float = 0.0

    # Burn rates at multiple windows
    burn_rates: list[BurnRateResult] = field(default_factory=list)

    # Alert thresholds
    fast_burn_alert: bool = False     # 5m window, 14.4x burn rate
    slow_burn_alert: bool = False     # 1h window, 6x burn rate

    explanation: str = ""

    @property
    def is_healthy(self) -> bool:
        return self.status == SLOStatus.MET

    @property
    def worst_burn_rate(self) -> float:
        if not self.burn_rates:
            return 0.0
        return max(br.burn_rate for br in self.burn_rates if br.sufficient_data)


# Multi-window burn rate thresholds (from Google SRE book)
BURN_RATE_WINDOWS = [
    ("5m", 300, 14.4),       # fast burn: pages
    ("30m", 1800, 6.0),      # medium burn: tickets
    ("1h", 3600, 3.0),       # slow burn: review
    ("6h", 21600, 1.5),      # sustained: monitor
]


class SLOStore:
    """Manages SLO definitions and evaluations."""

    def __init__(self) -> None:
        self._definitions: dict[str, SLODefinition] = {}
        # Manual good/bad event tracking for SLIs not backed by metric store
        self._event_log: dict[str, deque[tuple[datetime, bool]]] = {}

    def define_slo(self, slo: SLODefinition) -> None:
        self._definitions[slo.slo_id] = slo

    def remove_slo(self, slo_id: str) -> None:
        self._definitions.pop(slo_id, None)
        self._event_log.pop(slo_id, None)

    def get_definition(self, slo_id: str) -> SLODefinition | None:
        return self._definitions.get(slo_id)

    def list_definitions(self, service: str | None = None) -> list[SLODefinition]:
        if service:
            return [d for d in self._definitions.values() if d.service == service]
        return list(self._definitions.values())

    def record_event(self, slo_id: str, good: bool, at: datetime | None = None) -> None:
        """Record a good/bad event for manual SLI tracking."""
        at = at or _utc_now()
        if slo_id not in self._event_log:
            self._event_log[slo_id] = deque(maxlen=100_000)
        self._event_log[slo_id].append((at, good))

    def evaluate(self, slo_id: str, metric_store: MetricStore | None = None,
                 now: datetime | None = None) -> SLOEvaluation:
        """Evaluate an SLO against current data."""
        now = now or _utc_now()
        slo = self._definitions.get(slo_id)
        if not slo:
            return SLOEvaluation(
                slo_id=slo_id, service="", sli_type=SLIType.LATENCY,
                target=0.0, status=SLOStatus.NO_DATA,
                explanation=f"SLO {slo_id} not defined",
            )

        result = SLOEvaluation(
            slo_id=slo_id, service=slo.service,
            sli_type=slo.sli_type, target=slo.target,
            error_budget_fraction=slo.error_budget_fraction,
            evaluated_at=now,
        )

        # Compute burn rates at multiple windows
        for label, window_secs, alert_threshold in BURN_RATE_WINDOWS:
            br = self._compute_burn_rate(slo, window_secs, label, now, metric_store)
            result.burn_rates.append(br)

        # Also compute for the full SLO window
        full_br = self._compute_burn_rate(
            slo, slo.window_seconds, f"{slo.window_days}d", now, metric_store
        )
        result.burn_rates.append(full_br)

        # Determine status from burn rates
        has_data = any(br.sufficient_data for br in result.burn_rates)
        if not has_data:
            result.status = SLOStatus.NO_DATA
            result.explanation = "Insufficient data for evaluation"
            return result

        # Error budget consumption from full window
        if full_br.sufficient_data:
            result.error_budget_consumed_percent = min(
                (full_br.observed_error_rate / max(slo.error_budget_fraction, 1e-9)) * 100,
                100.0,
            )
            result.error_budget_remaining = max(
                0.0, slo.error_budget_fraction - full_br.observed_error_rate
            )

        # Multi-window alert evaluation
        for br in result.burn_rates:
            if not br.sufficient_data:
                continue
            for label, _, threshold in BURN_RATE_WINDOWS:
                if br.window_label == label and br.burn_rate >= threshold:
                    if label == "5m":
                        result.fast_burn_alert = True
                    elif label in ("30m", "1h"):
                        result.slow_burn_alert = True

        # Status determination
        if result.error_budget_remaining <= 0:
            result.status = SLOStatus.BREACHED
            result.explanation = "Error budget exhausted"
        elif result.fast_burn_alert or result.slow_burn_alert:
            result.status = SLOStatus.AT_RISK
            result.explanation = (
                f"Elevated burn rate detected: "
                f"fast_burn={result.fast_burn_alert}, slow_burn={result.slow_burn_alert}"
            )
        else:
            result.status = SLOStatus.MET
            result.explanation = "SLO is being met"

        return result

    def evaluate_all(self, metric_store: MetricStore | None = None,
                     now: datetime | None = None) -> list[SLOEvaluation]:
        """Evaluate all defined SLOs."""
        return [self.evaluate(slo_id, metric_store, now) for slo_id in self._definitions]

    # --- Internal ---

    def _compute_burn_rate(
        self, slo: SLODefinition, window_seconds: float, label: str,
        now: datetime, metric_store: MetricStore | None,
    ) -> BurnRateResult:
        result = BurnRateResult(window_label=label, window_seconds=window_seconds)

        # Try event log first
        events = self._event_log.get(slo.slo_id)
        if events:
            cutoff = now - timedelta(seconds=window_seconds)
            window_events = [(t, good) for t, good in events if t >= cutoff]
            if len(window_events) >= 5:
                result.total_count = len(window_events)
                result.good_count = sum(1 for _, good in window_events if good)
                result.bad_count = result.total_count - result.good_count
                result.observed_error_rate = result.bad_count / max(result.total_count, 1)
                result.burn_rate = result.observed_error_rate / max(slo.error_budget_fraction, 1e-9)
                result.sufficient_data = True
                return result

        # Try metric store
        if metric_store and slo.sli_type == SLIType.LATENCY:
            return self._compute_latency_burn_rate(slo, window_seconds, label, now, metric_store)

        return result

    def _compute_latency_burn_rate(
        self, slo: SLODefinition, window_seconds: float, label: str,
        now: datetime, metric_store: MetricStore,
    ) -> BurnRateResult:
        result = BurnRateResult(window_label=label, window_seconds=window_seconds)

        series_list = metric_store.get_series_for_service(slo.service)
        latency_series = [
            s for s in series_list if s.metric_name == slo.latency_metric
        ]
        if not latency_series:
            return result

        start = now - timedelta(seconds=window_seconds)
        all_values: list[float] = []
        for series in latency_series:
            all_values.extend(series.values_in_window(start, now))

        if len(all_values) < 5:
            return result

        threshold_seconds = slo.threshold_ms / 1000.0
        good = sum(1 for v in all_values if v <= threshold_seconds)
        total = len(all_values)
        bad = total - good

        result.total_count = total
        result.good_count = good
        result.bad_count = bad
        result.observed_error_rate = bad / max(total, 1)
        result.burn_rate = result.observed_error_rate / max(slo.error_budget_fraction, 1e-9)
        result.sufficient_data = True
        return result
