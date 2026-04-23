"""SLO / Error Budget framework.

Defines Service Level Indicators (SLIs), Service Level Objectives (SLOs),
and computes burn rates and error budgets from stored metrics.

Concepts:
- SLI: a measurable indicator (e.g., "proportion of requests < 500ms")
- SLO: a target on an SLI (e.g., "99.9% over 30 days")
- Error budget: how much failure is allowed (1 - target = 0.1%)
- Burn rate: how fast the error budget is being consumed
  (burn rate 1.0 = consuming at exactly the budget rate,
   burn rate 10.0 = will exhaust budget in 1/10th the window)

Multi-window alerting:
- Fast burn (5m window, burn rate > 14.4x) = page immediately
- Slow burn (1h window, burn rate > 6x) = ticket
- Budget exhaustion (30d window, remaining < 0%) = escalate

Usage:
    slo_engine = SLOEngine(metric_store)
    slo_engine.register(SLODefinition(
        name="checkout-latency",
        service="checkout-svc",
        sli=LatencySLI(metric_name="http_request_duration_seconds", threshold_ms=500),
        target=0.999,
        window_days=30,
    ))
    status = slo_engine.evaluate("checkout-latency")
    print(f"Budget remaining: {status.budget_remaining_pct:.1f}%")
    print(f"Burn rate (5m): {status.burn_rate_5m:.1f}x")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

from netobserv.telemetry.store.metric_store import MetricStore


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class SLIType(str, Enum):
    LATENCY = "latency"           # proportion of requests below threshold
    AVAILABILITY = "availability" # proportion of non-error responses
    THROUGHPUT = "throughput"      # requests per second above minimum
    CUSTOM = "custom"             # user-defined good/total ratio


@dataclass
class LatencySLI:
    """SLI: proportion of requests with latency below threshold."""
    metric_name: str                    # e.g., "http_request_duration_seconds"
    threshold_ms: float                 # e.g., 500.0 — requests under this are "good"
    sli_type: SLIType = SLIType.LATENCY

    def evaluate(self, values: list[float]) -> float:
        """Returns the proportion of good events (0.0 to 1.0)."""
        if not values:
            return 1.0  # no data = assume good
        threshold_sec = self.threshold_ms / 1000.0
        good = sum(1 for v in values if v <= threshold_sec)
        return good / len(values)


@dataclass
class AvailabilitySLI:
    """SLI: proportion of non-error responses."""
    total_metric: str                   # e.g., "http_requests_total"
    error_metric: str                   # e.g., "http_errors_total"
    sli_type: SLIType = SLIType.AVAILABILITY

    def evaluate(self, total_values: list[float], error_values: list[float]) -> float:
        """Returns availability ratio."""
        total = sum(total_values) if total_values else 0
        errors = sum(error_values) if error_values else 0
        if total == 0:
            return 1.0
        return max(0.0, (total - errors) / total)


@dataclass
class CustomSLI:
    """SLI: user-defined good/total metric ratio."""
    good_metric: str
    total_metric: str
    sli_type: SLIType = SLIType.CUSTOM

    def evaluate(self, good_values: list[float], total_values: list[float]) -> float:
        total = sum(total_values) if total_values else 0
        good = sum(good_values) if good_values else 0
        if total == 0:
            return 1.0
        return min(1.0, good / total)


# Union type for SLI definitions
SLIDefinition = LatencySLI | AvailabilitySLI | CustomSLI


@dataclass
class SLODefinition:
    """A Service Level Objective."""
    name: str                           # unique identifier
    service: str                        # which service this applies to
    sli: SLIDefinition                  # the indicator to measure
    target: float                       # e.g., 0.999 = 99.9%
    window_days: int = 30               # evaluation window

    @property
    def error_budget_ratio(self) -> float:
        """The fraction of events allowed to be bad."""
        return 1.0 - self.target

    @property
    def target_pct(self) -> str:
        return f"{self.target * 100:.2f}%"


class BurnRateSeverity(str, Enum):
    CRITICAL = "critical"     # fast burn — page immediately
    WARNING = "warning"       # slow burn — create ticket
    INFO = "info"             # within budget
    EXHAUSTED = "exhausted"   # budget fully consumed


@dataclass
class BurnRateWindow:
    """Burn rate computed over a specific time window."""
    window_name: str                    # "5m", "1h", "6h", "30d"
    window_seconds: float
    burn_rate: float                    # 1.0 = normal consumption, 10.0 = 10x
    current_sli: float                  # measured SLI in this window
    good_count: int = 0
    total_count: int = 0


@dataclass
class SLOStatus:
    """Current status of an SLO."""
    slo_name: str
    service: str
    target: float
    current_sli: float                  # overall SLI across the full window
    budget_total: float                 # total error budget (1 - target)
    budget_consumed: float              # how much budget has been used
    budget_remaining_pct: float         # percentage of budget remaining
    severity: BurnRateSeverity = BurnRateSeverity.INFO
    evaluated_at: datetime = field(default_factory=_utc_now)

    # Multi-window burn rates
    burn_rate_5m: float = 0.0
    burn_rate_1h: float = 0.0
    burn_rate_6h: float = 0.0
    burn_rates: list[BurnRateWindow] = field(default_factory=list)

    @property
    def is_breached(self) -> bool:
        return self.budget_remaining_pct <= 0

    @property
    def is_burning_fast(self) -> bool:
        return self.burn_rate_5m > 14.4

    @property
    def is_burning_slow(self) -> bool:
        return self.burn_rate_1h > 6.0 and not self.is_burning_fast

    @property
    def summary(self) -> str:
        return (
            f"SLO '{self.slo_name}' ({self.service}): "
            f"SLI={self.current_sli:.4f} target={self.target:.4f} "
            f"budget_remaining={self.budget_remaining_pct:.1f}% "
            f"severity={self.severity.value}"
        )


# Multi-window burn rate thresholds (Google SRE book)
_BURN_RATE_THRESHOLDS = [
    ("5m", 300.0, 14.4, BurnRateSeverity.CRITICAL),
    ("1h", 3600.0, 6.0, BurnRateSeverity.WARNING),
    ("6h", 21600.0, 3.0, BurnRateSeverity.WARNING),
]


class SLOEngine:
    """Evaluates SLOs against stored metrics.

    Usage:
        engine = SLOEngine(metric_store)
        engine.register(SLODefinition(...))
        status = engine.evaluate("my-slo")
        all_statuses = engine.evaluate_all()
    """

    def __init__(self, metric_store: MetricStore) -> None:
        self._store = metric_store
        self._slos: dict[str, SLODefinition] = {}

    def register(self, slo: SLODefinition) -> None:
        self._slos[slo.name] = slo

    def unregister(self, name: str) -> None:
        self._slos.pop(name, None)

    def get_slo(self, name: str) -> SLODefinition | None:
        return self._slos.get(name)

    def list_slos(self) -> list[SLODefinition]:
        return list(self._slos.values())

    def evaluate(self, slo_name: str, now: datetime | None = None) -> SLOStatus:
        """Evaluate a single SLO and return its current status."""
        now = now or _utc_now()
        slo = self._slos.get(slo_name)
        if not slo:
            return SLOStatus(
                slo_name=slo_name, service="", target=0,
                current_sli=0, budget_total=0, budget_consumed=0,
                budget_remaining_pct=0,
                severity=BurnRateSeverity.EXHAUSTED,
            )

        # Compute SLI over the full window
        window = timedelta(days=slo.window_days)
        full_sli = self._compute_sli(slo, now - window, now)

        # Compute error budget
        budget_total = slo.error_budget_ratio
        budget_consumed = max(0.0, 1.0 - full_sli) if full_sli < 1.0 else 0.0
        if budget_total > 0:
            budget_remaining_pct = max(0.0, (1.0 - budget_consumed / budget_total)) * 100
        else:
            budget_remaining_pct = 100.0 if full_sli >= slo.target else 0.0

        # Compute multi-window burn rates
        burn_rates: list[BurnRateWindow] = []
        burn_5m = 0.0
        burn_1h = 0.0
        burn_6h = 0.0

        for window_name, window_secs, threshold, _ in _BURN_RATE_THRESHOLDS:
            window_start = now - timedelta(seconds=window_secs)
            window_sli = self._compute_sli(slo, window_start, now)
            error_rate_in_window = max(0.0, 1.0 - window_sli)

            if budget_total > 0:
                burn_rate = error_rate_in_window / budget_total
            else:
                burn_rate = 0.0 if error_rate_in_window == 0 else float('inf')

            burn_rates.append(BurnRateWindow(
                window_name=window_name,
                window_seconds=window_secs,
                burn_rate=burn_rate,
                current_sli=window_sli,
            ))

            if window_name == "5m":
                burn_5m = burn_rate
            elif window_name == "1h":
                burn_1h = burn_rate
            elif window_name == "6h":
                burn_6h = burn_rate

        # Determine severity
        severity = BurnRateSeverity.INFO
        if budget_remaining_pct <= 0:
            severity = BurnRateSeverity.EXHAUSTED
        elif burn_5m > 14.4:
            severity = BurnRateSeverity.CRITICAL
        elif burn_1h > 6.0:
            severity = BurnRateSeverity.WARNING

        return SLOStatus(
            slo_name=slo_name,
            service=slo.service,
            target=slo.target,
            current_sli=full_sli,
            budget_total=budget_total,
            budget_consumed=budget_consumed,
            budget_remaining_pct=budget_remaining_pct,
            severity=severity,
            burn_rate_5m=burn_5m,
            burn_rate_1h=burn_1h,
            burn_rate_6h=burn_6h,
            burn_rates=burn_rates,
        )

    def evaluate_all(self, now: datetime | None = None) -> list[SLOStatus]:
        """Evaluate all registered SLOs."""
        return [self.evaluate(name, now=now) for name in self._slos]

    def breached_slos(self, now: datetime | None = None) -> list[SLOStatus]:
        """Return only SLOs that are breached or burning fast."""
        return [
            s for s in self.evaluate_all(now=now)
            if s.severity in {BurnRateSeverity.CRITICAL, BurnRateSeverity.WARNING,
                             BurnRateSeverity.EXHAUSTED}
        ]

    def _compute_sli(self, slo: SLODefinition, start: datetime, end: datetime) -> float:
        """Compute the SLI value over a time window from stored metrics."""
        sli = slo.sli

        if isinstance(sli, LatencySLI):
            series_list = self._store.get_series_for_service(slo.service)
            values: list[float] = []
            for series in series_list:
                if series.metric_name == sli.metric_name:
                    values.extend(series.values_in_window(start, end))
            return sli.evaluate(values)

        elif isinstance(sli, AvailabilitySLI):
            series_list = self._store.get_series_for_service(slo.service)
            total_vals: list[float] = []
            error_vals: list[float] = []
            for series in series_list:
                if series.metric_name == sli.total_metric:
                    total_vals.extend(series.values_in_window(start, end))
                elif series.metric_name == sli.error_metric:
                    error_vals.extend(series.values_in_window(start, end))
            return sli.evaluate(total_vals, error_vals)

        elif isinstance(sli, CustomSLI):
            series_list = self._store.get_series_for_service(slo.service)
            good_vals: list[float] = []
            total_vals2: list[float] = []
            for series in series_list:
                if series.metric_name == sli.good_metric:
                    good_vals.extend(series.values_in_window(start, end))
                elif series.metric_name == sli.total_metric:
                    total_vals2.extend(series.values_in_window(start, end))
            return sli.evaluate(good_vals, total_vals2)

        return 1.0
