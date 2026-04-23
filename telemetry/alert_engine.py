"""Alerting subsystem — rule evaluation, state machine, and notification routing.

Consumes:
- SLO burn rate violations (from SLOEngine)
- Metric anomalies (from MetricAnomalyDetector)
- Custom alert rules

Produces:
- Alert state transitions (firing → pending → resolved)
- Grouped/deduplicated notifications
- Webhook/sink dispatch

Alert lifecycle:
    INACTIVE → rule triggers → PENDING (for pending_seconds)
    PENDING → still firing after pending_seconds → FIRING
    FIRING → condition clears → RESOLVED
    FIRING → silenced → SILENCED
    Any state → repeated trigger → deduplicated (no double-fire)

Usage:
    engine = AlertEngine()
    engine.add_rule(AlertRule(
        name="checkout-latency-burn",
        condition=SLOBurnCondition(slo_name="checkout-latency", min_severity="warning"),
        severity=AlertSeverity.PAGE,
        labels={"team": "checkout", "env": "prod"},
    ))
    engine.add_sink(WebhookSink(url="https://hooks.slack.com/..."))
    engine.evaluate(slo_statuses, anomalies)
"""

from __future__ import annotations

import json
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Protocol
from urllib.request import Request, urlopen

from netobserv.telemetry.slo_engine import BurnRateSeverity, SLOStatus
from netobserv.telemetry.processing.metric_anomaly_detector import MetricAnomaly


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class AlertSeverity(str, Enum):
    PAGE = "page"             # wake someone up
    TICKET = "ticket"         # create a ticket, don't page
    INFO = "info"             # informational, log only


class AlertState(str, Enum):
    INACTIVE = "inactive"
    PENDING = "pending"       # condition met, waiting for pending_seconds
    FIRING = "firing"         # confirmed active alert
    RESOLVED = "resolved"     # was firing, now cleared
    SILENCED = "silenced"     # manually suppressed


# --- Alert conditions ---

@dataclass(slots=True)
class SLOBurnCondition:
    """Alert fires when an SLO's burn rate exceeds threshold."""
    slo_name: str
    min_severity: str = "warning"     # "warning", "critical", "exhausted"

    def evaluate(self, slo_statuses: dict[str, SLOStatus]) -> bool:
        status = slo_statuses.get(self.slo_name)
        if not status:
            return False
        severity_order = {"info": 0, "warning": 1, "critical": 2, "exhausted": 3}
        return severity_order.get(status.severity.value, 0) >= severity_order.get(self.min_severity, 1)

    def describe(self, slo_statuses: dict[str, SLOStatus]) -> str:
        status = slo_statuses.get(self.slo_name)
        if not status:
            return f"SLO {self.slo_name} not found"
        return status.summary


@dataclass(slots=True)
class AnomalyCondition:
    """Alert fires when anomalies are detected for a metric/service."""
    metric_pattern: str = ""          # match metric name (empty = any)
    service: str = ""                 # match service (empty = any)
    min_severity: str = "medium"      # "low", "medium", "high", "critical"

    def evaluate(self, anomalies: list[MetricAnomaly]) -> bool:
        matched = self._match(anomalies)
        return len(matched) > 0

    def describe(self, anomalies: list[MetricAnomaly]) -> str:
        matched = self._match(anomalies)
        if not matched:
            return "No matching anomalies"
        return f"{len(matched)} anomalies: " + ", ".join(
            f"{a.metric_name} ({a.severity})" for a in matched[:3]
        )

    def _match(self, anomalies: list[MetricAnomaly]) -> list[MetricAnomaly]:
        severity_order = {"low": 0, "medium": 1, "high": 2, "critical": 3}
        min_sev = severity_order.get(self.min_severity, 1)
        result: list[MetricAnomaly] = []
        for a in anomalies:
            if self.metric_pattern and self.metric_pattern not in a.metric_name:
                continue
            if self.service and a.service != self.service:
                continue
            if severity_order.get(a.severity, 0) < min_sev:
                continue
            result.append(a)
        return result


@dataclass(slots=True)
class CustomCondition:
    """Alert fires based on a callable predicate."""
    predicate: Callable[[], bool] = field(default=lambda: False)
    description: str = "custom condition"

    def evaluate(self, **kwargs: Any) -> bool:
        return self.predicate()

    def describe(self, **kwargs: Any) -> str:
        return self.description


AlertCondition = SLOBurnCondition | AnomalyCondition | CustomCondition


# --- Alert rule ---

@dataclass
class AlertRule:
    """Definition of an alert rule."""
    name: str
    condition: AlertCondition
    severity: AlertSeverity = AlertSeverity.TICKET
    labels: dict[str, str] = field(default_factory=dict)
    annotations: dict[str, str] = field(default_factory=dict)
    pending_seconds: float = 0.0      # how long condition must hold before firing
    repeat_interval_seconds: float = 300.0  # min time between repeat notifications
    group_key: str = ""               # alerts with same group_key are grouped


# --- Alert instance (live state) ---

@dataclass
class Alert:
    """A live alert instance with state machine."""
    rule_name: str
    state: AlertState = AlertState.INACTIVE
    severity: AlertSeverity = AlertSeverity.TICKET
    labels: dict[str, str] = field(default_factory=dict)
    annotations: dict[str, str] = field(default_factory=dict)
    description: str = ""
    # Timing
    first_triggered: datetime | None = None
    last_triggered: datetime | None = None
    last_notified: datetime | None = None
    resolved_at: datetime | None = None
    # Grouping
    group_key: str = ""
    fire_count: int = 0

    @property
    def fingerprint(self) -> str:
        label_str = ",".join(f"{k}={v}" for k, v in sorted(self.labels.items()))
        return f"{self.rule_name}|{label_str}"


# --- Notification sink protocol ---

class AlertSink(Protocol):
    def send(self, alerts: list[Alert]) -> bool: ...


@dataclass
class WebhookSink:
    """Sends alerts to an HTTP webhook endpoint."""
    url: str
    headers: dict[str, str] = field(default_factory=dict)
    timeout_seconds: float = 10.0

    def send(self, alerts: list[Alert]) -> bool:
        payload = {
            "alerts": [
                {
                    "rule": a.rule_name,
                    "state": a.state.value,
                    "severity": a.severity.value,
                    "description": a.description,
                    "labels": a.labels,
                    "annotations": a.annotations,
                    "first_triggered": a.first_triggered.isoformat() if a.first_triggered else None,
                    "fire_count": a.fire_count,
                }
                for a in alerts
            ],
            "count": len(alerts),
            "timestamp": _utc_now().isoformat(),
        }
        try:
            req = Request(
                self.url,
                data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json", **self.headers},
                method="POST",
            )
            with urlopen(req, timeout=self.timeout_seconds) as resp:
                return resp.status < 400
        except Exception:
            return False


@dataclass
class InMemorySink:
    """Captures alerts in memory (for testing)."""
    sent: list[list[Alert]] = field(default_factory=list)

    def send(self, alerts: list[Alert]) -> bool:
        self.sent.append(list(alerts))
        return True

    @property
    def total_notifications(self) -> int:
        return sum(len(batch) for batch in self.sent)

    @property
    def all_alerts(self) -> list[Alert]:
        return [a for batch in self.sent for a in batch]


# --- Silence rule ---

@dataclass(slots=True)
class SilenceRule:
    """Suppress alerts matching specific labels."""
    matchers: dict[str, str]          # label key → value to match
    expires_at: datetime
    reason: str = ""

    def matches(self, alert: Alert) -> bool:
        for k, v in self.matchers.items():
            if alert.labels.get(k) != v:
                return False
        return True

    @property
    def is_expired(self) -> bool:
        return _utc_now() > self.expires_at


# --- Alert engine ---

@dataclass
class AlertEngineStats:
    evaluations: int = 0
    alerts_fired: int = 0
    alerts_resolved: int = 0
    notifications_sent: int = 0
    notifications_suppressed: int = 0
    notifications_failed: int = 0


class AlertEngine:
    """Core alert evaluation engine.

    Usage:
        engine = AlertEngine()
        engine.add_rule(AlertRule(...))
        engine.add_sink(InMemorySink())
        engine.evaluate(slo_statuses, anomalies)
    """

    def __init__(self) -> None:
        self._rules: dict[str, AlertRule] = {}
        self._alerts: dict[str, Alert] = {}       # fingerprint → Alert
        self._sinks: list[Any] = []                # AlertSink instances
        self._silences: list[SilenceRule] = []
        self._stats = AlertEngineStats()

    def add_rule(self, rule: AlertRule) -> None:
        self._rules[rule.name] = rule

    def remove_rule(self, name: str) -> None:
        self._rules.pop(name, None)

    def add_sink(self, sink: Any) -> None:
        self._sinks.append(sink)

    def add_silence(self, silence: SilenceRule) -> None:
        self._silences.append(silence)

    def evaluate(
        self,
        slo_statuses: dict[str, SLOStatus] | None = None,
        anomalies: list[MetricAnomaly] | None = None,
        now: datetime | None = None,
    ) -> list[Alert]:
        """Evaluate all rules and process state transitions.

        Returns list of alerts that changed state (newly firing or resolved).
        """
        now = now or _utc_now()
        slo_statuses = slo_statuses or {}
        anomalies = anomalies or []
        self._stats.evaluations += 1

        # Clean expired silences
        self._silences = [s for s in self._silences if not s.is_expired]

        changed: list[Alert] = []

        for rule in self._rules.values():
            # Evaluate condition
            is_firing = self._evaluate_condition(rule.condition, slo_statuses, anomalies)
            description = self._describe_condition(rule.condition, slo_statuses, anomalies)

            alert = self._get_or_create_alert(rule)
            prev_state = alert.state

            if is_firing:
                alert.last_triggered = now
                alert.description = description
                if alert.first_triggered is None:
                    alert.first_triggered = now

                if alert.state == AlertState.INACTIVE or alert.state == AlertState.RESOLVED:
                    if rule.pending_seconds > 0:
                        alert.state = AlertState.PENDING
                    else:
                        alert.state = AlertState.FIRING
                        alert.fire_count += 1
                        self._stats.alerts_fired += 1
                elif alert.state == AlertState.PENDING:
                    elapsed = (now - alert.first_triggered).total_seconds()
                    if elapsed >= rule.pending_seconds:
                        alert.state = AlertState.FIRING
                        alert.fire_count += 1
                        self._stats.alerts_fired += 1
                # FIRING stays FIRING (dedup)
            else:
                if alert.state == AlertState.FIRING:
                    alert.state = AlertState.RESOLVED
                    alert.resolved_at = now
                    self._stats.alerts_resolved += 1
                elif alert.state == AlertState.PENDING:
                    alert.state = AlertState.INACTIVE
                    alert.first_triggered = None

            if alert.state != prev_state:
                changed.append(alert)

        # Send notifications for firing/resolved alerts
        to_notify = [a for a in changed if a.state in {AlertState.FIRING, AlertState.RESOLVED}]
        if to_notify:
            self._notify(to_notify, now)

        return changed

    def get_active_alerts(self) -> list[Alert]:
        return [a for a in self._alerts.values()
                if a.state in {AlertState.FIRING, AlertState.PENDING}]

    def get_all_alerts(self) -> list[Alert]:
        return list(self._alerts.values())

    @property
    def stats(self) -> AlertEngineStats:
        return self._stats

    # --- Internal ---

    def _evaluate_condition(
        self, condition: AlertCondition,
        slo_statuses: dict[str, SLOStatus],
        anomalies: list[MetricAnomaly],
    ) -> bool:
        if isinstance(condition, SLOBurnCondition):
            return condition.evaluate(slo_statuses)
        elif isinstance(condition, AnomalyCondition):
            return condition.evaluate(anomalies)
        elif isinstance(condition, CustomCondition):
            return condition.evaluate()
        return False

    def _describe_condition(
        self, condition: AlertCondition,
        slo_statuses: dict[str, SLOStatus],
        anomalies: list[MetricAnomaly],
    ) -> str:
        if isinstance(condition, SLOBurnCondition):
            return condition.describe(slo_statuses)
        elif isinstance(condition, AnomalyCondition):
            return condition.describe(anomalies)
        elif isinstance(condition, CustomCondition):
            return condition.describe()
        return ""

    def _get_or_create_alert(self, rule: AlertRule) -> Alert:
        # Use rule name + labels as fingerprint
        alert = Alert(
            rule_name=rule.name,
            severity=rule.severity,
            labels=dict(rule.labels),
            annotations=dict(rule.annotations),
            group_key=rule.group_key or rule.name,
        )
        fp = alert.fingerprint
        if fp not in self._alerts:
            self._alerts[fp] = alert
        return self._alerts[fp]

    def _notify(self, alerts: list[Alert], now: datetime) -> None:
        """Send notifications, respecting silences and repeat intervals."""
        to_send: list[Alert] = []
        for alert in alerts:
            # Check silences
            if any(s.matches(alert) for s in self._silences):
                alert.state = AlertState.SILENCED
                self._stats.notifications_suppressed += 1
                continue
            # Check repeat interval
            rule = self._rules.get(alert.rule_name)
            if rule and alert.last_notified:
                elapsed = (now - alert.last_notified).total_seconds()
                if elapsed < rule.repeat_interval_seconds and alert.state == AlertState.FIRING:
                    self._stats.notifications_suppressed += 1
                    continue
            to_send.append(alert)
            alert.last_notified = now

        if not to_send:
            return

        # Group by group_key
        groups: dict[str, list[Alert]] = defaultdict(list)
        for alert in to_send:
            groups[alert.group_key].append(alert)

        # Send each group to all sinks
        for group_key, group_alerts in groups.items():
            for sink in self._sinks:
                try:
                    success = sink.send(group_alerts)
                    if success:
                        self._stats.notifications_sent += len(group_alerts)
                    else:
                        self._stats.notifications_failed += len(group_alerts)
                except Exception:
                    self._stats.notifications_failed += len(group_alerts)
