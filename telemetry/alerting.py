"""Alerting subsystem — rule evaluation, state machine, notification routing.

Components:
- AlertRule: defines when to fire (metric threshold, SLO burn rate, anomaly)
- Alert: stateful object with lifecycle (pending → firing → resolved)
- AlertManager: evaluates rules, manages alert state, deduplicates, groups
- NotificationRouter: routes alerts to sinks by severity/service/label
- WebhookSink: sends alert payloads to HTTP endpoints

Alert state machine:
    INACTIVE → PENDING (condition met, waiting for for_duration)
    PENDING → FIRING (condition sustained through for_duration)
    FIRING → RESOLVED (condition no longer met for resolve_duration)
    PENDING → INACTIVE (condition cleared before for_duration elapsed)

Grouping:
    Multiple alerts with the same group_key are coalesced into one
    notification. E.g., 10 pods failing on the same service = 1 alert.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Protocol
from uuid import uuid4

from netobserv.telemetry.processing.metric_anomaly_detector import MetricAnomaly
from netobserv.telemetry.slo import SLOEvaluation, SLOStatus


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class AlertSeverity(str, Enum):
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class AlertState(str, Enum):
    INACTIVE = "inactive"
    PENDING = "pending"
    FIRING = "firing"
    RESOLVED = "resolved"


class AlertSource(str, Enum):
    METRIC_ANOMALY = "metric_anomaly"
    SLO_BURN_RATE = "slo_burn_rate"
    SLO_BUDGET_EXHAUSTED = "slo_budget_exhausted"
    CUSTOM = "custom"


@dataclass
class AlertRule:
    """Defines when an alert should fire."""
    rule_id: str
    name: str
    severity: AlertSeverity = AlertSeverity.WARNING
    source: AlertSource = AlertSource.CUSTOM
    description: str = ""

    # Timing
    for_duration_seconds: float = 0.0     # must be true for this long before firing
    resolve_duration_seconds: float = 60.0  # must be false for this long to resolve

    # Routing
    service: str = ""                     # if set, only evaluate for this service
    group_key: str = ""                   # alerts with same group_key coalesce
    labels: dict[str, str] = field(default_factory=dict)
    notification_channels: list[str] = field(default_factory=lambda: ["default"])

    # SLO-specific
    slo_id: str = ""
    burn_rate_threshold: float = 0.0

    # Metric anomaly-specific
    metric_name_pattern: str = ""
    anomaly_type: str = ""

    # Custom condition (callable returning bool)
    condition: Callable[[], bool] | None = None

    # Silencing
    silenced: bool = False
    silenced_until: datetime | None = None


@dataclass
class Alert:
    """A stateful alert instance."""
    alert_id: str = field(default_factory=lambda: f"alert-{uuid4().hex[:12]}")
    rule_id: str = ""
    name: str = ""
    severity: AlertSeverity = AlertSeverity.WARNING
    state: AlertState = AlertState.INACTIVE
    source: AlertSource = AlertSource.CUSTOM

    # Timing
    first_triggered: datetime | None = None
    last_triggered: datetime | None = None
    fired_at: datetime | None = None
    resolved_at: datetime | None = None

    # Context
    service: str = ""
    labels: dict[str, str] = field(default_factory=dict)
    annotations: dict[str, str] = field(default_factory=dict)
    group_key: str = ""

    # Evidence
    value: float = 0.0
    threshold: float = 0.0
    description: str = ""

    @property
    def is_active(self) -> bool:
        return self.state in {AlertState.PENDING, AlertState.FIRING}

    @property
    def duration_seconds(self) -> float:
        if not self.first_triggered:
            return 0.0
        end = self.resolved_at or _utc_now()
        return (end - self.first_triggered).total_seconds()


@dataclass(slots=True)
class AlertNotification:
    """Payload sent to notification sinks."""
    alert: Alert
    channel: str
    timestamp: datetime = field(default_factory=_utc_now)
    group_size: int = 1               # how many alerts coalesced into this


class NotificationSink(Protocol):
    """Protocol for notification sinks."""
    def send(self, notification: AlertNotification) -> bool: ...


class WebhookSink:
    """Sends alert notifications to an HTTP webhook endpoint.

    In production this would use httpx/aiohttp. For now it records
    payloads for testing and provides the contract.
    """

    def __init__(self, url: str = "", name: str = "webhook") -> None:
        self.url = url
        self.name = name
        self.sent: list[AlertNotification] = []
        self.failed: int = 0

    def send(self, notification: AlertNotification) -> bool:
        payload = {
            "alert_id": notification.alert.alert_id,
            "name": notification.alert.name,
            "severity": notification.alert.severity.value,
            "state": notification.alert.state.value,
            "service": notification.alert.service,
            "description": notification.alert.description,
            "value": notification.alert.value,
            "labels": notification.alert.labels,
            "group_size": notification.group_size,
            "timestamp": notification.timestamp.isoformat(),
        }
        # In production: httpx.post(self.url, json=payload)
        self.sent.append(notification)
        return True


class LogSink:
    """Logs alert notifications. Always-on fallback sink."""

    def __init__(self) -> None:
        self.logged: list[AlertNotification] = []

    def send(self, notification: AlertNotification) -> bool:
        self.logged.append(notification)
        return True


class NotificationRouter:
    """Routes notifications to sinks by channel name."""

    def __init__(self) -> None:
        self._sinks: dict[str, list[NotificationSink]] = defaultdict(list)
        self._default_sink = LogSink()
        self._sinks["default"].append(self._default_sink)
        self._total_sent = 0
        self._total_failed = 0

    def register_sink(self, channel: str, sink: NotificationSink) -> None:
        self._sinks[channel].append(sink)

    def route(self, notification: AlertNotification) -> int:
        """Route a notification to its channel sinks. Returns count sent."""
        sent = 0
        sinks = self._sinks.get(notification.channel, self._sinks["default"])
        for sink in sinks:
            try:
                if sink.send(notification):
                    sent += 1
                    self._total_sent += 1
                else:
                    self._total_failed += 1
            except Exception:
                self._total_failed += 1
        return sent

    @property
    def stats(self) -> dict[str, int]:
        return {"total_sent": self._total_sent, "total_failed": self._total_failed}


class AlertManager:
    """Evaluates alert rules, manages state, routes notifications.

    Usage:
        manager = AlertManager()
        manager.add_rule(AlertRule(
            rule_id="slo-checkout-latency",
            name="Checkout latency SLO burn rate",
            severity=AlertSeverity.CRITICAL,
            source=AlertSource.SLO_BURN_RATE,
            slo_id="checkout-latency",
            burn_rate_threshold=14.4,
        ))
        manager.register_sink("pagerduty", pagerduty_sink)

        # Evaluate periodically
        manager.evaluate_slo(slo_evaluation)
        manager.evaluate_anomaly(metric_anomaly)
    """

    def __init__(self, router: NotificationRouter | None = None) -> None:
        self._rules: dict[str, AlertRule] = {}
        self._alerts: dict[str, Alert] = {}  # rule_id → Alert
        self._router = router or NotificationRouter()
        self._total_evaluations = 0
        self._total_fired = 0
        self._total_resolved = 0

    def add_rule(self, rule: AlertRule) -> None:
        self._rules[rule.rule_id] = rule

    def remove_rule(self, rule_id: str) -> None:
        self._rules.pop(rule_id, None)
        self._alerts.pop(rule_id, None)

    def silence_rule(self, rule_id: str, until: datetime | None = None) -> None:
        rule = self._rules.get(rule_id)
        if rule:
            rule.silenced = True
            rule.silenced_until = until

    def unsilence_rule(self, rule_id: str) -> None:
        rule = self._rules.get(rule_id)
        if rule:
            rule.silenced = False
            rule.silenced_until = None

    def register_sink(self, channel: str, sink: NotificationSink) -> None:
        self._router.register_sink(channel, sink)

    # --- Evaluation ---

    def evaluate_slo(self, evaluation: SLOEvaluation, now: datetime | None = None) -> list[Alert]:
        """Evaluate SLO-based alert rules against an SLO evaluation result."""
        now = now or _utc_now()
        fired: list[Alert] = []

        for rule in self._rules.values():
            if rule.source not in {AlertSource.SLO_BURN_RATE, AlertSource.SLO_BUDGET_EXHAUSTED}:
                continue
            if rule.slo_id and rule.slo_id != evaluation.slo_id:
                continue
            if rule.service and rule.service != evaluation.service:
                continue

            condition_met = False
            value = 0.0
            description = ""

            if rule.source == AlertSource.SLO_BURN_RATE:
                worst = evaluation.worst_burn_rate
                value = worst
                condition_met = worst >= rule.burn_rate_threshold
                description = (
                    f"SLO '{evaluation.slo_id}' burn rate {worst:.1f}x "
                    f"(threshold: {rule.burn_rate_threshold}x)"
                )
            elif rule.source == AlertSource.SLO_BUDGET_EXHAUSTED:
                condition_met = evaluation.status == SLOStatus.BREACHED
                value = evaluation.error_budget_consumed_percent
                description = (
                    f"SLO '{evaluation.slo_id}' error budget exhausted "
                    f"({evaluation.error_budget_consumed_percent:.1f}% consumed)"
                )

            alert = self._transition(rule, condition_met, value, description, now)
            if alert and alert.state == AlertState.FIRING:
                fired.append(alert)

        return fired

    def evaluate_anomaly(self, anomaly: MetricAnomaly, now: datetime | None = None) -> list[Alert]:
        """Evaluate anomaly-based alert rules."""
        now = now or _utc_now()
        fired: list[Alert] = []

        for rule in self._rules.values():
            if rule.source != AlertSource.METRIC_ANOMALY:
                continue
            if rule.metric_name_pattern and rule.metric_name_pattern != anomaly.metric_name:
                continue
            if rule.anomaly_type and rule.anomaly_type != anomaly.anomaly_type:
                continue
            if rule.service and rule.service != anomaly.service:
                continue

            alert = self._transition(
                rule, True, anomaly.current_value,
                anomaly.explanation, now,
            )
            if alert and alert.state == AlertState.FIRING:
                fired.append(alert)

        return fired

    def evaluate_custom(self, rule_id: str, condition: bool, value: float = 0.0,
                        description: str = "", now: datetime | None = None) -> Alert | None:
        """Evaluate a custom alert rule."""
        now = now or _utc_now()
        rule = self._rules.get(rule_id)
        if not rule:
            return None
        return self._transition(rule, condition, value, description, now)

    def resolve_stale(self, max_age_seconds: float = 300.0,
                      now: datetime | None = None) -> list[Alert]:
        """Resolve alerts that haven't been re-triggered within max_age."""
        now = now or _utc_now()
        cutoff = now - timedelta(seconds=max_age_seconds)
        resolved: list[Alert] = []
        for alert in self._alerts.values():
            if alert.state == AlertState.FIRING and alert.last_triggered:
                if alert.last_triggered < cutoff:
                    alert.state = AlertState.RESOLVED
                    alert.resolved_at = now
                    self._total_resolved += 1
                    resolved.append(alert)
                    self._notify(alert, self._rules.get(alert.rule_id))
        return resolved

    # --- Queries ---

    def get_alert(self, rule_id: str) -> Alert | None:
        return self._alerts.get(rule_id)

    def active_alerts(self) -> list[Alert]:
        return [a for a in self._alerts.values() if a.is_active]

    def firing_alerts(self) -> list[Alert]:
        return [a for a in self._alerts.values() if a.state == AlertState.FIRING]

    def all_alerts(self) -> list[Alert]:
        return list(self._alerts.values())

    @property
    def stats(self) -> dict[str, int]:
        return {
            "rules": len(self._rules),
            "total_evaluations": self._total_evaluations,
            "total_fired": self._total_fired,
            "total_resolved": self._total_resolved,
            "active": len(self.active_alerts()),
            "firing": len(self.firing_alerts()),
            "notification": self._router.stats,
        }

    # --- Internal ---

    def _transition(self, rule: AlertRule, condition_met: bool, value: float,
                    description: str, now: datetime) -> Alert | None:
        """State machine transition for an alert."""
        self._total_evaluations += 1

        # Check silencing
        if rule.silenced:
            if rule.silenced_until and now >= rule.silenced_until:
                rule.silenced = False
            else:
                return None

        alert = self._alerts.get(rule.rule_id)
        if not alert:
            alert = Alert(
                rule_id=rule.rule_id,
                name=rule.name,
                severity=rule.severity,
                source=rule.source,
                service=rule.service,
                labels=dict(rule.labels),
                group_key=rule.group_key or rule.rule_id,
            )
            self._alerts[rule.rule_id] = alert

        old_state = alert.state
        alert.value = value
        alert.description = description

        if condition_met:
            alert.last_triggered = now
            if alert.state == AlertState.INACTIVE or alert.state == AlertState.RESOLVED:
                alert.first_triggered = now
                if rule.for_duration_seconds <= 0:
                    alert.state = AlertState.FIRING
                    alert.fired_at = now
                    self._total_fired += 1
                else:
                    alert.state = AlertState.PENDING
            elif alert.state == AlertState.PENDING:
                elapsed = (now - (alert.first_triggered or now)).total_seconds()
                if elapsed >= rule.for_duration_seconds:
                    alert.state = AlertState.FIRING
                    alert.fired_at = now
                    self._total_fired += 1
        else:
            if alert.state == AlertState.PENDING:
                alert.state = AlertState.INACTIVE
            elif alert.state == AlertState.FIRING:
                if alert.last_triggered:
                    since_last = (now - alert.last_triggered).total_seconds()
                    if since_last >= rule.resolve_duration_seconds:
                        alert.state = AlertState.RESOLVED
                        alert.resolved_at = now
                        self._total_resolved += 1

        # Notify on state change
        if alert.state != old_state and alert.state in {AlertState.FIRING, AlertState.RESOLVED}:
            self._notify(alert, rule)

        return alert

    def _notify(self, alert: Alert, rule: AlertRule | None) -> None:
        channels = rule.notification_channels if rule else ["default"]
        for channel in channels:
            notification = AlertNotification(alert=alert, channel=channel)
            self._router.route(notification)
