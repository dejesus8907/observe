"""Metric cardinality policy definitions.

Provides default and per-metric policies used by the cardinality limiter.
This is where NetObserv encodes safe label practices so metric ingestion
cannot silently destroy the platform with unbounded series growth.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from fnmatch import fnmatch
from typing import Iterable


DEFAULT_BLOCKED_LABELS = {
    "trace_id",
    "span_id",
    "request_id",
    "operation_id",
    "user_id",
    "session_id",
    "client_ip",
    "pod_uid",
    "container_id",
}


@dataclass(slots=True)
class MetricCardinalityRule:
    pattern: str
    max_series_per_metric: int | None = None
    max_values_per_label: int | None = None
    blocked_labels: set[str] = field(default_factory=set)
    allowed_labels: set[str] | None = None
    exemplar_limit_per_series: int | None = None

    def matches(self, metric_name: str) -> bool:
        return fnmatch(metric_name, self.pattern)


@dataclass(slots=True)
class MetricCardinalityPolicy:
    default_max_series_per_metric: int = 10_000
    default_max_values_per_label: int = 1_000
    default_blocked_labels: set[str] = field(default_factory=lambda: set(DEFAULT_BLOCKED_LABELS))
    default_allowed_labels: set[str] | None = None
    default_exemplar_limit_per_series: int = 16
    rules: list[MetricCardinalityRule] = field(default_factory=list)

    def resolve(self, metric_name: str) -> MetricCardinalityRule:
        effective = MetricCardinalityRule(
            pattern=metric_name,
            max_series_per_metric=self.default_max_series_per_metric,
            max_values_per_label=self.default_max_values_per_label,
            blocked_labels=set(self.default_blocked_labels),
            allowed_labels=(set(self.default_allowed_labels) if self.default_allowed_labels is not None else None),
            exemplar_limit_per_series=self.default_exemplar_limit_per_series,
        )
        for rule in self.rules:
            if not rule.matches(metric_name):
                continue
            if rule.max_series_per_metric is not None:
                effective.max_series_per_metric = rule.max_series_per_metric
            if rule.max_values_per_label is not None:
                effective.max_values_per_label = rule.max_values_per_label
            if rule.blocked_labels:
                effective.blocked_labels |= set(rule.blocked_labels)
            if rule.allowed_labels is not None:
                effective.allowed_labels = set(rule.allowed_labels)
            if rule.exemplar_limit_per_series is not None:
                effective.exemplar_limit_per_series = rule.exemplar_limit_per_series
        return effective

    @classmethod
    def from_rules(cls, rules: Iterable[MetricCardinalityRule], **kwargs) -> "MetricCardinalityPolicy":
        return cls(rules=list(rules), **kwargs)
