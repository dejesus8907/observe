"""Cardinality limiter for metric ingestion.

Guards against label cardinality explosion — the #1 cause of metric-storage
OOM and query collapse in production. Supports both simple global limits and
policy-driven per-metric overrides.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from threading import Lock
from typing import Any

from netobserv.telemetry.store.cardinality_policy import MetricCardinalityPolicy


@dataclass(slots=True)
class CardinalityViolation:
    metric_name: str
    violation_type: str
    label_key: str = ""
    label_value: str = ""
    current_count: int = 0
    limit: int = 0


@dataclass(slots=True)
class LimiterStats:
    total_checked: int = 0
    total_accepted: int = 0
    total_dropped: int = 0
    series_limit_drops: int = 0
    label_value_drops: int = 0
    labels_stripped: int = 0


class CardinalityLimiter:
    """Guards against metric cardinality explosion.

    The limiter tracks seen series and label values per metric and applies an
    effective policy resolved for the incoming metric name.
    """

    def __init__(
        self,
        max_series_per_metric: int = 10_000,
        max_values_per_label: int = 1_000,
        blocked_labels: set[str] | None = None,
        allowed_labels: set[str] | None = None,
        policy: MetricCardinalityPolicy | None = None,
    ) -> None:
        self._policy = policy or MetricCardinalityPolicy(
            default_max_series_per_metric=max_series_per_metric,
            default_max_values_per_label=max_values_per_label,
            default_blocked_labels=set(blocked_labels or set()),
            default_allowed_labels=(set(allowed_labels) if allowed_labels is not None else None),
        )
        self._lock = Lock()
        self._series_fingerprints: dict[str, set[str]] = defaultdict(set)
        self._label_values: dict[tuple[str, str], set[str]] = defaultdict(set)
        self._stats = LimiterStats()
        self._violations: list[CardinalityViolation] = []

    def check(self, metric_name: str, labels: dict[str, str]) -> tuple[bool, dict[str, str]]:
        self._stats.total_checked += 1
        effective = self._policy.resolve(metric_name)
        cleaned = self._strip_labels(labels, effective.blocked_labels, effective.allowed_labels)
        fingerprint = self._fingerprint(cleaned)

        with self._lock:
            existing_series = self._series_fingerprints[metric_name]
            if fingerprint not in existing_series and len(existing_series) >= (effective.max_series_per_metric or 0):
                self._stats.total_dropped += 1
                self._stats.series_limit_drops += 1
                self._violations.append(CardinalityViolation(
                    metric_name=metric_name,
                    violation_type="series_limit",
                    current_count=len(existing_series),
                    limit=effective.max_series_per_metric or 0,
                ))
                return False, cleaned

            for label_key, label_value in cleaned.items():
                values = self._label_values[(metric_name, label_key)]
                if label_value not in values and len(values) >= (effective.max_values_per_label or 0):
                    self._stats.total_dropped += 1
                    self._stats.label_value_drops += 1
                    self._violations.append(CardinalityViolation(
                        metric_name=metric_name,
                        violation_type="label_value_limit",
                        label_key=label_key,
                        label_value=label_value,
                        current_count=len(values),
                        limit=effective.max_values_per_label or 0,
                    ))
                    return False, cleaned

            self._stats.total_accepted += 1
            existing_series.add(fingerprint)
            for label_key, label_value in cleaned.items():
                self._label_values[(metric_name, label_key)].add(label_value)
            return True, cleaned

    def register_existing_series(self, metric_name: str, labels: dict[str, str]) -> None:
        with self._lock:
            self._series_fingerprints[metric_name].add(self._fingerprint(labels))
            for k, v in labels.items():
                self._label_values[(metric_name, k)].add(v)

    def exemplar_limit_for(self, metric_name: str) -> int:
        rule = self._policy.resolve(metric_name)
        return int(rule.exemplar_limit_per_series or 0)

    def get_violations(self, limit: int = 50) -> list[CardinalityViolation]:
        return self._violations[-limit:]

    @property
    def stats(self) -> LimiterStats:
        return self._stats

    def cardinality_report(self) -> dict[str, Any]:
        with self._lock:
            report: dict[str, Any] = {
                "metrics": {},
                "total_series": sum(len(v) for v in self._series_fingerprints.values()),
                "hottest_labels": [],
            }
            for metric_name, series in sorted(self._series_fingerprints.items(), key=lambda x: len(x[1]), reverse=True)[:20]:
                resolved = self._policy.resolve(metric_name)
                report["metrics"][metric_name] = {
                    "series_count": len(series),
                    "limit": resolved.max_series_per_metric,
                    "utilization": len(series) / max(resolved.max_series_per_metric or 1, 1),
                    "blocked_labels": sorted(resolved.blocked_labels),
                    "allowed_labels": sorted(resolved.allowed_labels) if resolved.allowed_labels is not None else None,
                    "exemplar_limit_per_series": resolved.exemplar_limit_per_series,
                }

            label_cards = [((metric, label), len(vals)) for (metric, label), vals in self._label_values.items()]
            label_cards.sort(key=lambda x: x[1], reverse=True)
            for (metric, label), count in label_cards[:10]:
                report["hottest_labels"].append({
                    "metric": metric,
                    "label": label,
                    "unique_values": count,
                    "limit": self._policy.resolve(metric).max_values_per_label,
                })
            return report

    def _strip_labels(self, labels: dict[str, str], blocked: set[str], allowed: set[str] | None) -> dict[str, str]:
        cleaned: dict[str, str] = {}
        for k, v in labels.items():
            if k in blocked:
                self._stats.labels_stripped += 1
                continue
            if allowed is not None and k not in allowed:
                self._stats.labels_stripped += 1
                continue
            cleaned[k] = v
        return cleaned

    @staticmethod
    def _fingerprint(labels: dict[str, str]) -> str:
        return "|".join(f"{k}={v}" for k, v in sorted(labels.items()))
