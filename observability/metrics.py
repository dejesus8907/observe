"""Prometheus-based metrics for the NetObserv platform.

This module intentionally centralizes metric definitions instead of letting every
subsystem create ad-hoc counters. Ad-hoc metric creation is how platforms end
up with duplicate names, inconsistent label sets, and dashboards full of junk.

The collector below keeps the API simple for callers while enforcing a stable
metric contract.
"""

from __future__ import annotations

import contextlib
import threading
import time
from functools import lru_cache
from typing import Iterator

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, REGISTRY

_BUCKETS = (0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0)


class MetricsCollector:
    """Central registry of platform metrics.

    The collector exposes small, explicit helper methods so the rest of the
    codebase does not have to know Prometheus label syntax or metric names.
    That keeps instrumentation readable and reduces the chance of cardinality
    disasters from free-form labels.
    """

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        r = registry or REGISTRY
        self._lock = threading.Lock()
        self._active_workflow_count = 0

        # Discovery
        self.discovery_targets_total = Counter(
            "netobserv_discovery_targets_total",
            "Total discovery targets attempted",
            registry=r,
        )
        self.discovery_successes_total = Counter(
            "netobserv_discovery_successes_total",
            "Successful discovery collections",
            ["connector"],
            registry=r,
        )
        self.discovery_failures_total = Counter(
            "netobserv_discovery_failures_total",
            "Failed discovery collections",
            ["connector", "error_class"],
            registry=r,
        )
        self.discovery_collection_duration_seconds = Histogram(
            "netobserv_discovery_collection_duration_seconds",
            "Discovery collection duration per connector call",
            ["connector", "result"],
            buckets=_BUCKETS,
            registry=r,
        )

        # Inventory / parsing / normalization
        self.inventory_targets_emitted_total = Counter(
            "netobserv_inventory_targets_emitted_total",
            "Inventory targets emitted by source",
            ["source_type"],
            registry=r,
        )
        self.parser_records_total = Counter(
            "netobserv_parser_records_total",
            "Parsed records emitted by parser",
            ["parser", "record_type"],
            registry=r,
        )
        self.normalization_warnings_total = Counter(
            "netobserv_normalization_warnings_total",
            "Normalization warnings emitted",
            ["warning_type"],
            registry=r,
        )

        # Topology
        self.topology_edges_inferred_total = Counter(
            "netobserv_topology_edges_inferred_total",
            "Topology edges inferred",
            ["edge_type"],
            registry=r,
        )
        self.topology_conflicts_total = Counter(
            "netobserv_topology_conflicts_total",
            "Topology conflicts detected",
            ["conflict_type"],
            registry=r,
        )

        # Drift / sync / replay
        self.drift_records_total = Counter(
            "netobserv_drift_records_total",
            "Drift records generated",
            ["mismatch_type", "severity"],
            registry=r,
        )
        self.sync_plans_total = Counter(
            "netobserv_sync_plans_total",
            "Sync plans generated",
            ["mode"],
            registry=r,
        )
        self.sync_actions_total = Counter(
            "netobserv_sync_actions_total",
            "Sync actions evaluated or executed",
            ["action", "result"],
            registry=r,
        )
        self.replay_runs_total = Counter(
            "netobserv_replay_runs_total",
            "Replay runs executed",
            ["result"],
            registry=r,
        )

        # Workflow lifecycle
        self.workflow_runs_total = Counter(
            "netobserv_workflow_runs_total",
            "Workflow runs created",
            ["workflow_type"],
            registry=r,
        )
        self.workflow_terminal_total = Counter(
            "netobserv_workflow_terminal_total",
            "Workflow terminal states recorded",
            ["workflow_type", "status"],
            registry=r,
        )
        self.workflow_stage_duration_seconds = Histogram(
            "netobserv_workflow_stage_duration_seconds",
            "Workflow stage execution duration",
            ["stage", "workflow_type"],
            buckets=_BUCKETS,
            registry=r,
        )
        self.workflow_stage_total = Counter(
            "netobserv_workflow_stage_total",
            "Workflow stages completed",
            ["stage", "workflow_type", "result"],
            registry=r,
        )

        # Active workflows and component health are gauges because they describe
        # the current state of the world rather than a running total.
        self.active_workflows = Gauge(
            "netobserv_active_workflows",
            "Currently running workflows",
            registry=r,
        )
        self.component_health = Gauge(
            "netobserv_component_health",
            "Component health status (1=healthy, 0=degraded/unhealthy)",
            ["component"],
            registry=r,
        )
        self.component_last_success_unixtime = Gauge(
            "netobserv_component_last_success_unixtime",
            "Unix time of the last known component success",
            ["component"],
            registry=r,
        )

        # Generic operation duration helper. The name is intentionally broad so
        # modules can instrument meaningful work without creating one-off metrics
        # for every micro-operation.
        self.operation_duration_seconds = Histogram(
            "netobserv_operation_duration_seconds",
            "Duration of a named platform operation",
            ["operation", "result"],
            buckets=_BUCKETS,
            registry=r,
        )

        # Snapshot info
        self.snapshots_total = Counter(
            "netobserv_snapshots_total",
            "Total snapshots created",
            ["status"],
            registry=r,
        )

    @staticmethod
    def _clean(value: str | None, *, default: str = "unknown") -> str:
        """Normalize free-form label values into stable low-cardinality strings."""
        cleaned = str(value or "").strip().lower().replace(" ", "_")
        return cleaned or default

    def record_discovery_target(self) -> None:
        self.discovery_targets_total.inc()

    def record_discovery_success(self, connector: str) -> None:
        self.discovery_successes_total.labels(connector=self._clean(connector)).inc()

    def record_discovery_failure(self, connector: str, error_class: str) -> None:
        self.discovery_failures_total.labels(
            connector=self._clean(connector),
            error_class=self._clean(error_class),
        ).inc()

    def record_discovery_duration(
        self,
        connector: str,
        duration: float,
        *,
        result: str = "success",
    ) -> None:
        self.discovery_collection_duration_seconds.labels(
            connector=self._clean(connector),
            result=self._clean(result),
        ).observe(max(duration, 0.0))

    def record_inventory_targets(self, source_type: str, count: int = 1) -> None:
        if count <= 0:
            return
        self.inventory_targets_emitted_total.labels(
            source_type=self._clean(source_type)
        ).inc(count)

    def record_parser_record(self, parser: str, record_type: str, count: int = 1) -> None:
        if count <= 0:
            return
        self.parser_records_total.labels(
            parser=self._clean(parser),
            record_type=self._clean(record_type),
        ).inc(count)

    def record_normalization_warning(self, warning_type: str) -> None:
        self.normalization_warnings_total.labels(
            warning_type=self._clean(warning_type)
        ).inc()

    def record_topology_edge(self, edge_type: str) -> None:
        self.topology_edges_inferred_total.labels(edge_type=self._clean(edge_type)).inc()

    def record_topology_conflict(self, conflict_type: str) -> None:
        self.topology_conflicts_total.labels(
            conflict_type=self._clean(conflict_type)
        ).inc()

    def record_drift(self, mismatch_type: str, severity: str) -> None:
        self.drift_records_total.labels(
            mismatch_type=self._clean(mismatch_type),
            severity=self._clean(severity),
        ).inc()

    def record_sync_plan(self, mode: str) -> None:
        self.sync_plans_total.labels(mode=self._clean(mode)).inc()

    def record_sync_action(self, action: str, result: str) -> None:
        self.sync_actions_total.labels(
            action=self._clean(action),
            result=self._clean(result),
        ).inc()

    def record_replay(self, result: str) -> None:
        self.replay_runs_total.labels(result=self._clean(result)).inc()

    def record_workflow_created(self, workflow_type: str) -> None:
        self.workflow_runs_total.labels(workflow_type=self._clean(workflow_type)).inc()

    def record_workflow_terminal(self, workflow_type: str, status: str) -> None:
        self.workflow_terminal_total.labels(
            workflow_type=self._clean(workflow_type),
            status=self._clean(status),
        ).inc()

    def record_stage_duration(
        self,
        stage: str,
        workflow_type: str,
        duration: float,
        *,
        result: str | None = None,
    ) -> None:
        self.workflow_stage_duration_seconds.labels(
            stage=self._clean(stage), workflow_type=self._clean(workflow_type)
        ).observe(max(duration, 0.0))
        if result is not None:
            self.workflow_stage_total.labels(
                stage=self._clean(stage),
                workflow_type=self._clean(workflow_type),
                result=self._clean(result),
            ).inc()

    def inc_active_workflows(self) -> None:
        with self._lock:
            self._active_workflow_count += 1
            self.active_workflows.set(self._active_workflow_count)

    def dec_active_workflows(self) -> None:
        with self._lock:
            self._active_workflow_count = max(0, self._active_workflow_count - 1)
            self.active_workflows.set(self._active_workflow_count)

    def set_component_health(self, component: str, healthy: bool) -> None:
        self.component_health.labels(component=self._clean(component)).set(1 if healthy else 0)
        if healthy:
            self.component_last_success_unixtime.labels(
                component=self._clean(component)
            ).set(time.time())

    @contextlib.contextmanager
    def time_operation(self, operation: str) -> Iterator[None]:
        """Observe the duration of an operation and classify success vs failure.

        This helper lets callers instrument work without manually handling start
        and stop timestamps every time.
        """
        started = time.perf_counter()
        result = "success"
        try:
            yield
        except Exception:
            result = "failure"
            raise
        finally:
            self.operation_duration_seconds.labels(
                operation=self._clean(operation),
                result=self._clean(result),
            ).observe(max(time.perf_counter() - started, 0.0))

    def record_snapshot(self, status: str) -> None:
        self.snapshots_total.labels(status=self._clean(status)).inc()


@lru_cache
def get_metrics() -> MetricsCollector:
    """Return the global metrics collector singleton."""
    return MetricsCollector()
