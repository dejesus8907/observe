from __future__ import annotations

from prometheus_client import CollectorRegistry

from netobserv.observability.metrics import MetricsCollector


def _sample_value(metric, labels: dict[str, str]) -> float:
    return metric.labels(**labels)._value.get()  # type: ignore[attr-defined]


def test_active_workflows_never_go_negative() -> None:
    metrics = MetricsCollector(registry=CollectorRegistry())
    metrics.dec_active_workflows()
    metrics.inc_active_workflows()
    metrics.dec_active_workflows()
    metrics.dec_active_workflows()

    assert metrics._active_workflow_count == 0


def test_operation_timer_records_success_and_failure() -> None:
    metrics = MetricsCollector(registry=CollectorRegistry())

    with metrics.time_operation("normalize"):
        pass

    try:
        with metrics.time_operation("normalize"):
            raise RuntimeError("boom")
    except RuntimeError:
        pass

    samples = metrics.operation_duration_seconds.collect()[0].samples
    success = [s for s in samples if s.labels.get("result") == "success"]
    failure = [s for s in samples if s.labels.get("result") == "failure"]
    assert success
    assert failure


def test_component_health_sets_last_success_only_when_healthy() -> None:
    metrics = MetricsCollector(registry=CollectorRegistry())
    metrics.set_component_health("db", False)
    assert _sample_value(metrics.component_health, {"component": "db"}) == 0

    metrics.set_component_health("db", True)
    assert _sample_value(metrics.component_health, {"component": "db"}) == 1
    assert _sample_value(metrics.component_last_success_unixtime, {"component": "db"}) > 0
