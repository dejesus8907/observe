"""Metric ingestion adapters — OTLP and Prometheus formats.

OtlpMetricAdapter: normalizes OTLP metric data (ExportMetricsServiceRequest
style dicts) into TelemetryEnvelopes with MetricPoint payloads. Handles
counters, gauges, histograms, and summaries. Extracts exemplars.

PrometheusAdapter: normalizes Prometheus scrape/remote-write style data
(metric name + labels + value) into TelemetryEnvelopes. Simpler format
but covers the common case of infrastructure metrics.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from netobserv.telemetry.envelope import TelemetryEnvelope
from netobserv.telemetry.enums import MetricType, Severity, SignalType, SourceType
from netobserv.telemetry.identity import RequestRef, TargetRef
from netobserv.telemetry.resource_normalizer import ResourceNormalizer
from netobserv.telemetry.signals import Exemplar, MetricPoint


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _nanos_to_dt(nanos: int) -> datetime | None:
    if not nanos:
        return None
    return datetime.fromtimestamp(nanos / 1_000_000_000, tz=timezone.utc)


# Shared normalizer instance
_normalizer = ResourceNormalizer()


def _extract_attributes(attrs: list[dict] | dict) -> dict[str, str]:
    return _normalizer.flatten_otlp_attributes(attrs)


def _build_target(resource_attrs: dict[str, str]) -> TargetRef:
    return _normalizer.normalize(resource_attrs)


def _extract_exemplars(exemplar_list: list[dict]) -> list[Exemplar]:
    results: list[Exemplar] = []
    for ex in exemplar_list:
        trace_id = str(ex.get("trace_id", ex.get("traceId", "")))
        span_id = str(ex.get("span_id", ex.get("spanId", "")))
        if not trace_id:
            # Check filtered_attributes for trace.id
            for attr in ex.get("filtered_attributes", []):
                if attr.get("key") == "trace_id":
                    val = attr.get("value", {})
                    trace_id = str(val.get("stringValue", val)) if isinstance(val, dict) else str(val)
        if trace_id:
            results.append(Exemplar(
                trace_id=trace_id,
                span_id=span_id or None,
                value=float(ex.get("value", ex.get("as_double", 0.0))),
                timestamp=_nanos_to_dt(ex.get("time_unix_nano", 0)),
                attrs=_extract_attributes(ex.get("filtered_attributes", [])),
            ))
    return results


class OtlpMetricAdapter:
    """Normalizes OTLP metric data into TelemetryEnvelopes.

    Usage:
        adapter = OtlpMetricAdapter()
        envelopes = adapter.process_resource_metrics(otlp_data)
        for env in envelopes:
            metric_store.store_metric(env)
    """

    def process_resource_metrics(
        self, resource_metrics_list: list[dict[str, Any]]
    ) -> list[TelemetryEnvelope]:
        envelopes: list[TelemetryEnvelope] = []
        for rm in resource_metrics_list:
            resource = rm.get("resource", {})
            resource_attrs = _extract_attributes(resource.get("attributes", []))
            target = _build_target(resource_attrs)

            for scope_metrics in rm.get("scope_metrics", []):
                for metric in scope_metrics.get("metrics", []):
                    envs = self._metric_to_envelopes(metric, target)
                    envelopes.extend(envs)
        return envelopes

    def process_metrics(
        self, metrics: list[dict[str, Any]],
        resource_attrs: dict[str, str] | None = None,
    ) -> list[TelemetryEnvelope]:
        """Convenience: process a flat list of metric dicts."""
        target = _build_target(resource_attrs or {})
        envelopes: list[TelemetryEnvelope] = []
        for metric in metrics:
            envelopes.extend(self._metric_to_envelopes(metric, target))
        return envelopes

    def _metric_to_envelopes(
        self, metric: dict[str, Any], target: TargetRef
    ) -> list[TelemetryEnvelope]:
        name = str(metric.get("name", ""))
        unit = str(metric.get("unit", ""))
        if not name:
            return []

        # Determine metric type and data points
        if "gauge" in metric:
            return self._process_gauge(name, unit, metric["gauge"], target)
        if "sum" in metric:
            return self._process_sum(name, unit, metric["sum"], target)
        if "histogram" in metric:
            return self._process_histogram(name, unit, metric["histogram"], target)
        if "summary" in metric:
            return self._process_summary(name, unit, metric["summary"], target)

        # Fallback: treat as simple gauge-like data
        if "data_points" in metric:
            return self._process_gauge(name, unit, metric, target)
        return []

    def _process_gauge(self, name, unit, data, target) -> list[TelemetryEnvelope]:
        return [
            self._point_to_envelope(name, unit, MetricType.GAUGE, dp, target)
            for dp in data.get("data_points", [])
        ]

    def _process_sum(self, name, unit, data, target) -> list[TelemetryEnvelope]:
        is_mono = data.get("is_monotonic", False)
        mt = MetricType.COUNTER if is_mono else MetricType.GAUGE
        return [
            self._point_to_envelope(name, unit, mt, dp, target, is_monotonic=is_mono)
            for dp in data.get("data_points", [])
        ]

    def _process_histogram(self, name, unit, data, target) -> list[TelemetryEnvelope]:
        results: list[TelemetryEnvelope] = []
        for dp in data.get("data_points", []):
            labels = _extract_attributes(dp.get("attributes", []))
            exemplars = _extract_exemplars(dp.get("exemplars", []))
            ts = _nanos_to_dt(dp.get("time_unix_nano", 0)) or _utc_now()

            point = MetricPoint(
                metric_name=name,
                metric_type=MetricType.HISTOGRAM,
                value=float(dp.get("sum", 0.0)) / max(int(dp.get("count", 1)), 1),
                unit=unit,
                labels=labels,
                bucket_bounds=dp.get("explicit_bounds", []),
                bucket_counts=dp.get("bucket_counts", []),
                histogram_sum=float(dp.get("sum", 0.0)),
                histogram_count=int(dp.get("count", 0)),
                exemplars=exemplars,
                start_time=_nanos_to_dt(dp.get("start_time_unix_nano", 0)),
            )
            request = None
            if exemplars:
                request = RequestRef(exemplar_trace_id=exemplars[0].trace_id)

            results.append(TelemetryEnvelope(
                signal_type=SignalType.METRIC,
                source_type=SourceType.OTLP,
                target=target,
                request=request,
                observed_at=ts,
                payload=point,
                ingestion_pipeline="otlp_metric",
            ))
        return results

    def _process_summary(self, name, unit, data, target) -> list[TelemetryEnvelope]:
        return [
            self._point_to_envelope(name, unit, MetricType.SUMMARY, dp, target)
            for dp in data.get("data_points", [])
        ]

    def _point_to_envelope(self, name, unit, metric_type, dp, target,
                           is_monotonic=False) -> TelemetryEnvelope:
        labels = _extract_attributes(dp.get("attributes", []))
        value = float(dp.get("as_double", dp.get("as_int", dp.get("value", 0.0))))
        ts = _nanos_to_dt(dp.get("time_unix_nano", 0)) or _utc_now()
        exemplars = _extract_exemplars(dp.get("exemplars", []))

        point = MetricPoint(
            metric_name=name,
            metric_type=metric_type,
            value=value,
            unit=unit,
            labels=labels,
            exemplars=exemplars,
            start_time=_nanos_to_dt(dp.get("start_time_unix_nano", 0)),
            is_monotonic=is_monotonic,
        )
        request = None
        if exemplars:
            request = RequestRef(exemplar_trace_id=exemplars[0].trace_id)

        return TelemetryEnvelope(
            signal_type=SignalType.METRIC,
            source_type=SourceType.OTLP,
            target=target,
            request=request,
            observed_at=ts,
            payload=point,
            ingestion_pipeline="otlp_metric",
        )


class PrometheusAdapter:
    """Normalizes Prometheus-style metric data into TelemetryEnvelopes.

    Accepts the simple format: {name, labels, value, timestamp, exemplar?}

    Usage:
        adapter = PrometheusAdapter()
        envelopes = adapter.process_samples(samples)
    """

    def process_samples(
        self,
        samples: list[dict[str, Any]],
        default_service: str = "",
    ) -> list[TelemetryEnvelope]:
        envelopes: list[TelemetryEnvelope] = []
        for sample in samples:
            env = self._sample_to_envelope(sample, default_service)
            if env:
                envelopes.append(env)
        return envelopes

    def _sample_to_envelope(self, sample: dict[str, Any],
                             default_service: str) -> TelemetryEnvelope | None:
        name = str(sample.get("name", sample.get("metric_name", "")))
        if not name:
            return None

        labels = dict(sample.get("labels", {}))
        value = float(sample.get("value", 0.0))
        ts = sample.get("timestamp")
        if isinstance(ts, (int, float)):
            ts = datetime.fromtimestamp(ts, tz=timezone.utc)
        elif not isinstance(ts, datetime):
            ts = _utc_now()

        # Build target via normalizer from Prometheus labels
        if default_service and "service" not in labels and "job" not in labels:
            labels["service"] = default_service
        result = _normalizer.from_prometheus_labels(labels)
        target = result.target
        # Remove mapped label keys from the metric labels
        metric_labels = {k: v for k, v in labels.items()
                        if k not in result.mapped_fields.values()
                        and k not in ("service", "job")}

        # Infer metric type from name conventions
        if name.endswith("_total"):
            metric_type = MetricType.COUNTER
        elif name.endswith("_bucket") or name.endswith("_sum") or name.endswith("_count"):
            metric_type = MetricType.HISTOGRAM
        else:
            metric_type = MetricType.GAUGE

        # Extract exemplar if present
        exemplars: list[Exemplar] = []
        ex_data = sample.get("exemplar")
        if ex_data and isinstance(ex_data, dict):
            trace_id = str(ex_data.get("trace_id", ""))
            if trace_id:
                exemplars.append(Exemplar(
                    trace_id=trace_id,
                    span_id=ex_data.get("span_id"),
                    value=float(ex_data.get("value", value)),
                ))

        point = MetricPoint(
            metric_name=name,
            metric_type=metric_type,
            value=value,
            labels=metric_labels,
            exemplars=exemplars,
        )

        request = None
        if exemplars:
            request = RequestRef(exemplar_trace_id=exemplars[0].trace_id)

        return TelemetryEnvelope(
            signal_type=SignalType.METRIC,
            source_type=SourceType.PROMETHEUS,
            target=target,
            request=request,
            observed_at=ts,
            payload=point,
            ingestion_pipeline="prometheus",
        )
