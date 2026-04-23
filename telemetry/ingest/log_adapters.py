"""Log ingestion adapters — OTLP and structured log formats.

OtlpLogAdapter: normalizes OTLP LogRecord data into TelemetryEnvelopes.
Extracts trace_id/span_id from log records for trace/log hard-linking.

StructuredLogAdapter: normalizes JSON-structured application logs
(from stdout/stderr, log files, or log shipping agents) into
TelemetryEnvelopes. Parses trace_id from common field patterns.

Both adapters use the ResourceNormalizer for consistent target metadata.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from netobserv.telemetry.envelope import TelemetryEnvelope
from netobserv.telemetry.enums import Severity, SignalType, SourceType
from netobserv.telemetry.identity import RequestRef, TargetRef
from netobserv.telemetry.resource_normalizer import ResourceNormalizer
from netobserv.telemetry.signals import LogRecord


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _nanos_to_dt(nanos: int) -> datetime | None:
    if not nanos:
        return None
    return datetime.fromtimestamp(nanos / 1_000_000_000, tz=timezone.utc)


_normalizer = ResourceNormalizer()

# OTLP severity number → Severity enum
_SEVERITY_MAP: dict[int, Severity] = {
    0: Severity.UNSPECIFIED,
    1: Severity.TRACE, 2: Severity.TRACE, 3: Severity.TRACE, 4: Severity.TRACE,
    5: Severity.DEBUG, 6: Severity.DEBUG, 7: Severity.DEBUG, 8: Severity.DEBUG,
    9: Severity.INFO, 10: Severity.INFO, 11: Severity.INFO, 12: Severity.INFO,
    13: Severity.WARN, 14: Severity.WARN, 15: Severity.WARN, 16: Severity.WARN,
    17: Severity.ERROR, 18: Severity.ERROR, 19: Severity.ERROR, 20: Severity.ERROR,
    21: Severity.FATAL, 22: Severity.FATAL, 23: Severity.FATAL, 24: Severity.FATAL,
}

# Common severity string variants → canonical
_SEVERITY_STRINGS: dict[str, Severity] = {
    "trace": Severity.TRACE, "finest": Severity.TRACE,
    "debug": Severity.DEBUG, "fine": Severity.DEBUG,
    "info": Severity.INFO, "information": Severity.INFO, "notice": Severity.INFO,
    "warn": Severity.WARN, "warning": Severity.WARN,
    "error": Severity.ERROR, "err": Severity.ERROR, "severe": Severity.ERROR,
    "fatal": Severity.FATAL, "critical": Severity.FATAL, "crit": Severity.FATAL,
    "emergency": Severity.FATAL, "emerg": Severity.FATAL, "alert": Severity.FATAL,
}

# Common field names for trace_id in structured logs
_TRACE_ID_FIELDS = [
    "trace_id", "traceId", "traceID", "trace-id",
    "x-trace-id", "X-Trace-Id", "dd.trace_id",
    "logging.googleapis.com/trace",
]

_SPAN_ID_FIELDS = [
    "span_id", "spanId", "spanID", "span-id",
    "dd.span_id",
]

# Regex for extracting trace_id from unstructured log body
_TRACE_ID_PATTERN = re.compile(
    r'(?:trace[_\-.]?id[=: ]+)([0-9a-fA-F]{16,32})',
    re.IGNORECASE,
)
_SPAN_ID_PATTERN = re.compile(
    r'(?:span[_\-.]?id[=: ]+)([0-9a-fA-F]{16})',
    re.IGNORECASE,
)


class OtlpLogAdapter:
    """Normalizes OTLP log data into TelemetryEnvelopes.

    Usage:
        adapter = OtlpLogAdapter()
        envelopes = adapter.process_resource_logs(otlp_data)
    """

    def process_resource_logs(
        self, resource_logs_list: list[dict[str, Any]]
    ) -> list[TelemetryEnvelope]:
        envelopes: list[TelemetryEnvelope] = []
        for rl in resource_logs_list:
            resource = rl.get("resource", {})
            resource_attrs = _normalizer.flatten_otlp_attributes(
                resource.get("attributes", [])
            )
            target = _normalizer.normalize(resource_attrs)

            for scope_logs in rl.get("scope_logs", []):
                for log_record in scope_logs.get("log_records", []):
                    env = self._log_to_envelope(log_record, target)
                    if env:
                        envelopes.append(env)
        return envelopes

    def process_logs(
        self, logs: list[dict[str, Any]],
        resource_attrs: dict[str, str] | None = None,
    ) -> list[TelemetryEnvelope]:
        """Convenience: process a flat list of log record dicts."""
        target = _normalizer.normalize(resource_attrs or {})
        envelopes: list[TelemetryEnvelope] = []
        for log in logs:
            env = self._log_to_envelope(log, target)
            if env:
                envelopes.append(env)
        return envelopes

    def _log_to_envelope(
        self, log: dict[str, Any], base_target: TargetRef
    ) -> TelemetryEnvelope | None:
        # Body
        body_obj = log.get("body", {})
        if isinstance(body_obj, dict):
            body = str(body_obj.get("stringValue", body_obj.get("string_value", str(body_obj))))
        else:
            body = str(body_obj)

        # Severity
        sev_num = int(log.get("severity_number", 0))
        sev_text = str(log.get("severity_text", "")).lower().strip()
        severity = _SEVERITY_MAP.get(sev_num, Severity.UNSPECIFIED)
        if severity == Severity.UNSPECIFIED and sev_text:
            severity = _SEVERITY_STRINGS.get(sev_text, Severity.UNSPECIFIED)

        # Timestamp
        ts = _nanos_to_dt(log.get("time_unix_nano", 0))
        observed_ts = _nanos_to_dt(log.get("observed_time_unix_nano", 0))
        ts = ts or observed_ts or _utc_now()

        # Trace/span identity
        trace_id = str(log.get("trace_id", "")) or None
        span_id = str(log.get("span_id", "")) or None
        if trace_id == "":
            trace_id = None
        if span_id == "":
            span_id = None

        # Attributes
        log_attrs = _normalizer.flatten_otlp_attributes(log.get("attributes", []))

        record = LogRecord(
            body=body,
            severity=severity,
            severity_number=sev_num,
            trace_id=trace_id,
            span_id=span_id,
            attrs=log_attrs,
        )

        request = None
        if trace_id:
            request = RequestRef(trace_id=trace_id, span_id=span_id)

        return TelemetryEnvelope(
            signal_type=SignalType.LOG,
            source_type=SourceType.OTLP,
            target=base_target,
            request=request,
            observed_at=ts,
            severity=severity,
            payload=record,
            ingestion_pipeline="otlp_log",
        )


class StructuredLogAdapter:
    """Normalizes JSON-structured application logs into TelemetryEnvelopes.

    Handles logs from application stdout/stderr, log files, or log
    shipping agents (Fluentd, Vector, Filebeat). Parses trace_id from
    common field names and from the log body as a fallback.

    Usage:
        adapter = StructuredLogAdapter()
        envelopes = adapter.process_logs(log_entries, default_service="api")
    """

    def process_logs(
        self, entries: list[dict[str, Any]], default_service: str = ""
    ) -> list[TelemetryEnvelope]:
        return [
            env for entry in entries
            if (env := self._entry_to_envelope(entry, default_service)) is not None
        ]

    def _entry_to_envelope(
        self, entry: dict[str, Any], default_service: str
    ) -> TelemetryEnvelope | None:
        # Body: "message", "msg", "log", or the whole entry
        body = str(
            entry.get("message",
            entry.get("msg",
            entry.get("log",
            entry.get("body", ""))))
        )

        # Severity
        level = str(
            entry.get("level",
            entry.get("severity",
            entry.get("log.level",
            entry.get("loglevel", "info"))))
        ).lower().strip()
        severity = _SEVERITY_STRINGS.get(level, Severity.INFO)

        # Timestamp
        ts = entry.get("timestamp", entry.get("@timestamp", entry.get("time")))
        if isinstance(ts, str):
            try:
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                ts = _utc_now()
        elif isinstance(ts, (int, float)):
            ts = datetime.fromtimestamp(ts, tz=timezone.utc)
        else:
            ts = _utc_now()

        # Extract trace_id from known fields
        trace_id = None
        span_id = None
        for field in _TRACE_ID_FIELDS:
            if field in entry:
                trace_id = str(entry[field])
                break
        for field in _SPAN_ID_FIELDS:
            if field in entry:
                span_id = str(entry[field])
                break

        # Fallback: extract from body via regex
        if not trace_id and body:
            match = _TRACE_ID_PATTERN.search(body)
            if match:
                trace_id = match.group(1)
        if not span_id and body:
            match = _SPAN_ID_PATTERN.search(body)
            if match:
                span_id = match.group(1)

        # Service name
        service = str(
            entry.get("service",
            entry.get("service.name",
            entry.get("app",
            entry.get("application", default_service))))
        ) or None

        # Additional attributes
        skip_keys = {
            "message", "msg", "log", "body", "level", "severity",
            "timestamp", "@timestamp", "time", "service", "service.name",
            "app", "application", "log.level", "loglevel",
        }
        skip_keys.update(_TRACE_ID_FIELDS)
        skip_keys.update(_SPAN_ID_FIELDS)
        attrs = {k: str(v) for k, v in entry.items() if k not in skip_keys}

        record = LogRecord(
            body=body,
            severity=severity,
            trace_id=trace_id,
            span_id=span_id,
            attrs=attrs,
            logger_name=entry.get("logger", entry.get("logger_name")),
        )

        target = TargetRef(service=service)
        request = None
        if trace_id:
            request = RequestRef(trace_id=trace_id, span_id=span_id)

        return TelemetryEnvelope(
            signal_type=SignalType.LOG,
            source_type=SourceType.APP_LOG,
            target=target,
            request=request,
            observed_at=ts,
            severity=severity,
            payload=record,
            ingestion_pipeline="structured_log",
        )
