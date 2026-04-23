"""Trace-log correlator — hard-links logs to traces via trace_id.

Provides bidirectional navigation:
- trace → logs: "Show me all logs from this trace"
- log → trace: "Show me the trace this log belongs to"
- span → logs: "Show me logs emitted during this span"

Also supports:
- Error log → trace correlation: "This error log has trace_id, show me the full request flow"
- Log-based trace discovery: "Find traces that have error logs"
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from netobserv.telemetry.envelope import TelemetryEnvelope
from netobserv.telemetry.enums import Severity, SignalType
from netobserv.telemetry.signals import LogRecord, SpanRecord
from netobserv.telemetry.store.log_store import LogStore, LogSearchQuery
from netobserv.telemetry.store.trace_store import TraceStore, TraceView


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class TraceLogCorrelation:
    """Result of correlating a trace with its logs."""
    trace_id: str
    trace: TraceView | None = None
    logs: list[TelemetryEnvelope] = field(default_factory=list)
    error_logs: list[TelemetryEnvelope] = field(default_factory=list)
    services_with_logs: set[str] = field(default_factory=set)
    method: str = "trace_id"           # trace_id or time_window

    @property
    def log_count(self) -> int:
        return len(self.logs)

    @property
    def error_count(self) -> int:
        return len(self.error_logs)

    @property
    def has_errors(self) -> bool:
        return len(self.error_logs) > 0


@dataclass(slots=True)
class SpanLogView:
    """Logs associated with a specific span."""
    trace_id: str
    span_id: str
    service: str
    operation: str
    logs: list[TelemetryEnvelope] = field(default_factory=list)

    @property
    def log_count(self) -> int:
        return len(self.logs)


class TraceLogCorrelator:
    """Links logs to traces via trace_id.

    Usage:
        correlator = TraceLogCorrelator(trace_store, log_store)

        # Trace → logs
        result = correlator.logs_for_trace("trace-abc")

        # Log → trace
        trace = correlator.trace_for_log(log_envelope)

        # Span → logs
        span_logs = correlator.logs_for_span("trace-abc", "span-1")

        # Find traces with errors from logs
        error_traces = correlator.traces_with_error_logs(service="api")
    """

    def __init__(self, trace_store: TraceStore, log_store: LogStore) -> None:
        self._traces = trace_store
        self._logs = log_store

    def logs_for_trace(self, trace_id: str) -> TraceLogCorrelation:
        """Get all logs linked to a trace via trace_id."""
        result = TraceLogCorrelation(trace_id=trace_id)

        # Get the trace
        result.trace = self._traces.get_trace(trace_id)

        # Get logs with this trace_id
        result.logs = self._logs.get_logs_for_trace(trace_id)

        # Classify
        for env in result.logs:
            if env.target.service:
                result.services_with_logs.add(env.target.service)
            payload = env.payload
            if isinstance(payload, LogRecord):
                from netobserv.telemetry.store.log_store import _severity_rank
                if _severity_rank(payload.severity) >= _severity_rank(Severity.ERROR):
                    result.error_logs.append(env)

        return result

    def trace_for_log(self, log_envelope: TelemetryEnvelope) -> TraceView | None:
        """Given a log entry, find its trace (if trace_id is present)."""
        trace_id = self._extract_trace_id(log_envelope)
        if not trace_id:
            return None
        return self._traces.get_trace(trace_id)

    def logs_for_span(self, trace_id: str, span_id: str) -> SpanLogView:
        """Get logs emitted during a specific span."""
        all_logs = self._logs.get_logs_for_trace(trace_id)
        span_logs = []
        service = ""
        operation = ""

        for env in all_logs:
            payload = env.payload
            if isinstance(payload, LogRecord) and payload.span_id == span_id:
                span_logs.append(env)

        # Get span details from trace store
        trace = self._traces.get_trace(trace_id)
        if trace:
            for span_env in trace.spans:
                sp = span_env.payload
                if isinstance(sp, SpanRecord) and sp.span_id == span_id:
                    service = sp.service_name or span_env.target.service or ""
                    operation = sp.operation_name
                    break

        return SpanLogView(
            trace_id=trace_id,
            span_id=span_id,
            service=service,
            operation=operation,
            logs=span_logs,
        )

    def traces_with_error_logs(
        self, service: str | None = None, limit: int = 50
    ) -> list[TraceLogCorrelation]:
        """Find traces that have error-level logs.

        This is a powerful discovery tool: "show me all failing requests
        with their full trace context."
        """
        error_logs = self._logs.get_error_logs(service=service, limit=200)

        # Group by trace_id
        trace_ids_seen: set[str] = set()
        results: list[TraceLogCorrelation] = []

        for env in error_logs:
            trace_id = self._extract_trace_id(env)
            if not trace_id or trace_id in trace_ids_seen:
                continue
            trace_ids_seen.add(trace_id)

            result = self.logs_for_trace(trace_id)
            results.append(result)
            if len(results) >= limit:
                break

        return results

    def log_timeline(
        self, trace_id: str
    ) -> list[tuple[datetime, str, str, str]]:
        """Get a chronological timeline of logs for a trace.

        Returns list of (timestamp, service, severity, body_preview).
        """
        logs = self._logs.get_logs_for_trace(trace_id)
        timeline: list[tuple[datetime, str, str, str]] = []

        for env in logs:
            payload = env.payload
            if isinstance(payload, LogRecord):
                ts = env.observed_at
                service = env.target.service or "unknown"
                sev = payload.severity.value
                preview = payload.body[:120]
                timeline.append((ts, service, sev, preview))

        timeline.sort(key=lambda x: x[0])
        return timeline

    # --- Internal ---

    @staticmethod
    def _extract_trace_id(envelope: TelemetryEnvelope) -> str | None:
        """Extract trace_id from a log envelope."""
        if envelope.request and envelope.request.trace_id:
            return envelope.request.trace_id
        payload = envelope.payload
        if isinstance(payload, LogRecord) and payload.trace_id:
            return payload.trace_id
        return None
