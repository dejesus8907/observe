"""In-memory log storage with indexed search.

Stores TelemetryEnvelopes containing LogRecords, indexed by:
- trace_id (for trace/log hard-linking)
- service name
- severity level
- time window

Supports full-text body substring search and attribute filtering.
"""

from __future__ import annotations

import threading
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from netobserv.telemetry.envelope import TelemetryEnvelope
from netobserv.telemetry.enums import Severity, SignalType
from netobserv.telemetry.signals import LogRecord


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class LogSearchQuery:
    """Query parameters for log search."""
    service: str | None = None
    trace_id: str | None = None
    span_id: str | None = None
    min_severity: Severity | None = None
    body_contains: str | None = None
    attrs_match: dict[str, str] | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None
    limit: int = 100

    def matches(self, envelope: TelemetryEnvelope) -> bool:
        """Test if an envelope matches this query."""
        payload = envelope.payload
        if not isinstance(payload, LogRecord):
            return False

        if self.service and envelope.target.service != self.service:
            return False

        if self.trace_id:
            log_trace = payload.trace_id or (
                envelope.request.trace_id if envelope.request else None
            )
            if log_trace != self.trace_id:
                return False

        if self.span_id and payload.span_id != self.span_id:
            return False

        if self.min_severity:
            if _severity_rank(payload.severity) < _severity_rank(self.min_severity):
                return False

        if self.body_contains:
            if self.body_contains.lower() not in payload.body.lower():
                return False

        if self.attrs_match:
            for k, v in self.attrs_match.items():
                if payload.attrs.get(k) != v:
                    return False

        if self.start_time and envelope.observed_at < self.start_time:
            return False
        if self.end_time and envelope.observed_at > self.end_time:
            return False

        return True


_SEVERITY_RANK = {
    Severity.UNSPECIFIED: 0, Severity.TRACE: 1, Severity.DEBUG: 2,
    Severity.INFO: 3, Severity.WARN: 4, Severity.ERROR: 5, Severity.FATAL: 6,
}


def _severity_rank(s: Severity) -> int:
    return _SEVERITY_RANK.get(s, 0)


class LogStore:
    """In-memory log storage with search indexes.

    Usage:
        store = LogStore()
        store.store_log(envelope)
        logs = store.search(LogSearchQuery(service="api", min_severity=Severity.ERROR))
        logs = store.get_logs_for_trace("trace-abc")
    """

    def __init__(self, max_logs: int = 200_000, max_age_seconds: float = 900.0) -> None:
        self._max_logs = max_logs
        self._max_age = timedelta(seconds=max_age_seconds)
        self._lock = threading.Lock()

        # Primary storage: ordered deque for FIFO eviction
        self._logs: deque[TelemetryEnvelope] = deque(maxlen=max_logs)

        # Indexes
        self._trace_index: dict[str, list[int]] = defaultdict(list)  # trace_id → positions
        self._service_index: dict[str, list[int]] = defaultdict(list)
        self._severity_index: dict[Severity, list[int]] = defaultdict(list)

        self._total_stored = 0
        self._position = 0  # monotonic counter for indexing

    def store_log(self, envelope: TelemetryEnvelope) -> None:
        if envelope.signal_type != SignalType.LOG:
            return
        payload = envelope.payload
        if not isinstance(payload, LogRecord):
            return

        with self._lock:
            pos = self._position
            self._position += 1
            self._logs.append(envelope)
            self._total_stored += 1

            # Index by trace_id
            trace_id = payload.trace_id or (
                envelope.request.trace_id if envelope.request else None
            )
            if trace_id:
                self._trace_index[trace_id].append(pos)

            # Index by service
            service = envelope.target.service
            if service:
                self._service_index[service].append(pos)

            # Index by severity
            self._severity_index[payload.severity].append(pos)

    def store_logs(self, envelopes: list[TelemetryEnvelope]) -> None:
        for env in envelopes:
            self.store_log(env)

    def get_logs_for_trace(self, trace_id: str) -> list[TelemetryEnvelope]:
        """Get all logs linked to a trace_id (hard-link via trace_id field)."""
        with self._lock:
            results: list[TelemetryEnvelope] = []
            for env in self._logs:
                payload = env.payload
                if not isinstance(payload, LogRecord):
                    continue
                log_trace = payload.trace_id or (
                    env.request.trace_id if env.request else None
                )
                if log_trace == trace_id:
                    results.append(env)
            return results

    def get_logs_for_service(self, service: str, limit: int = 100) -> list[TelemetryEnvelope]:
        """Get recent logs for a service."""
        with self._lock:
            results: list[TelemetryEnvelope] = []
            for env in reversed(self._logs):
                if env.target.service == service:
                    results.append(env)
                    if len(results) >= limit:
                        break
            return results

    def get_error_logs(self, service: str | None = None, limit: int = 100) -> list[TelemetryEnvelope]:
        """Get recent error and fatal logs."""
        with self._lock:
            results: list[TelemetryEnvelope] = []
            for env in reversed(self._logs):
                payload = env.payload
                if not isinstance(payload, LogRecord):
                    continue
                if _severity_rank(payload.severity) < _severity_rank(Severity.ERROR):
                    continue
                if service and env.target.service != service:
                    continue
                results.append(env)
                if len(results) >= limit:
                    break
            return results

    def search(self, query: LogSearchQuery) -> list[TelemetryEnvelope]:
        """Search logs with flexible query parameters."""
        with self._lock:
            results: list[TelemetryEnvelope] = []
            for env in reversed(self._logs):
                if query.matches(env):
                    results.append(env)
                    if len(results) >= query.limit:
                        break
            return results

    def count_by_severity(self, service: str | None = None,
                          window_seconds: float = 300.0) -> dict[str, int]:
        """Count logs by severity within a time window."""
        now = _utc_now()
        cutoff = now - timedelta(seconds=window_seconds)
        counts: dict[str, int] = {}
        with self._lock:
            for env in self._logs:
                if env.observed_at < cutoff:
                    continue
                if service and env.target.service != service:
                    continue
                payload = env.payload
                if isinstance(payload, LogRecord):
                    key = payload.severity.value
                    counts[key] = counts.get(key, 0) + 1
        return counts

    def evict_expired(self, now: datetime | None = None) -> int:
        """Remove logs older than max_age."""
        now = now or _utc_now()
        cutoff = now - self._max_age
        evicted = 0
        with self._lock:
            while self._logs and self._logs[0].observed_at < cutoff:
                self._logs.popleft()
                evicted += 1
        return evicted

    @property
    def stats(self) -> dict[str, int]:
        with self._lock:
            traced = sum(1 for env in self._logs
                        if isinstance(env.payload, LogRecord)
                        and (env.payload.trace_id or
                             (env.request and env.request.trace_id)))
            return {
                "log_count": len(self._logs),
                "total_stored": self._total_stored,
                "traced_logs": traced,
                "service_count": len(self._service_index),
                "trace_ids_indexed": len(self._trace_index),
            }
