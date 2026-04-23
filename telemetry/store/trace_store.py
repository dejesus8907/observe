"""In-memory trace and span storage with indexed lookups.

Stores TelemetryEnvelopes containing SpanRecords. Provides fast
lookups by trace_id, service name, and time window. This is not a
production database — it's bounded in-memory storage for the
correlation engine to query against.

Design:
- Primary index: trace_id → list of envelopes (all spans in a trace)
- Service index: service_name → set of trace_ids
- Time-bounded: automatic eviction of traces older than max_age
- Thread-safe via simple locking (asyncio-compatible)
"""

from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from netobserv.telemetry.envelope import TelemetryEnvelope
from netobserv.telemetry.enums import SignalType
from netobserv.telemetry.signals import SpanRecord


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class TraceView:
    """Assembled view of a complete trace."""
    trace_id: str
    spans: list[TelemetryEnvelope] = field(default_factory=list)
    root_span: TelemetryEnvelope | None = None
    services: set[str] = field(default_factory=set)
    started_at: datetime | None = None
    ended_at: datetime | None = None
    has_errors: bool = False

    @property
    def span_count(self) -> int:
        return len(self.spans)

    @property
    def duration_ms(self) -> float:
        if self.started_at and self.ended_at:
            return (self.ended_at - self.started_at).total_seconds() * 1000
        return 0.0

    @property
    def service_count(self) -> int:
        return len(self.services)


class TraceStore:
    """In-memory span storage with trace assembly and service indexing."""

    def __init__(self, max_traces: int = 50_000, max_age_seconds: float = 600.0) -> None:
        self._max_traces = max_traces
        self._max_age = timedelta(seconds=max_age_seconds)
        self._lock = threading.Lock()

        # Primary: trace_id → list of span envelopes
        self._traces: dict[str, list[TelemetryEnvelope]] = defaultdict(list)
        # trace_id → timestamp of first span seen
        self._trace_times: dict[str, datetime] = {}
        # Service index: service_name → set of trace_ids
        self._service_index: dict[str, set[str]] = defaultdict(set)
        # Stats
        self._total_spans_ingested = 0
        self._total_evicted = 0

    def store_span(self, envelope: TelemetryEnvelope) -> None:
        """Store a span envelope. Must have signal_type=SPAN and SpanRecord payload."""
        if envelope.signal_type != SignalType.SPAN:
            return
        payload = envelope.payload
        if not isinstance(payload, SpanRecord):
            return

        trace_id = payload.trace_id
        if not trace_id:
            return

        with self._lock:
            self._traces[trace_id].append(envelope)
            self._total_spans_ingested += 1

            if trace_id not in self._trace_times:
                self._trace_times[trace_id] = envelope.observed_at or _utc_now()

            # Service index
            service = payload.service_name or envelope.target.service
            if service:
                self._service_index[service].add(trace_id)

            # Evict if over capacity
            if len(self._traces) > self._max_traces:
                self._evict_oldest()

    def store_spans(self, envelopes: list[TelemetryEnvelope]) -> None:
        """Batch store multiple span envelopes."""
        for env in envelopes:
            self.store_span(env)

    def get_trace(self, trace_id: str) -> TraceView | None:
        """Assemble a complete trace view from stored spans."""
        with self._lock:
            spans = self._traces.get(trace_id)
            if not spans:
                return None
            return self._assemble_trace(trace_id, spans)

    def get_traces_for_service(
        self, service_name: str, limit: int = 100
    ) -> list[TraceView]:
        """Get recent traces that include a given service."""
        with self._lock:
            trace_ids = list(self._service_index.get(service_name, set()))[:limit]
            return [
                self._assemble_trace(tid, self._traces[tid])
                for tid in trace_ids
                if tid in self._traces
            ]

    def get_traces_in_window(
        self, start: datetime, end: datetime, limit: int = 100
    ) -> list[TraceView]:
        """Get traces whose first span falls within the time window."""
        results: list[TraceView] = []
        with self._lock:
            for trace_id, first_seen in self._trace_times.items():
                if start <= first_seen <= end and trace_id in self._traces:
                    results.append(
                        self._assemble_trace(trace_id, self._traces[trace_id])
                    )
                    if len(results) >= limit:
                        break
        return results

    def get_spans_by_service(self, service_name: str) -> list[TelemetryEnvelope]:
        """Get all stored span envelopes for a service."""
        with self._lock:
            trace_ids = self._service_index.get(service_name, set())
            spans: list[TelemetryEnvelope] = []
            for tid in trace_ids:
                for env in self._traces.get(tid, []):
                    payload = env.payload
                    if isinstance(payload, SpanRecord):
                        svc = payload.service_name or env.target.service
                        if svc == service_name:
                            spans.append(env)
            return spans

    def evict_expired(self, now: datetime | None = None) -> int:
        """Remove traces older than max_age. Returns count evicted."""
        now = now or _utc_now()
        cutoff = now - self._max_age
        evicted = 0
        with self._lock:
            expired_ids = [
                tid for tid, ts in self._trace_times.items()
                if ts < cutoff
            ]
            for tid in expired_ids:
                self._remove_trace(tid)
                evicted += 1
        self._total_evicted += evicted
        return evicted

    @property
    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "trace_count": len(self._traces),
                "span_count": sum(len(s) for s in self._traces.values()),
                "service_count": len(self._service_index),
                "total_spans_ingested": self._total_spans_ingested,
                "total_evicted": self._total_evicted,
            }

    # -- Internal --

    def _assemble_trace(self, trace_id: str, spans: list[TelemetryEnvelope]) -> TraceView:
        """Build a TraceView from a list of span envelopes."""
        view = TraceView(trace_id=trace_id)
        for env in spans:
            view.spans.append(env)
            payload = env.payload
            if isinstance(payload, SpanRecord):
                svc = payload.service_name or env.target.service
                if svc:
                    view.services.add(svc)
                if payload.is_root_span:
                    view.root_span = env
                if payload.is_error:
                    view.has_errors = True
                if payload.start_time:
                    if view.started_at is None or payload.start_time < view.started_at:
                        view.started_at = payload.start_time
                if payload.end_time:
                    if view.ended_at is None or payload.end_time > view.ended_at:
                        view.ended_at = payload.end_time
        return view

    def _evict_oldest(self) -> None:
        """Remove the oldest trace to stay under max_traces."""
        if not self._trace_times:
            return
        oldest_id = min(self._trace_times, key=self._trace_times.get)  # type: ignore
        self._remove_trace(oldest_id)
        self._total_evicted += 1

    def _remove_trace(self, trace_id: str) -> None:
        """Remove a trace and clean up all indexes."""
        spans = self._traces.pop(trace_id, [])
        self._trace_times.pop(trace_id, None)
        for env in spans:
            payload = env.payload
            if isinstance(payload, SpanRecord):
                svc = payload.service_name or env.target.service
                if svc and svc in self._service_index:
                    self._service_index[svc].discard(trace_id)
                    if not self._service_index[svc]:
                        del self._service_index[svc]
