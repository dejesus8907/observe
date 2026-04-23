"""In-memory profile storage with indexed lookups.

Stores TelemetryEnvelopes containing ProfileRecords, indexed by:
- service name (which service was profiled?)
- profile type (cpu, heap, goroutine, etc.)
- trace_id (for profile-to-trace correlation)
- time window (for temporal correlation with metrics/traces)
"""

from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from netobserv.telemetry.envelope import TelemetryEnvelope
from netobserv.telemetry.enums import SignalType
from netobserv.telemetry.signals import ProfileRecord


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class ProfileStore:
    """In-memory profile storage with indexed lookups.

    Usage:
        store = ProfileStore()
        store.store_profile(envelope)
        profiles = store.get_profiles_for_service("api", profile_type="cpu")
        profiles = store.get_profiles_for_trace("trace-abc")
    """

    def __init__(self, max_profiles: int = 10_000, max_age_seconds: float = 3600.0) -> None:
        self._max_profiles = max_profiles
        self._max_age = timedelta(seconds=max_age_seconds)
        self._lock = threading.Lock()

        # Primary storage: profile_id (envelope_id) → envelope
        self._profiles: dict[str, TelemetryEnvelope] = {}
        # Insertion order for eviction
        self._insertion_order: list[str] = []

        # Indexes
        self._service_index: dict[str, list[str]] = defaultdict(list)
        self._type_index: dict[str, list[str]] = defaultdict(list)
        self._trace_index: dict[str, list[str]] = defaultdict(list)
        self._time_index: list[tuple[datetime, str]] = []

        self._total_stored = 0

    def store_profile(self, envelope: TelemetryEnvelope) -> None:
        """Store a profile envelope."""
        if envelope.signal_type != SignalType.PROFILE:
            return
        payload = envelope.payload
        if not isinstance(payload, ProfileRecord):
            return

        pid = envelope.envelope_id
        service = envelope.target.service or ""
        profile_type = payload.profile_type
        trace_id = payload.trace_id
        observed_at = envelope.observed_at or _utc_now()

        with self._lock:
            if len(self._profiles) >= self._max_profiles:
                self._evict_oldest()

            self._profiles[pid] = envelope
            self._insertion_order.append(pid)
            self._total_stored += 1

            if service:
                self._service_index[service].append(pid)
            if profile_type:
                self._type_index[profile_type].append(pid)
            if trace_id:
                self._trace_index[trace_id].append(pid)
            self._time_index.append((observed_at, pid))

    def store_profiles(self, envelopes: list[TelemetryEnvelope]) -> None:
        for env in envelopes:
            self.store_profile(env)

    # --- Queries ---

    def get_profiles_for_service(
        self, service: str,
        profile_type: str | None = None,
        limit: int = 50,
    ) -> list[TelemetryEnvelope]:
        """Get profiles for a service, optionally filtered by type."""
        with self._lock:
            pids = self._service_index.get(service, [])
            results: list[TelemetryEnvelope] = []
            for pid in reversed(pids):
                env = self._profiles.get(pid)
                if not env:
                    continue
                if profile_type:
                    payload = env.payload
                    if isinstance(payload, ProfileRecord) and payload.profile_type != profile_type:
                        continue
                results.append(env)
                if len(results) >= limit:
                    break
            return results

    def get_profiles_for_trace(self, trace_id: str) -> list[TelemetryEnvelope]:
        """Get profiles linked to a specific trace."""
        with self._lock:
            pids = self._trace_index.get(trace_id, [])
            return [self._profiles[pid] for pid in pids if pid in self._profiles]

    def get_profiles_by_type(self, profile_type: str, limit: int = 50) -> list[TelemetryEnvelope]:
        """Get profiles of a specific type across all services."""
        with self._lock:
            pids = self._type_index.get(profile_type, [])
            return [
                self._profiles[pid] for pid in reversed(pids[-limit:])
                if pid in self._profiles
            ]

    def get_profiles_in_window(
        self, start: datetime, end: datetime,
        service: str | None = None,
        profile_type: str | None = None,
        limit: int = 50,
    ) -> list[TelemetryEnvelope]:
        """Get profiles within a time window."""
        results: list[TelemetryEnvelope] = []
        with self._lock:
            for ts, pid in reversed(self._time_index):
                if ts < start:
                    break
                if ts > end:
                    continue
                env = self._profiles.get(pid)
                if not env:
                    continue
                if service and env.target.service != service:
                    continue
                if profile_type:
                    payload = env.payload
                    if isinstance(payload, ProfileRecord) and payload.profile_type != profile_type:
                        continue
                results.append(env)
                if len(results) >= limit:
                    break
        return results

    def evict_expired(self, now: datetime | None = None) -> int:
        """Remove profiles older than max_age."""
        now = now or _utc_now()
        cutoff = now - self._max_age
        evicted = 0
        with self._lock:
            stale = [
                pid for pid in self._insertion_order
                if pid in self._profiles and (self._profiles[pid].observed_at or _utc_now()) < cutoff
            ]
            for pid in stale:
                self._remove_profile(pid)
                evicted += 1
        return evicted

    @property
    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "profile_count": len(self._profiles),
                "service_count": len(self._service_index),
                "type_count": len(self._type_index),
                "trace_linked_count": sum(len(v) for v in self._trace_index.values()),
                "total_stored": self._total_stored,
            }

    # --- Internal ---

    def _evict_oldest(self) -> None:
        if self._insertion_order:
            oldest = self._insertion_order[0]
            self._remove_profile(oldest)

    def _remove_profile(self, pid: str) -> None:
        env = self._profiles.pop(pid, None)
        if pid in self._insertion_order:
            self._insertion_order.remove(pid)
        if not env:
            return
        # Clean indexes
        service = env.target.service or ""
        if service and service in self._service_index:
            pids = self._service_index[service]
            if pid in pids:
                pids.remove(pid)
            if not pids:
                del self._service_index[service]

        payload = env.payload
        if isinstance(payload, ProfileRecord):
            if payload.profile_type and payload.profile_type in self._type_index:
                pids = self._type_index[payload.profile_type]
                if pid in pids:
                    pids.remove(pid)
            if payload.trace_id and payload.trace_id in self._trace_index:
                pids = self._trace_index[payload.trace_id]
                if pid in pids:
                    pids.remove(pid)
                if not pids:
                    del self._trace_index[payload.trace_id]

        self._time_index = [(ts, p) for ts, p in self._time_index if p != pid]
