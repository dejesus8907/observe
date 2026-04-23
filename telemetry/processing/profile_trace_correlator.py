"""Profile-trace correlator — links profiles to traces and metrics.

Profiles answer "why" after metrics/traces show "what." This module
provides the navigation jumps:

- trace → profile: "this trace was slow, show me the CPU hotspot"
- metric anomaly → profile: "CPU is spiking, show me the flamegraph"
- service saturation → profile: "this service is saturated, what code is hot?"
- profile → trace: "this function is hot, show me traces going through it"

Correlation strategies:
1. Direct trace link (profile has trace_id) — highest confidence
2. Service + time window — match profiles near a slow trace/metric spike
3. Service + profile type — show the latest profile for a service
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from netobserv.telemetry.envelope import TelemetryEnvelope
from netobserv.telemetry.enums import SignalType
from netobserv.telemetry.signals import ProfileRecord, SpanRecord, TopFunction
from netobserv.telemetry.store.profile_store import ProfileStore
from netobserv.telemetry.store.trace_store import TraceStore, TraceView


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class ProfileTraceCorrelation:
    """Result of correlating a profile to traces."""
    profile_envelope_id: str
    profile_type: str
    service: str
    method: str                       # "trace_id", "time_window", "none"
    related_traces: list[TraceView] = field(default_factory=list)
    top_functions: list[TopFunction] = field(default_factory=list)
    confidence: float = 0.0
    explanation: str = ""

    @property
    def has_traces(self) -> bool:
        return len(self.related_traces) > 0

    @property
    def hotspot(self) -> str:
        """The hottest function in the correlated profile."""
        if self.top_functions:
            return self.top_functions[0].function_name
        return ""


@dataclass(slots=True)
class TraceProfileCorrelation:
    """Result of finding profiles for a trace."""
    trace_id: str
    profiles: list[TelemetryEnvelope] = field(default_factory=list)
    method: str = "none"              # "trace_id", "service_window", "none"
    confidence: float = 0.0
    explanation: str = ""

    @property
    def has_profiles(self) -> bool:
        return len(self.profiles) > 0

    @property
    def cpu_hotspot(self) -> str:
        """The top CPU function across correlated profiles."""
        for env in self.profiles:
            payload = env.payload
            if isinstance(payload, ProfileRecord) and payload.profile_type == "cpu":
                if payload.top_functions:
                    return payload.top_functions[0].function_name
        return ""


class ProfileTraceCorrelator:
    """Correlates profiles to traces and vice versa.

    Usage:
        correlator = ProfileTraceCorrelator(profile_store, trace_store)

        # Slow trace → find CPU profile
        result = correlator.profiles_for_trace("trace-abc")
        if result.has_profiles:
            print(f"CPU hotspot: {result.cpu_hotspot}")

        # CPU spike → find profiles and related traces
        profiles = correlator.profiles_for_service("api", "cpu")
        for env in profiles:
            result = correlator.traces_for_profile(env)
    """

    def __init__(
        self,
        profile_store: ProfileStore,
        trace_store: TraceStore,
        correlation_window_seconds: float = 120.0,
    ) -> None:
        self._profiles = profile_store
        self._traces = trace_store
        self._window = timedelta(seconds=correlation_window_seconds)

    def profiles_for_trace(
        self, trace_id: str, profile_type: str | None = None,
    ) -> TraceProfileCorrelation:
        """Find profiles related to a trace.

        Strategy 1: Direct trace_id link (profile was captured during this trace)
        Strategy 2: Service + time window (profile captured near the trace's time)
        """
        result = TraceProfileCorrelation(trace_id=trace_id)

        # Strategy 1: Direct trace link
        linked = self._profiles.get_profiles_for_trace(trace_id)
        if profile_type:
            linked = [
                e for e in linked
                if isinstance(e.payload, ProfileRecord) and e.payload.profile_type == profile_type
            ]
        if linked:
            result.profiles = linked
            result.method = "trace_id"
            result.confidence = 0.95
            result.explanation = f"Found {len(linked)} profile(s) directly linked to trace {trace_id}"
            return result

        # Strategy 2: Service + time window
        trace = self._traces.get_trace(trace_id)
        if trace and trace.services and trace.started_at:
            for service in trace.services:
                start = trace.started_at - self._window / 2
                end = (trace.ended_at or trace.started_at) + self._window / 2
                window_profiles = self._profiles.get_profiles_in_window(
                    start, end, service=service, profile_type=profile_type,
                )
                if window_profiles:
                    result.profiles = window_profiles
                    result.method = "service_window"
                    result.confidence = 0.55
                    result.explanation = (
                        f"Found {len(window_profiles)} profile(s) for service "
                        f"'{service}' near trace time window"
                    )
                    return result

        result.explanation = "No profiles found for this trace"
        return result

    def traces_for_profile(self, profile_envelope: TelemetryEnvelope) -> ProfileTraceCorrelation:
        """Find traces related to a profile."""
        payload = profile_envelope.payload
        if not isinstance(payload, ProfileRecord):
            return ProfileTraceCorrelation(
                profile_envelope_id=profile_envelope.envelope_id,
                profile_type="", service="", method="none",
            )

        service = profile_envelope.target.service or ""
        result = ProfileTraceCorrelation(
            profile_envelope_id=profile_envelope.envelope_id,
            profile_type=payload.profile_type,
            service=service,
            method="none",
            top_functions=list(payload.top_functions),
        )

        # Strategy 1: Direct trace link
        if payload.trace_id:
            trace = self._traces.get_trace(payload.trace_id)
            if trace:
                result.related_traces = [trace]
                result.method = "trace_id"
                result.confidence = 0.95
                result.explanation = f"Profile directly linked to trace {payload.trace_id}"
                return result

        # Strategy 2: Service + time window
        if service and profile_envelope.observed_at:
            start = profile_envelope.observed_at - self._window / 2
            end = profile_envelope.observed_at + self._window / 2
            traces = self._traces.get_traces_for_service(service, limit=20)
            window_traces = [
                t for t in traces
                if t.started_at and start <= t.started_at <= end
            ]
            if window_traces:
                result.related_traces = window_traces
                result.method = "time_window"
                result.confidence = 0.45
                result.explanation = (
                    f"Found {len(window_traces)} trace(s) for service "
                    f"'{service}' near profile capture time"
                )
                return result

        result.explanation = "No traces found for this profile"
        return result

    def profiles_for_service(
        self, service: str, profile_type: str = "cpu", limit: int = 10,
    ) -> list[TelemetryEnvelope]:
        """Get recent profiles for a service (for saturation analysis)."""
        return self._profiles.get_profiles_for_service(
            service, profile_type=profile_type, limit=limit,
        )

    def hotspots_for_service(
        self, service: str, profile_type: str = "cpu",
    ) -> list[TopFunction]:
        """Get the top hot functions for a service from latest profile."""
        profiles = self._profiles.get_profiles_for_service(
            service, profile_type=profile_type, limit=1,
        )
        if profiles:
            payload = profiles[0].payload
            if isinstance(payload, ProfileRecord):
                return list(payload.top_functions)
        return []
