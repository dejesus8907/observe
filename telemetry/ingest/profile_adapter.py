"""Profile ingestion adapter — normalizes profiling data into TelemetryEnvelopes.

Accepts profile data in simplified pprof-compatible dict format
(what a pprof/Pyroscope/Parca ingestion endpoint would produce after
binary decoding) and outputs TelemetryEnvelopes with ProfileRecord payloads.

The actual binary pprof/JFR parsing is not part of this module — it handles
the normalized representation after protocol-specific decoders have run.

Supported profile types:
- cpu: CPU time samples (nanoseconds)
- heap/allocs: Memory allocation (bytes/count)
- goroutine: Goroutine counts
- mutex: Mutex contention (nanoseconds)
- block: Block contention (nanoseconds)
- wall: Wall-clock time
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from netobserv.telemetry.envelope import TelemetryEnvelope
from netobserv.telemetry.enums import Severity, SignalType, SourceType
from netobserv.telemetry.identity import RequestRef, TargetRef
from netobserv.telemetry.resource_normalizer import ResourceNormalizer
from netobserv.telemetry.signals import (
    ProfileRecord,
    ProfileSample,
    StackFrame,
    TopFunction,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


_normalizer = ResourceNormalizer()

# Profile type → default unit
_PROFILE_UNITS: dict[str, str] = {
    "cpu": "nanoseconds",
    "wall": "nanoseconds",
    "heap": "bytes",
    "allocs": "bytes",
    "goroutine": "count",
    "mutex": "nanoseconds",
    "block": "nanoseconds",
    "threads": "count",
}

# Max samples to store per profile to bound memory
_MAX_SAMPLES_PER_PROFILE = 100
# Max top functions to extract
_MAX_TOP_FUNCTIONS = 20


class ProfileAdapter:
    """Normalizes profiling data into TelemetryEnvelopes.

    Usage:
        adapter = ProfileAdapter()
        envelopes = adapter.process_profiles(profile_dicts)
        for env in envelopes:
            profile_store.store_profile(env)
    """

    def process_profiles(
        self,
        profiles: list[dict[str, Any]],
        resource_attrs: dict[str, str] | None = None,
    ) -> list[TelemetryEnvelope]:
        """Process a list of profile dicts into envelopes."""
        base_target = _normalizer.normalize(resource_attrs or {})
        envelopes: list[TelemetryEnvelope] = []
        for profile in profiles:
            env = self._profile_to_envelope(profile, base_target)
            if env:
                envelopes.append(env)
        return envelopes

    def process_pprof(
        self, pprof_data: dict[str, Any],
        resource_attrs: dict[str, str] | None = None,
    ) -> list[TelemetryEnvelope]:
        """Process a pprof-style profile dict.

        Expected format (simplified from pprof protobuf):
        {
            "profile_type": "cpu",
            "duration_nanos": 30000000000,
            "samples": [
                {"stack": ["main.handler", "net/http.ServeHTTP"], "value": 5000},
                ...
            ],
            "resource": {"service.name": "api", ...},
            "labels": {"trace_id": "abc123", ...},
        }
        """
        resource = pprof_data.get("resource", resource_attrs or {})
        target = _normalizer.normalize(resource)
        env = self._profile_to_envelope(pprof_data, target)
        return [env] if env else []

    def _profile_to_envelope(
        self, profile: dict[str, Any], base_target: TargetRef,
    ) -> TelemetryEnvelope | None:
        profile_type = str(profile.get("profile_type", profile.get("type", "")))
        if not profile_type:
            return None

        # Parse timing
        duration_nanos = profile.get("duration_nanos", 0)
        duration_ms = duration_nanos / 1_000_000 if duration_nanos else float(profile.get("duration_ms", 0))
        timestamp = profile.get("timestamp")
        if isinstance(timestamp, (int, float)):
            observed_at = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        elif isinstance(timestamp, datetime):
            observed_at = timestamp
        else:
            observed_at = _utc_now()

        # Parse samples
        raw_samples = profile.get("samples", [])
        samples: list[ProfileSample] = []
        total_value = 0

        # Function aggregation for top functions
        func_self: dict[str, int] = {}
        func_total: dict[str, int] = {}

        for raw in raw_samples[:_MAX_SAMPLES_PER_PROFILE]:
            stack_data = raw.get("stack", [])
            value = int(raw.get("value", raw.get("count", 1)))
            total_value += value

            frames: list[StackFrame] = []
            for frame_data in stack_data:
                if isinstance(frame_data, str):
                    frames.append(StackFrame(function_name=frame_data))
                elif isinstance(frame_data, dict):
                    frames.append(StackFrame(
                        function_name=frame_data.get("function", frame_data.get("name", "")),
                        file_name=frame_data.get("file", ""),
                        line_number=int(frame_data.get("line", 0)),
                        module=frame_data.get("module", ""),
                    ))

            sample_labels = raw.get("labels", {})
            samples.append(ProfileSample(stack=frames, value=value, labels=sample_labels))

            # Aggregate: self value = leaf function, total = all in stack
            if frames:
                leaf = frames[0].function_name
                func_self[leaf] = func_self.get(leaf, 0) + value
                for f in frames:
                    fname = f.function_name
                    func_total[fname] = func_total.get(fname, 0) + value

        # Compute total from all samples (not just stored ones)
        if len(raw_samples) > _MAX_SAMPLES_PER_PROFILE:
            for raw in raw_samples[_MAX_SAMPLES_PER_PROFILE:]:
                total_value += int(raw.get("value", raw.get("count", 1)))

        # Build top functions
        top_functions: list[TopFunction] = []
        for fname, self_val in sorted(func_self.items(), key=lambda x: x[1], reverse=True)[:_MAX_TOP_FUNCTIONS]:
            total_val = func_total.get(fname, self_val)
            pct = (self_val / max(total_value, 1)) * 100
            top_functions.append(TopFunction(
                function_name=fname,
                self_value=self_val,
                total_value=total_val,
                percentage=round(pct, 2),
            ))

        # Extract trace link from labels
        labels = dict(profile.get("labels", {}))
        trace_id = labels.pop("trace_id", labels.pop("traceID", None))
        span_id = labels.pop("span_id", labels.pop("spanID", None))

        unit = _PROFILE_UNITS.get(profile_type, profile.get("unit", ""))
        fmt = str(profile.get("format", "pprof"))

        record = ProfileRecord(
            profile_type=profile_type,
            sample_count=len(raw_samples),
            duration_ms=duration_ms,
            format=fmt,
            attrs=dict(profile.get("attrs", {})),
            trace_id=trace_id,
            span_id=span_id,
            top_functions=top_functions,
            total_value=total_value,
            unit=unit,
            samples=samples,
            labels=labels,
        )

        # Build target — enrich from profile-level metadata
        target = base_target
        service = profile.get("service", labels.get("service"))
        if service:
            target = target.merge(TargetRef(service=service))

        # Request ref
        request = None
        if trace_id:
            request = RequestRef(trace_id=trace_id, span_id=span_id)

        return TelemetryEnvelope(
            signal_type=SignalType.PROFILE,
            source_type=SourceType.PPROF,
            target=target,
            request=request,
            observed_at=observed_at,
            severity=Severity.UNSPECIFIED,
            payload=record,
            ingestion_pipeline="profile",
        )
