"""TelemetryEnvelope — the universal signal container.

Every piece of telemetry entering NetObserv is wrapped in a
TelemetryEnvelope before processing. This ensures:

1. Every signal carries consistent target metadata (TargetRef)
2. Every signal can optionally carry request identity (RequestRef)
3. Signal type and source type are always known
4. Timestamps are normalized

The envelope does NOT contain the raw source payload. It contains
the normalized signal payload (MetricPoint, LogRecord, SpanRecord,
ProfileRecord, or NetworkEventRef).

Design:
- The payload is typed as a Union so the envelope is signal-agnostic
  but type-safe at the point of use.
- The envelope is immutable after construction (frozen=False for
  enrichment passes, but payload is set once).
- The correlation engine uses target + request + signal_type to
  decide how to route and join signals.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Union
from uuid import uuid4

from .enums import Severity, SignalType, SourceType
from .identity import RequestRef, TargetRef
from .signals import (
    LogRecord,
    MetricPoint,
    NetworkEventRef,
    ProfileRecord,
    SpanRecord,
)

# The union of all signal payload types
SignalPayload = Union[MetricPoint, LogRecord, SpanRecord, ProfileRecord, NetworkEventRef, None]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _new_envelope_id() -> str:
    return f"env-{uuid4().hex[:16]}"


@dataclass
class TelemetryEnvelope:
    """Universal container for any observability signal.

    This is the canonical unit of telemetry in NetObserv. Every ingestion
    path normalizes its source data into envelopes before processing.
    """
    # Classification
    signal_type: SignalType
    source_type: SourceType

    # Identity (the cross-signal correlation spine)
    target: TargetRef
    request: RequestRef | None = None

    # Timestamps
    observed_at: datetime = field(default_factory=_utc_now)
    received_at: datetime = field(default_factory=_utc_now)

    # Severity (normalized across all signal types)
    severity: Severity = Severity.UNSPECIFIED

    # The actual signal data
    payload: SignalPayload = None

    # Envelope metadata
    envelope_id: str = field(default_factory=_new_envelope_id)
    attrs: dict[str, Any] = field(default_factory=dict)

    # Ingestion metadata (set by the gateway)
    ingestion_pipeline: str | None = None
    schema_version: str = "1.0"

    # --- Correlation helpers ---

    def has_trace_identity(self) -> bool:
        """True if this envelope carries a trace ID (hard-linkable)."""
        if self.request and self.request.has_trace_identity():
            return True
        if isinstance(self.payload, SpanRecord) and self.payload.trace_id:
            return True
        if isinstance(self.payload, LogRecord) and self.payload.trace_id:
            return True
        return False

    def trace_id(self) -> str | None:
        """Extract the trace ID from wherever it lives."""
        if self.request and self.request.trace_id:
            return self.request.trace_id
        if isinstance(self.payload, SpanRecord):
            return self.payload.trace_id or None
        if isinstance(self.payload, LogRecord):
            return self.payload.trace_id
        if isinstance(self.payload, ProfileRecord):
            return self.payload.trace_id
        return None

    def has_exemplar(self) -> bool:
        """True if this envelope carries an exemplar (metric→trace bridge)."""
        if self.request and self.request.has_exemplar():
            return True
        if isinstance(self.payload, MetricPoint) and self.payload.exemplars:
            return True
        return False

    def exemplar_trace_ids(self) -> list[str]:
        """Extract all exemplar trace IDs from the payload."""
        ids: list[str] = []
        if self.request and self.request.exemplar_trace_id:
            ids.append(self.request.exemplar_trace_id)
        if isinstance(self.payload, MetricPoint):
            for ex in self.payload.exemplars:
                if ex.trace_id:
                    ids.append(ex.trace_id)
        return ids

    def is_network_signal(self) -> bool:
        """True if this is from the network layer."""
        return self.signal_type == SignalType.NETWORK_EVENT or self.source_type in {
            SourceType.GNMI, SourceType.SNMP_TRAP, SourceType.SYSLOG,
            SourceType.SSH_POLL_DIFF, SourceType.NETCONF,
        }

    def is_app_signal(self) -> bool:
        """True if this is from the application/infrastructure layer."""
        return self.source_type in {
            SourceType.OTLP, SourceType.PROMETHEUS,
            SourceType.PROMETHEUS_REMOTE_WRITE,
            SourceType.K8S_API, SourceType.CONTAINER_RUNTIME,
            SourceType.SERVICE_MESH, SourceType.PPROF, SourceType.APP_LOG,
        }

    def can_correlate_with(self, other: TelemetryEnvelope) -> bool:
        """Quick check: could these two envelopes be related?

        Returns True if they share trace identity OR if their targets
        overlap. This is a fast pre-filter, not a definitive correlation.
        """
        # Hard link: same trace
        if self.has_trace_identity() and other.has_trace_identity():
            if self.trace_id() == other.trace_id():
                return True

        # Soft link: overlapping targets
        if self.target.matches(other.target):
            return True

        return False

    def fingerprint(self) -> str:
        """Deterministic envelope identity for deduplication."""
        parts = [
            self.signal_type.value,
            self.source_type.value,
            self.target.fingerprint(),
            self.observed_at.isoformat() if self.observed_at else "",
        ]
        if self.request:
            parts.append(self.request.fingerprint())
        raw = "|".join(parts)
        return hashlib.sha256(raw.encode()).hexdigest()[:24]

    def with_enriched_target(self, **updates: Any) -> TelemetryEnvelope:
        """Return a copy with additional target metadata fields set.

        Used by enrichment passes to fill in metadata from k8s API,
        service mesh, or network topology.
        """
        current = {
            f.name: getattr(self.target, f.name)
            for f in self.target.__dataclass_fields__.values()
        }
        current.update({k: v for k, v in updates.items() if v is not None})
        new_target = TargetRef(**current)
        return TelemetryEnvelope(
            signal_type=self.signal_type,
            source_type=self.source_type,
            target=new_target,
            request=self.request,
            observed_at=self.observed_at,
            received_at=self.received_at,
            severity=self.severity,
            payload=self.payload,
            envelope_id=self.envelope_id,
            attrs=dict(self.attrs),
            ingestion_pipeline=self.ingestion_pipeline,
            schema_version=self.schema_version,
        )

    def with_request(self, request: RequestRef) -> TelemetryEnvelope:
        """Return a copy with request identity attached."""
        return TelemetryEnvelope(
            signal_type=self.signal_type,
            source_type=self.source_type,
            target=self.target,
            request=request,
            observed_at=self.observed_at,
            received_at=self.received_at,
            severity=self.severity,
            payload=self.payload,
            envelope_id=self.envelope_id,
            attrs=dict(self.attrs),
            ingestion_pipeline=self.ingestion_pipeline,
            schema_version=self.schema_version,
        )
