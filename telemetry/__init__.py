"""NetObserv telemetry — canonical multi-signal observability model.

This package defines the foundational types for cross-signal correlation:

- TelemetryEnvelope: universal signal container
- TargetRef: consistent target metadata identity
- RequestRef: request/trace identity for hard-linking
- SignalType/SourceType: signal classification

Every observability signal — whether a Prometheus metric, an OTLP span,
a syslog message, or a gNMI interface event — is normalized into a
TelemetryEnvelope with a TargetRef before processing.
"""

from .enums import (
    MetricType,
    Severity,
    SignalType,
    SourceType,
    SpanKind,
    SpanStatusCode,
)
from .identity import RequestRef, TargetRef, WorkloadRef
from .envelope import TelemetryEnvelope, SignalPayload
from .signals import (
    Exemplar,
    LogRecord,
    MetricPoint,
    NetworkEventRef,
    ProfileRecord,
    ProfileSample,
    SpanEvent,
    SpanLink,
    SpanRecord,
    StackFrame,
    TopFunction,
)

__all__ = [
    # Enums
    "SignalType", "SourceType", "Severity", "SpanKind", "SpanStatusCode", "MetricType",
    # Identity
    "TargetRef", "RequestRef", "WorkloadRef",
    # Envelope
    "TelemetryEnvelope", "SignalPayload",
    # Signals
    "MetricPoint", "Exemplar", "LogRecord", "SpanRecord", "SpanEvent", "SpanLink",
    "ProfileRecord", "ProfileSample", "StackFrame", "TopFunction", "NetworkEventRef",
]
