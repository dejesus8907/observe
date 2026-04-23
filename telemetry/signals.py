"""Signal-specific payload types.

Each signal type has its own data shape because metrics, logs, traces,
and profiles have fundamentally different semantics. These are not
interchangeable — a MetricPoint is a numeric sample, a SpanRecord is
a DAG node, a LogRecord is a timestamped text entry.

In Phase 0 these are structural definitions only. Ingestion, storage,
and processing logic will be added in later phases.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .enums import MetricType, Severity, SpanKind, SpanStatusCode


# ---------------------------------------------------------------------------
# Metric signals
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class Exemplar:
    """A concrete trace reference attached to a metric observation.

    This is the bridge from aggregate metrics to individual request traces.
    Without exemplars, metric-to-trace correlation is guesswork.
    """
    trace_id: str
    span_id: str | None = None
    value: float = 0.0
    timestamp: datetime | None = None
    attrs: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class MetricPoint:
    """A single metric data point.

    Represents one sample from a counter, gauge, histogram, or summary.
    """
    metric_name: str
    metric_type: MetricType = MetricType.GAUGE
    value: float = 0.0
    unit: str = ""
    labels: dict[str, str] = field(default_factory=dict)

    # Histogram-specific
    bucket_bounds: list[float] = field(default_factory=list)
    bucket_counts: list[int] = field(default_factory=list)
    histogram_sum: float = 0.0
    histogram_count: int = 0

    # Exemplars on this data point (the metric-to-trace bridge)
    exemplars: list[Exemplar] = field(default_factory=list)

    # Aggregation metadata
    start_time: datetime | None = None
    is_monotonic: bool = False


# ---------------------------------------------------------------------------
# Log signals
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class LogRecord:
    """A single structured log entry.

    The trace_id and span_id fields enable trace/log hard-linking.
    """
    body: str = ""
    severity: Severity = Severity.INFO
    severity_number: int = 0
    trace_id: str | None = None
    span_id: str | None = None
    attrs: dict[str, Any] = field(default_factory=dict)

    # Source context
    logger_name: str | None = None
    source_file: str | None = None
    source_line: int | None = None


# ---------------------------------------------------------------------------
# Trace / span signals
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class SpanRecord:
    """A single span within a distributed trace.

    Spans form a DAG (directed acyclic graph) via parent_span_id.
    The service graph is extracted from the parent→child service edges.
    """
    trace_id: str = ""
    span_id: str = ""
    parent_span_id: str | None = None
    operation_name: str = ""
    service_name: str = ""
    kind: SpanKind = SpanKind.UNSPECIFIED
    status: SpanStatusCode = SpanStatusCode.UNSET
    status_message: str = ""

    start_time: datetime | None = None
    end_time: datetime | None = None
    duration_ms: float = 0.0

    attrs: dict[str, Any] = field(default_factory=dict)
    events: list[SpanEvent] = field(default_factory=list)
    links: list[SpanLink] = field(default_factory=list)

    @property
    def is_root_span(self) -> bool:
        return self.parent_span_id is None or self.parent_span_id == ""

    @property
    def is_error(self) -> bool:
        return self.status == SpanStatusCode.ERROR


@dataclass(slots=True)
class SpanEvent:
    """An event annotation on a span (e.g., exception, log message)."""
    name: str = ""
    timestamp: datetime | None = None
    attrs: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SpanLink:
    """A link from one span to another (cross-trace or async)."""
    trace_id: str = ""
    span_id: str = ""
    attrs: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Profile signals
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class StackFrame:
    """A single frame in a profile stack trace."""
    function_name: str = ""
    file_name: str = ""
    line_number: int = 0
    module: str = ""


@dataclass(slots=True)
class ProfileSample:
    """A single sample in a profile (one stack trace + value)."""
    stack: list[StackFrame] = field(default_factory=list)
    value: int = 0                    # sample count or bytes
    labels: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class TopFunction:
    """A function ranked by self/total time in a profile."""
    function_name: str = ""
    module: str = ""
    self_value: int = 0               # value attributed directly to this function
    total_value: int = 0              # value including callees
    percentage: float = 0.0           # percentage of total profile


@dataclass(slots=True)
class ProfileRecord:
    """A profiling snapshot (CPU, memory, goroutine, etc.).

    Contains aggregated profile data: top functions, total samples,
    and optional raw samples. The raw pprof/JFR binary is NOT stored
    here — only the extracted metadata and analysis results.
    """
    profile_type: str = ""            # cpu, heap, allocs, goroutine, mutex, block, wall
    sample_count: int = 0
    duration_ms: float = 0.0
    format: str = "pprof"             # pprof, jfr, perf, pyroscope
    attrs: dict[str, Any] = field(default_factory=dict)

    # Link to trace for profile-to-trace correlation
    trace_id: str | None = None
    span_id: str | None = None

    # Extracted analysis
    top_functions: list[TopFunction] = field(default_factory=list)
    total_value: int = 0              # sum of all sample values
    unit: str = ""                    # nanoseconds, bytes, count

    # Optional raw samples (bounded — only stored for small profiles)
    samples: list[ProfileSample] = field(default_factory=list)

    # Collection metadata
    labels: dict[str, str] = field(default_factory=dict)

    @property
    def has_trace_link(self) -> bool:
        return self.trace_id is not None

    @property
    def top_function_name(self) -> str:
        """The hottest function in this profile."""
        if self.top_functions:
            return self.top_functions[0].function_name
        return ""


# ---------------------------------------------------------------------------
# Network event reference (bridge to existing streaming pipeline)
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class NetworkEventRef:
    """Reference to an existing NetObserv network ChangeEvent.

    This bridges the new telemetry model to the existing network
    streaming pipeline without duplicating the event data.
    """
    event_id: str = ""
    kind: str = ""                # ChangeKind value
    device_id: str = ""
    interface_name: str | None = None
    neighbor_device_id: str | None = None
    field_path: str | None = None
    current_value: Any = None
