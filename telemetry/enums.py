"""Canonical enums for the multi-signal telemetry model.

These classify every piece of telemetry by what kind of signal it is
and where it came from. They are the first thing the correlation engine
checks when deciding how to route and join signals.
"""

from enum import Enum


class SignalType(str, Enum):
    """What kind of observability signal this is.

    Each type has different semantics, storage patterns, and query shapes:
    - METRIC: numeric time-series (counters, gauges, histograms)
    - LOG: timestamped text/structured records
    - TRACE: distributed request flow (collection of spans)
    - SPAN: single unit of work within a trace
    - PROFILE: CPU/memory/goroutine flamegraph sample
    - DUMP: heap dump, core dump, thread dump
    - NETWORK_EVENT: interface/BGP/LLDP/topology state change
    """
    METRIC = "metric"
    LOG = "log"
    TRACE = "trace"
    SPAN = "span"
    PROFILE = "profile"
    DUMP = "dump"
    NETWORK_EVENT = "network_event"


class SourceType(str, Enum):
    """Where the signal was collected from.

    This determines the normalization path and trust level of the signal.
    App-layer sources (OTLP, Prometheus) and network-layer sources
    (gNMI, SNMP) are intentionally in the same enum so the correlation
    engine can reason about cross-layer joins.
    """
    # App / infra sources
    OTLP = "otlp"
    PROMETHEUS = "prometheus"
    PROMETHEUS_REMOTE_WRITE = "prometheus_remote_write"
    K8S_API = "k8s_api"
    CONTAINER_RUNTIME = "container_runtime"
    SERVICE_MESH = "service_mesh"
    PPROF = "pprof"
    APP_LOG = "app_log"

    # Network sources (mirrors existing EventSource values)
    GNMI = "gnmi"
    SNMP_TRAP = "snmp_trap"
    SYSLOG = "syslog"
    SSH_POLL_DIFF = "ssh_poll_diff"
    NETCONF = "netconf"

    # Synthetic / test
    SYNTHETIC = "synthetic"
    UNKNOWN = "unknown"


class Severity(str, Enum):
    """Normalized severity across all signal types.

    Aligns with OpenTelemetry severity number ranges and syslog levels.
    """
    TRACE = "trace"       # finest detail
    DEBUG = "debug"
    INFO = "info"
    WARN = "warn"
    ERROR = "error"
    FATAL = "fatal"
    UNSPECIFIED = "unspecified"


class SpanKind(str, Enum):
    """OpenTelemetry span kind."""
    INTERNAL = "internal"
    SERVER = "server"
    CLIENT = "client"
    PRODUCER = "producer"
    CONSUMER = "consumer"
    UNSPECIFIED = "unspecified"


class SpanStatusCode(str, Enum):
    """OpenTelemetry span status."""
    UNSET = "unset"
    OK = "ok"
    ERROR = "error"


class MetricType(str, Enum):
    """Metric instrument type."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"
    EXPONENTIAL_HISTOGRAM = "exponential_histogram"
    UNSPECIFIED = "unspecified"
