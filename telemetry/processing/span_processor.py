"""Span processor — extracts service dependency edges from traces.

When a span in service A has a child span in service B, that's a
service edge: A → B. The processor accumulates these edges with
latency/error statistics to build the service dependency graph.

It also detects:
- Root spans (entry points into the system)
- Error propagation paths
- Cross-service call patterns (RPC, HTTP, messaging)
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

from netobserv.telemetry.envelope import TelemetryEnvelope
from netobserv.telemetry.enums import SignalType, SpanKind, SpanStatusCode
from netobserv.telemetry.signals import SpanRecord


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class ServiceEdge:
    """A directed dependency edge between two services.

    Extracted from parent→child span relationships in traces.
    """
    source_service: str
    target_service: str
    operation: str = ""
    edge_type: str = "rpc"         # rpc, http, grpc, messaging, database

    # Aggregated statistics (updated as spans are processed)
    call_count: int = 0
    error_count: int = 0
    retry_count: int = 0           # calls detected as retries
    total_duration_ms: float = 0.0
    last_seen: datetime | None = None

    # Version tracking
    source_versions: set[str] = field(default_factory=set)
    target_versions: set[str] = field(default_factory=set)

    @property
    def error_rate(self) -> float:
        return self.error_count / max(self.call_count, 1)

    @property
    def retry_rate(self) -> float:
        return self.retry_count / max(self.call_count, 1)

    @property
    def avg_duration_ms(self) -> float:
        return self.total_duration_ms / max(self.call_count, 1)

    @property
    def edge_key(self) -> str:
        return f"{self.source_service}→{self.target_service}"

    def record_call(self, duration_ms: float, is_error: bool, at: datetime | None = None,
                    is_retry: bool = False) -> None:
        self.call_count += 1
        self.total_duration_ms += duration_ms
        if is_error:
            self.error_count += 1
        if is_retry:
            self.retry_count += 1
        self.last_seen = at or _utc_now()


class ServiceKind(str, Enum):
    """Classification of a service in the dependency graph."""
    INTERNAL = "internal"          # owned microservice
    EXTERNAL = "external"          # third-party API
    DATABASE = "database"          # database (postgres, mysql, etc.)
    CACHE = "cache"                # cache (redis, memcached)
    QUEUE = "queue"                # message queue (kafka, rabbitmq)
    GATEWAY = "gateway"            # API gateway / load balancer
    UNKNOWN = "unknown"


@dataclass(slots=True)
class ServiceNode:
    """A service in the dependency graph."""
    service_name: str
    span_count: int = 0
    error_count: int = 0
    root_span_count: int = 0
    last_seen: datetime | None = None
    operations: set[str] = field(default_factory=set)
    # Target metadata from most recent span
    namespace: str | None = None
    workload_name: str | None = None
    pod_count: int = 0
    known_pods: set[str] = field(default_factory=set)
    # Version and classification
    service_kind: ServiceKind = ServiceKind.UNKNOWN
    known_versions: set[str] = field(default_factory=set)
    current_version: str | None = None


class SpanProcessor:
    """Processes span envelopes to extract service graph edges.

    Usage:
        processor = SpanProcessor()
        for envelope in incoming_spans:
            processor.process(envelope)
        edges = processor.get_edges()
        nodes = processor.get_nodes()
    """

    def __init__(self) -> None:
        # span_id → (service_name, envelope) for parent lookups
        self._span_index: dict[str, tuple[str, TelemetryEnvelope]] = {}
        # edge_key → ServiceEdge
        self._edges: dict[str, ServiceEdge] = {}
        # service_name → ServiceNode
        self._nodes: dict[str, ServiceNode] = {}
        # Retry detection: (parent_span_id, target_service) → call count
        self._parent_target_counts: dict[tuple[str, str], int] = defaultdict(int)
        # Stats
        self._processed = 0
        self._edges_created = 0

    def process(self, envelope: TelemetryEnvelope) -> ServiceEdge | None:
        """Process a single span envelope.

        Returns the ServiceEdge if a new cross-service edge was detected,
        or None if this span is intra-service or has no parent.
        """
        if envelope.signal_type != SignalType.SPAN:
            return None
        payload = envelope.payload
        if not isinstance(payload, SpanRecord):
            return None

        self._processed += 1
        service = payload.service_name or envelope.target.service or ""
        if not service:
            return None

        # Extract version
        version = (
            payload.attrs.get("service.version")
            or envelope.target.service_version
            or ""
        )

        # Update service node
        node = self._nodes.get(service)
        if not node:
            node = ServiceNode(service_name=service)
            self._nodes[service] = node
        node.span_count += 1
        node.last_seen = envelope.observed_at
        if payload.operation_name:
            node.operations.add(payload.operation_name)
        if payload.is_error:
            node.error_count += 1
        if payload.is_root_span:
            node.root_span_count += 1
        # Version tracking
        if version:
            node.known_versions.add(version)
            node.current_version = version
        # Service classification
        if node.service_kind == ServiceKind.UNKNOWN:
            node.service_kind = self._classify_service(payload)
        # Enrich from target metadata
        if envelope.target.namespace and not node.namespace:
            node.namespace = envelope.target.namespace
        if envelope.target.workload_name and not node.workload_name:
            node.workload_name = envelope.target.workload_name
        if envelope.target.pod and envelope.target.pod not in node.known_pods:
            node.known_pods.add(envelope.target.pod)
            node.pod_count = len(node.known_pods)

        # Index this span for parent lookups
        self._span_index[payload.span_id] = (service, envelope)

        # Check for cross-service edge (parent in different service)
        edge = None
        if payload.parent_span_id and payload.parent_span_id in self._span_index:
            parent_service, parent_env = self._span_index[payload.parent_span_id]
            if parent_service and parent_service != service:
                # Retry detection: multiple calls from same parent to same service
                retry_key = (payload.parent_span_id, service)
                self._parent_target_counts[retry_key] += 1
                is_retry = self._parent_target_counts[retry_key] > 1

                edge = self._record_edge(
                    source_service=parent_service,
                    target_service=service,
                    operation=payload.operation_name,
                    duration_ms=payload.duration_ms,
                    is_error=payload.is_error,
                    is_retry=is_retry,
                    at=envelope.observed_at,
                    payload=payload,
                    source_version=parent_env.target.service_version or "",
                    target_version=version,
                )
        return edge

    def process_batch(self, envelopes: list[TelemetryEnvelope]) -> list[ServiceEdge]:
        """Process a batch of spans, returning any new/updated edges."""
        edges: list[ServiceEdge] = []
        for env in envelopes:
            edge = self.process(env)
            if edge:
                edges.append(edge)
        return edges

    def get_edges(self) -> list[ServiceEdge]:
        return list(self._edges.values())

    def get_edge(self, source: str, target: str) -> ServiceEdge | None:
        key = f"{source}→{target}"
        return self._edges.get(key)

    def get_nodes(self) -> list[ServiceNode]:
        return list(self._nodes.values())

    def get_node(self, service: str) -> ServiceNode | None:
        return self._nodes.get(service)

    def get_dependencies(self, service: str) -> list[ServiceEdge]:
        """Get all services that 'service' calls (outgoing edges)."""
        return [e for e in self._edges.values() if e.source_service == service]

    def get_dependents(self, service: str) -> list[ServiceEdge]:
        """Get all services that call 'service' (incoming edges)."""
        return [e for e in self._edges.values() if e.target_service == service]

    @property
    def stats(self) -> dict[str, int]:
        return {
            "spans_processed": self._processed,
            "service_count": len(self._nodes),
            "edge_count": len(self._edges),
            "edges_created": self._edges_created,
            "span_index_size": len(self._span_index),
        }

    def _record_edge(
        self, *, source_service: str, target_service: str,
        operation: str, duration_ms: float, is_error: bool,
        is_retry: bool = False,
        at: datetime | None, payload: SpanRecord,
        source_version: str = "", target_version: str = "",
    ) -> ServiceEdge:
        key = f"{source_service}→{target_service}"
        edge = self._edges.get(key)
        if not edge:
            edge_type = self._infer_edge_type(payload)
            edge = ServiceEdge(
                source_service=source_service,
                target_service=target_service,
                operation=operation,
                edge_type=edge_type,
            )
            self._edges[key] = edge
            self._edges_created += 1
        edge.record_call(duration_ms, is_error, at, is_retry=is_retry)
        if source_version:
            edge.source_versions.add(source_version)
        if target_version:
            edge.target_versions.add(target_version)
        return edge

    @staticmethod
    def _classify_service(payload: SpanRecord) -> ServiceKind:
        """Classify a service from its span attributes."""
        attrs = payload.attrs
        # Database services
        if "db.system" in attrs:
            return ServiceKind.DATABASE
        # Cache services
        db_sys = attrs.get("db.system", "").lower()
        if db_sys in ("redis", "memcached", "elasticache"):
            return ServiceKind.CACHE
        # Message queue services
        if "messaging.system" in attrs:
            return ServiceKind.QUEUE
        if payload.kind in (SpanKind.PRODUCER, SpanKind.CONSUMER):
            return ServiceKind.QUEUE
        # External services (peer.service or server span with no k8s metadata)
        if "peer.service" in attrs:
            return ServiceKind.EXTERNAL
        # Gateway (root spans serving HTTP)
        if payload.is_root_span and payload.kind == SpanKind.SERVER:
            if "http.method" in attrs or "http.request.method" in attrs:
                return ServiceKind.GATEWAY
        # Internal by default
        return ServiceKind.INTERNAL

    @staticmethod
    def _infer_edge_type(payload: SpanRecord) -> str:
        """Infer the type of cross-service call from span attributes."""
        attrs = payload.attrs
        if "rpc.system" in attrs:
            system = attrs["rpc.system"]
            if system == "grpc":
                return "grpc"
            return "rpc"
        if "http.method" in attrs or "http.request.method" in attrs:
            return "http"
        if "db.system" in attrs:
            return "database"
        if "messaging.system" in attrs:
            return "messaging"
        if payload.kind == SpanKind.PRODUCER or payload.kind == SpanKind.CONSUMER:
            return "messaging"
        if payload.kind == SpanKind.CLIENT:
            return "rpc"
        return "rpc"
