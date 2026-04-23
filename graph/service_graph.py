"""Service dependency graph.

Built from SpanProcessor edges, this graph provides:
- Service-to-service call topology
- Golden signals per service (latency, error rate, throughput)
- Dependency tree traversal (what does service X depend on?)
- Impact analysis (what breaks if service X fails?)

This is the app-layer equivalent of the network topology graph.
The cross_layer_mapper bridges between them.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from netobserv.telemetry.processing.span_processor import ServiceEdge, ServiceNode


@dataclass(slots=True)
class GoldenSignals:
    """The four golden signals for a service."""
    latency_avg_ms: float = 0.0
    latency_p99_ms: float = 0.0
    error_rate: float = 0.0
    throughput_rpm: float = 0.0      # requests per minute
    saturation: float = 0.0          # 0.0-1.0, from profiles (Phase 4)


@dataclass(slots=True)
class ServiceGraphNode:
    """A service in the dependency graph with aggregated signals."""
    service_name: str
    node_data: ServiceNode | None = None
    golden_signals: GoldenSignals = field(default_factory=GoldenSignals)

    # Placement metadata (filled by cross-layer mapper)
    pods: list[str] = field(default_factory=list)
    nodes: list[str] = field(default_factory=list)        # k8s nodes
    namespace: str | None = None


class ServiceGraph:
    """Directed graph of service dependencies.

    Usage:
        graph = ServiceGraph()
        graph.update_from_edges(span_processor.get_edges())
        graph.update_from_nodes(span_processor.get_nodes())
        deps = graph.dependencies("api-gateway")
        impacted = graph.impact_radius("database-svc")
    """

    def __init__(self) -> None:
        self._nodes: dict[str, ServiceGraphNode] = {}
        # Adjacency: service → list of target services
        self._outgoing: dict[str, list[str]] = defaultdict(list)
        self._incoming: dict[str, list[str]] = defaultdict(list)
        # Edge data
        self._edges: dict[str, ServiceEdge] = {}

    def update_from_edges(self, edges: list[ServiceEdge]) -> None:
        """Add or update edges from span processor output."""
        for edge in edges:
            key = edge.edge_key
            self._edges[key] = edge

            # Ensure nodes exist
            for svc in (edge.source_service, edge.target_service):
                if svc not in self._nodes:
                    self._nodes[svc] = ServiceGraphNode(service_name=svc)

            # Update adjacency
            if edge.target_service not in self._outgoing[edge.source_service]:
                self._outgoing[edge.source_service].append(edge.target_service)
            if edge.source_service not in self._incoming[edge.target_service]:
                self._incoming[edge.target_service].append(edge.source_service)

    def update_from_nodes(self, nodes: list[ServiceNode]) -> None:
        """Update node metadata from span processor output."""
        for node in nodes:
            graph_node = self._nodes.get(node.service_name)
            if not graph_node:
                graph_node = ServiceGraphNode(service_name=node.service_name)
                self._nodes[node.service_name] = graph_node
            graph_node.node_data = node
            if node.namespace:
                graph_node.namespace = node.namespace
            graph_node.pods = list(node.known_pods)

            # Compute golden signals from node stats
            if node.span_count > 0:
                graph_node.golden_signals.error_rate = node.error_count / node.span_count

    def update_node_placement(self, service: str, *, pods: list[str] | None = None,
                               nodes: list[str] | None = None,
                               namespace: str | None = None) -> None:
        """Update placement metadata (called by cross-layer mapper)."""
        graph_node = self._nodes.get(service)
        if not graph_node:
            graph_node = ServiceGraphNode(service_name=service)
            self._nodes[service] = graph_node
        if pods:
            graph_node.pods = pods
        if nodes:
            graph_node.nodes = nodes
        if namespace:
            graph_node.namespace = namespace

    # --- Queries ---

    def get_node(self, service: str) -> ServiceGraphNode | None:
        return self._nodes.get(service)

    def get_edge(self, source: str, target: str) -> ServiceEdge | None:
        return self._edges.get(f"{source}→{target}")

    def dependencies(self, service: str) -> list[ServiceEdge]:
        """Services that 'service' calls (outgoing)."""
        return [
            self._edges[f"{service}→{t}"]
            for t in self._outgoing.get(service, [])
            if f"{service}→{t}" in self._edges
        ]

    def dependents(self, service: str) -> list[ServiceEdge]:
        """Services that call 'service' (incoming)."""
        return [
            self._edges[f"{s}→{service}"]
            for s in self._incoming.get(service, [])
            if f"{s}→{service}" in self._edges
        ]

    def impact_radius(self, service: str, max_depth: int = 5) -> list[str]:
        """All services transitively affected if 'service' fails.

        BFS traversal of incoming edges (who depends on this service?).
        """
        affected: list[str] = []
        visited: set[str] = {service}
        queue: list[tuple[str, int]] = [(service, 0)]

        while queue:
            current, depth = queue.pop(0)
            if depth >= max_depth:
                continue
            for dependent in self._incoming.get(current, []):
                if dependent not in visited:
                    visited.add(dependent)
                    affected.append(dependent)
                    queue.append((dependent, depth + 1))
        return affected

    def dependency_tree(self, service: str, max_depth: int = 5) -> dict[str, Any]:
        """Recursive dependency tree from a service (what does it need?)."""
        visited: set[str] = set()

        def _build(svc: str, depth: int) -> dict[str, Any]:
            if depth >= max_depth or svc in visited:
                return {"service": svc, "dependencies": [], "truncated": True}
            visited.add(svc)
            deps = []
            for target in self._outgoing.get(svc, []):
                edge = self._edges.get(f"{svc}→{target}")
                dep_tree = _build(target, depth + 1)
                dep_tree["edge"] = {
                    "call_count": edge.call_count if edge else 0,
                    "error_rate": edge.error_rate if edge else 0,
                    "avg_duration_ms": edge.avg_duration_ms if edge else 0,
                    "edge_type": edge.edge_type if edge else "unknown",
                }
                deps.append(dep_tree)
            return {"service": svc, "dependencies": deps, "truncated": False}

        return _build(service, 0)

    def all_services(self) -> list[str]:
        return list(self._nodes.keys())

    def all_edges(self) -> list[ServiceEdge]:
        return list(self._edges.values())

    @property
    def stats(self) -> dict[str, int]:
        return {
            "service_count": len(self._nodes),
            "edge_count": len(self._edges),
        }
