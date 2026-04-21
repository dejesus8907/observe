"""In-memory topology graph queries using networkx."""

from __future__ import annotations

from typing import Any

from netobserv.models.canonical import CanonicalDevice, CanonicalTopologyEdge


class TopologyGraph:
    """
    Provides graph-based topology queries over a set of topology edges.

    Backed by networkx for path finding and neighbor traversal.
    networkx is loaded lazily — the rest of the platform does not depend on it.
    """

    def __init__(
        self,
        devices: list[CanonicalDevice],
        edges: list[CanonicalTopologyEdge],
    ) -> None:
        self.devices = {d.device_id: d for d in devices}
        self.edges = edges
        self._graph: Any = None

    def _ensure_graph(self) -> Any:
        if self._graph is not None:
            return self._graph
        try:
            import networkx as nx  # type: ignore[import-untyped]
        except ImportError:
            raise RuntimeError(
                "networkx is required for graph queries — install netobserv[graph]"
            )
        G = nx.Graph()
        for device_id, device in self.devices.items():
            G.add_node(device_id, hostname=device.hostname, site=device.site)
        for edge in self.edges:
            G.add_edge(
                edge.source_node_id,
                edge.target_node_id,
                edge_id=edge.edge_id,
                edge_type=edge.edge_type.value,
                confidence=edge.confidence_score,
                source_interface=edge.source_interface,
                target_interface=edge.target_interface,
            )
        self._graph = G
        return G

    def get_neighbors(self, device_id: str) -> list[dict[str, Any]]:
        """Return direct topology neighbors of a device."""
        G = self._ensure_graph()
        if device_id not in G:
            return []
        result = []
        for neighbor_id in G.neighbors(device_id):
            device = self.devices.get(neighbor_id)
            edge_data = G.get_edge_data(device_id, neighbor_id, {})
            result.append(
                {
                    "device_id": neighbor_id,
                    "hostname": device.hostname if device else neighbor_id,
                    "edge_type": edge_data.get("edge_type"),
                    "confidence": edge_data.get("confidence"),
                    "source_interface": edge_data.get("source_interface"),
                    "target_interface": edge_data.get("target_interface"),
                }
            )
        return result

    def find_path(
        self, source_id: str, target_id: str
    ) -> list[str] | None:
        """Return the shortest hop path between two devices."""
        import networkx as nx  # type: ignore[import-untyped]

        G = self._ensure_graph()
        try:
            path = nx.shortest_path(G, source_id, target_id)
            return path  # type: ignore[return-value]
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return None

    def find_all_paths(
        self, source_id: str, target_id: str, cutoff: int = 6
    ) -> list[list[str]]:
        """Return all simple paths up to cutoff length."""
        import networkx as nx  # type: ignore[import-untyped]

        G = self._ensure_graph()
        try:
            paths = list(
                nx.all_simple_paths(G, source_id, target_id, cutoff=cutoff)
            )
            return paths  # type: ignore[return-value]
        except (nx.NodeNotFound, nx.NetworkXNoPath):
            return []

    def connected_components(self) -> list[list[str]]:
        """Return groups of device IDs that are in the same connected component."""
        import networkx as nx  # type: ignore[import-untyped]

        G = self._ensure_graph()
        return [list(c) for c in nx.connected_components(G)]  # type: ignore[return-value]

    def summary(self) -> dict[str, Any]:
        G = self._ensure_graph()
        import networkx as nx  # type: ignore[import-untyped]

        return {
            "node_count": G.number_of_nodes(),
            "edge_count": G.number_of_edges(),
            "connected_components": nx.number_connected_components(G),
            "is_connected": nx.is_connected(G) if G.number_of_nodes() > 0 else False,
        }
