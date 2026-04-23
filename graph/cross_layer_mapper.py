"""Cross-layer mapper — bridges service graph to network topology.

This is the core differentiator: mapping service-layer symptoms
(latency spike, error burst) to network-layer state (interface flap,
BGP session drop, link degradation).

The mapping chain:
    service edge (A→B)
    → pod endpoints (A runs on pods [p1,p2], B runs on [p3,p4])
    → node placement (p1 on node-1, p3 on node-3)
    → network device mapping (node-1 connects via leaf-1:Eth1)
    → network path (leaf-1 → spine-1 → leaf-3)
    → network state (any degraded edges on this path?)

Data sources:
    - Service graph: service→pods (from span processor)
    - Placement registry: pod→node (from k8s API or manual config)
    - Network topology: node→device→interface (from existing NetObserv)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class PodPlacement:
    """Where a pod is running in the infrastructure."""
    pod: str
    node: str
    namespace: str = ""
    service: str = ""
    host_ip: str = ""
    pod_ip: str = ""


@dataclass(slots=True)
class NodeNetworkMapping:
    """Maps a k8s node to its network device and interfaces."""
    node: str
    device_id: str                    # network device (e.g., leaf switch)
    interfaces: list[str] = field(default_factory=list)
    management_ip: str = ""


@dataclass(slots=True)
class NetworkPathSegment:
    """One hop in a network path between two nodes."""
    device_id: str
    interface: str
    peer_device_id: str | None = None
    peer_interface: str | None = None
    is_degraded: bool = False
    edge_id: str | None = None


@dataclass(slots=True)
class ServiceNetworkMapping:
    """Complete mapping from a service edge to network path segments."""
    source_service: str
    target_service: str
    source_pods: list[str] = field(default_factory=list)
    target_pods: list[str] = field(default_factory=list)
    source_nodes: list[str] = field(default_factory=list)
    target_nodes: list[str] = field(default_factory=list)
    source_devices: list[str] = field(default_factory=list)
    target_devices: list[str] = field(default_factory=list)
    network_path: list[NetworkPathSegment] = field(default_factory=list)
    has_degraded_path: bool = False
    confidence: float = 1.0
    confidence_reasons: list[str] = field(default_factory=list)

    @property
    def degraded_segments(self) -> list[NetworkPathSegment]:
        return [s for s in self.network_path if s.is_degraded]


class CrossLayerMapper:
    """Maps service-layer topology to network-layer topology.

    Usage:
        mapper = CrossLayerMapper()
        # Register infrastructure knowledge
        mapper.register_pod("api-pod-1", node="worker-1", service="api")
        mapper.register_node_network("worker-1", device_id="leaf-1",
                                      interfaces=["Eth1", "Eth2"])
        # Query
        mapping = mapper.map_service_edge("api", "payment")
        if mapping.has_degraded_path:
            print("Network issue affecting service edge!")
    """

    def __init__(self) -> None:
        # pod → PodPlacement
        self._pods: dict[str, PodPlacement] = {}
        # node → NodeNetworkMapping
        self._node_network: dict[str, NodeNetworkMapping] = {}
        # service → set of pods
        self._service_pods: dict[str, set[str]] = {}
        # device_id → set of neighbor device_ids (from network topology)
        self._device_neighbors: dict[str, set[str]] = {}
        # (device, interface) → (peer_device, peer_interface, is_degraded, edge_id)
        self._link_state: dict[tuple[str, str], tuple[str, str, bool, str]] = {}
        # freshness/health of k8s-backed inventory domains
        self._inventory_health: dict[str, tuple[bool, float]] = {}

    # --- Registration ---

    def clear_k8s_inventory(self) -> None:
        """Clear only K8s-derived placement and service membership state."""
        self._pods.clear()
        self._service_pods.clear()

    def upsert_pod(self, namespace: str, pod: str, *, node_name: str, pod_ip: str = "", service: str = "") -> None:
        self.register_pod(pod, node=node_name, namespace=namespace, service=service, pod_ip=pod_ip)

    def add_service_to_node(self, service: str, node: str) -> None:
        # Ensure node exists in membership view even if no pod explicitly re-registered with service.
        for placement in self._pods.values():
            if placement.node == node and placement.pod not in self._service_pods.get(service, set()):
                if service not in self._service_pods:
                    self._service_pods[service] = set()
                self._service_pods[service].add(placement.pod)

    def bind_node_to_device(self, node: str, device_id: str) -> None:
        existing = self._node_network.get(node)
        interfaces = existing.interfaces if existing else []
        management_ip = existing.management_ip if existing else ""
        self.register_node_network(node, device_id=device_id, interfaces=interfaces, management_ip=management_ip)

    def add_interface_to_node(self, node: str, interface: str) -> None:
        existing = self._node_network.get(node)
        if existing is None:
            self.register_node_network(node, device_id="", interfaces=[interface])
            return
        if interface not in existing.interfaces:
            existing.interfaces.append(interface)


    def register_pod(self, pod: str, *, node: str, service: str = "",
                     namespace: str = "", host_ip: str = "", pod_ip: str = "") -> None:
        placement = PodPlacement(
            pod=pod, node=node, namespace=namespace,
            service=service, host_ip=host_ip, pod_ip=pod_ip,
        )
        self._pods[pod] = placement
        if service:
            if service not in self._service_pods:
                self._service_pods[service] = set()
            self._service_pods[service].add(pod)

    def register_node_network(self, node: str, *, device_id: str,
                               interfaces: list[str] | None = None,
                               management_ip: str = "") -> None:
        self._node_network[node] = NodeNetworkMapping(
            node=node, device_id=device_id,
            interfaces=interfaces or [],
            management_ip=management_ip,
        )

    def register_link(self, device_id: str, interface: str, *,
                      peer_device: str, peer_interface: str = "",
                      is_degraded: bool = False, edge_id: str = "") -> None:
        """Register a network link state (from topology patcher)."""
        self._link_state[(device_id, interface)] = (
            peer_device, peer_interface, is_degraded, edge_id
        )
        if device_id not in self._device_neighbors:
            self._device_neighbors[device_id] = set()
        self._device_neighbors[device_id].add(peer_device)

    def update_link_state(self, device_id: str, interface: str,
                          is_degraded: bool) -> None:
        """Update degradation state of a link."""
        existing = self._link_state.get((device_id, interface))
        if existing:
            peer_dev, peer_iface, _, edge_id = existing
            self._link_state[(device_id, interface)] = (
                peer_dev, peer_iface, is_degraded, edge_id
            )

    def sync_from_topology_patcher(self, live_edges: list[Any]) -> None:
        """Bulk sync link state from topology patcher's LiveEdgeState list."""
        for edge in live_edges:
            src = getattr(edge, "source_node_id", "")
            tgt = getattr(edge, "target_node_id", "")
            src_iface = getattr(edge, "source_interface", "") or ""
            tgt_iface = getattr(edge, "target_interface", "") or ""
            degraded = getattr(edge, "is_degraded", False)
            edge_id = getattr(edge, "edge_id", "")

            if src and src_iface:
                self.register_link(src, src_iface, peer_device=tgt,
                                   peer_interface=tgt_iface,
                                   is_degraded=degraded, edge_id=edge_id)
            if tgt and tgt_iface:
                self.register_link(tgt, tgt_iface, peer_device=src,
                                   peer_interface=src_iface,
                                   is_degraded=degraded, edge_id=edge_id)


    def set_inventory_health(self, domain: str, *, healthy: bool, staleness_seconds: float = 0.0) -> None:
        """Expose freshness of upstream inventory domains to mapping confidence."""
        self._inventory_health[domain] = (healthy, max(0.0, float(staleness_seconds)))

    def inventory_health(self) -> dict[str, tuple[bool, float]]:
        return dict(self._inventory_health)

    # --- Mapping queries ---

    def map_service_edge(self, source_service: str, target_service: str) -> ServiceNetworkMapping:
        """Map a service-to-service edge to its network path.

        Returns a ServiceNetworkMapping with pod→node→device→path info
        and degradation status.
        """
        mapping = ServiceNetworkMapping(
            source_service=source_service,
            target_service=target_service,
        )

        # Step 1: service → pods
        mapping.source_pods = list(self._service_pods.get(source_service, set()))
        mapping.target_pods = list(self._service_pods.get(target_service, set()))

        # Step 2: pods → nodes
        source_nodes: set[str] = set()
        for pod in mapping.source_pods:
            placement = self._pods.get(pod)
            if placement:
                source_nodes.add(placement.node)
        target_nodes: set[str] = set()
        for pod in mapping.target_pods:
            placement = self._pods.get(pod)
            if placement:
                target_nodes.add(placement.node)
        mapping.source_nodes = list(source_nodes)
        mapping.target_nodes = list(target_nodes)

        # Step 3: nodes → network devices
        source_devices: set[str] = set()
        for node in source_nodes:
            nm = self._node_network.get(node)
            if nm:
                source_devices.add(nm.device_id)
        target_devices: set[str] = set()
        for node in target_nodes:
            nm = self._node_network.get(node)
            if nm:
                target_devices.add(nm.device_id)
        mapping.source_devices = list(source_devices)
        mapping.target_devices = list(target_devices)

        # Step 4: find network path between devices
        for src_dev in source_devices:
            for tgt_dev in target_devices:
                segments = self._find_path_segments(src_dev, tgt_dev)
                mapping.network_path.extend(segments)

        mapping.has_degraded_path = any(s.is_degraded for s in mapping.network_path)
        mapping.confidence, mapping.confidence_reasons = self._inventory_confidence(mapping)
        return mapping

    def get_services_on_node(self, node: str) -> list[str]:
        """Get all services with pods on a given node."""
        services: set[str] = set()
        # Check placement-level service field
        node_pods: set[str] = set()
        for pod, placement in self._pods.items():
            if placement.node == node:
                node_pods.add(pod)
                if placement.service:
                    services.add(placement.service)
        # Check service_pods index (populated by add_service_to_node)
        for service, pods in self._service_pods.items():
            if pods & node_pods:
                services.add(service)
        return sorted(services)

    def get_services_affected_by_device(self, device_id: str) -> list[str]:
        """Get all services whose traffic could cross a given network device."""
        affected: list[str] = []
        # Find nodes connected to this device
        nodes = [n for n, nm in self._node_network.items() if nm.device_id == device_id]
        for node in nodes:
            affected.extend(self.get_services_on_node(node))
        return list(set(affected))

    def get_services_affected_by_link(self, device_id: str, interface: str) -> list[str]:
        """Get services whose traffic might use a specific link."""
        # This link connects device_id to some peer
        link = self._link_state.get((device_id, interface))
        if not link:
            return []
        peer_device = link[0]
        # Services on nodes connected to either device
        services: set[str] = set()
        for dev in (device_id, peer_device):
            services.update(self.get_services_affected_by_device(dev))
        return list(services)

    @property
    def stats(self) -> dict[str, int]:
        return {
            "pods_registered": len(self._pods),
            "nodes_mapped": len(self._node_network),
            "services_tracked": len(self._service_pods),
            "links_tracked": len(self._link_state),
        }

    # --- Internal ---

    def _inventory_confidence(self, mapping: ServiceNetworkMapping) -> tuple[float, list[str]]:
        score = 1.0
        reasons: list[str] = []
        if not mapping.source_pods or not mapping.target_pods:
            score *= 0.6
            reasons.append('missing_service_membership')
        if (mapping.source_pods or mapping.target_pods) and not (mapping.source_nodes or mapping.target_nodes):
            score *= 0.7
            reasons.append('missing_pod_node_mapping')
        if (mapping.source_nodes or mapping.target_nodes) and not (mapping.source_devices or mapping.target_devices):
            score *= 0.7
            reasons.append('missing_node_network_mapping')
        for domain, (healthy, staleness) in self._inventory_health.items():
            if healthy:
                continue
            if domain in {'pods', 'services', 'endpoint_slices', 'nodes'}:
                score *= 0.7
                reasons.append(f'stale_{domain}')
                if staleness > 300:
                    score *= 0.8
        return max(0.0, min(1.0, score)), reasons


    def _find_path_segments(self, src_device: str, tgt_device: str) -> list[NetworkPathSegment]:
        """Find network path segments between two devices.

        Simple BFS through registered links. Returns the segments with
        their degradation status.
        """
        if src_device == tgt_device:
            return []

        # BFS
        visited: set[str] = {src_device}
        # queue: (current_device, path_so_far)
        queue: list[tuple[str, list[NetworkPathSegment]]] = [(src_device, [])]

        while queue:
            current, path = queue.pop(0)
            # Check all links from current device
            for (dev, iface), (peer_dev, peer_iface, degraded, edge_id) in self._link_state.items():
                if dev != current or peer_dev in visited:
                    continue
                segment = NetworkPathSegment(
                    device_id=dev,
                    interface=iface,
                    peer_device_id=peer_dev,
                    peer_interface=peer_iface,
                    is_degraded=degraded,
                    edge_id=edge_id,
                )
                new_path = path + [segment]
                if peer_dev == tgt_device:
                    return new_path
                visited.add(peer_dev)
                queue.append((peer_dev, new_path))

        return []  # no path found
