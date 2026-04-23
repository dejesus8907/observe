"""Kubernetes inventory — authoritative cluster state.

This is the source of truth for what exists in the cluster RIGHT NOW.
Other modules (resource_normalizer, cross_layer_mapper, span_processor)
do inference from telemetry metadata. This module does discovery from
the Kubernetes API.

The distinction matters:
- Inference: "I saw pod='api-abc' in an OTLP span, so it probably exists"
- Discovery: "I queried/watched k8s, pod 'api-abc' is Running on node
  'worker-3', owned by deployment 'api-gateway' in namespace 'prod'"

Responsibilities:
1. Maintain an authoritative pod/service/node/endpoint inventory
2. Answer queries: "what service owns this pod?", "what node runs this pod?",
   "what pods back this service?", "what endpoints does this service have?"
3. Provide an enrichment API: given a partial TargetRef with just pod name,
   fill in namespace, node, service, workload_kind, workload_name
4. Track deployment state: which version is deployed where

Data flow:
    K8sWatcherAdapter → K8sInventory → CrossLayerMapper (auto-sync)
                                     → InventoryEnricher (query)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable

from netobserv.telemetry.identity import TargetRef


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class PodPhase(str, Enum):
    PENDING = "Pending"
    RUNNING = "Running"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    UNKNOWN = "Unknown"


@dataclass
class PodRecord:
    """Authoritative state of a pod."""
    name: str
    namespace: str
    node: str = ""
    phase: PodPhase = PodPhase.UNKNOWN
    pod_ip: str = ""
    host_ip: str = ""
    # Ownership
    service: str = ""
    workload_kind: str = ""           # deployment, statefulset, daemonset
    workload_name: str = ""
    # Containers
    containers: list[str] = field(default_factory=list)
    # Labels for service matching
    labels: dict[str, str] = field(default_factory=dict)
    # Timestamps
    created_at: datetime = field(default_factory=_utc_now)
    last_seen: datetime = field(default_factory=_utc_now)

    def to_target_ref(self) -> TargetRef:
        """Convert to a fully populated TargetRef."""
        return TargetRef(
            namespace=self.namespace,
            node=self.node,
            pod=self.name,
            container=self.containers[0] if self.containers else None,
            service=self.service or None,
            workload_kind=self.workload_kind or None,
            workload_name=self.workload_name or None,
        )


@dataclass
class ServiceRecord:
    """Authoritative state of a k8s service."""
    name: str
    namespace: str
    service_type: str = "ClusterIP"    # ClusterIP, NodePort, LoadBalancer
    cluster_ip: str = ""
    selector: dict[str, str] = field(default_factory=dict)
    ports: list[dict[str, Any]] = field(default_factory=list)
    # Resolved backing pods (from endpoints)
    backing_pods: set[str] = field(default_factory=set)
    last_seen: datetime = field(default_factory=_utc_now)


@dataclass
class NodeRecord:
    """Authoritative state of a k8s node."""
    name: str
    internal_ip: str = ""
    external_ip: str = ""
    labels: dict[str, str] = field(default_factory=dict)
    annotations: dict[str, str] = field(default_factory=dict)
    # Network device mapping (from annotations or manual config)
    device_id: str = ""
    interfaces: list[str] = field(default_factory=list)
    # Capacity
    allocatable_cpu: str = ""
    allocatable_memory: str = ""
    last_seen: datetime = field(default_factory=_utc_now)


class K8sInventory:
    """Authoritative Kubernetes cluster inventory.

    This is the central registry that other modules query to resolve
    workload identity. It is populated by K8sWatcherAdapter and consumed
    by InventoryEnricher, CrossLayerMapper, and the correlation engine.

    Usage:
        inventory = K8sInventory()

        # Populated by watcher
        inventory.upsert_pod(PodRecord(name="api-1", namespace="prod", ...))

        # Queried by other modules
        pod = inventory.get_pod("api-1", "prod")
        service = inventory.service_for_pod("api-1", "prod")
        pods = inventory.pods_for_service("api-gateway", "prod")
        node = inventory.node_for_pod("api-1", "prod")
    """

    def __init__(self) -> None:
        # (namespace, name) → record
        self._pods: dict[tuple[str, str], PodRecord] = {}
        self._services: dict[tuple[str, str], ServiceRecord] = {}
        self._nodes: dict[str, NodeRecord] = {}

        # Derived indexes
        # (namespace, label_key, label_value) → set of pod names
        self._pod_label_index: dict[tuple[str, str, str], set[str]] = {}
        # pod_name (no namespace) → list of (namespace, name) keys
        self._pod_name_index: dict[str, list[tuple[str, str]]] = {}

        # Listeners for inventory changes
        self._on_change: list[Callable[[str, str, Any], None]] = []

    # --- Mutation (called by K8sWatcherAdapter) ---

    def upsert_pod(self, pod: PodRecord) -> None:
        key = (pod.namespace, pod.name)
        self._pods[key] = pod
        # Update name index
        if pod.name not in self._pod_name_index:
            self._pod_name_index[pod.name] = []
        if key not in self._pod_name_index[pod.name]:
            self._pod_name_index[pod.name].append(key)
        # Update label index
        for lk, lv in pod.labels.items():
            idx_key = (pod.namespace, lk, lv)
            if idx_key not in self._pod_label_index:
                self._pod_label_index[idx_key] = set()
            self._pod_label_index[idx_key].add(pod.name)
        self._notify("pod_upsert", pod.name, pod)

    def remove_pod(self, name: str, namespace: str) -> None:
        key = (namespace, name)
        pod = self._pods.pop(key, None)
        if pod:
            if name in self._pod_name_index:
                self._pod_name_index[name] = [
                    k for k in self._pod_name_index[name] if k != key
                ]
                if not self._pod_name_index[name]:
                    del self._pod_name_index[name]
        self._notify("pod_remove", name, None)

    def upsert_service(self, svc: ServiceRecord) -> None:
        self._services[(svc.namespace, svc.name)] = svc
        self._notify("service_upsert", svc.name, svc)

    def remove_service(self, name: str, namespace: str) -> None:
        self._services.pop((namespace, name), None)
        self._notify("service_remove", name, None)

    def upsert_node(self, node: NodeRecord) -> None:
        self._nodes[node.name] = node
        self._notify("node_upsert", node.name, node)

    def remove_node(self, name: str) -> None:
        self._nodes.pop(name, None)
        self._notify("node_remove", name, None)

    def bind_service_pods(self, service_name: str, namespace: str,
                          pod_names: set[str]) -> None:
        """Update which pods back a service (from Endpoints events)."""
        key = (namespace, service_name)
        svc = self._services.get(key)
        if svc:
            svc.backing_pods = pod_names
        # Also update pod→service mapping
        for pod_name in pod_names:
            pod_key = (namespace, pod_name)
            pod = self._pods.get(pod_key)
            if pod and not pod.service:
                pod.service = service_name

    # --- Queries (called by enricher, mapper, correlator) ---

    def get_pod(self, name: str, namespace: str = "") -> PodRecord | None:
        """Get a pod by name and namespace. If namespace is empty, searches all."""
        if namespace:
            return self._pods.get((namespace, name))
        candidates = self._pod_name_index.get(name, [])
        if candidates:
            return self._pods.get(candidates[0])
        return None

    def get_service(self, name: str, namespace: str = "") -> ServiceRecord | None:
        if namespace:
            return self._services.get((namespace, name))
        for (ns, sn), svc in self._services.items():
            if sn == name:
                return svc
        return None

    def get_node(self, name: str) -> NodeRecord | None:
        return self._nodes.get(name)

    def service_for_pod(self, pod_name: str, namespace: str = "") -> str | None:
        """What service owns this pod?"""
        pod = self.get_pod(pod_name, namespace)
        if pod and pod.service:
            return pod.service
        # Search services whose backing_pods include this pod
        for (ns, _), svc in self._services.items():
            if namespace and ns != namespace:
                continue
            if pod_name in svc.backing_pods:
                return svc.name
        # Try label matching
        if pod:
            for (ns, _), svc in self._services.items():
                if ns != pod.namespace:
                    continue
                if svc.selector and all(
                    pod.labels.get(k) == v for k, v in svc.selector.items()
                ):
                    return svc.name
        return None

    def node_for_pod(self, pod_name: str, namespace: str = "") -> str | None:
        """What node runs this pod?"""
        pod = self.get_pod(pod_name, namespace)
        return pod.node if pod else None

    def pods_for_service(self, service_name: str, namespace: str = "") -> list[PodRecord]:
        """What pods back this service?"""
        svc = self.get_service(service_name, namespace)
        if not svc:
            return []
        ns = namespace or svc.namespace
        return [
            self._pods[k] for k in [(ns, p) for p in svc.backing_pods]
            if k in self._pods
        ]

    def pods_on_node(self, node_name: str) -> list[PodRecord]:
        """What pods are running on this node?"""
        return [p for p in self._pods.values() if p.node == node_name]

    def device_for_node(self, node_name: str) -> str | None:
        """What network device is this node connected to?"""
        node = self._nodes.get(node_name)
        return node.device_id if node and node.device_id else None

    def services_on_node(self, node_name: str) -> list[str]:
        """What services have pods on this node?"""
        services: set[str] = set()
        for pod in self.pods_on_node(node_name):
            if pod.service:
                services.add(pod.service)
        return list(services)

    # --- Enrichment ---

    def enrich_target(self, target: TargetRef) -> TargetRef:
        """Fill gaps in a TargetRef from authoritative inventory.

        This is the key method: takes a partial TargetRef assembled from
        telemetry metadata and fills in missing fields from the actual
        cluster state.

        Example:
            Input:  TargetRef(pod="api-abc")
            Output: TargetRef(pod="api-abc", namespace="prod", node="worker-3",
                             service="api-gateway", workload_kind="deployment",
                             workload_name="api-gateway")
        """
        updates: dict[str, Any] = {}

        # If we have a pod name, look it up
        if target.pod:
            # Try namespace-qualified first, then name-only fallback
            pod = None
            if target.namespace:
                pod = self.get_pod(target.pod, target.namespace)
            if not pod:
                pod = self.get_pod(target.pod, "")  # search all namespaces
            if pod:
                if not target.namespace:
                    updates["namespace"] = pod.namespace
                if not target.node:
                    updates["node"] = pod.node
                if not target.service and pod.service:
                    updates["service"] = pod.service
                if not target.workload_kind and pod.workload_kind:
                    updates["workload_kind"] = pod.workload_kind
                if not target.workload_name and pod.workload_name:
                    updates["workload_name"] = pod.workload_name
                if not target.container and pod.containers:
                    updates["container"] = pod.containers[0]

        # If we have a service but no pod info, get the backing pods
        if target.service and not target.pod:
            svc_pods = self.pods_for_service(target.service, target.namespace or "")
            if svc_pods:
                first = svc_pods[0]
                if not target.namespace:
                    updates["namespace"] = first.namespace
                if not target.node:
                    updates["node"] = first.node

        # If we have a node, get the device mapping
        node_name = target.node or updates.get("node")
        if node_name and not target.device_id:
            device = self.device_for_node(node_name)
            if device:
                updates["device_id"] = device

        if not updates:
            return target
        return target.merge(TargetRef(**updates))

    # --- Stats ---

    @property
    def stats(self) -> dict[str, int]:
        return {
            "pods": len(self._pods),
            "services": len(self._services),
            "nodes": len(self._nodes),
            "pods_with_service": sum(1 for p in self._pods.values() if p.service),
            "nodes_with_device": sum(1 for n in self._nodes.values() if n.device_id),
        }

    def on_change(self, callback: Callable[[str, str, Any], None]) -> None:
        """Register a listener for inventory changes."""
        self._on_change.append(callback)

    def _notify(self, event_type: str, name: str, record: Any) -> None:
        for cb in self._on_change:
            try:
                cb(event_type, name, record)
            except Exception:
                pass
