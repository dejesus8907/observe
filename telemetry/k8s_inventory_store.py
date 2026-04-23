from __future__ import annotations

"""Authoritative Kubernetes inventory state for NetObserv.

This module keeps normalized, queryable current-state inventory derived from
watch streams. It is intentionally state-first, not notification-first.
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class K8sObjectMeta:
    uid: str
    name: str
    namespace: str = ""
    labels: dict[str, str] = field(default_factory=dict)
    annotations: dict[str, str] = field(default_factory=dict)
    resource_version: str = ""


@dataclass(slots=True)
class PodRecord:
    meta: K8sObjectMeta
    node_name: str = ""
    pod_ip: str = ""
    host_ip: str = ""
    phase: str = ""
    owner_references: list[dict[str, Any]] = field(default_factory=list)
    container_names: list[str] = field(default_factory=list)
    service_account: str = ""


@dataclass(slots=True)
class ServiceRecord:
    meta: K8sObjectMeta
    selector: dict[str, str] = field(default_factory=dict)
    cluster_ip: str = ""
    service_type: str = "ClusterIP"
    ports: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class EndpointAddress:
    ip: str = ""
    pod_name: str | None = None
    node_name: str | None = None
    target_ref_kind: str | None = None
    ready: bool = True


@dataclass(slots=True)
class EndpointSliceRecord:
    meta: K8sObjectMeta
    service_name: str
    address_type: str = "IPv4"
    endpoints: list[EndpointAddress] = field(default_factory=list)
    ports: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class NodeRecord:
    meta: K8sObjectMeta
    internal_ip: str = ""
    external_ip: str = ""
    device_id: str = ""
    interfaces: list[str] = field(default_factory=list)


@dataclass(slots=True)
class WorkloadRecord:
    meta: K8sObjectMeta
    kind: str
    replicas: int | None = None
    selector: dict[str, str] = field(default_factory=dict)
    owner_references: list[dict[str, Any]] = field(default_factory=list)


class K8sInventoryStore:
    def __init__(self) -> None:
        self._pods_by_ns_name: dict[tuple[str, str], PodRecord] = {}
        self._pods_by_uid: dict[str, PodRecord] = {}
        self._pods_by_ip: dict[str, PodRecord] = {}
        self._services_by_ns_name: dict[tuple[str, str], ServiceRecord] = {}
        self._services_by_uid: dict[str, ServiceRecord] = {}
        self._nodes_by_name: dict[str, NodeRecord] = {}
        self._nodes_by_uid: dict[str, NodeRecord] = {}
        self._nodes_by_ip: dict[str, NodeRecord] = {}
        self._endpoint_slices_by_ns_name: dict[tuple[str, str], EndpointSliceRecord] = {}
        self._workloads_by_ns_kind_name: dict[tuple[str, str, str], WorkloadRecord] = {}
        self._workloads_by_uid: dict[str, WorkloadRecord] = {}

    # Pods
    def upsert_pod(self, record: PodRecord) -> None:
        key = (record.meta.namespace, record.meta.name)
        old = self._pods_by_ns_name.get(key)
        if old and old.pod_ip:
            self._pods_by_ip.pop(old.pod_ip, None)
        self._pods_by_ns_name[key] = record
        self._pods_by_uid[record.meta.uid] = record
        if record.pod_ip:
            self._pods_by_ip[record.pod_ip] = record

    def delete_pod(self, namespace: str, name: str) -> PodRecord | None:
        key = (namespace, name)
        old = self._pods_by_ns_name.pop(key, None)
        if not old:
            return None
        self._pods_by_uid.pop(old.meta.uid, None)
        if old.pod_ip:
            self._pods_by_ip.pop(old.pod_ip, None)
        return old

    def get_pod(self, namespace: str, name: str) -> PodRecord | None:
        return self._pods_by_ns_name.get((namespace, name))

    def get_pod_by_uid(self, uid: str) -> PodRecord | None:
        return self._pods_by_uid.get(uid)

    def get_pod_by_ip(self, pod_ip: str) -> PodRecord | None:
        return self._pods_by_ip.get(pod_ip)

    def list_pods(self, namespace: str | None = None) -> list[PodRecord]:
        vals = self._pods_by_ns_name.values()
        if namespace is None:
            return list(vals)
        return [p for p in vals if p.meta.namespace == namespace]

    # Services
    def upsert_service(self, record: ServiceRecord) -> None:
        key = (record.meta.namespace, record.meta.name)
        self._services_by_ns_name[key] = record
        self._services_by_uid[record.meta.uid] = record

    def delete_service(self, namespace: str, name: str) -> ServiceRecord | None:
        key = (namespace, name)
        old = self._services_by_ns_name.pop(key, None)
        if not old:
            return None
        self._services_by_uid.pop(old.meta.uid, None)
        return old

    def get_service(self, namespace: str, name: str) -> ServiceRecord | None:
        return self._services_by_ns_name.get((namespace, name))

    def list_services(self, namespace: str | None = None) -> list[ServiceRecord]:
        vals = self._services_by_ns_name.values()
        if namespace is None:
            return list(vals)
        return [s for s in vals if s.meta.namespace == namespace]

    def get_service_by_cluster_ip(self, cluster_ip: str) -> ServiceRecord | None:
        for service in self._services_by_ns_name.values():
            if service.cluster_ip == cluster_ip:
                return service
        return None

    # Nodes
    def upsert_node(self, record: NodeRecord) -> None:
        old = self._nodes_by_name.get(record.meta.name)
        if old:
            for ip in (old.internal_ip, old.external_ip):
                if ip:
                    self._nodes_by_ip.pop(ip, None)
        self._nodes_by_name[record.meta.name] = record
        self._nodes_by_uid[record.meta.uid] = record
        for ip in (record.internal_ip, record.external_ip):
            if ip:
                self._nodes_by_ip[ip] = record

    def delete_node(self, name: str) -> NodeRecord | None:
        old = self._nodes_by_name.pop(name, None)
        if not old:
            return None
        self._nodes_by_uid.pop(old.meta.uid, None)
        for ip in (old.internal_ip, old.external_ip):
            if ip:
                self._nodes_by_ip.pop(ip, None)
        return old

    def get_node(self, name: str) -> NodeRecord | None:
        return self._nodes_by_name.get(name)

    def get_node_by_ip(self, ip: str) -> NodeRecord | None:
        return self._nodes_by_ip.get(ip)

    def list_nodes(self) -> list[NodeRecord]:
        return list(self._nodes_by_name.values())

    # Endpoint slices
    def upsert_endpoint_slice(self, record: EndpointSliceRecord) -> None:
        self._endpoint_slices_by_ns_name[(record.meta.namespace, record.meta.name)] = record

    def delete_endpoint_slice(self, namespace: str, name: str) -> EndpointSliceRecord | None:
        return self._endpoint_slices_by_ns_name.pop((namespace, name), None)

    def get_endpoint_slice(self, namespace: str, name: str) -> EndpointSliceRecord | None:
        return self._endpoint_slices_by_ns_name.get((namespace, name))

    def list_endpoint_slices(self, namespace: str | None = None) -> list[EndpointSliceRecord]:
        vals = self._endpoint_slices_by_ns_name.values()
        if namespace is None:
            return list(vals)
        return [e for e in vals if e.meta.namespace == namespace]

    def list_endpoint_slices_for_service(self, namespace: str, service_name: str) -> list[EndpointSliceRecord]:
        return [
            e for e in self._endpoint_slices_by_ns_name.values()
            if e.meta.namespace == namespace and e.service_name == service_name
        ]

    # Workloads
    def upsert_workload(self, record: WorkloadRecord) -> None:
        key = (record.meta.namespace, record.kind, record.meta.name)
        self._workloads_by_ns_kind_name[key] = record
        self._workloads_by_uid[record.meta.uid] = record

    def delete_workload(self, namespace: str, kind: str, name: str) -> WorkloadRecord | None:
        old = self._workloads_by_ns_kind_name.pop((namespace, kind, name), None)
        if old:
            self._workloads_by_uid.pop(old.meta.uid, None)
        return old

    def get_workload(self, namespace: str, kind: str, name: str) -> WorkloadRecord | None:
        return self._workloads_by_ns_kind_name.get((namespace, kind, name))

    def list_workloads(self, namespace: str | None = None) -> list[WorkloadRecord]:
        vals = self._workloads_by_ns_kind_name.values()
        if namespace is None:
            return list(vals)
        return [w for w in vals if w.meta.namespace == namespace]

    @property
    def stats(self) -> dict[str, int]:
        return {
            "pods": len(self._pods_by_ns_name),
            "services": len(self._services_by_ns_name),
            "nodes": len(self._nodes_by_name),
            "endpoint_slices": len(self._endpoint_slices_by_ns_name),
            "workloads": len(self._workloads_by_ns_kind_name),
        }
