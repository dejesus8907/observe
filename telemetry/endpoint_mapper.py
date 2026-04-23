from __future__ import annotations

"""Resolve service endpoints to pods/nodes/IPs from inventory state."""

from dataclasses import dataclass

from netobserv.telemetry.k8s_inventory_store import K8sInventoryStore


@dataclass(slots=True)
class ResolvedEndpoint:
    namespace: str
    service_name: str
    endpoint_ip: str
    pod_name: str | None = None
    node_name: str | None = None
    ready: bool = True


class EndpointMapper:
    def __init__(self, inventory: K8sInventoryStore) -> None:
        self._inventory = inventory

    def get_service_endpoints(self, namespace: str, service_name: str) -> list[ResolvedEndpoint]:
        resolved: list[ResolvedEndpoint] = []
        for es in self._inventory.list_endpoint_slices_for_service(namespace, service_name):
            for ep in es.endpoints:
                node_name = ep.node_name
                pod_name = ep.pod_name
                if pod_name and not node_name:
                    pod = self._inventory.get_pod(namespace, pod_name)
                    if pod:
                        node_name = pod.node_name
                elif ep.ip and not pod_name:
                    pod = self._inventory.get_pod_by_ip(ep.ip)
                    if pod:
                        pod_name = pod.meta.name
                        node_name = pod.node_name
                resolved.append(
                    ResolvedEndpoint(
                        namespace=namespace,
                        service_name=service_name,
                        endpoint_ip=ep.ip,
                        pod_name=pod_name,
                        node_name=node_name,
                        ready=ep.ready,
                    )
                )
        return sorted(resolved, key=lambda ep: (ep.endpoint_ip, ep.pod_name or "", ep.node_name or ""))

    def get_backing_pod_for_ip(self, ip: str):
        return self._inventory.get_pod_by_ip(ip)

    def get_services_for_ip(self, ip: str) -> list[ResolvedEndpoint]:
        matches: list[ResolvedEndpoint] = []
        for slice_record in self._inventory.list_endpoint_slices():
            for ep in slice_record.endpoints:
                if ep.ip != ip:
                    continue
                namespace = slice_record.meta.namespace
                service_name = slice_record.service_name
                pod_name = ep.pod_name
                node_name = ep.node_name
                if not pod_name:
                    pod = self._inventory.get_pod_by_ip(ip)
                    if pod:
                        pod_name = pod.meta.name
                        node_name = node_name or pod.node_name
                matches.append(
                    ResolvedEndpoint(
                        namespace=namespace,
                        service_name=service_name,
                        endpoint_ip=ip,
                        pod_name=pod_name,
                        node_name=node_name,
                        ready=ep.ready,
                    )
                )
        return sorted(matches, key=lambda ep: (ep.namespace, ep.service_name, ep.endpoint_ip))
