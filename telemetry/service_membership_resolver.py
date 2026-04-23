from __future__ import annotations

"""Resolve service membership from Service selectors and EndpointSlices."""

from netobserv.telemetry.k8s_inventory_store import K8sInventoryStore, PodRecord, ServiceRecord


class ServiceMembershipResolver:
    def __init__(self, inventory: K8sInventoryStore) -> None:
        self._inventory = inventory

    def get_pods_for_service(self, namespace: str, service_name: str) -> list[PodRecord]:
        service = self._inventory.get_service(namespace, service_name)
        if service is None:
            return []

        selector_matches = self._match_selector(service)
        slice_matches = self._match_endpoint_slices(namespace, service_name)
        merged: dict[tuple[str, str], PodRecord] = {
            (p.meta.namespace, p.meta.name): p for p in selector_matches
        }
        for pod in slice_matches:
            merged[(pod.meta.namespace, pod.meta.name)] = pod
        return list(merged.values())

    def get_services_for_pod(self, namespace: str, pod_name: str) -> list[ServiceRecord]:
        pod = self._inventory.get_pod(namespace, pod_name)
        if pod is None:
            return []
        services: list[ServiceRecord] = []
        for svc in self._inventory.list_services(namespace):
            if self._pod_matches_service(pod, svc):
                services.append(svc)
                continue
            slices = self._inventory.list_endpoint_slices_for_service(namespace, svc.meta.name)
            if any(ep.pod_name == pod_name for es in slices for ep in es.endpoints):
                services.append(svc)
        return services

    def _match_selector(self, service: ServiceRecord) -> list[PodRecord]:
        if not service.selector:
            return []
        result: list[PodRecord] = []
        for pod in self._inventory.list_pods(service.meta.namespace):
            if self._pod_matches_service(pod, service):
                result.append(pod)
        return result

    def _match_endpoint_slices(self, namespace: str, service_name: str) -> list[PodRecord]:
        result: dict[tuple[str, str], PodRecord] = {}
        for es in self._inventory.list_endpoint_slices_for_service(namespace, service_name):
            for ep in es.endpoints:
                if ep.pod_name:
                    pod = self._inventory.get_pod(namespace, ep.pod_name)
                    if pod:
                        result[(pod.meta.namespace, pod.meta.name)] = pod
                elif ep.ip:
                    pod = self._inventory.get_pod_by_ip(ep.ip)
                    if pod:
                        result[(pod.meta.namespace, pod.meta.name)] = pod
        return list(result.values())

    @staticmethod
    def _pod_matches_service(pod: PodRecord, service: ServiceRecord) -> bool:
        selector = service.selector
        if not selector:
            return False
        labels = pod.meta.labels
        return all(labels.get(k) == v for k, v in selector.items())
