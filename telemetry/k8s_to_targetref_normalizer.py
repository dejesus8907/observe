from __future__ import annotations

"""Convert Kubernetes inventory records into canonical TargetRef objects."""

from netobserv.telemetry.identity import TargetRef
from netobserv.telemetry.k8s_inventory_store import NodeRecord, PodRecord, ServiceRecord
from netobserv.telemetry.workload_owner_resolver import ResolvedWorkloadIdentity


class K8sToTargetRefNormalizer:
    def __init__(self, cluster_name: str | None = None, environment: str | None = None) -> None:
        self._cluster_name = cluster_name
        self._environment = environment

    def from_pod(
        self,
        pod: PodRecord,
        *,
        workload: ResolvedWorkloadIdentity | None = None,
        service_name: str | None = None,
        node: NodeRecord | None = None,
    ) -> TargetRef:
        service = service_name or pod.meta.labels.get("app.kubernetes.io/name") or pod.meta.labels.get("app")
        return TargetRef(
            environment=self._environment,
            cluster=self._cluster_name,
            namespace=pod.meta.namespace or None,
            node=pod.node_name or None,
            workload_kind=workload.kind if workload else None,
            workload_name=workload.name if workload else None,
            pod=pod.meta.name,
            service=service or None,
            instance_id=pod.meta.uid,
            attrs={"pod_ip": pod.pod_ip, "host_ip": pod.host_ip, **({"node_internal_ip": node.internal_ip} if node and node.internal_ip else {})},
        )

    def from_service(self, service: ServiceRecord) -> TargetRef:
        return TargetRef(
            environment=self._environment,
            cluster=self._cluster_name,
            namespace=service.meta.namespace or None,
            service=service.meta.name,
            attrs={"cluster_ip": service.cluster_ip, "service_type": service.service_type},
        )

    def from_node(self, node: NodeRecord) -> TargetRef:
        return TargetRef(
            environment=self._environment,
            cluster=self._cluster_name,
            node=node.meta.name,
            device_id=node.device_id or None,
            attrs={"internal_ip": node.internal_ip, "external_ip": node.external_ip},
        )
