from __future__ import annotations

"""Resolve stable workload identity from Kubernetes owner references."""

from dataclasses import dataclass

from netobserv.telemetry.k8s_inventory_store import K8sInventoryStore, PodRecord, WorkloadRecord


@dataclass(slots=True)
class ResolvedWorkloadIdentity:
    namespace: str
    kind: str
    name: str
    pod_name: str | None = None


class WorkloadOwnerResolver:
    def __init__(self, inventory: K8sInventoryStore) -> None:
        self._inventory = inventory

    def resolve_pod(self, pod: PodRecord) -> ResolvedWorkloadIdentity:
        ns = pod.meta.namespace
        owner_refs = pod.owner_references or []
        if not owner_refs:
            return ResolvedWorkloadIdentity(namespace=ns, kind="Pod", name=pod.meta.name, pod_name=pod.meta.name)

        primary = self._primary_owner(owner_refs)
        if not primary:
            return ResolvedWorkloadIdentity(namespace=ns, kind="Pod", name=pod.meta.name, pod_name=pod.meta.name)

        kind = str(primary.get("kind", "Pod"))
        name = str(primary.get("name", pod.meta.name))

        if kind == "ReplicaSet":
            rs = self._inventory.get_workload(ns, "ReplicaSet", name)
            if rs:
                higher = self._primary_owner(rs.owner_references)
                if higher and higher.get("kind") == "Deployment":
                    return ResolvedWorkloadIdentity(namespace=ns, kind="Deployment", name=str(higher.get("name")), pod_name=pod.meta.name)
            return ResolvedWorkloadIdentity(namespace=ns, kind="ReplicaSet", name=name, pod_name=pod.meta.name)

        return ResolvedWorkloadIdentity(namespace=ns, kind=kind, name=name, pod_name=pod.meta.name)

    def resolve_from_pod_name(self, namespace: str, pod_name: str) -> ResolvedWorkloadIdentity | None:
        pod = self._inventory.get_pod(namespace, pod_name)
        if pod is None:
            return None
        return self.resolve_pod(pod)

    @staticmethod
    def _primary_owner(owner_refs: list[dict]) -> dict | None:
        if not owner_refs:
            return None
        for ref in owner_refs:
            if ref.get("controller") is True:
                return ref
        return owner_refs[0]
