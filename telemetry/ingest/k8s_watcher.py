from __future__ import annotations

"""State-first Kubernetes watcher and resilient inventory updater for NetObserv.

Supports both injected/synthetic events for tests and live bootstrap + watch
integration via the Kubernetes Python client.
"""

from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import threading
import time
from typing import Any, Iterable, Protocol

from netobserv.graph.cross_layer_mapper import CrossLayerMapper
from netobserv.telemetry.endpoint_mapper import EndpointMapper
from netobserv.telemetry.k8s_inventory_store import (
    EndpointAddress,
    EndpointSliceRecord,
    K8sObjectMeta,
    K8sInventoryStore,
    NodeRecord,
    PodRecord,
    ServiceRecord,
    WorkloadRecord,
)
from netobserv.telemetry.service_membership_resolver import ServiceMembershipResolver
from netobserv.telemetry.workload_owner_resolver import WorkloadOwnerResolver

try:  # pragma: no cover
    from prometheus_client import Counter, Gauge
except Exception:  # pragma: no cover
    Counter = Gauge = None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _now_seconds() -> float:
    return time.time()


class K8sEventType(str, Enum):
    ADDED = "ADDED"
    MODIFIED = "MODIFIED"
    DELETED = "DELETED"
    BOOTSTRAP = "BOOTSTRAP"


class K8sResourceKind(str, Enum):
    POD = "Pod"
    SERVICE = "Service"
    NODE = "Node"
    ENDPOINTS = "Endpoints"
    ENDPOINT_SLICE = "EndpointSlice"
    DEPLOYMENT = "Deployment"
    REPLICA_SET = "ReplicaSet"
    STATEFUL_SET = "StatefulSet"
    DAEMON_SET = "DaemonSet"
    NAMESPACE = "Namespace"


@dataclass(slots=True)
class K8sEvent:
    event_type: K8sEventType
    kind: K8sResourceKind
    name: str
    namespace: str = ""
    uid: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    spec: dict[str, Any] = field(default_factory=dict)
    status: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=_utc_now)

    @property
    def resource_key(self) -> tuple[str, str, str]:
        return self.kind.value, self.namespace, self.name


@dataclass(slots=True)
class WatcherStats:
    events_received: int = 0
    events_processed: int = 0
    events_dropped: int = 0
    events_coalesced: int = 0
    queue_depth: int = 0
    errors: int = 0
    bootstrap_objects: int = 0
    live_watch_events: int = 0
    watch_restarts: int = 0
    reconciliation_runs: int = 0
    watch_failures: int = 0
    overloads: int = 0


@dataclass(slots=True)
class K8sWatchSpec:
    kind: K8sResourceKind
    namespace: str | None = None
    label_selector: str | None = None
    field_selector: str | None = None

    @property
    def worker_key(self) -> tuple[str, str | None]:
        return self.kind.value, self.namespace


@dataclass(slots=True)
class K8sBootstrapConfig:
    kubeconfig: str | None = None
    context: str | None = None
    namespace: str | None = None
    in_cluster: bool = False


class K8sRuntimeProvider(Protocol):
    def bootstrap_resources(self, spec: K8sWatchSpec) -> Iterable[dict[str, Any]]: ...
    def watch_resources(self, spec: K8sWatchSpec, *, resource_version: str | None = None, timeout_seconds: int = 30) -> Iterable[dict[str, Any]]: ...


class K8sWatchError(RuntimeError):
    pass


class K8sResourceVersionExpired(K8sWatchError):
    pass


class K8sWatchThrottled(K8sWatchError):
    pass


class K8sWatchUnauthorized(K8sWatchError):
    pass


class K8sWatchTimeout(K8sWatchError):
    pass


class KubernetesPythonRuntime:
    """Thin adapter over the Kubernetes Python client."""

    def __init__(self, config: K8sBootstrapConfig | None = None) -> None:
        self._config = config or K8sBootstrapConfig()
        try:
            from kubernetes import client, config as k8s_config, watch
            from kubernetes.client import ApiException
        except Exception as exc:  # pragma: no cover
            raise RuntimeError("The kubernetes package is required for live watch/bootstrap integration") from exc
        self._client = client
        self._watch_cls = watch.Watch
        self._api_exception = ApiException
        if self._config.in_cluster:
            k8s_config.load_incluster_config()
        else:
            k8s_config.load_kube_config(config_file=self._config.kubeconfig, context=self._config.context)
        self._core = client.CoreV1Api()
        self._apps = client.AppsV1Api()
        self._discovery = client.DiscoveryV1Api()

    def _serialize(self, obj: Any) -> dict[str, Any]:
        if obj is None:
            return {}
        if hasattr(obj, "to_dict"):
            return obj.to_dict()
        if isinstance(obj, dict):
            return obj
        raise TypeError(f"Unsupported Kubernetes object type: {type(obj)!r}")

    def bootstrap_resources(self, spec: K8sWatchSpec) -> Iterable[dict[str, Any]]:
        list_fn, _ = self._ops_for(spec.kind, spec.namespace)
        kwargs = self._common_kwargs(spec)
        result = list_fn(**kwargs)
        items = getattr(result, "items", None)
        if items is None and isinstance(result, dict):
            items = result.get("items", [])
        return [self._serialize(item) for item in items or []]

    def watch_resources(self, spec: K8sWatchSpec, *, resource_version: str | None = None, timeout_seconds: int = 30) -> Iterable[dict[str, Any]]:
        list_fn, _ = self._ops_for(spec.kind, spec.namespace)
        kwargs = self._common_kwargs(spec)
        if resource_version:
            kwargs["resource_version"] = resource_version
        kwargs["timeout_seconds"] = timeout_seconds
        watcher = self._watch_cls()
        try:
            for event in watcher.stream(list_fn, **kwargs):
                obj = self._serialize(event.get("object"))
                yield {"type": event.get("type", "MODIFIED"), "object": obj}
        except self._api_exception as exc:  # pragma: no cover
            status = getattr(exc, "status", None)
            msg = str(exc).lower()
            if status == 410 or "too old resource version" in msg:
                raise K8sResourceVersionExpired(str(exc)) from exc
            if status == 429 or "too many requests" in msg:
                raise K8sWatchThrottled(str(exc)) from exc
            if status in {401, 403}:
                raise K8sWatchUnauthorized(str(exc)) from exc
            if status in {408, 504} or "timed out" in msg or "timeout" in msg:
                raise K8sWatchTimeout(str(exc)) from exc
            raise K8sWatchError(str(exc)) from exc
        except TimeoutError as exc:  # pragma: no cover
            raise K8sWatchTimeout(str(exc)) from exc
        except Exception as exc:  # pragma: no cover
            raise K8sWatchError(str(exc)) from exc

    def _common_kwargs(self, spec: K8sWatchSpec) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        if spec.label_selector:
            kwargs["label_selector"] = spec.label_selector
        if spec.field_selector:
            kwargs["field_selector"] = spec.field_selector
        return kwargs

    def _ops_for(self, kind: K8sResourceKind, namespace: str | None) -> tuple[Any, bool]:
        namespaced = namespace is not None
        if kind == K8sResourceKind.POD:
            return (self._core.list_namespaced_pod if namespaced else self._core.list_pod_for_all_namespaces, namespaced)
        if kind == K8sResourceKind.SERVICE:
            return (self._core.list_namespaced_service if namespaced else self._core.list_service_for_all_namespaces, namespaced)
        if kind == K8sResourceKind.NODE:
            return (self._core.list_node, False)
        if kind == K8sResourceKind.ENDPOINTS:
            return (self._core.list_namespaced_endpoints if namespaced else self._core.list_endpoints_for_all_namespaces, namespaced)
        if kind == K8sResourceKind.ENDPOINT_SLICE:
            return (self._discovery.list_namespaced_endpoint_slice if namespaced else self._discovery.list_endpoint_slice_for_all_namespaces, namespaced)
        if kind == K8sResourceKind.DEPLOYMENT:
            return (self._apps.list_namespaced_deployment if namespaced else self._apps.list_deployment_for_all_namespaces, namespaced)
        if kind == K8sResourceKind.REPLICA_SET:
            return (self._apps.list_namespaced_replica_set if namespaced else self._apps.list_replica_set_for_all_namespaces, namespaced)
        if kind == K8sResourceKind.STATEFUL_SET:
            return (self._apps.list_namespaced_stateful_set if namespaced else self._apps.list_stateful_set_for_all_namespaces, namespaced)
        if kind == K8sResourceKind.DAEMON_SET:
            return (self._apps.list_namespaced_daemon_set if namespaced else self._apps.list_daemon_set_for_all_namespaces, namespaced)
        if kind == K8sResourceKind.NAMESPACE:
            return (self._core.list_namespace, False)
        raise ValueError(f"Unsupported watch kind: {kind}")


class K8sWatcherMetrics:
    def __init__(self) -> None:
        self._enabled = Gauge is not None and Counter is not None
        if not self._enabled:
            return
        try:
            self.watch_failures_total = Counter('netobserv_k8s_watch_failures_total', 'Total Kubernetes watch failures', ['resource', 'namespace', 'error_type'])
            self.watch_restarts_total = Counter('netobserv_k8s_watch_restarts_total', 'Total Kubernetes watch restarts', ['resource', 'namespace', 'reason'])
            self.reconciliations_total = Counter('netobserv_k8s_reconciliations_total', 'Total Kubernetes reconciliation runs', ['resource', 'namespace'])
            self.inventory_staleness_seconds = Gauge('netobserv_k8s_inventory_staleness_seconds', 'Seconds since last successful watch or reconciliation', ['resource', 'namespace'])
            self.worker_health = Gauge('netobserv_k8s_worker_health', 'Worker health (1 healthy, 0 unhealthy)', ['resource', 'namespace'])
            self.queue_depth = Gauge('netobserv_k8s_queue_depth', 'Kubernetes watcher queue depth', ['resource', 'namespace'])
            self.events_dropped_total = Counter('netobserv_k8s_events_dropped_total', 'Dropped Kubernetes events due to overload', ['resource', 'namespace'])
            self.events_coalesced_total = Counter('netobserv_k8s_events_coalesced_total', 'Coalesced Kubernetes events', ['resource', 'namespace'])
            self.overload_state = Gauge('netobserv_k8s_overload_state', 'Overload state (1 overloaded)', ['resource', 'namespace'])
        except ValueError:  # duplicate registry during focused tests
            self._enabled = False
            return

    def _ns(self, namespace: str | None) -> str:
        return namespace or '_all_'

    def mark_watch_failure(self, spec: K8sWatchSpec, error_type: str) -> None:
        if self._enabled:
            self.watch_failures_total.labels(spec.kind.value, self._ns(spec.namespace), error_type).inc()
            self.worker_health.labels(spec.kind.value, self._ns(spec.namespace)).set(0)

    def mark_watch_restart(self, spec: K8sWatchSpec, reason: str) -> None:
        if self._enabled:
            self.watch_restarts_total.labels(spec.kind.value, self._ns(spec.namespace), reason).inc()

    def mark_reconcile(self, spec: K8sWatchSpec) -> None:
        if self._enabled:
            self.reconciliations_total.labels(spec.kind.value, self._ns(spec.namespace)).inc()

    def mark_worker_healthy(self, spec: K8sWatchSpec) -> None:
        if self._enabled:
            self.worker_health.labels(spec.kind.value, self._ns(spec.namespace)).set(1)
            self.overload_state.labels(spec.kind.value, self._ns(spec.namespace)).set(0)

    def set_staleness(self, spec: K8sWatchSpec, seconds: float) -> None:
        if self._enabled:
            self.inventory_staleness_seconds.labels(spec.kind.value, self._ns(spec.namespace)).set(max(0.0, seconds))

    def set_queue_depth(self, spec: K8sWatchSpec, depth: int) -> None:
        if self._enabled:
            self.queue_depth.labels(spec.kind.value, self._ns(spec.namespace)).set(depth)

    def mark_event_dropped(self, spec: K8sWatchSpec) -> None:
        if self._enabled:
            self.events_dropped_total.labels(spec.kind.value, self._ns(spec.namespace)).inc()
            self.overload_state.labels(spec.kind.value, self._ns(spec.namespace)).set(1)

    def mark_event_coalesced(self, spec: K8sWatchSpec) -> None:
        if self._enabled:
            self.events_coalesced_total.labels(spec.kind.value, self._ns(spec.namespace)).inc()


@dataclass(slots=True)
class ResourceWorkerState:
    spec: K8sWatchSpec
    thread: threading.Thread | None = None
    last_success_ts: float = 0.0
    last_reconcile_ts: float = 0.0
    failures: int = 0
    restarts: int = 0
    running: bool = False
    pending_depth: int = 0
    events_dropped: int = 0
    events_coalesced: int = 0


@dataclass(slots=True)
class WatchServiceConfig:
    watch_timeout_seconds: int = 30
    reconnect_base_seconds: float = 0.25
    reconnect_max_seconds: float = 5.0
    reconcile_interval_seconds: float = 60.0
    stale_after_seconds: float = 120.0
    monitor_interval_seconds: float = 1.0
    per_resource_queue_max: int = 512
    idle_sleep_seconds: float = 0.05
    reconcile_interval_overrides: dict[str, float] = field(default_factory=dict)

    def reconcile_interval_for(self, spec: K8sWatchSpec) -> float:
        return self.reconcile_interval_overrides.get(spec.kind.value, self.reconcile_interval_seconds)


class K8sWatcher:
    def __init__(self, inventory: K8sInventoryStore, *, mapper: CrossLayerMapper | None = None, runtime: K8sRuntimeProvider | None = None) -> None:
        self._inventory = inventory
        self._mapper = mapper
        self._runtime = runtime
        self._stats = WatcherStats()
        self._membership = ServiceMembershipResolver(inventory)
        self._endpoint_mapper = EndpointMapper(inventory)
        self._owners = WorkloadOwnerResolver(inventory)
        self._resource_versions: dict[tuple[K8sResourceKind, str | None], str] = {}
        self._lock = threading.RLock()
        self._pending_events: OrderedDict[tuple[str, str, str], K8sEvent] = OrderedDict()
        self._pending_by_worker: dict[tuple[str, str | None], int] = {}

    @classmethod
    def from_kubernetes(cls, inventory: K8sInventoryStore, *, mapper: CrossLayerMapper | None = None, config: K8sBootstrapConfig | None = None) -> 'K8sWatcher':
        return cls(inventory, mapper=mapper, runtime=KubernetesPythonRuntime(config))

    def enqueue(self, event: K8sEvent, *, max_pending_for_worker: int | None = None) -> bool:
        worker_key = (event.kind.value, event.namespace or None)
        with self._lock:
            key = event.resource_key
            if key in self._pending_events:
                self._pending_events[key] = event
                self._stats.events_coalesced += 1
                return True
            current = self._pending_by_worker.get(worker_key, 0)
            if max_pending_for_worker is not None and current >= max_pending_for_worker:
                self._stats.events_dropped += 1
                self._stats.overloads += 1
                return False
            self._pending_events[key] = event
            self._pending_by_worker[worker_key] = current + 1
            self._stats.events_received += 1
            self._stats.queue_depth = len(self._pending_events)
            return True

    def process_event(self, event: K8sEvent) -> None:
        with self._lock:
            self._apply(event)
        self._stats.events_processed += 1
        self._stats.queue_depth = len(self._pending_events)

    def drain(self, *, max_items: int | None = None) -> int:
        processed = 0
        while True:
            if max_items is not None and processed >= max_items:
                break
            with self._lock:
                if not self._pending_events:
                    break
                key, event = self._pending_events.popitem(last=False)
                worker_key = (event.kind.value, event.namespace or None)
                self._pending_by_worker[worker_key] = max(0, self._pending_by_worker.get(worker_key, 1) - 1)
            try:
                self.process_event(event)
                processed += 1
            except Exception:
                self._stats.errors += 1
                raise
        self._stats.queue_depth = len(self._pending_events)
        return processed

    def pending_depth_for_spec(self, spec: K8sWatchSpec) -> int:
        return self._pending_by_worker.get(spec.worker_key, 0)

    def process_raw(self, raw: dict[str, Any], *, default_kind: K8sResourceKind | None = None, bootstrap: bool = False, max_pending_for_worker: int | None = None) -> bool:
        event_type = K8sEventType.BOOTSTRAP if bootstrap else K8sEventType(raw.get('type', 'ADDED'))
        obj = raw.get('object', raw)
        meta = obj.get('metadata', {})
        kind = default_kind or K8sResourceKind(obj.get('kind', 'Pod'))
        return self.enqueue(K8sEvent(
            event_type=event_type,
            kind=kind,
            name=meta.get('name', ''),
            namespace=meta.get('namespace', ''),
            uid=meta.get('uid', ''),
            metadata=meta,
            spec=obj.get('spec', {}),
            status=obj.get('status', {}),
        ), max_pending_for_worker=max_pending_for_worker)

    def bootstrap(self, specs: Iterable[K8sWatchSpec]) -> int:
        if self._runtime is None:
            raise RuntimeError('No live Kubernetes runtime configured for bootstrap')
        count = 0
        for spec in specs:
            count += self.bootstrap_spec(spec)
        return count

    def bootstrap_spec(self, spec: K8sWatchSpec) -> int:
        if self._runtime is None:
            raise RuntimeError('No live Kubernetes runtime configured for bootstrap')
        count = 0
        max_rv = ''
        for obj in self._runtime.bootstrap_resources(spec):
            self.process_raw(obj, default_kind=spec.kind, bootstrap=True)
            md = obj.get('metadata', {})
            rv = str(md.get('resourceVersion', ''))
            if rv > max_rv:
                max_rv = rv
            count += 1
        if max_rv:
            self._resource_versions[(spec.kind, spec.namespace)] = max_rv
        self._stats.bootstrap_objects += count
        self.drain()
        return count

    def reconcile_spec(self, spec: K8sWatchSpec) -> int:
        return self.bootstrap_spec(spec)

    def reset_resource_version(self, spec: K8sWatchSpec) -> None:
        self._resource_versions.pop((spec.kind, spec.namespace), None)

    def watch_once(self, spec: K8sWatchSpec, *, timeout_seconds: int = 30, max_events: int | None = None, max_pending_for_worker: int | None = None) -> int:
        if self._runtime is None:
            raise RuntimeError('No live Kubernetes runtime configured for watch')
        processed = 0
        rv = self._resource_versions.get((spec.kind, spec.namespace))
        for raw in self._runtime.watch_resources(spec, resource_version=rv, timeout_seconds=timeout_seconds):
            accepted = self.process_raw(raw, default_kind=spec.kind, max_pending_for_worker=max_pending_for_worker)
            if not accepted:
                continue
            processed += 1
            self._stats.live_watch_events += 1
            obj = raw.get('object', raw)
            md = obj.get('metadata', {})
            new_rv = str(md.get('resourceVersion', ''))
            if new_rv:
                self._resource_versions[(spec.kind, spec.namespace)] = new_rv
            if max_events is not None and processed >= max_events:
                break
        self.drain()
        return processed

    @property
    def stats(self) -> dict[str, int]:
        return {
            **self._inventory.stats,
            'events_received': self._stats.events_received,
            'events_processed': self._stats.events_processed,
            'events_dropped': self._stats.events_dropped,
            'events_coalesced': self._stats.events_coalesced,
            'queue_depth': self._stats.queue_depth,
            'errors': self._stats.errors,
            'bootstrap_objects': self._stats.bootstrap_objects,
            'live_watch_events': self._stats.live_watch_events,
            'watch_restarts': self._stats.watch_restarts,
            'reconciliation_runs': self._stats.reconciliation_runs,
            'watch_failures': self._stats.watch_failures,
            'overloads': self._stats.overloads,
        }

    def _apply(self, event: K8sEvent) -> None:
        if event.kind == K8sResourceKind.POD:
            self._apply_pod(event)
        elif event.kind == K8sResourceKind.SERVICE:
            self._apply_service(event)
        elif event.kind == K8sResourceKind.NODE:
            self._apply_node(event)
        elif event.kind in {K8sResourceKind.ENDPOINTS, K8sResourceKind.ENDPOINT_SLICE}:
            self._apply_endpoint_slice(event)
        elif event.kind in {K8sResourceKind.DEPLOYMENT, K8sResourceKind.REPLICA_SET, K8sResourceKind.STATEFUL_SET, K8sResourceKind.DAEMON_SET}:
            self._apply_workload(event)
        if self._mapper is not None:
            self._sync_mapper()

    def _meta(self, event: K8sEvent) -> K8sObjectMeta:
        md = event.metadata or {}
        return K8sObjectMeta(
            uid=event.uid or md.get('uid', f"{event.kind.value}:{event.namespace}:{event.name}"),
            name=event.name,
            namespace=event.namespace,
            labels=dict(md.get('labels', {}) or {}),
            annotations=dict(md.get('annotations', {}) or {}),
            resource_version=str(md.get('resourceVersion', '')),
        )

    def _apply_pod(self, event: K8sEvent) -> None:
        if event.event_type == K8sEventType.DELETED:
            self._inventory.delete_pod(event.namespace, event.name)
            return
        spec = event.spec or {}
        status = event.status or {}
        pod = PodRecord(
            meta=self._meta(event),
            node_name=spec.get('nodeName', '') or '',
            pod_ip=status.get('podIP', '') or '',
            host_ip=status.get('hostIP', '') or '',
            phase=status.get('phase', '') or '',
            owner_references=list(event.metadata.get('ownerReferences', []) or []),
            container_names=[c.get('name', '') for c in spec.get('containers', []) if c.get('name')],
            service_account=spec.get('serviceAccountName', '') or '',
        )
        self._inventory.upsert_pod(pod)

    def _apply_service(self, event: K8sEvent) -> None:
        if event.event_type == K8sEventType.DELETED:
            self._inventory.delete_service(event.namespace, event.name)
            return
        spec = event.spec or {}
        service = ServiceRecord(
            meta=self._meta(event),
            selector=dict(spec.get('selector', {}) or {}),
            cluster_ip=spec.get('clusterIP', '') or '',
            service_type=spec.get('type', 'ClusterIP') or 'ClusterIP',
            ports=list(spec.get('ports', []) or []),
        )
        self._inventory.upsert_service(service)

    def _apply_node(self, event: K8sEvent) -> None:
        if event.event_type == K8sEventType.DELETED:
            self._inventory.delete_node(event.name)
            return
        addresses = list((event.status or {}).get('addresses', []) or [])
        internal_ip = next((a.get('address', '') for a in addresses if a.get('type') == 'InternalIP'), '')
        external_ip = next((a.get('address', '') for a in addresses if a.get('type') == 'ExternalIP'), '')
        annotations = event.metadata.get('annotations', {}) or {}
        interfaces = [i.strip() for i in str(annotations.get('netobserv.io/interfaces', '')).split(',') if i.strip()]
        node = NodeRecord(
            meta=self._meta(event),
            internal_ip=internal_ip,
            external_ip=external_ip,
            device_id=str(annotations.get('netobserv.io/device-id', '') or ''),
            interfaces=interfaces,
        )
        self._inventory.upsert_node(node)

    def _apply_endpoint_slice(self, event: K8sEvent) -> None:
        if event.event_type == K8sEventType.DELETED:
            self._inventory.delete_endpoint_slice(event.namespace, event.name)
            return
        spec = event.spec or {}
        labels = event.metadata.get('labels', {}) or {}
        service_name = labels.get('kubernetes.io/service-name', '') or spec.get('serviceName', '') or event.name
        endpoints: list[EndpointAddress] = []
        for endpoint in list(spec.get('endpoints', []) or []):
            addresses = list(endpoint.get('addresses', []) or [])
            target_ref = endpoint.get('targetRef', {}) or {}
            for address in addresses:
                endpoints.append(EndpointAddress(
                    ip=address,
                    pod_name=target_ref.get('name'),
                    node_name=endpoint.get('nodeName') or None,
                    target_ref_kind=target_ref.get('kind'),
                    ready=bool(endpoint.get('conditions', {}).get('ready', True)),
                ))
        record = EndpointSliceRecord(
            meta=self._meta(event),
            service_name=service_name,
            address_type=str(spec.get('addressType', 'IPv4') or 'IPv4'),
            endpoints=endpoints,
            ports=list(spec.get('ports', []) or []),
        )
        self._inventory.upsert_endpoint_slice(record)

    def _apply_workload(self, event: K8sEvent) -> None:
        kind = event.kind.value
        if event.event_type == K8sEventType.DELETED:
            self._inventory.delete_workload(event.namespace, kind, event.name)
            return
        spec = event.spec or {}
        selector = dict((spec.get('selector') or {}).get('matchLabels', {}) or {})
        record = WorkloadRecord(
            meta=self._meta(event),
            kind=kind,
            replicas=spec.get('replicas'),
            selector=selector,
            owner_references=list(event.metadata.get('ownerReferences', []) or []),
        )
        self._inventory.upsert_workload(record)

    def _sync_mapper(self) -> None:
        if self._mapper is None:
            return
        self._mapper.clear_k8s_inventory()
        for pod in self._inventory.list_pods():
            self._mapper.upsert_pod(pod.meta.namespace, pod.meta.name, node_name=pod.node_name, pod_ip=pod.pod_ip)
            for service in self._membership.get_services_for_pod(pod.meta.namespace, pod.meta.name):
                self._mapper.add_service_to_node(service.meta.name, pod.node_name)
        for node in self._inventory.list_nodes():
            if node.device_id:
                self._mapper.bind_node_to_device(node.meta.name, node.device_id)
            for iface in node.interfaces:
                self._mapper.add_interface_to_node(node.meta.name, iface)


class K8sLongRunningWatcherService:
    def __init__(self, watcher: K8sWatcher, specs: list[K8sWatchSpec], *, config: WatchServiceConfig | None = None, metrics: K8sWatcherMetrics | None = None) -> None:
        self._watcher = watcher
        self._specs = list(specs)
        self._config = config or WatchServiceConfig()
        self._metrics = metrics or K8sWatcherMetrics()
        self._stop = threading.Event()
        self._workers: dict[tuple[str, str | None], ResourceWorkerState] = {spec.worker_key: ResourceWorkerState(spec=spec) for spec in self._specs}
        self._monitor_thread: threading.Thread | None = None

    def start(self) -> None:
        self._stop.clear()
        for state in self._workers.values():
            if state.thread and state.thread.is_alive():
                continue
            state.thread = threading.Thread(target=self._worker_loop, args=(state,), daemon=True)
            state.running = True
            state.thread.start()
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()

    def stop(self) -> None:
        self._stop.set()
        for state in self._workers.values():
            if state.thread:
                state.thread.join(timeout=2.0)
            state.running = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=2.0)

    def join(self, timeout: float | None = None) -> None:
        end = None if timeout is None else (_now_seconds() + timeout)
        for state in self._workers.values():
            if state.thread is None:
                continue
            remaining = None if end is None else max(0.0, end - _now_seconds())
            state.thread.join(timeout=remaining)

    @property
    def worker_states(self) -> dict[tuple[str, str | None], ResourceWorkerState]:
        return self._workers

    def _worker_loop(self, state: ResourceWorkerState) -> None:
        spec = state.spec
        backoff = self._config.reconnect_base_seconds
        self._watcher.bootstrap_spec(spec)
        state.last_success_ts = _now_seconds()
        state.last_reconcile_ts = state.last_success_ts
        self._metrics.mark_worker_healthy(spec)
        while not self._stop.is_set():
            now = _now_seconds()
            if now - state.last_reconcile_ts >= self._config.reconcile_interval_for(spec):
                self._watcher.reconcile_spec(spec)
                state.last_reconcile_ts = _now_seconds()
                self._watcher._stats.reconciliation_runs += 1
                self._metrics.mark_reconcile(spec)
                self._metrics.mark_worker_healthy(spec)
                state.last_success_ts = _now_seconds()
            try:
                processed = self._watcher.watch_once(spec, timeout_seconds=self._config.watch_timeout_seconds, max_pending_for_worker=self._config.per_resource_queue_max)
                state.pending_depth = self._watcher.pending_depth_for_spec(spec)
                state.last_success_ts = _now_seconds()
                self._metrics.set_queue_depth(spec, state.pending_depth)
                self._metrics.mark_worker_healthy(spec)
                backoff = self._config.reconnect_base_seconds
                if processed == 0:
                    self._stop.wait(self._config.idle_sleep_seconds)
            except K8sResourceVersionExpired:
                state.restarts += 1
                self._watcher._stats.watch_restarts += 1
                self._watcher.reset_resource_version(spec)
                self._watcher.bootstrap_spec(spec)
                state.last_success_ts = _now_seconds()
                self._metrics.mark_watch_restart(spec, 'resource_version_expired')
                self._metrics.mark_worker_healthy(spec)
                backoff = self._config.reconnect_base_seconds
            except (K8sWatchThrottled, K8sWatchTimeout) as exc:
                state.failures += 1
                self._watcher._stats.watch_failures += 1
                etype = 'throttled' if isinstance(exc, K8sWatchThrottled) else 'timeout'
                self._metrics.mark_watch_failure(spec, etype)
                self._metrics.set_staleness(spec, _now_seconds() - state.last_success_ts if state.last_success_ts else self._config.stale_after_seconds)
                self._stop.wait(min(self._config.reconnect_max_seconds, max(backoff, self._config.idle_sleep_seconds)))
                backoff = min(self._config.reconnect_max_seconds, backoff * 2)
                state.restarts += 1
                self._watcher._stats.watch_restarts += 1
                self._metrics.mark_watch_restart(spec, etype)
            except K8sWatchUnauthorized as exc:
                state.failures += 1
                self._watcher._stats.watch_failures += 1
                self._metrics.mark_watch_failure(spec, 'unauthorized')
                self._metrics.set_staleness(spec, _now_seconds() - state.last_success_ts if state.last_success_ts else self._config.stale_after_seconds)
                self._stop.wait(self._config.reconnect_max_seconds)
            except Exception:
                state.failures += 1
                self._watcher._stats.watch_failures += 1
                self._metrics.mark_watch_failure(spec, 'generic')
                self._metrics.set_staleness(spec, _now_seconds() - state.last_success_ts if state.last_success_ts else self._config.stale_after_seconds)
                self._stop.wait(backoff)
                backoff = min(self._config.reconnect_max_seconds, backoff * 2)
                state.restarts += 1
                self._watcher._stats.watch_restarts += 1
                self._metrics.mark_watch_restart(spec, 'retry')

    def _monitor_loop(self) -> None:
        while not self._stop.is_set():
            inventory_health: dict[str, tuple[bool, float]] = {}
            for state in self._workers.values():
                state.pending_depth = self._watcher.pending_depth_for_spec(state.spec)
                self._metrics.set_queue_depth(state.spec, state.pending_depth)
                age = _now_seconds() - state.last_success_ts if state.last_success_ts else self._config.stale_after_seconds
                self._metrics.set_staleness(state.spec, age)
                healthy = age <= self._config.stale_after_seconds and state.pending_depth < self._config.per_resource_queue_max
                if healthy:
                    self._metrics.mark_worker_healthy(state.spec)
                else:
                    self._metrics.mark_watch_failure(state.spec, 'stale' if age > self._config.stale_after_seconds else 'overloaded')
                domain = self._inventory_domain_for_kind(state.spec.kind)
                prev = inventory_health.get(domain, (True, 0.0))
                inventory_health[domain] = (prev[0] and healthy, max(prev[1], age))
            if self._watcher._mapper is not None:
                for domain, (healthy, staleness) in inventory_health.items():
                    self._watcher._mapper.set_inventory_health(domain, healthy=healthy, staleness_seconds=staleness)
            self._stop.wait(self._config.monitor_interval_seconds)

    @staticmethod
    def _inventory_domain_for_kind(kind: K8sResourceKind) -> str:
        if kind == K8sResourceKind.POD:
            return 'pods'
        if kind == K8sResourceKind.SERVICE:
            return 'services'
        if kind in {K8sResourceKind.ENDPOINTS, K8sResourceKind.ENDPOINT_SLICE}:
            return 'endpoint_slices'
        if kind == K8sResourceKind.NODE:
            return 'nodes'
        return 'workloads'
