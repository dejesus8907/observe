"""Kubernetes adapter for NetObserv streaming pipeline.

Watches Kubernetes state changes and emits ChangeEvents into the existing
event bus. This adapter adds K8s awareness to the correlation engine so it
can correlate pod failures, node readiness, and service endpoint changes
with network events.

Design principles:
- Read-only: never mutates K8s state
- State-transition-only: emits only on meaningful changes, not every informer update
- Dedup: suppresses duplicate events within a configurable window
- Thin: translates K8s objects into existing ChangeEvent model, no new schemas
- Relation-aware: carries pod→node, service→pod, pod→deployment edges
  in raw_payload["relations"] for the causal graph engine

Integration points:
- Streaming pipeline: ChangeEvent via publish_fn
- Telemetry pipeline: TelemetryEnvelope via telemetry_fn (optional)
- Inventory: K8sInventory via inventory
- Cross-layer mapper: CrossLayerMapper via mapper

V1 scope (per proposal Section 16):
- Pod watcher: READY, NOT_READY, RESTARTED, CRASH_LOOP, EVICTED
- Node watcher: READY, NOT_READY, PRESSURE, NETWORK_UNAVAILABLE
- Service+EndpointSlice watcher: ENDPOINTS_CHANGED, ENDPOINTS_EMPTY
- K8s Warning events → K8S_WARNING_EVENT
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from threading import Thread, Lock
from typing import Any, Callable
from uuid import uuid4

from netobserv.streaming.events import (
    ChangeEvent, ChangeKind, ChangeSeverity, EventSource,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _new_id() -> str:
    return uuid4().hex[:16]


# ============================================================================
# Configuration
# ============================================================================

@dataclass
class KubernetesAdapterConfig:
    """Configuration for the Kubernetes adapter."""
    cluster_name: str = "default"

    # Namespace filtering
    namespaces_allowlist: set[str] | None = None
    namespaces_denylist: set[str] = field(default_factory=lambda: {"kube-system"})

    # Resource watches
    watch_pods: bool = True
    watch_nodes: bool = True
    watch_services: bool = True
    watch_endpoint_slices: bool = True
    watch_events: bool = True

    # Label filtering
    include_labels: list[str] = field(default_factory=lambda: [
        "app", "component", "k8s-app", "app.kubernetes.io/name",
    ])

    # Rate limiting and dedup
    dedup_window_seconds: float = 30.0
    crash_loop_restart_threshold: int = 3

    # State transition only — suppress no-op updates
    emit_state_transitions_only: bool = True


# ============================================================================
# K8s object snapshots (lightweight, no raw K8s dependency)
# ============================================================================

@dataclass
class PodSnapshot:
    """Minimal pod state for change detection."""
    name: str
    namespace: str
    uid: str = ""
    node_name: str = ""
    pod_ip: str = ""
    phase: str = ""
    ready: bool = False
    restart_count: int = 0
    owner_kind: str = ""
    owner_name: str = ""
    labels: dict[str, str] = field(default_factory=dict)
    containers: list[str] = field(default_factory=list)
    conditions: list[str] = field(default_factory=list)


@dataclass
class NodeSnapshot:
    name: str
    uid: str = ""
    internal_ip: str = ""
    external_ip: str = ""
    ready: bool = True
    conditions: list[str] = field(default_factory=list)
    labels: dict[str, str] = field(default_factory=dict)


@dataclass
class ServiceSnapshot:
    name: str
    namespace: str
    uid: str = ""
    cluster_ip: str = ""
    service_type: str = "ClusterIP"
    selector: dict[str, str] = field(default_factory=dict)
    endpoint_count: int = 0


@dataclass
class EndpointSliceSnapshot:
    name: str
    namespace: str
    service_name: str = ""
    endpoint_ips: list[str] = field(default_factory=list)
    endpoint_count: int = 0


# ============================================================================
# Relation model — edges for the causal graph
# ============================================================================

@dataclass
class K8sRelation:
    """A typed relation between K8s entities."""
    relation_type: str    # RUNS_ON, HAS_ENDPOINT, OWNED_BY, SELECTED_BY, HOSTS
    source_kind: str
    source_name: str
    source_namespace: str
    target_kind: str
    target_name: str
    target_namespace: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            "type": self.relation_type,
            "source_kind": self.source_kind,
            "source_name": self.source_name,
            "source_namespace": self.source_namespace,
            "target_kind": self.target_kind,
            "target_name": self.target_name,
            "target_namespace": self.target_namespace,
        }


# ============================================================================
# Dedup tracker
# ============================================================================

class DedupTracker:
    """Suppress duplicate events within a time window."""

    def __init__(self, window_seconds: float = 30.0) -> None:
        self._window = timedelta(seconds=window_seconds)
        self._seen: dict[str, datetime] = {}
        self._lock = Lock()
        self.suppressed_count = 0

    def should_emit(self, dedup_key: str, now: datetime | None = None) -> bool:
        now = now or _utc_now()
        with self._lock:
            last = self._seen.get(dedup_key)
            if last and (now - last) < self._window:
                self.suppressed_count += 1
                return False
            self._seen[dedup_key] = now
            return True

    def cleanup(self, now: datetime | None = None) -> int:
        now = now or _utc_now()
        cutoff = now - self._window * 2
        with self._lock:
            stale = [k for k, v in self._seen.items() if v < cutoff]
            for k in stale:
                del self._seen[k]
            return len(stale)


# ============================================================================
# Event builder — translates K8s state changes into ChangeEvents
# ============================================================================

class K8sEventBuilder:
    """Builds ChangeEvent objects from K8s state transitions."""

    def __init__(self, cluster_name: str) -> None:
        self._cluster = cluster_name

    def pod_not_ready(self, pod: PodSnapshot) -> ChangeEvent:
        return self._build(
            kind=ChangeKind.K8S_POD_NOT_READY,
            severity=ChangeSeverity.HIGH,
            device_id=pod.node_name or self._cluster,
            field_path="pod.ready",
            previous_value=True,
            current_value=False,
            summary=f"Pod {pod.namespace}/{pod.name} became NotReady",
            payload=self._pod_payload(pod),
            relations=self._pod_relations(pod),
            dedup_key=f"k8s:pod_not_ready:{self._cluster}:{pod.namespace}:{pod.name}",
        )

    def pod_ready(self, pod: PodSnapshot) -> ChangeEvent:
        return self._build(
            kind=ChangeKind.K8S_POD_READY,
            severity=ChangeSeverity.INFO,
            device_id=pod.node_name or self._cluster,
            field_path="pod.ready",
            previous_value=False,
            current_value=True,
            summary=f"Pod {pod.namespace}/{pod.name} became Ready",
            payload=self._pod_payload(pod),
            relations=self._pod_relations(pod),
            dedup_key=f"k8s:pod_ready:{self._cluster}:{pod.namespace}:{pod.name}",
        )

    def pod_restarted(self, pod: PodSnapshot, prev_restarts: int) -> ChangeEvent:
        return self._build(
            kind=ChangeKind.K8S_POD_RESTARTED,
            severity=ChangeSeverity.MEDIUM,
            device_id=pod.node_name or self._cluster,
            field_path="pod.restart_count",
            previous_value=prev_restarts,
            current_value=pod.restart_count,
            summary=f"Pod {pod.namespace}/{pod.name} restarted (count: {pod.restart_count})",
            payload=self._pod_payload(pod),
            relations=self._pod_relations(pod),
            dedup_key=f"k8s:pod_restart:{self._cluster}:{pod.namespace}:{pod.name}:{pod.restart_count}",
        )

    def pod_crash_loop(self, pod: PodSnapshot) -> ChangeEvent:
        return self._build(
            kind=ChangeKind.K8S_POD_CRASH_LOOP,
            severity=ChangeSeverity.CRITICAL,
            device_id=pod.node_name or self._cluster,
            field_path="pod.crash_loop",
            current_value=True,
            summary=f"Pod {pod.namespace}/{pod.name} in CrashLoopBackOff ({pod.restart_count} restarts)",
            payload=self._pod_payload(pod),
            relations=self._pod_relations(pod),
            dedup_key=f"k8s:crash_loop:{self._cluster}:{pod.namespace}:{pod.name}",
        )

    def pod_evicted(self, pod: PodSnapshot) -> ChangeEvent:
        return self._build(
            kind=ChangeKind.K8S_POD_EVICTED,
            severity=ChangeSeverity.HIGH,
            device_id=pod.node_name or self._cluster,
            field_path="pod.phase",
            current_value="Evicted",
            summary=f"Pod {pod.namespace}/{pod.name} evicted",
            payload=self._pod_payload(pod),
            relations=self._pod_relations(pod),
            dedup_key=f"k8s:pod_evicted:{self._cluster}:{pod.namespace}:{pod.name}",
        )

    def node_not_ready(self, node: NodeSnapshot) -> ChangeEvent:
        return self._build(
            kind=ChangeKind.K8S_NODE_NOT_READY,
            severity=ChangeSeverity.CRITICAL,
            device_id=node.name,
            field_path="node.condition.Ready",
            previous_value=True,
            current_value=False,
            summary=f"Node {node.name} became NotReady",
            payload=self._node_payload(node),
            relations=[],
            dedup_key=f"k8s:node_not_ready:{self._cluster}:{node.name}",
        )

    def node_ready(self, node: NodeSnapshot) -> ChangeEvent:
        return self._build(
            kind=ChangeKind.K8S_NODE_READY,
            severity=ChangeSeverity.INFO,
            device_id=node.name,
            field_path="node.condition.Ready",
            previous_value=False,
            current_value=True,
            summary=f"Node {node.name} became Ready",
            payload=self._node_payload(node),
            relations=[],
            dedup_key=f"k8s:node_ready:{self._cluster}:{node.name}",
        )

    def node_pressure(self, node: NodeSnapshot, pressure_type: str) -> ChangeEvent:
        return self._build(
            kind=ChangeKind.K8S_NODE_PRESSURE,
            severity=ChangeSeverity.HIGH,
            device_id=node.name,
            field_path=f"node.condition.{pressure_type}",
            current_value=True,
            summary=f"Node {node.name} has {pressure_type}",
            payload={**self._node_payload(node), "pressure_type": pressure_type},
            relations=[],
            dedup_key=f"k8s:node_pressure:{self._cluster}:{node.name}:{pressure_type}",
        )

    def node_network_unavailable(self, node: NodeSnapshot) -> ChangeEvent:
        return self._build(
            kind=ChangeKind.K8S_NODE_NETWORK_UNAVAILABLE,
            severity=ChangeSeverity.CRITICAL,
            device_id=node.name,
            field_path="node.condition.NetworkUnavailable",
            current_value=True,
            summary=f"Node {node.name} network unavailable",
            payload=self._node_payload(node),
            relations=[],
            dedup_key=f"k8s:node_net_unavail:{self._cluster}:{node.name}",
        )

    def service_endpoints_changed(self, svc: ServiceSnapshot,
                                   prev_count: int) -> ChangeEvent:
        return self._build(
            kind=ChangeKind.K8S_SERVICE_ENDPOINTS_CHANGED,
            severity=ChangeSeverity.MEDIUM,
            device_id=self._cluster,
            field_path="service.endpoint_count",
            previous_value=prev_count,
            current_value=svc.endpoint_count,
            summary=f"Service {svc.namespace}/{svc.name} endpoints {prev_count}→{svc.endpoint_count}",
            payload=self._service_payload(svc),
            relations=[],
            dedup_key=f"k8s:svc_ep_change:{self._cluster}:{svc.namespace}:{svc.name}",
        )

    def service_endpoints_empty(self, svc: ServiceSnapshot) -> ChangeEvent:
        return self._build(
            kind=ChangeKind.K8S_SERVICE_ENDPOINTS_EMPTY,
            severity=ChangeSeverity.CRITICAL,
            device_id=self._cluster,
            field_path="service.endpoint_count",
            previous_value=None,
            current_value=0,
            summary=f"Service {svc.namespace}/{svc.name} has ZERO endpoints",
            payload=self._service_payload(svc),
            relations=[],
            dedup_key=f"k8s:svc_ep_empty:{self._cluster}:{svc.namespace}:{svc.name}",
        )

    def k8s_warning_event(self, namespace: str, name: str, reason: str,
                           message: str, involved_kind: str,
                           involved_name: str, node: str = "") -> ChangeEvent:
        return self._build(
            kind=ChangeKind.K8S_WARNING_EVENT,
            severity=ChangeSeverity.MEDIUM,
            device_id=node or self._cluster,
            field_path=f"event.{reason}",
            current_value=message[:200],
            summary=f"K8s Warning: {reason} on {involved_kind}/{involved_name}",
            payload={
                "cluster": self._cluster,
                "namespace": namespace,
                "reason": reason,
                "message": message[:500],
                "involved_kind": involved_kind,
                "involved_name": involved_name,
            },
            relations=[],
            dedup_key=f"k8s:event:{self._cluster}:{namespace}:{involved_kind}:{involved_name}:{reason}",
        )

    # --- Internal ---

    def _build(self, kind: ChangeKind, severity: ChangeSeverity,
               device_id: str, summary: str, payload: dict,
               relations: list[dict], dedup_key: str,
               field_path: str = "", previous_value: Any = None,
               current_value: Any = None) -> ChangeEvent:
        payload["cluster"] = self._cluster
        payload["relations"] = relations
        return ChangeEvent(
            kind=kind,
            severity=severity,
            source=EventSource.K8S_WATCH,
            device_id=device_id,
            device_hostname=device_id,
            field_path=field_path,
            previous_value=previous_value,
            current_value=current_value,
            raw_payload=payload,
            dedup_key=dedup_key,
            human_summary=summary,
        )

    def _pod_payload(self, pod: PodSnapshot) -> dict:
        return {
            "namespace": pod.namespace,
            "pod_name": pod.name,
            "pod_uid": pod.uid,
            "node_name": pod.node_name,
            "pod_ip": pod.pod_ip,
            "phase": pod.phase,
            "ready": pod.ready,
            "restart_count": pod.restart_count,
            "owner_kind": pod.owner_kind,
            "owner_name": pod.owner_name,
            "labels": pod.labels,
        }

    def _pod_relations(self, pod: PodSnapshot) -> list[dict]:
        rels = []
        if pod.node_name:
            rels.append(K8sRelation(
                relation_type="RUNS_ON",
                source_kind="Pod", source_name=pod.name,
                source_namespace=pod.namespace,
                target_kind="Node", target_name=pod.node_name,
            ).to_dict())
        if pod.owner_kind and pod.owner_name:
            rels.append(K8sRelation(
                relation_type="OWNED_BY",
                source_kind="Pod", source_name=pod.name,
                source_namespace=pod.namespace,
                target_kind=pod.owner_kind, target_name=pod.owner_name,
                target_namespace=pod.namespace,
            ).to_dict())
        return rels

    def _node_payload(self, node: NodeSnapshot) -> dict:
        return {
            "node_name": node.name,
            "internal_ip": node.internal_ip,
            "external_ip": node.external_ip,
            "ready": node.ready,
            "conditions": node.conditions,
            "labels": node.labels,
        }

    def _service_payload(self, svc: ServiceSnapshot) -> dict:
        return {
            "namespace": svc.namespace,
            "service_name": svc.name,
            "cluster_ip": svc.cluster_ip,
            "service_type": svc.service_type,
            "selector": svc.selector,
            "endpoint_count": svc.endpoint_count,
        }


# ============================================================================
# State tracker — detects meaningful transitions
# ============================================================================

class K8sStateTracker:
    """Tracks K8s object state and detects transitions."""

    def __init__(self, config: KubernetesAdapterConfig) -> None:
        self._config = config
        self._pods: dict[tuple[str, str], PodSnapshot] = {}       # (ns, name)
        self._nodes: dict[str, NodeSnapshot] = {}
        self._services: dict[tuple[str, str], ServiceSnapshot] = {}
        self._endpoint_counts: dict[tuple[str, str], int] = {}    # (ns, svc) -> count
        self._lock = Lock()

    def process_pod(self, pod: PodSnapshot, builder: K8sEventBuilder) -> list[ChangeEvent]:
        """Detect pod state transitions and return events."""
        key = (pod.namespace, pod.name)
        events: list[ChangeEvent] = []

        with self._lock:
            old = self._pods.get(key)

            if old is None:
                # New pod — only emit if not ready (ready pods aren't interesting)
                if not pod.ready:
                    events.append(builder.pod_not_ready(pod))
            else:
                # Ready transition
                if old.ready and not pod.ready:
                    events.append(builder.pod_not_ready(pod))
                elif not old.ready and pod.ready:
                    events.append(builder.pod_ready(pod))

                # Restart count increased
                if pod.restart_count > old.restart_count:
                    events.append(builder.pod_restarted(pod, old.restart_count))
                    if pod.restart_count >= self._config.crash_loop_restart_threshold:
                        events.append(builder.pod_crash_loop(pod))

            # Eviction
            if pod.phase == "Failed" and "Evicted" in pod.conditions:
                events.append(builder.pod_evicted(pod))

            self._pods[key] = pod

        return events

    def process_pod_deleted(self, namespace: str, name: str) -> None:
        with self._lock:
            self._pods.pop((namespace, name), None)

    def process_node(self, node: NodeSnapshot, builder: K8sEventBuilder) -> list[ChangeEvent]:
        events: list[ChangeEvent] = []

        with self._lock:
            old = self._nodes.get(node.name)

            if old is None:
                if not node.ready:
                    events.append(builder.node_not_ready(node))
            else:
                if old.ready and not node.ready:
                    events.append(builder.node_not_ready(node))
                elif not old.ready and node.ready:
                    events.append(builder.node_ready(node))

            # Pressure conditions
            for cond in node.conditions:
                if "Pressure" in cond and (old is None or cond not in old.conditions):
                    events.append(builder.node_pressure(node, cond))
                if cond == "NetworkUnavailable" and (old is None or cond not in old.conditions):
                    events.append(builder.node_network_unavailable(node))

            self._nodes[node.name] = node

        return events

    def process_service_endpoints(self, namespace: str, service_name: str,
                                   endpoint_count: int,
                                   svc_snapshot: ServiceSnapshot | None,
                                   builder: K8sEventBuilder) -> list[ChangeEvent]:
        events: list[ChangeEvent] = []
        key = (namespace, service_name)

        with self._lock:
            prev_count = self._endpoint_counts.get(key)

            if prev_count is not None and prev_count != endpoint_count:
                svc = svc_snapshot or ServiceSnapshot(
                    name=service_name, namespace=namespace,
                    endpoint_count=endpoint_count,
                )
                svc.endpoint_count = endpoint_count

                if endpoint_count == 0:
                    events.append(builder.service_endpoints_empty(svc))
                else:
                    events.append(builder.service_endpoints_changed(svc, prev_count))

            self._endpoint_counts[key] = endpoint_count

        return events

    @property
    def tracked_pods(self) -> int:
        return len(self._pods)

    @property
    def tracked_nodes(self) -> int:
        return len(self._nodes)


# ============================================================================
# Main adapter
# ============================================================================

class KubernetesAdapter:
    """Watches Kubernetes state changes and emits ChangeEvents.

    Usage:
        adapter = KubernetesAdapter(
            config=KubernetesAdapterConfig(cluster_name="prod-east-1"),
            publish_fn=event_bus.publish_sync,
        )
        # Feed K8s events (from informer, test harness, or replay)
        adapter.process_pod_event("ADDED", pod_dict)
        adapter.process_node_event("MODIFIED", node_dict)
    """

    def __init__(
        self,
        config: KubernetesAdapterConfig | None = None,
        publish_fn: Callable[[ChangeEvent], None] | None = None,
        mapper: Any = None,          # CrossLayerMapper
        inventory: Any = None,        # K8sInventory
    ) -> None:
        self._config = config or KubernetesAdapterConfig()
        self._publish_fn = publish_fn
        self._mapper = mapper
        self._inventory = inventory

        self._builder = K8sEventBuilder(self._config.cluster_name)
        self._tracker = K8sStateTracker(self._config)
        self._dedup = DedupTracker(self._config.dedup_window_seconds)

        # Stats
        self._events_emitted = 0
        self._events_processed = 0
        self._events_suppressed = 0

    # --- Pod events ---

    def process_pod_event(self, event_type: str, pod_dict: dict) -> list[ChangeEvent]:
        """Process a pod add/update/delete event from K8s informer."""
        self._events_processed += 1

        if event_type == "DELETED":
            ns = pod_dict.get("namespace", "")
            name = pod_dict.get("name", "")
            self._tracker.process_pod_deleted(ns, name)
            return []

        pod = self._parse_pod(pod_dict)
        if not self._namespace_allowed(pod.namespace):
            return []

        events = self._tracker.process_pod(pod, self._builder)
        emitted = self._emit_events(events)

        # Update inventory if available
        if self._inventory and hasattr(self._inventory, 'upsert_pod'):
            self._update_inventory_pod(pod)

        return emitted

    # --- Node events ---

    def process_node_event(self, event_type: str, node_dict: dict) -> list[ChangeEvent]:
        self._events_processed += 1

        node = self._parse_node(node_dict)
        events = self._tracker.process_node(node, self._builder)
        emitted = self._emit_events(events)

        # Register node→device mapping in cross-layer mapper
        if self._mapper and node.internal_ip:
            try:
                self._mapper.register_node_network(node.name, device_id=node.name)
            except Exception:
                pass

        return emitted

    # --- Service events ---

    def process_service_event(self, event_type: str, svc_dict: dict) -> list[ChangeEvent]:
        self._events_processed += 1
        svc = self._parse_service(svc_dict)
        if not self._namespace_allowed(svc.namespace):
            return []
        # Service state tracked via endpoint counts, not directly
        return []

    # --- EndpointSlice events ---

    def process_endpoint_slice_event(self, event_type: str,
                                      eps_dict: dict) -> list[ChangeEvent]:
        self._events_processed += 1

        namespace = eps_dict.get("namespace", "")
        service_name = eps_dict.get("service_name", "")
        if not self._namespace_allowed(namespace) or not service_name:
            return []

        endpoints = eps_dict.get("endpoints", [])
        endpoint_count = len(endpoints)

        events = self._tracker.process_service_endpoints(
            namespace, service_name, endpoint_count, None, self._builder,
        )
        return self._emit_events(events)

    # --- K8s Warning events ---

    def process_k8s_event(self, event_dict: dict) -> list[ChangeEvent]:
        """Process a K8s Event object (Warning type only)."""
        self._events_processed += 1

        event_type = event_dict.get("type", "Normal")
        if event_type != "Warning":
            return []

        reason = event_dict.get("reason", "Unknown")
        message = event_dict.get("message", "")
        namespace = event_dict.get("namespace", "")
        involved = event_dict.get("involved_object", {})
        involved_kind = involved.get("kind", "")
        involved_name = involved.get("name", "")
        node = event_dict.get("node", "")

        evt = self._builder.k8s_warning_event(
            namespace, involved_name, reason, message,
            involved_kind, involved_name, node,
        )
        return self._emit_events([evt])

    # --- Batch processing ---

    def process_batch(self, events: list[dict]) -> int:
        """Process a batch of raw K8s events. Returns count emitted."""
        total = 0
        for raw in events:
            kind = raw.get("kind", "")
            event_type = raw.get("event_type", "MODIFIED")
            obj = raw.get("object", raw)

            if kind == "Pod":
                total += len(self.process_pod_event(event_type, obj))
            elif kind == "Node":
                total += len(self.process_node_event(event_type, obj))
            elif kind == "Service":
                total += len(self.process_service_event(event_type, obj))
            elif kind == "EndpointSlice":
                total += len(self.process_endpoint_slice_event(event_type, obj))
            elif kind == "Event":
                total += len(self.process_k8s_event(obj))
        return total

    # --- Stats ---

    @property
    def stats(self) -> dict[str, Any]:
        return {
            "events_processed": self._events_processed,
            "events_emitted": self._events_emitted,
            "events_suppressed": self._events_suppressed,
            "dedup_suppressed": self._dedup.suppressed_count,
            "tracked_pods": self._tracker.tracked_pods,
            "tracked_nodes": self._tracker.tracked_nodes,
            "cluster": self._config.cluster_name,
        }

    # --- Internal ---

    def _emit_events(self, events: list[ChangeEvent]) -> list[ChangeEvent]:
        emitted: list[ChangeEvent] = []
        for evt in events:
            if evt.dedup_key and not self._dedup.should_emit(evt.dedup_key):
                self._events_suppressed += 1
                continue
            if self._publish_fn:
                try:
                    self._publish_fn(evt)
                except Exception:
                    pass
            self._events_emitted += 1
            emitted.append(evt)
        return emitted

    def _namespace_allowed(self, namespace: str) -> bool:
        if self._config.namespaces_allowlist is not None:
            return namespace in self._config.namespaces_allowlist
        return namespace not in self._config.namespaces_denylist

    def _parse_pod(self, d: dict) -> PodSnapshot:
        metadata = d.get("metadata", d)
        status = d.get("status", d)
        spec = d.get("spec", d)
        labels = metadata.get("labels", {})
        # Filter to configured labels
        filtered_labels = {
            k: v for k, v in labels.items()
            if k in self._config.include_labels
        }

        # Owner reference
        owner_refs = metadata.get("ownerReferences",
                                   metadata.get("owner_references", []))
        owner_kind = ""
        owner_name = ""
        if owner_refs:
            primary = next(
                (r for r in owner_refs if r.get("controller")), owner_refs[0]
            )
            owner_kind = primary.get("kind", "")
            owner_name = primary.get("name", "")

        # Ready condition
        conditions = status.get("conditions", [])
        ready = False
        condition_types = []
        for c in conditions:
            if isinstance(c, dict):
                if c.get("type") == "Ready" and c.get("status") == "True":
                    ready = True
                condition_types.append(c.get("type", ""))
            elif isinstance(c, str):
                condition_types.append(c)

        # Restart count
        container_statuses = status.get("containerStatuses",
                                        status.get("container_statuses", []))
        restart_count = sum(
            cs.get("restartCount", cs.get("restart_count", 0))
            for cs in container_statuses
        ) if container_statuses else status.get("restart_count", 0)

        return PodSnapshot(
            name=metadata.get("name", d.get("name", "")),
            namespace=metadata.get("namespace", d.get("namespace", "")),
            uid=metadata.get("uid", d.get("uid", "")),
            node_name=spec.get("nodeName", spec.get("node_name",
                       d.get("node_name", ""))),
            pod_ip=status.get("podIP", status.get("pod_ip",
                    d.get("pod_ip", ""))),
            phase=status.get("phase", d.get("phase", "")),
            ready=ready or d.get("ready", False),
            restart_count=restart_count,
            owner_kind=owner_kind or d.get("owner_kind", ""),
            owner_name=owner_name or d.get("owner_name", ""),
            labels=filtered_labels or d.get("labels", {}),
            conditions=condition_types,
        )

    def _parse_node(self, d: dict) -> NodeSnapshot:
        metadata = d.get("metadata", d)
        status = d.get("status", d)
        labels = metadata.get("labels", {})
        filtered = {k: v for k, v in labels.items()
                    if k in self._config.include_labels}

        addresses = status.get("addresses", [])
        internal_ip = ""
        external_ip = ""
        for addr in addresses:
            if isinstance(addr, dict):
                if addr.get("type") == "InternalIP":
                    internal_ip = addr.get("address", "")
                elif addr.get("type") == "ExternalIP":
                    external_ip = addr.get("address", "")

        conditions = status.get("conditions", [])
        ready = False
        condition_types = []
        for c in conditions:
            if isinstance(c, dict):
                ctype = c.get("type", "")
                cstatus = c.get("status", "")
                condition_types.append(ctype)
                if ctype == "Ready" and cstatus == "True":
                    ready = True
            elif isinstance(c, str):
                condition_types.append(c)

        return NodeSnapshot(
            name=metadata.get("name", d.get("name", "")),
            uid=metadata.get("uid", d.get("uid", "")),
            internal_ip=internal_ip or d.get("internal_ip", ""),
            external_ip=external_ip or d.get("external_ip", ""),
            ready=ready if conditions else d.get("ready", True),
            conditions=condition_types or d.get("conditions", []),
            labels=filtered or d.get("labels", {}),
        )

    def _parse_service(self, d: dict) -> ServiceSnapshot:
        metadata = d.get("metadata", d)
        spec = d.get("spec", d)
        return ServiceSnapshot(
            name=metadata.get("name", d.get("name", "")),
            namespace=metadata.get("namespace", d.get("namespace", "")),
            uid=metadata.get("uid", d.get("uid", "")),
            cluster_ip=spec.get("clusterIP", spec.get("cluster_ip",
                        d.get("cluster_ip", ""))),
            service_type=spec.get("type", d.get("service_type", "ClusterIP")),
            selector=spec.get("selector", d.get("selector", {})),
        )

    def _update_inventory_pod(self, pod: PodSnapshot) -> None:
        try:
            from netobserv.telemetry.k8s_inventory import PodRecord
            self._inventory.upsert_pod(PodRecord(
                name=pod.name, namespace=pod.namespace,
                node=pod.node_name, service=pod.labels.get("app", ""),
                workload_kind=pod.owner_kind.lower() if pod.owner_kind else "",
                workload_name=pod.owner_name,
            ))
        except Exception:
            pass
