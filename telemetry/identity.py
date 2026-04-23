"""Cross-signal identity models.

TargetRef identifies WHAT is being observed (service, pod, node, etc.).
RequestRef identifies WHICH request/trace flow a signal belongs to.

These two objects are the correlation spine. Any two signals sharing
the same TargetRef fields within a time window are candidate-correlated.
Any two signals sharing the same trace_id are hard-linked.

Design decisions:
- All fields are Optional because not every source provides every field.
  A gNMI interface event will have node/device but no pod/container.
  An OTLP span will have service/pod but may not have node.
  The correlation engine matches on whatever fields are present.
- Fields use str (not enums) for values because the actual values
  come from diverse sources (k8s labels, OTel attributes, SNMP MIBs).
- The fingerprint methods produce deterministic keys for indexing
  and deduplication.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, fields
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class TargetRef:
    """Identifies the infrastructure target a signal is about.

    This is the "who/where" of every telemetry signal. Consistent target
    metadata across all signals is the first requirement for cross-signal
    correlation.

    Fields are ordered from broadest scope (tenant) to narrowest (instance).
    """
    # Organizational scope
    tenant_id: str | None = None
    environment: str | None = None        # prod, staging, dev

    # Cluster / region scope
    cluster: str | None = None
    region: str | None = None
    zone: str | None = None               # availability zone

    # Kubernetes scope
    namespace: str | None = None
    node: str | None = None

    # Workload scope
    workload_kind: str | None = None      # deployment, statefulset, daemonset
    workload_name: str | None = None
    pod: str | None = None
    container: str | None = None

    # Service scope
    service: str | None = None
    service_version: str | None = None

    # Instance scope
    instance_id: str | None = None        # unique instance identifier

    # Network scope (bridges to existing NetObserv network identity)
    device_id: str | None = None          # network device hostname/id
    interface_name: str | None = None     # network interface
    neighbor_device_id: str | None = None

    # Additional attributes (for source-specific metadata)
    attrs: dict[str, str] = field(default_factory=dict)

    def fingerprint(self) -> str:
        """Deterministic identity key from all non-None fields.

        Two TargetRefs with the same fingerprint identify the same target.
        """
        parts = []
        for f in fields(self):
            if f.name == "attrs":
                continue
            val = getattr(self, f.name)
            if val is not None:
                parts.append(f"{f.name}={val}")
        raw = "|".join(parts)
        return hashlib.sha256(raw.encode()).hexdigest()[:24]

    def matches(self, other: TargetRef, *, require_fields: list[str] | None = None) -> bool:
        """Check if two TargetRefs refer to the same target.

        Matching is field-by-field on non-None values. If a field is None
        on either side, it's treated as "unknown" and skipped. If
        require_fields is provided, those fields must be present and
        equal on both sides.
        """
        if require_fields:
            for fname in require_fields:
                a = getattr(self, fname, None)
                b = getattr(other, fname, None)
                if a is None or b is None or a != b:
                    return False

        for f in fields(self):
            if f.name == "attrs":
                continue
            a = getattr(self, f.name)
            b = getattr(other, f.name)
            if a is not None and b is not None and a != b:
                return False
        return True

    def overlap_score(self, other: TargetRef) -> float:
        """Compute how much two TargetRefs overlap (0.0 to 1.0).

        Higher score = more shared identity fields = stronger correlation.
        """
        matched = 0
        compared = 0
        for f in fields(self):
            if f.name == "attrs":
                continue
            a = getattr(self, f.name)
            b = getattr(other, f.name)
            if a is not None and b is not None:
                compared += 1
                if a == b:
                    matched += 1
        return matched / max(compared, 1)

    def merge(self, other: TargetRef) -> TargetRef:
        """Produce a new TargetRef filling in None fields from other."""
        kwargs: dict[str, Any] = {}
        for f in fields(self):
            if f.name == "attrs":
                merged_attrs = {**other.attrs, **self.attrs}
                kwargs["attrs"] = merged_attrs
                continue
            self_val = getattr(self, f.name)
            other_val = getattr(other, f.name)
            kwargs[f.name] = self_val if self_val is not None else other_val
        return TargetRef(**kwargs)

    @property
    def is_network_target(self) -> bool:
        """True if this target has network-layer identity (device/interface)."""
        return self.device_id is not None

    @property
    def is_app_target(self) -> bool:
        """True if this target has app-layer identity (service/pod)."""
        return self.service is not None or self.pod is not None

    @property
    def is_cross_layer(self) -> bool:
        """True if this target has both network and app identity."""
        return self.is_network_target and self.is_app_target


@dataclass(slots=True)
class RequestRef:
    """Identifies a specific request or trace flow.

    This is the "which request" of a telemetry signal. Two signals
    sharing the same trace_id are hard-linked regardless of target.
    """
    trace_id: str | None = None
    span_id: str | None = None
    parent_span_id: str | None = None
    request_id: str | None = None         # application-level request ID
    operation_id: str | None = None       # RPC method / HTTP route
    session_id: str | None = None         # user session

    # Exemplar bridge: stored on metrics to point at a representative trace
    exemplar_trace_id: str | None = None
    exemplar_span_id: str | None = None
    exemplar_timestamp: datetime | None = None

    def has_trace_identity(self) -> bool:
        """True if this ref carries a distributed trace ID."""
        return self.trace_id is not None

    def has_exemplar(self) -> bool:
        """True if this ref carries an exemplar link."""
        return self.exemplar_trace_id is not None

    def trace_key(self) -> str | None:
        """Primary correlation key for trace-based linking."""
        return self.trace_id

    def matches_trace(self, other: RequestRef) -> bool:
        """True if both refs share the same trace."""
        if self.trace_id and other.trace_id:
            return self.trace_id == other.trace_id
        return False

    def fingerprint(self) -> str:
        """Deterministic identity key."""
        parts = []
        for f in fields(self):
            if f.name == "exemplar_timestamp":
                val = getattr(self, f.name)
                if val is not None:
                    parts.append(f"{f.name}={val.isoformat()}")
                continue
            val = getattr(self, f.name)
            if val is not None:
                parts.append(f"{f.name}={val}")
        raw = "|".join(parts)
        return hashlib.sha256(raw.encode()).hexdigest()[:24]


@dataclass(slots=True)
class WorkloadRef:
    """Shorthand for Kubernetes workload identity.

    This is a convenience for constructing TargetRef from k8s metadata.
    """
    namespace: str
    workload_kind: str        # deployment, statefulset, daemonset, job
    workload_name: str
    pod: str | None = None
    container: str | None = None
    node: str | None = None

    def to_target_ref(self, **extras: Any) -> TargetRef:
        """Convert to a full TargetRef."""
        return TargetRef(
            namespace=self.namespace,
            workload_kind=self.workload_kind,
            workload_name=self.workload_name,
            pod=self.pod,
            container=self.container,
            node=self.node,
            **extras,
        )
