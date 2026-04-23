"""Resource attribute normalizer — canonical mapping from telemetry attributes to TargetRef.

This is the single source of truth for how raw telemetry resource
attributes (OTLP, Prometheus, K8s, etc.) map to TargetRef fields.

Every ingestion adapter MUST use this normalizer instead of maintaining
its own mapping copy. If you need a new attribute mapping, add it here.

Responsibilities:
1. Canonical attribute key → TargetRef field mapping
2. Source-specific attribute parsing (OTLP list format, Prometheus flat labels, K8s metadata)
3. Collision precedence rules (when multiple attributes set the same field)
4. Workload kind detection from attribute keys
5. Attribute value normalization (lowercase, strip, etc.)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from netobserv.telemetry.identity import TargetRef


# ============================================================================
# Canonical mapping: attribute key → TargetRef field name
# ============================================================================

# Ordered by precedence within each target field — first match wins.
# This means k8s-specific attributes take precedence over generic ones
# when both are present (e.g., k8s.namespace.name beats service.namespace).

CANONICAL_ATTRIBUTE_MAP: dict[str, str] = {
    # Service identity
    "service.name": "service",
    "service.version": "service_version",

    # Namespace (k8s-specific takes precedence over OTel generic)
    "k8s.namespace.name": "namespace",
    "service.namespace": "namespace",

    # Instance
    "service.instance.id": "instance_id",

    # Kubernetes workload
    "k8s.pod.name": "pod",
    "k8s.container.name": "container",
    "k8s.node.name": "node",
    "k8s.cluster.name": "cluster",
    "k8s.deployment.name": "workload_name",
    "k8s.statefulset.name": "workload_name",
    "k8s.daemonset.name": "workload_name",
    "k8s.replicaset.name": "workload_name",
    "k8s.job.name": "workload_name",
    "k8s.cronjob.name": "workload_name",

    # Cloud / region
    "cloud.region": "region",
    "cloud.availability_zone": "zone",
    "cloud.provider": "attrs",            # goes to attrs, not a TargetRef field

    # Environment
    "deployment.environment": "environment",
    "deployment.environment.name": "environment",

    # Host (fallback for node)
    "host.name": "node",
    "host.id": "instance_id",

    # Tenant
    "tenant.id": "tenant_id",
}

# Attribute keys that determine workload_kind
WORKLOAD_KIND_KEYS: dict[str, str] = {
    "k8s.deployment.name": "deployment",
    "k8s.statefulset.name": "statefulset",
    "k8s.daemonset.name": "daemonset",
    "k8s.replicaset.name": "replicaset",
    "k8s.job.name": "job",
    "k8s.cronjob.name": "cronjob",
}

# Field precedence: when multiple attribute keys map to the same TargetRef
# field, the first one found in this order wins.
FIELD_PRECEDENCE: dict[str, list[str]] = {
    "namespace": ["k8s.namespace.name", "service.namespace"],
    "node": ["k8s.node.name", "host.name"],
    "instance_id": ["service.instance.id", "host.id"],
    "workload_name": [
        "k8s.deployment.name", "k8s.statefulset.name",
        "k8s.daemonset.name", "k8s.replicaset.name",
        "k8s.job.name", "k8s.cronjob.name",
    ],
    "environment": ["deployment.environment", "deployment.environment.name"],
}


# ============================================================================
# Prometheus label → TargetRef mapping
# ============================================================================

PROMETHEUS_LABEL_MAP: dict[str, str] = {
    "job": "service",
    "instance": "instance_id",
    "namespace": "namespace",
    "pod": "pod",
    "container": "container",
    "node": "node",
    "cluster": "cluster",
    "env": "environment",
    "environment": "environment",
    "region": "region",
    "zone": "zone",
    "service": "service",
}


# ============================================================================
# K8s API metadata → TargetRef mapping
# ============================================================================

K8S_METADATA_MAP: dict[str, str] = {
    "metadata.namespace": "namespace",
    "metadata.name": "pod",
    "spec.nodeName": "node",
    "spec.serviceAccountName": "attrs",
    "metadata.labels.app": "service",
    "metadata.labels.app.kubernetes.io/name": "service",
    "metadata.labels.app.kubernetes.io/version": "service_version",
    "metadata.labels.app.kubernetes.io/component": "attrs",
}


# ============================================================================
# Normalizer
# ============================================================================

@dataclass(slots=True)
class NormalizationResult:
    """Result of normalizing resource attributes into a TargetRef."""
    target: TargetRef
    mapped_fields: dict[str, str]        # field_name → source attribute key
    unmapped_attrs: dict[str, str]       # attributes that didn't map to any field
    collisions: list[str]                # fields where multiple attrs competed


class ResourceNormalizer:
    """Normalizes telemetry resource attributes into TargetRef.

    Usage:
        normalizer = ResourceNormalizer()

        # From OTLP resource attributes (list of {key, value})
        result = normalizer.from_otlp_attributes(attrs_list)

        # From Prometheus labels (flat dict)
        result = normalizer.from_prometheus_labels(labels)

        # From K8s API object metadata
        result = normalizer.from_k8s_metadata(metadata_dict)

        # From any flat dict (auto-detect source)
        target = normalizer.normalize(flat_attrs)
    """

    def normalize(self, attrs: dict[str, str]) -> TargetRef:
        """Normalize a flat attribute dict into a TargetRef.

        Applies the canonical mapping with precedence rules.
        """
        result = self._apply_mapping(attrs, CANONICAL_ATTRIBUTE_MAP)
        return result.target

    def normalize_with_details(self, attrs: dict[str, str]) -> NormalizationResult:
        """Normalize with full details about what mapped where."""
        return self._apply_mapping(attrs, CANONICAL_ATTRIBUTE_MAP)

    def from_otlp_attributes(self, attrs_list: list[dict[str, Any]] | dict[str, Any]) -> NormalizationResult:
        """Normalize OTLP-style attribute list into TargetRef.

        OTLP attributes come as [{key: "k", value: {stringValue: "v"}}].
        """
        flat = self.flatten_otlp_attributes(attrs_list)
        return self._apply_mapping(flat, CANONICAL_ATTRIBUTE_MAP)

    def from_prometheus_labels(self, labels: dict[str, str]) -> NormalizationResult:
        """Normalize Prometheus labels into TargetRef."""
        return self._apply_mapping(labels, PROMETHEUS_LABEL_MAP)

    def from_k8s_metadata(self, metadata: dict[str, Any]) -> NormalizationResult:
        """Normalize K8s API object metadata into TargetRef."""
        flat = self._flatten_k8s_metadata(metadata)
        return self._apply_mapping(flat, K8S_METADATA_MAP)

    def merge_targets(self, *targets: TargetRef) -> TargetRef:
        """Merge multiple TargetRefs with left-to-right precedence."""
        if not targets:
            return TargetRef()
        result = targets[0]
        for t in targets[1:]:
            result = result.merge(t)
        return result

    # --- OTLP attribute parsing ---

    @staticmethod
    def flatten_otlp_attributes(attrs: list[dict[str, Any]] | dict[str, Any]) -> dict[str, str]:
        """Convert OTLP attribute list to flat dict.

        Handles all OTLP value types:
        - stringValue, intValue, doubleValue, boolValue
        - arrayValue, kvlistValue (serialized to string)
        """
        if isinstance(attrs, dict):
            return {str(k): str(v) for k, v in attrs.items()}

        result: dict[str, str] = {}
        for attr in attrs:
            key = str(attr.get("key", ""))
            if not key:
                continue
            value_obj = attr.get("value", {})
            if isinstance(value_obj, dict):
                for vtype in ("stringValue", "intValue", "doubleValue", "boolValue"):
                    if vtype in value_obj:
                        result[key] = str(value_obj[vtype])
                        break
                else:
                    # arrayValue or kvlistValue — serialize
                    result[key] = str(value_obj)
            else:
                result[key] = str(value_obj)
        return result

    # --- Internal ---

    def _apply_mapping(
        self, attrs: dict[str, str], mapping: dict[str, str]
    ) -> NormalizationResult:
        """Apply an attribute mapping to produce a TargetRef."""
        target_kwargs: dict[str, Any] = {}
        mapped_fields: dict[str, str] = {}
        unmapped: dict[str, str] = {}
        collisions: list[str] = []
        extra_attrs: dict[str, str] = {}
        workload_kind: str | None = None

        # Resolve precedence: for fields with multiple candidate keys,
        # use the first present key.
        resolved_keys: dict[str, str] = {}  # target_field → winning attr key
        for attr_key, target_field in mapping.items():
            if attr_key not in attrs:
                continue
            if target_field == "attrs":
                extra_attrs[attr_key] = attrs[attr_key]
                continue
            if target_field in resolved_keys:
                # Check if this key has lower precedence
                precedence = FIELD_PRECEDENCE.get(target_field, [])
                existing_key = resolved_keys[target_field]
                if precedence:
                    existing_idx = precedence.index(existing_key) if existing_key in precedence else 999
                    new_idx = precedence.index(attr_key) if attr_key in precedence else 999
                    if new_idx < existing_idx:
                        collisions.append(
                            f"{target_field}: {attr_key} overrides {existing_key}"
                        )
                        resolved_keys[target_field] = attr_key
                    else:
                        collisions.append(
                            f"{target_field}: {existing_key} kept over {attr_key}"
                        )
                continue
            resolved_keys[target_field] = attr_key

        # Apply resolved mappings
        for target_field, attr_key in resolved_keys.items():
            value = self._normalize_value(target_field, attrs[attr_key])
            target_kwargs[target_field] = value
            mapped_fields[target_field] = attr_key

            # Detect workload kind
            if attr_key in WORKLOAD_KIND_KEYS:
                workload_kind = WORKLOAD_KIND_KEYS[attr_key]

        if workload_kind and "workload_kind" not in target_kwargs:
            target_kwargs["workload_kind"] = workload_kind

        # Collect unmapped attributes
        mapped_keys = set(mapping.keys())
        for k, v in attrs.items():
            if k not in mapped_keys:
                unmapped[k] = v
                extra_attrs[k] = v

        if extra_attrs:
            target_kwargs["attrs"] = extra_attrs

        target = TargetRef(**target_kwargs)
        return NormalizationResult(
            target=target,
            mapped_fields=mapped_fields,
            unmapped_attrs=unmapped,
            collisions=collisions,
        )

    @staticmethod
    def _normalize_value(field: str, value: str) -> str:
        """Normalize attribute values for consistency."""
        value = value.strip()
        # Environment values are lowercased for consistent matching
        if field == "environment":
            return value.lower()
        return value

    @staticmethod
    def _flatten_k8s_metadata(metadata: dict[str, Any]) -> dict[str, str]:
        """Flatten nested K8s metadata dict to dot-notation keys."""
        flat: dict[str, str] = {}

        def _walk(obj: Any, prefix: str = "") -> None:
            if isinstance(obj, dict):
                for k, v in obj.items():
                    key = f"{prefix}.{k}" if prefix else k
                    if isinstance(v, (dict, list)):
                        _walk(v, key)
                    else:
                        flat[key] = str(v)
            elif isinstance(obj, list):
                for i, v in enumerate(obj):
                    _walk(v, f"{prefix}[{i}]")

        _walk(metadata)
        return flat
