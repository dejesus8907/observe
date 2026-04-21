"""Canonical data model — vendor-agnostic internal representation.

This module is the contract layer for the whole platform. If the models here
accept sloppy input, every downstream subsystem gets poisoned with normalized
looking garbage.

The goal of the enhancements in this version is simple:
- coerce common field formats into stable canonical values,
- reject obviously invalid structures early,
- remove duplicate list content while preserving intent,
- keep compatibility with the rest of the codebase.
"""

from __future__ import annotations

import ipaddress
import uuid
from datetime import datetime
from typing import Any, ClassVar, Union

from pydantic import BaseModel, Field, field_validator, model_validator

from netobserv.models.enums import (
    BGPSessionState,
    DeviceStatus,
    DriftSeverity,
    EdgeConfidence,
    EdgeType,
    InterfaceAdminState,
    InterfaceOperState,
    InterfaceType,
    InventorySourceType,
    MismatchType,
    NormalizationWarning,
    OSPFState,
    RouteProtocol,
    RouteType,
    SnapshotStatus,
    SyncActionType,
    VLANStatus,
)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _new_id() -> str:
    return str(uuid.uuid4())



def _now() -> datetime:
    return datetime.utcnow()



def _clean_text(value: Any) -> str | None:
    """Trim strings and collapse empty content to ``None``.

    These models are an integration boundary. A depressing amount of incoming
    data arrives as padded strings or pseudo-null tokens.
    """

    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None
    return str(value).strip() or None



def _clean_required_text(value: Any) -> str:
    cleaned = _clean_text(value)
    if cleaned is None:
        raise ValueError("value must not be empty")
    return cleaned



def _dedupe_preserve_order(values: list[Any]) -> list[Any]:
    seen: set[Any] = set()
    deduped: list[Any] = []
    for value in values:
        key = value if isinstance(value, (str, int, float, bool, tuple)) else repr(value)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(value)
    return deduped



def _clean_string_list(values: Any, *, lowercase: bool = False) -> list[str]:
    if values is None:
        return []
    if isinstance(values, str):
        values = [values]
    cleaned: list[str] = []
    for value in values:
        text = _clean_text(value)
        if text is None:
            continue
        cleaned.append(text.lower() if lowercase else text)
    return _dedupe_preserve_order(cleaned)



def _clean_vlan_list(values: Any) -> list[int]:
    if values is None:
        return []
    if isinstance(values, (str, int)):
        values = [values]

    result: list[int] = []
    for value in values:
        if value is None or value == "":
            continue
        if isinstance(value, int):
            vlan = value
        else:
            text = str(value).strip()
            if not text:
                continue
            vlan = int(text)
        if vlan < 1 or vlan > 4094:
            raise ValueError(f"VLAN id out of range: {vlan}")
        result.append(vlan)
    return sorted(set(result))



def _normalize_ip_or_prefix(value: Any, *, network: bool) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None
    try:
        if "/" in text:
            iface = ipaddress.ip_interface(text)
            if network:
                return str(iface.network)
            return str(iface)
        addr = ipaddress.ip_address(text)
        return str(addr)
    except ValueError:
        return text



def _infer_ip_family(value: str | None) -> int:
    if value is None:
        return 4
    try:
        text = value.split("/", 1)[0]
        return ipaddress.ip_address(text).version
    except ValueError:
        return 6 if ":" in value else 4



def _normalize_mac(value: Any) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None
    hex_only = "".join(ch for ch in text if ch.isalnum()).lower()
    if len(hex_only) != 12:
        return text.lower()
    return ":".join(hex_only[i : i + 2] for i in range(0, 12, 2))


class CanonicalBaseModel(BaseModel):
    """Base class with common cleanup behavior.

    We deliberately keep the model permissive enough for the rest of the code
    to keep working, but we validate assignment so later mutations do not drift
    into nonsense.
    """

    model_config: ClassVar[dict[str, Any]] = {
        "validate_assignment": True,
        "extra": "ignore",
    }


# ---------------------------------------------------------------------------
# Data quality metadata attached to every canonical object
# ---------------------------------------------------------------------------


class DataQualityMetadata(CanonicalBaseModel):
    """Quality and provenance metadata attached to a canonical object."""

    source_system: str = "unknown"
    source_timestamp: datetime | None = None
    completeness_score: float = Field(default=1.0, ge=0.0, le=1.0)
    parsing_confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    normalization_warnings: list[NormalizationWarning] = Field(default_factory=list)
    unresolved_references: list[str] = Field(default_factory=list)
    freshness_status: str = "fresh"
    raw_vendor_data: dict[str, Any] | None = None

    @field_validator("source_system", "freshness_status", mode="before")
    @classmethod
    def _clean_meta_text(cls, value: Any) -> str:
        return _clean_required_text(value) if value is not None else "unknown"

    @field_validator("unresolved_references", mode="before")
    @classmethod
    def _clean_unresolved_refs(cls, value: Any) -> list[str]:
        return _clean_string_list(value)

    @field_validator("normalization_warnings", mode="before")
    @classmethod
    def _dedupe_warnings(cls, value: Any) -> list[NormalizationWarning]:
        if value is None:
            return []
        if isinstance(value, (str, NormalizationWarning)):
            value = [value]
        warnings = [
            item if isinstance(item, NormalizationWarning) else NormalizationWarning(str(item))
            for item in value
        ]
        return _dedupe_preserve_order(warnings)


# ---------------------------------------------------------------------------
# Snapshot
# ---------------------------------------------------------------------------


class CanonicalSnapshot(CanonicalBaseModel):
    """A time-scoped persistent representation of observed network state."""

    snapshot_id: str = Field(default_factory=_new_id)
    workflow_id: str
    scope: str = "global"  # site / namespace / tag filter label
    status: SnapshotStatus = SnapshotStatus.IN_PROGRESS
    started_at: datetime = Field(default_factory=_now)
    completed_at: datetime | None = None
    device_count: int = Field(default=0, ge=0)
    interface_count: int = Field(default=0, ge=0)
    edge_count: int = Field(default=0, ge=0)
    drift_count: int = Field(default=0, ge=0)
    collection_errors: int = Field(default=0, ge=0)
    notes: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("workflow_id", "scope", "notes", mode="before")
    @classmethod
    def _clean_snapshot_text(cls, value: Any) -> str:
        return _clean_required_text(value) if value not in (None, "") else "global"

    @model_validator(mode="after")
    def _validate_completion_order(self) -> "CanonicalSnapshot":
        if self.completed_at is not None and self.completed_at < self.started_at:
            raise ValueError("completed_at cannot be earlier than started_at")
        return self


# ---------------------------------------------------------------------------
# Device
# ---------------------------------------------------------------------------


class CanonicalDevice(CanonicalBaseModel):
    """Vendor-neutral device record."""

    device_id: str = Field(default_factory=_new_id)
    hostname: str
    management_ip: str | None = None
    vendor: str | None = None
    model: str | None = None
    serial_number: str | None = None
    software_version: str | None = None
    device_type: str | None = None
    device_role: str | None = None
    site: str | None = None
    region: str | None = None
    rack: str | None = None
    status: DeviceStatus = DeviceStatus.UNKNOWN
    namespace: str | None = None
    platform: str | None = None
    tags: list[str] = Field(default_factory=list)
    custom_fields: dict[str, Any] = Field(default_factory=dict)
    source_system: str = "unknown"
    source_timestamp: datetime | None = None
    snapshot_id: str | None = None
    quality: DataQualityMetadata = Field(default_factory=DataQualityMetadata)

    @field_validator(
        "hostname",
        "vendor",
        "model",
        "serial_number",
        "software_version",
        "device_type",
        "device_role",
        "site",
        "region",
        "rack",
        "namespace",
        "platform",
        "source_system",
        mode="before",
    )
    @classmethod
    def _clean_device_text(cls, value: Any) -> str | None:
        return _clean_text(value)

    @field_validator("hostname", mode="after")
    @classmethod
    def _hostname_required(cls, value: str | None) -> str:
        if value is None:
            raise ValueError("hostname must not be empty")
        return value

    @field_validator("management_ip", mode="before")
    @classmethod
    def _normalize_mgmt_ip(cls, value: Any) -> str | None:
        return _normalize_ip_or_prefix(value, network=False)

    @field_validator("tags", mode="before")
    @classmethod
    def _normalize_tags(cls, value: Any) -> list[str]:
        return _clean_string_list(value, lowercase=False)


# ---------------------------------------------------------------------------
# Interface
# ---------------------------------------------------------------------------


class CanonicalInterface(CanonicalBaseModel):
    """Vendor-neutral interface record."""

    interface_id: str = Field(default_factory=_new_id)
    device_id: str
    name: str
    normalized_name: str | None = None
    type: InterfaceType = InterfaceType.UNKNOWN
    admin_state: InterfaceAdminState = InterfaceAdminState.UNKNOWN
    oper_state: InterfaceOperState = InterfaceOperState.UNKNOWN
    mtu: int | None = Field(default=None, ge=0)
    speed: int | None = Field(default=None, ge=0)  # kbps in this model
    description: str | None = None
    mac_address: str | None = None
    ip_addresses: list[str] = Field(default_factory=list)
    vrf: str | None = None
    vlan_mode: str | None = None   # access / trunk / hybrid
    access_vlan: int | None = Field(default=None, ge=1, le=4094)
    trunk_vlans: list[int] = Field(default_factory=list)
    allowed_vlans: list[int] = Field(default_factory=list)
    lag_parent: str | None = None
    lag_members: list[str] = Field(default_factory=list)
    is_enabled: bool = True
    duplex: str | None = None
    source_timestamp: datetime | None = None
    snapshot_id: str | None = None
    quality: DataQualityMetadata = Field(default_factory=DataQualityMetadata)

    @field_validator(
        "device_id",
        "name",
        "normalized_name",
        "description",
        "vrf",
        "vlan_mode",
        "lag_parent",
        "duplex",
        mode="before",
    )
    @classmethod
    def _clean_interface_text(cls, value: Any) -> str | None:
        return _clean_text(value)

    @field_validator("device_id", "name", mode="after")
    @classmethod
    def _required_interface_text(cls, value: str | None) -> str:
        if value is None:
            raise ValueError("required interface field must not be empty")
        return value

    @field_validator("mac_address", mode="before")
    @classmethod
    def _normalize_mac_address(cls, value: Any) -> str | None:
        return _normalize_mac(value)

    @field_validator("ip_addresses", mode="before")
    @classmethod
    def _normalize_ip_list(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            value = [value]
        addresses = [
            normalized
            for item in value
            if (normalized := _normalize_ip_or_prefix(item, network=False)) is not None
        ]
        return _dedupe_preserve_order(addresses)

    @field_validator("trunk_vlans", "allowed_vlans", mode="before")
    @classmethod
    def _normalize_vlan_lists(cls, value: Any) -> list[int]:
        return _clean_vlan_list(value)

    @field_validator("lag_members", mode="before")
    @classmethod
    def _normalize_lag_members(cls, value: Any) -> list[str]:
        return _clean_string_list(value)


# ---------------------------------------------------------------------------
# IP Address
# ---------------------------------------------------------------------------


class CanonicalIPAddress(CanonicalBaseModel):
    """Vendor-neutral IP address record."""

    address_id: str = Field(default_factory=_new_id)
    address: str  # CIDR notation e.g. 10.0.0.1/24
    device_id: str | None = None
    interface_id: str | None = None
    vrf: str | None = None
    dns_name: str | None = None
    is_primary: bool = False
    status: str = "active"
    family: int = 4  # 4 or 6
    snapshot_id: str | None = None
    quality: DataQualityMetadata = Field(default_factory=DataQualityMetadata)

    @field_validator("address", mode="before")
    @classmethod
    def _normalize_address(cls, value: Any) -> str:
        normalized = _normalize_ip_or_prefix(value, network=False)
        if normalized is None:
            raise ValueError("address must not be empty")
        return normalized

    @field_validator("device_id", "interface_id", "vrf", "dns_name", "status", mode="before")
    @classmethod
    def _clean_address_text(cls, value: Any) -> str | None:
        return _clean_text(value)

    @model_validator(mode="after")
    def _sync_address_family(self) -> "CanonicalIPAddress":
        self.family = _infer_ip_family(self.address)
        return self


# ---------------------------------------------------------------------------
# Subnet
# ---------------------------------------------------------------------------


class CanonicalSubnet(CanonicalBaseModel):
    """IP subnet / prefix."""

    subnet_id: str = Field(default_factory=_new_id)
    prefix: str   # CIDR e.g. 10.0.0.0/24
    vrf: str | None = None
    site: str | None = None
    description: str | None = None
    status: str = "active"
    family: int = 4
    snapshot_id: str | None = None
    quality: DataQualityMetadata = Field(default_factory=DataQualityMetadata)

    @field_validator("prefix", mode="before")
    @classmethod
    def _normalize_prefix(cls, value: Any) -> str:
        normalized = _normalize_ip_or_prefix(value, network=True)
        if normalized is None:
            raise ValueError("prefix must not be empty")
        return normalized

    @field_validator("vrf", "site", "description", "status", mode="before")
    @classmethod
    def _clean_subnet_text(cls, value: Any) -> str | None:
        return _clean_text(value)

    @model_validator(mode="after")
    def _sync_subnet_family(self) -> "CanonicalSubnet":
        self.family = _infer_ip_family(self.prefix)
        return self


# ---------------------------------------------------------------------------
# VLAN
# ---------------------------------------------------------------------------


class CanonicalVLAN(CanonicalBaseModel):
    """Vendor-neutral VLAN record."""

    vlan_id: str = Field(default_factory=_new_id)
    vid: int = Field(ge=1, le=4094)
    name: str | None = None
    site: str | None = None
    namespace: str | None = None
    status: VLANStatus = VLANStatus.UNKNOWN
    attached_devices: list[str] = Field(default_factory=list)
    attached_interfaces: list[str] = Field(default_factory=list)
    snapshot_id: str | None = None
    quality: DataQualityMetadata = Field(default_factory=DataQualityMetadata)

    @field_validator("name", "site", "namespace", mode="before")
    @classmethod
    def _clean_vlan_text(cls, value: Any) -> str | None:
        return _clean_text(value)

    @field_validator("attached_devices", "attached_interfaces", mode="before")
    @classmethod
    def _normalize_vlan_refs(cls, value: Any) -> list[str]:
        return _clean_string_list(value)


# ---------------------------------------------------------------------------
# VRF
# ---------------------------------------------------------------------------


class CanonicalVRF(CanonicalBaseModel):
    """Virtual Routing and Forwarding instance."""

    vrf_id: str = Field(default_factory=_new_id)
    name: str
    rd: str | None = None   # Route Distinguisher
    device_id: str | None = None
    import_targets: list[str] = Field(default_factory=list)
    export_targets: list[str] = Field(default_factory=list)
    snapshot_id: str | None = None
    quality: DataQualityMetadata = Field(default_factory=DataQualityMetadata)

    @field_validator("name", "rd", "device_id", mode="before")
    @classmethod
    def _clean_vrf_text(cls, value: Any) -> str | None:
        return _clean_text(value)

    @field_validator("name", mode="after")
    @classmethod
    def _vrf_name_required(cls, value: str | None) -> str:
        if value is None:
            raise ValueError("name must not be empty")
        return value

    @field_validator("import_targets", "export_targets", mode="before")
    @classmethod
    def _normalize_rt_lists(cls, value: Any) -> list[str]:
        return _clean_string_list(value)


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------


class CanonicalRoute(CanonicalBaseModel):
    """Routing table entry."""

    route_id: str = Field(default_factory=_new_id)
    device_id: str
    prefix: str
    next_hop: str | None = None
    next_hop_interface: str | None = None
    protocol: RouteProtocol = RouteProtocol.UNKNOWN
    route_type: RouteType = RouteType.IPV4
    metric: int | None = Field(default=None, ge=0)
    preference: int | None = Field(default=None, ge=0)
    vrf: str | None = None
    is_active: bool = True
    age: int | None = Field(default=None, ge=0)  # seconds
    snapshot_id: str | None = None
    quality: DataQualityMetadata = Field(default_factory=DataQualityMetadata)

    @field_validator("device_id", mode="before")
    @classmethod
    def _normalize_route_device_id(cls, value: Any) -> str:
        return _clean_required_text(value)

    @field_validator("prefix", mode="before")
    @classmethod
    def _normalize_route_prefix(cls, value: Any) -> str:
        normalized = _normalize_ip_or_prefix(value, network=True)
        if normalized is None:
            raise ValueError("prefix must not be empty")
        return normalized

    @field_validator("next_hop", mode="before")
    @classmethod
    def _normalize_next_hop(cls, value: Any) -> str | None:
        return _normalize_ip_or_prefix(value, network=False)

    @field_validator("next_hop_interface", "vrf", mode="before")
    @classmethod
    def _clean_route_text(cls, value: Any) -> str | None:
        return _clean_text(value)

    @model_validator(mode="after")
    def _sync_route_type(self) -> "CanonicalRoute":
        self.route_type = RouteType.IPV6 if _infer_ip_family(self.prefix) == 6 else RouteType.IPV4
        return self


# ---------------------------------------------------------------------------
# Neighbor Relationship (LLDP / CDP)
# ---------------------------------------------------------------------------


class CanonicalNeighborRelationship(CanonicalBaseModel):
    """Discovered L2 neighbor relationship (LLDP / CDP)."""

    neighbor_id: str = Field(default_factory=_new_id)
    local_device_id: str
    local_interface: str
    remote_device_id: str | None = None
    remote_hostname: str | None = None
    remote_interface: str | None = None
    remote_management_ip: str | None = None
    remote_platform: str | None = None
    remote_vendor: str | None = None
    protocol: str = "lldp"  # lldp / cdp
    ttl: int | None = Field(default=None, ge=0)
    snapshot_id: str | None = None
    quality: DataQualityMetadata = Field(default_factory=DataQualityMetadata)

    @field_validator(
        "local_device_id",
        "local_interface",
        "remote_device_id",
        "remote_hostname",
        "remote_interface",
        "remote_platform",
        "remote_vendor",
        "protocol",
        mode="before",
    )
    @classmethod
    def _clean_neighbor_text(cls, value: Any) -> str | None:
        return _clean_text(value)

    @field_validator("local_device_id", "local_interface", mode="after")
    @classmethod
    def _neighbor_required(cls, value: str | None) -> str:
        if value is None:
            raise ValueError("required neighbor field must not be empty")
        return value

    @field_validator("remote_management_ip", mode="before")
    @classmethod
    def _normalize_remote_mgmt_ip(cls, value: Any) -> str | None:
        return _normalize_ip_or_prefix(value, network=False)


# ---------------------------------------------------------------------------
# Topology Edge
# ---------------------------------------------------------------------------


class CanonicalTopologyEdge(CanonicalBaseModel):
    """An inferred or confirmed topology edge between two nodes."""

    edge_id: str = Field(default_factory=_new_id)
    snapshot_id: str | None = None
    source_node_id: str
    target_node_id: str
    source_interface: str | None = None
    target_interface: str | None = None
    edge_type: EdgeType = EdgeType.PHYSICAL
    evidence_source: list[str] = Field(default_factory=list)
    evidence_count: int = Field(default=0, ge=0)
    confidence: EdgeConfidence = EdgeConfidence.LOW
    confidence_score: float = Field(default=0.0, ge=0.0, le=1.0)
    is_direct: bool = False
    conflict_flags: list[str] = Field(default_factory=list)
    inference_method: str | None = None
    created_at: datetime = Field(default_factory=_now)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator(
        "source_node_id",
        "target_node_id",
        "source_interface",
        "target_interface",
        "inference_method",
        mode="before",
    )
    @classmethod
    def _clean_edge_text(cls, value: Any) -> str | None:
        return _clean_text(value)

    @field_validator("source_node_id", "target_node_id", mode="after")
    @classmethod
    def _edge_required(cls, value: str | None) -> str:
        if value is None:
            raise ValueError("edge node id must not be empty")
        return value

    @field_validator("evidence_source", "conflict_flags", mode="before")
    @classmethod
    def _normalize_edge_lists(cls, value: Any) -> list[str]:
        return _clean_string_list(value)

    @model_validator(mode="after")
    def _sync_edge_quality(self) -> "CanonicalTopologyEdge":
        if self.source_node_id == self.target_node_id:
            raise ValueError("topology edge cannot connect a node to itself")
        if self.evidence_count == 0 and self.evidence_source:
            self.evidence_count = len(self.evidence_source)
        if self.confidence_score == 0.0:
            default_scores = {
                EdgeConfidence.HIGH: 0.95,
                EdgeConfidence.MEDIUM: 0.7,
                EdgeConfidence.LOW: 0.4,
                EdgeConfidence.CONFLICT: 0.1,
            }
            self.confidence_score = default_scores[self.confidence]
        return self


# ---------------------------------------------------------------------------
# L2 Domain
# ---------------------------------------------------------------------------


class CanonicalL2Domain(CanonicalBaseModel):
    """A logical L2 broadcast domain."""

    l2domain_id: str = Field(default_factory=_new_id)
    name: str | None = None
    vlan_ids: list[str] = Field(default_factory=list)
    device_ids: list[str] = Field(default_factory=list)
    site: str | None = None
    snapshot_id: str | None = None
    quality: DataQualityMetadata = Field(default_factory=DataQualityMetadata)

    @field_validator("name", "site", mode="before")
    @classmethod
    def _clean_l2_text(cls, value: Any) -> str | None:
        return _clean_text(value)

    @field_validator("vlan_ids", "device_ids", mode="before")
    @classmethod
    def _normalize_l2_refs(cls, value: Any) -> list[str]:
        return _clean_string_list(value)


# ---------------------------------------------------------------------------
# L3 Adjacency
# ---------------------------------------------------------------------------


class CanonicalL3Adjacency(CanonicalBaseModel):
    """A Layer-3 adjacency (routing protocol, point-to-point)."""

    adjacency_id: str = Field(default_factory=_new_id)
    local_device_id: str
    remote_device_id: str | None = None
    local_address: str | None = None
    remote_address: str | None = None
    protocol: str = "ospf"  # ospf / bgp / etc.
    state: str = OSPFState.UNKNOWN.value
    vrf: str | None = None
    metric: int | None = Field(default=None, ge=0)
    snapshot_id: str | None = None
    quality: DataQualityMetadata = Field(default_factory=DataQualityMetadata)

    @field_validator("local_device_id", mode="before")
    @classmethod
    def _normalize_local_device_id(cls, value: Any) -> str:
        return _clean_required_text(value)

    @field_validator("remote_device_id", "protocol", "state", "vrf", mode="before")
    @classmethod
    def _clean_l3_text(cls, value: Any) -> str | None:
        return _clean_text(value)

    @field_validator("local_address", "remote_address", mode="before")
    @classmethod
    def _normalize_l3_addrs(cls, value: Any) -> str | None:
        return _normalize_ip_or_prefix(value, network=False)


# ---------------------------------------------------------------------------
# BGP Session
# ---------------------------------------------------------------------------


class CanonicalBGPSession(CanonicalBaseModel):
    """BGP peer session record."""

    session_id: str = Field(default_factory=_new_id)
    device_id: str
    local_address: str | None = None
    remote_address: str
    local_asn: int | None = Field(default=None, ge=0)
    remote_asn: int | None = Field(default=None, ge=0)
    state: BGPSessionState = BGPSessionState.UNKNOWN
    vrf: str | None = None
    peer_group: str | None = None
    description: str | None = None
    uptime: int | None = Field(default=None, ge=0)  # seconds
    prefixes_received: int | None = Field(default=None, ge=0)
    prefixes_sent: int | None = Field(default=None, ge=0)
    snapshot_id: str | None = None
    quality: DataQualityMetadata = Field(default_factory=DataQualityMetadata)

    @field_validator("device_id", mode="before")
    @classmethod
    def _normalize_bgp_device_id(cls, value: Any) -> str:
        return _clean_required_text(value)

    @field_validator("local_address", "remote_address", mode="before")
    @classmethod
    def _normalize_bgp_addrs(cls, value: Any) -> str | None:
        normalized = _normalize_ip_or_prefix(value, network=False)
        if value is None:
            return None
        if normalized is None:
            raise ValueError("BGP address must not be empty")
        return normalized

    @field_validator("vrf", "peer_group", "description", mode="before")
    @classmethod
    def _clean_bgp_text(cls, value: Any) -> str | None:
        return _clean_text(value)


# ---------------------------------------------------------------------------
# ARP Entry
# ---------------------------------------------------------------------------


class CanonicalARPEntry(CanonicalBaseModel):
    """ARP / Neighbor Discovery entry."""

    arp_id: str = Field(default_factory=_new_id)
    device_id: str
    interface: str | None = None
    ip_address: str
    mac_address: str | None = None
    vrf: str | None = None
    state: str = "reachable"
    age: int | None = Field(default=None, ge=0)  # seconds
    family: int = 4
    snapshot_id: str | None = None
    quality: DataQualityMetadata = Field(default_factory=DataQualityMetadata)

    @field_validator("device_id", "interface", "vrf", "state", mode="before")
    @classmethod
    def _clean_arp_text(cls, value: Any) -> str | None:
        return _clean_text(value)

    @field_validator("device_id", mode="after")
    @classmethod
    def _arp_device_required(cls, value: str | None) -> str:
        if value is None:
            raise ValueError("device_id must not be empty")
        return value

    @field_validator("ip_address", mode="before")
    @classmethod
    def _normalize_arp_ip(cls, value: Any) -> str:
        normalized = _normalize_ip_or_prefix(value, network=False)
        if normalized is None:
            raise ValueError("ip_address must not be empty")
        return normalized

    @field_validator("mac_address", mode="before")
    @classmethod
    def _normalize_arp_mac(cls, value: Any) -> str | None:
        return _normalize_mac(value)

    @model_validator(mode="after")
    def _sync_arp_family(self) -> "CanonicalARPEntry":
        self.family = _infer_ip_family(self.ip_address)
        return self


# ---------------------------------------------------------------------------
# MAC Table Entry
# ---------------------------------------------------------------------------


class CanonicalMACEntry(CanonicalBaseModel):
    """MAC address table entry."""

    mac_entry_id: str = Field(default_factory=_new_id)
    device_id: str
    mac_address: str
    vlan_id: int | None = Field(default=None, ge=1, le=4094)
    interface: str | None = None
    type: str = "dynamic"  # dynamic / static / remote
    age: int | None = Field(default=None, ge=0)  # seconds
    snapshot_id: str | None = None
    quality: DataQualityMetadata = Field(default_factory=DataQualityMetadata)

    @field_validator("device_id", "interface", "type", mode="before")
    @classmethod
    def _clean_mac_text(cls, value: Any) -> str | None:
        return _clean_text(value)

    @field_validator("device_id", mode="after")
    @classmethod
    def _mac_device_required(cls, value: str | None) -> str:
        if value is None:
            raise ValueError("device_id must not be empty")
        return value

    @field_validator("mac_address", mode="before")
    @classmethod
    def _normalize_mac_entry_address(cls, value: Any) -> str:
        normalized = _normalize_mac(value)
        if normalized is None:
            raise ValueError("mac_address must not be empty")
        return normalized


# ---------------------------------------------------------------------------
# Inventory Source
# ---------------------------------------------------------------------------


class CanonicalInventorySource(CanonicalBaseModel):
    """A source of inventory targets for discovery."""

    source_id: str = Field(default_factory=_new_id)
    name: str
    source_type: InventorySourceType
    config: dict[str, Any] = Field(default_factory=dict)
    enabled: bool = True
    last_synced: datetime | None = None
    device_count: int = Field(default=0, ge=0)
    quality: DataQualityMetadata = Field(default_factory=DataQualityMetadata)

    @field_validator("name", mode="before")
    @classmethod
    def _normalize_source_name(cls, value: Any) -> str:
        return _clean_required_text(value)


# ---------------------------------------------------------------------------
# Drift Record
# ---------------------------------------------------------------------------


class CanonicalDriftRecord(CanonicalBaseModel):
    """A mismatch between intended and actual state."""

    drift_id: str = Field(default_factory=_new_id)
    snapshot_id: str | None = None
    workflow_id: str | None = None
    intended_ref: str | None = None
    actual_ref: str | None = None
    object_type: str
    mismatch_type: MismatchType
    field_name: str | None = None
    intended_value: Any = None
    actual_value: Any = None
    severity: DriftSeverity = DriftSeverity.MEDIUM
    explanation: str = ""
    remediation_hint: str = ""
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    evidence: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=_now)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator(
        "workflow_id",
        "intended_ref",
        "actual_ref",
        "object_type",
        "field_name",
        "explanation",
        "remediation_hint",
        mode="before",
    )
    @classmethod
    def _clean_drift_text(cls, value: Any) -> str | None:
        return _clean_text(value)

    @field_validator("object_type", mode="after")
    @classmethod
    def _drift_object_type_required(cls, value: str | None) -> str:
        if value is None:
            raise ValueError("object_type must not be empty")
        return value

    @field_validator("evidence", mode="before")
    @classmethod
    def _normalize_drift_evidence(cls, value: Any) -> list[str]:
        return _clean_string_list(value)


# ---------------------------------------------------------------------------
# Union type for all canonical objects
# ---------------------------------------------------------------------------

CanonicalObject = Union[
    CanonicalDevice,
    CanonicalInterface,
    CanonicalIPAddress,
    CanonicalSubnet,
    CanonicalVLAN,
    CanonicalVRF,
    CanonicalRoute,
    CanonicalNeighborRelationship,
    CanonicalTopologyEdge,
    CanonicalL2Domain,
    CanonicalL3Adjacency,
    CanonicalInventorySource,
    CanonicalSnapshot,
    CanonicalDriftRecord,
    CanonicalBGPSession,
    CanonicalARPEntry,
    CanonicalMACEntry,
]
