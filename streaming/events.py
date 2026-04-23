"""Canonical change event model for the real-time streaming engine.

Every input source — gNMI telemetry, SNMP traps, syslog, SSH polling diffs —
converges on this taxonomy before touching anything else in the platform.

Design constraints
------------------
1. A ChangeEvent is immutable once created. The engine appends to history;
   it does not mutate past events.
2. Every event carries enough context for the topology patcher and anomaly
   classifier to act without doing additional I/O.
3. Classification is strict: unknown events get ChangeKind.UNKNOWN and are
   persisted for later analysis, never silently dropped.
4. Severity maps to DoD INFOCON / operational urgency without coupling to a
   specific classification framework.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _new_id() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Event taxonomy
# ---------------------------------------------------------------------------


class ChangeKind(str, Enum):
    """The logical category of a network state change.

    These categories drive routing decisions in the event bus, topology patcher
    prioritization, and anomaly classifier feature extraction. They must remain
    stable — downstream subscribers key on them.
    """

    # Interface lifecycle
    INTERFACE_UP = "interface_up"
    INTERFACE_DOWN = "interface_down"
    INTERFACE_FLAP = "interface_flap"           # up→down→up within flap_window_seconds
    INTERFACE_ERR_DISABLED = "interface_err_disabled"
    INTERFACE_SPEED_CHANGE = "interface_speed_change"
    INTERFACE_DESCRIPTION_CHANGE = "interface_description_change"

    # Neighbor / adjacency
    NEIGHBOR_DISCOVERED = "neighbor_discovered"  # new LLDP/CDP neighbor appeared
    NEIGHBOR_LOST = "neighbor_lost"              # existing neighbor gone
    NEIGHBOR_CHANGED = "neighbor_changed"        # same port, different remote ID

    # BGP
    BGP_SESSION_ESTABLISHED = "bgp_session_established"
    BGP_SESSION_DROPPED = "bgp_session_dropped"
    BGP_PEER_FLAP = "bgp_peer_flap"
    BGP_PREFIX_WITHDRAW_MASS = "bgp_prefix_withdraw_mass"  # > threshold prefixes lost

    # OSPF
    OSPF_ADJACENCY_UP = "ospf_adjacency_up"
    OSPF_ADJACENCY_DOWN = "ospf_adjacency_down"
    OSPF_LSA_FLOOD = "ospf_lsa_flood"

    # Routing table
    ROUTE_ADDED = "route_added"
    ROUTE_WITHDRAWN = "route_withdrawn"
    ROUTE_CHANGED_NEXTHOP = "route_changed_nexthop"
    ROUTE_FLAP = "route_flap"

    # MAC / ARP
    MAC_MOVE = "mac_move"           # MAC seen on a different port — potential security event
    MAC_NEW = "mac_new"             # First-time MAC address seen on segment
    ARP_NEW = "arp_new"
    ARP_STALE = "arp_stale"

    # VLAN
    VLAN_CREATED = "vlan_created"
    VLAN_DELETED = "vlan_deleted"
    VLAN_STATE_CHANGE = "vlan_state_change"

    # Device
    DEVICE_REACHABILITY_LOST = "device_reachability_lost"
    DEVICE_REACHABILITY_RESTORED = "device_reachability_restored"
    DEVICE_REBOOTED = "device_rebooted"
    DEVICE_VERSION_CHANGE = "device_version_change"  # version string changed post-reboot

    # Security / anomaly indicators
    UNKNOWN_DEVICE_DETECTED = "unknown_device_detected"   # not in any inventory source
    TOPOLOGY_ANOMALY = "topology_anomaly"                 # edge not matching any known state
    CREDENTIAL_FAILURE = "credential_failure"             # auth failure during streaming probe
    CONFIGURATION_CHANGE = "configuration_change"         # config commit detected via syslog

    # Platform / telemetry
    TELEMETRY_SESSION_LOST = "telemetry_session_lost"
    TELEMETRY_SESSION_RESTORED = "telemetry_session_restored"
    SNMP_TRAP_UNCLASSIFIED = "snmp_trap_unclassified"
    SYSLOG_UNCLASSIFIED = "syslog_unclassified"

    # Kubernetes
    K8S_NODE_READY = "k8s_node_ready"
    K8S_NODE_NOT_READY = "k8s_node_not_ready"
    K8S_NODE_PRESSURE = "k8s_node_pressure"
    K8S_NODE_NETWORK_UNAVAILABLE = "k8s_node_network_unavailable"
    K8S_POD_READY = "k8s_pod_ready"
    K8S_POD_NOT_READY = "k8s_pod_not_ready"
    K8S_POD_RESTARTED = "k8s_pod_restarted"
    K8S_POD_CRASH_LOOP = "k8s_pod_crash_loop"
    K8S_POD_EVICTED = "k8s_pod_evicted"
    K8S_POD_SCHEDULE_FAILED = "k8s_pod_schedule_failed"
    K8S_SERVICE_ENDPOINTS_CHANGED = "k8s_service_endpoints_changed"
    K8S_SERVICE_ENDPOINTS_EMPTY = "k8s_service_endpoints_empty"
    K8S_DEPLOYMENT_ROLLOUT_STARTED = "k8s_deployment_rollout_started"
    K8S_DEPLOYMENT_ROLLOUT_STALLED = "k8s_deployment_rollout_stalled"
    K8S_DEPLOYMENT_REPLICA_MISMATCH = "k8s_deployment_replica_mismatch"
    K8S_WARNING_EVENT = "k8s_warning_event"

    UNKNOWN = "unknown"


class ChangeSeverity(str, Enum):
    """Operational severity aligned to DoD urgency tiers.

    CRITICAL — mission-impacting: C2 link down, unknown device on SCIF segment
    HIGH     — significant degradation: BGP session drop, device reboot
    MEDIUM   — notable but not immediately mission-impacting: route change
    LOW      — informational: interface description update, new ARP entry
    INFO     — telemetry noise / debug-level events
    """

    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class EventSource(str, Enum):
    """The ingest pipeline that produced this event."""

    GNMI = "gnmi"
    GNMI_STREAM = "gnmi"           # alias used by adapters and tests
    SNMP_TRAP = "snmp_trap"
    SYSLOG = "syslog"
    SSH_POLL_DIFF = "ssh_poll_diff"
    SSH_POLL = "ssh_poll_diff"     # alias used by tests
    NETCONF = "netconf"
    NETCONF_NOTIFICATION = "netconf"  # alias used by adapters
    SYNTHETIC = "synthetic"    # test / replay events
    K8S_WATCH = "k8s_watch"
    K8S_EVENT = "k8s_event"
    UNKNOWN = "unknown"


# ---------------------------------------------------------------------------
# Default severity map — can be overridden by policy config
# ---------------------------------------------------------------------------

_DEFAULT_SEVERITY: dict[ChangeKind, ChangeSeverity] = {
    ChangeKind.INTERFACE_UP: ChangeSeverity.INFO,
    ChangeKind.INTERFACE_DOWN: ChangeSeverity.HIGH,
    ChangeKind.INTERFACE_FLAP: ChangeSeverity.HIGH,
    ChangeKind.INTERFACE_ERR_DISABLED: ChangeSeverity.HIGH,
    ChangeKind.INTERFACE_SPEED_CHANGE: ChangeSeverity.MEDIUM,
    ChangeKind.INTERFACE_DESCRIPTION_CHANGE: ChangeSeverity.LOW,
    ChangeKind.NEIGHBOR_DISCOVERED: ChangeSeverity.MEDIUM,
    ChangeKind.NEIGHBOR_LOST: ChangeSeverity.HIGH,
    ChangeKind.NEIGHBOR_CHANGED: ChangeSeverity.HIGH,
    ChangeKind.BGP_SESSION_ESTABLISHED: ChangeSeverity.INFO,
    ChangeKind.BGP_SESSION_DROPPED: ChangeSeverity.CRITICAL,
    ChangeKind.BGP_PEER_FLAP: ChangeSeverity.CRITICAL,
    ChangeKind.BGP_PREFIX_WITHDRAW_MASS: ChangeSeverity.CRITICAL,
    ChangeKind.OSPF_ADJACENCY_UP: ChangeSeverity.INFO,
    ChangeKind.OSPF_ADJACENCY_DOWN: ChangeSeverity.HIGH,
    ChangeKind.OSPF_LSA_FLOOD: ChangeSeverity.MEDIUM,
    ChangeKind.ROUTE_ADDED: ChangeSeverity.LOW,
    ChangeKind.ROUTE_WITHDRAWN: ChangeSeverity.MEDIUM,
    ChangeKind.ROUTE_CHANGED_NEXTHOP: ChangeSeverity.HIGH,
    ChangeKind.ROUTE_FLAP: ChangeSeverity.HIGH,
    ChangeKind.MAC_MOVE: ChangeSeverity.CRITICAL,
    ChangeKind.MAC_NEW: ChangeSeverity.MEDIUM,
    ChangeKind.ARP_NEW: ChangeSeverity.LOW,
    ChangeKind.ARP_STALE: ChangeSeverity.INFO,
    ChangeKind.VLAN_CREATED: ChangeSeverity.MEDIUM,
    ChangeKind.VLAN_DELETED: ChangeSeverity.HIGH,
    ChangeKind.VLAN_STATE_CHANGE: ChangeSeverity.MEDIUM,
    ChangeKind.DEVICE_REACHABILITY_LOST: ChangeSeverity.CRITICAL,
    ChangeKind.DEVICE_REACHABILITY_RESTORED: ChangeSeverity.INFO,
    ChangeKind.DEVICE_REBOOTED: ChangeSeverity.HIGH,
    ChangeKind.DEVICE_VERSION_CHANGE: ChangeSeverity.HIGH,
    ChangeKind.UNKNOWN_DEVICE_DETECTED: ChangeSeverity.CRITICAL,
    ChangeKind.TOPOLOGY_ANOMALY: ChangeSeverity.CRITICAL,
    ChangeKind.CREDENTIAL_FAILURE: ChangeSeverity.HIGH,
    ChangeKind.CONFIGURATION_CHANGE: ChangeSeverity.HIGH,
    ChangeKind.TELEMETRY_SESSION_LOST: ChangeSeverity.MEDIUM,
    ChangeKind.TELEMETRY_SESSION_RESTORED: ChangeSeverity.INFO,
    ChangeKind.SNMP_TRAP_UNCLASSIFIED: ChangeSeverity.LOW,
    ChangeKind.SYSLOG_UNCLASSIFIED: ChangeSeverity.INFO,
    ChangeKind.UNKNOWN: ChangeSeverity.INFO,
}


def default_severity(kind: ChangeKind) -> ChangeSeverity:
    """Return the default severity for a ChangeKind."""
    return _DEFAULT_SEVERITY.get(kind, ChangeSeverity.INFO)


# ---------------------------------------------------------------------------
# Canonical ChangeEvent
# ---------------------------------------------------------------------------


class ChangeEvent(BaseModel):
    """Immutable canonical representation of a single network state change.

    This is the unit of currency for the entire streaming subsystem. Once
    created, events are never mutated — only acknowledged, routed, persisted,
    and analyzed.
    """

    model_config = {"frozen": True}

    event_id: str = Field(default_factory=_new_id)
    kind: ChangeKind
    severity: ChangeSeverity
    source: EventSource
    device_id: str           # canonical device ID or hostname if ID not yet resolved
    device_hostname: str | None = None
    interface_name: str | None = None
    detected_at: datetime = Field(default_factory=_utc_now)
    received_at: datetime = Field(default_factory=_utc_now)

    # State transition
    previous_value: Any | None = None
    current_value: Any | None = None
    field_path: str | None = None    # e.g. "interface.oper_state", "bgp.peer.192.0.2.1.state"

    # Topology context
    neighbor_device_id: str | None = None
    neighbor_interface: str | None = None
    vlan_id: int | None = None
    vrf: str | None = None
    prefix: str | None = None
    mac_address: str | None = None

    # Evidence
    raw_payload: dict[str, Any] = Field(default_factory=dict)
    confidence: float = 1.0          # 0.0–1.0
    is_confirmed: bool = True        # False = inferred / probabilistic
    dedup_key: str | None = None     # if set, identical dedup_key events are collapsed

    # Topology patch instruction (set by topology patcher after processing)
    topology_patch_applied: bool = False
    topology_delta_id: str | None = None

    # Operator-facing
    human_summary: str = ""

    def with_summary(self, summary: str) -> "ChangeEvent":
        return self.model_copy(update={"human_summary": summary})

    def with_topology_patch(self, delta_id: str) -> "ChangeEvent":
        return self.model_copy(
            update={"topology_patch_applied": True, "topology_delta_id": delta_id}
        )


# ---------------------------------------------------------------------------
# Flap detection window helper
# ---------------------------------------------------------------------------


class FlapWindow(BaseModel):
    """Tracks rapid up/down transitions to detect interface/BGP flapping."""

    model_config = {"frozen": False}

    device_id: str
    object_key: str     # e.g. "Ethernet1", "192.0.2.1"
    transitions: list[datetime] = Field(default_factory=list)
    flap_window_seconds: float = 60.0
    flap_threshold: int = 3          # transitions within window = flap

    def record_transition(self, at: datetime | None = None) -> None:
        now = at or _utc_now()
        cutoff = now.timestamp() - self.flap_window_seconds
        # Prune old transitions outside the window
        self.transitions = [t for t in self.transitions if t.timestamp() >= cutoff]
        self.transitions.append(now)

    @property
    def is_flapping(self) -> bool:
        return len(self.transitions) >= self.flap_threshold
