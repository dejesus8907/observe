"""Event classifier — maps raw inputs from every ingest source to ChangeEvents.

Each classifier method accepts a raw payload dict and returns a ChangeEvent
or None if the payload does not warrant an event. None means "I saw this, it
is noise at current thresholds" — not "I failed to parse it."

Classifier rules are explicit and documented. There is no ML here. If a DoD
operator needs to audit why an event was classified CRITICAL, they need to
be able to point to a deterministic rule, not a model weight.

Source-specific classifiers:
  GnmiClassifier      — gNMI subscription notifications (OpenConfig path model)
  SnmpTrapClassifier  — SNMP trap PDU parsed fields
  SyslogClassifier    — structured syslog message parsed fields
  SshPollClassifier   — diff between two consecutive SSH poll results

All classifiers operate on the same output type (ChangeEvent) and are
composed by the StreamingEngine.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from netobserv.streaming.events import (
    ChangeEvent,
    ChangeKind,
    ChangeSeverity,
    EventSource,
    FlapWindow,
    default_severity,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _make(
    kind: ChangeKind,
    source: EventSource,
    device_id: str,
    *,
    severity: ChangeSeverity | None = None,
    raw: dict[str, Any] | None = None,
    **kwargs: Any,
) -> ChangeEvent:
    return ChangeEvent(
        kind=kind,
        severity=severity or default_severity(kind),
        source=source,
        device_id=device_id,
        raw_payload=raw or {},
        **kwargs,
    )


# ---------------------------------------------------------------------------
# gNMI Classifier
# ---------------------------------------------------------------------------
# OpenConfig path → ChangeKind mapping.
# Paths are matched with prefix; the most specific match wins.

_GNMI_PATH_RULES: list[tuple[str, str, ChangeKind]] = [
    # interfaces
    ("openconfig-interfaces:interfaces/interface", "state/oper-status", ChangeKind.INTERFACE_DOWN),
    ("openconfig-interfaces:interfaces/interface", "state/oper-status", ChangeKind.INTERFACE_UP),
    ("openconfig-interfaces:interfaces/interface", "config/description", ChangeKind.INTERFACE_DESCRIPTION_CHANGE),
    # BGP
    ("openconfig-bgp:bgp/neighbors/neighbor", "state/session-state", ChangeKind.BGP_SESSION_ESTABLISHED),
    ("openconfig-bgp:bgp/neighbors/neighbor", "state/session-state", ChangeKind.BGP_SESSION_DROPPED),
    ("openconfig-bgp:bgp/neighbors/neighbor", "state/prefixes/received", ChangeKind.BGP_PREFIX_WITHDRAW_MASS),
    # OSPF
    ("openconfig-ospfv2:ospfv2", "areas/area/lsdb", ChangeKind.OSPF_LSA_FLOOD),
    # LLDP neighbors
    ("openconfig-lldp:lldp/interfaces/interface", "neighbors/neighbor", ChangeKind.NEIGHBOR_DISCOVERED),
]


class GnmiClassifier:
    """Classify gNMI subscription notifications.

    gNMI notifications arrive as:
      {
        "prefix": "openconfig-interfaces:interfaces/interface[name=Ethernet1]",
        "path": "state/oper-status",
        "val": "DOWN",
        "timestamp_ns": 1700000000000000000,
        "device_id": "router1"
      }
    """

    # BGP session state strings from OpenConfig
    _BGP_UP_STATES = {"ESTABLISHED"}
    _BGP_DOWN_STATES = {"IDLE", "ACTIVE", "CONNECT", "OPENSENT", "OPENCONFIRM"}
    _BGP_MASS_WITHDRAW_THRESHOLD = 100   # prefix drop by this many = mass withdraw

    def __init__(self, flap_tracker: dict[str, FlapWindow] | None = None) -> None:
        self._flap: dict[str, FlapWindow] = flap_tracker or {}

    def classify(self, notification: dict[str, Any]) -> ChangeEvent | None:
        device_id: str = str(notification.get("device_id", "unknown"))
        prefix: str = str(notification.get("prefix", ""))
        path: str = str(notification.get("path", ""))
        val: Any = notification.get("val")
        ts_ns: int | None = notification.get("timestamp_ns")
        detected_at = (
            datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)
            if ts_ns
            else _utc_now()
        )

        full_path = f"{prefix}/{path}".lower()

        # Interface oper-status
        if "oper-status" in path:
            iface_name = self._extract_key(prefix, "name") or self._extract_key(prefix, "interface")
            val_str = str(val).upper()
            if val_str in ("DOWN", "LOWER_LAYER_DOWN", "NOT_PRESENT"):
                kind = self._check_flap(device_id, iface_name or "unknown", ChangeKind.INTERFACE_DOWN, detected_at)
                return _make(
                    kind, EventSource.GNMI, device_id,
                    interface_name=iface_name,
                    previous_value=None,
                    current_value=val_str,
                    field_path="interface.oper_state",
                    detected_at=detected_at,
                    raw=notification,
                    human_summary=f"{device_id} {iface_name} is {val_str}",
                )
            elif val_str in ("UP",):
                kind = self._check_flap(device_id, iface_name or "unknown", ChangeKind.INTERFACE_UP, detected_at)
                return _make(
                    kind, EventSource.GNMI, device_id,
                    interface_name=iface_name,
                    current_value=val_str,
                    field_path="interface.oper_state",
                    detected_at=detected_at,
                    raw=notification,
                    human_summary=f"{device_id} {iface_name} is UP",
                )

        # BGP session state
        if "session-state" in path:
            peer_addr = self._extract_key(prefix, "neighbor-address") or self._extract_key(prefix, "peer-address")
            val_str = str(val).upper()
            if val_str in self._BGP_UP_STATES:
                return _make(
                    ChangeKind.BGP_SESSION_ESTABLISHED, EventSource.GNMI, device_id,
                    neighbor_device_id=peer_addr,
                    current_value=val_str,
                    field_path="bgp.session_state",
                    detected_at=detected_at,
                    raw=notification,
                    human_summary=f"{device_id} BGP session to {peer_addr} ESTABLISHED",
                )
            elif val_str in self._BGP_DOWN_STATES:
                kind = self._check_flap(device_id, f"bgp:{peer_addr}", ChangeKind.BGP_SESSION_DROPPED, detected_at)
                return _make(
                    kind, EventSource.GNMI, device_id,
                    neighbor_device_id=peer_addr,
                    current_value=val_str,
                    field_path="bgp.session_state",
                    detected_at=detected_at,
                    raw=notification,
                    human_summary=f"{device_id} BGP session to {peer_addr} {val_str}",
                )

        # Interface description
        if "description" in path and "interface" in full_path:
            iface_name = self._extract_key(prefix, "name")
            return _make(
                ChangeKind.INTERFACE_DESCRIPTION_CHANGE, EventSource.GNMI, device_id,
                interface_name=iface_name,
                current_value=val,
                field_path="interface.description",
                detected_at=detected_at,
                raw=notification,
                human_summary=f"{device_id} {iface_name} description changed to '{val}'",
            )

        # LLDP neighbor
        if "lldp" in full_path and "neighbor" in path:
            iface_name = self._extract_key(prefix, "name")
            remote_id = notification.get("neighbor_id") or str(val)
            return _make(
                ChangeKind.NEIGHBOR_DISCOVERED, EventSource.GNMI, device_id,
                interface_name=iface_name,
                neighbor_device_id=remote_id,
                detected_at=detected_at,
                raw=notification,
                human_summary=f"{device_id} discovered neighbor {remote_id} on {iface_name}",
            )

        return None

    def _check_flap(
        self,
        device_id: str,
        key: str,
        base_kind: ChangeKind,
        at: datetime,
    ) -> ChangeKind:
        """Return FLAP variant if transitions exceed threshold within window."""
        tracker_key = f"{device_id}:{key}"
        if tracker_key not in self._flap:
            self._flap[tracker_key] = FlapWindow(device_id=device_id, object_key=key)
        fw = self._flap[tracker_key]
        fw.record_transition(at)
        if fw.is_flapping:
            if base_kind in (ChangeKind.INTERFACE_UP, ChangeKind.INTERFACE_DOWN):
                return ChangeKind.INTERFACE_FLAP
            if base_kind in (ChangeKind.BGP_SESSION_ESTABLISHED, ChangeKind.BGP_SESSION_DROPPED):
                return ChangeKind.BGP_PEER_FLAP
        return base_kind

    @staticmethod
    def _extract_key(path: str, key: str) -> str | None:
        """Extract a key value from a gNMI path like ...interface[name=Ethernet1]..."""
        pattern = rf"\[{re.escape(key)}=([^\]]+)\]"
        match = re.search(pattern, path)
        return match.group(1) if match else None


# ---------------------------------------------------------------------------
# SNMP Trap Classifier
# ---------------------------------------------------------------------------
# OID-based dispatch table.

_SNMP_OID_RULES: dict[str, ChangeKind] = {
    "1.3.6.1.6.3.1.1.5.3": ChangeKind.INTERFACE_DOWN,       # linkDown
    "1.3.6.1.6.3.1.1.5.4": ChangeKind.INTERFACE_UP,         # linkUp
    "1.3.6.1.6.3.1.1.5.1": ChangeKind.DEVICE_REACHABILITY_RESTORED,  # coldStart
    "1.3.6.1.6.3.1.1.5.2": ChangeKind.DEVICE_REBOOTED,      # warmStart
    "1.3.6.1.2.1.2.2.1.8": ChangeKind.INTERFACE_DOWN,       # ifOperStatus change
    # Cisco-specific
    "1.3.6.1.4.1.9.9.187.0.0.1": ChangeKind.BGP_SESSION_ESTABLISHED,  # ciscoBgpPeerTransitions
    "1.3.6.1.4.1.9.9.187.0.0.2": ChangeKind.BGP_SESSION_DROPPED,
    "1.3.6.1.4.1.9.9.117.0.0.1": ChangeKind.DEVICE_REBOOTED,         # ccmCLIRunningConfigChanged
}


class SnmpTrapClassifier:
    """Classify SNMP trap PDUs.

    Expected input format:
      {
        "device_id": "router1",
        "source_ip": "10.0.0.1",
        "enterprise_oid": "1.3.6.1.6.3.1.1.5.3",
        "trap_oid": "1.3.6.1.6.3.1.1.5.3",
        "varbinds": {"1.3.6.1.2.1.2.2.1.1.5": "5", ...},
        "timestamp": "2026-04-21T12:00:00Z"
      }
    """

    def classify(self, trap: dict[str, Any]) -> ChangeEvent | None:
        device_id: str = str(trap.get("device_id", trap.get("source_ip", "unknown")))
        trap_oid: str = str(trap.get("trap_oid", trap.get("enterprise_oid", "")))
        varbinds: dict[str, Any] = trap.get("varbinds", {})

        # Resolve ChangeKind from OID (longest prefix wins)
        kind: ChangeKind | None = None
        best_len = 0
        for oid, candidate_kind in _SNMP_OID_RULES.items():
            if trap_oid.startswith(oid) and len(oid) > best_len:
                kind = candidate_kind
                best_len = len(oid)

        if kind is None:
            return _make(
                ChangeKind.SNMP_TRAP_UNCLASSIFIED, EventSource.SNMP_TRAP, device_id,
                raw=trap,
                human_summary=f"Unclassified SNMP trap OID {trap_oid} from {device_id}",
            )

        # Extract interface name from ifDescr varbind if available
        iface_name: str | None = None
        for oid_key, val in varbinds.items():
            if "2.1.2" in oid_key:   # ifDescr family
                iface_name = str(val)
                break

        return _make(
            kind, EventSource.SNMP_TRAP, device_id,
            interface_name=iface_name,
            raw=trap,
            human_summary=f"{device_id}: {kind.value}" + (f" on {iface_name}" if iface_name else ""),
        )


# ---------------------------------------------------------------------------
# Syslog Classifier
# ---------------------------------------------------------------------------

_SYSLOG_PATTERNS: list[tuple[re.Pattern[str], ChangeKind]] = [
    # Interface state
    (re.compile(r"Interface\s+(\S+).*?changed state to (down|administratively down)", re.IGNORECASE), ChangeKind.INTERFACE_DOWN),
    (re.compile(r"Interface\s+(\S+).*?changed state to up", re.IGNORECASE), ChangeKind.INTERFACE_UP),
    (re.compile(r"(\S+)\s+err-disabled", re.IGNORECASE), ChangeKind.INTERFACE_ERR_DISABLED),
    # BGP
    (re.compile(r"BGP.*?(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}).*?state.*?Established", re.IGNORECASE), ChangeKind.BGP_SESSION_ESTABLISHED),
    (re.compile(r"BGP.*?(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}).*?went from.*?to\s+(?:Idle|Active|Connect)", re.IGNORECASE), ChangeKind.BGP_SESSION_DROPPED),
    # OSPF
    (re.compile(r"OSPF.*?neighbor.*?(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}).*?FULL", re.IGNORECASE), ChangeKind.OSPF_ADJACENCY_UP),
    (re.compile(r"OSPF.*?neighbor.*?(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}).*?(?:down|dead)", re.IGNORECASE), ChangeKind.OSPF_ADJACENCY_DOWN),
    # Config change
    (re.compile(r"SYS-5-CONFIG_I|Configured from|commit.*success", re.IGNORECASE), ChangeKind.CONFIGURATION_CHANGE),
    # Reboot
    (re.compile(r"SYS-5-RELOAD|Rebooting|System restarting", re.IGNORECASE), ChangeKind.DEVICE_REBOOTED),
    # MAC move
    (re.compile(r"MAC.*?moved|mac-move detected", re.IGNORECASE), ChangeKind.MAC_MOVE),
    # LLDP
    (re.compile(r"LLDP.*?neighbor.*?discovered|new neighbor", re.IGNORECASE), ChangeKind.NEIGHBOR_DISCOVERED),
    (re.compile(r"LLDP.*?neighbor.*?aged out|neighbor.*?lost", re.IGNORECASE), ChangeKind.NEIGHBOR_LOST),
]

_IP_RE = re.compile(r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}")
_IFACE_RE = re.compile(r"\b(Ethernet|GigabitEthernet|TenGigabitEthernet|FastEthernet|Loopback|Port-channel|Vlan|Management)[\d/\.]+", re.IGNORECASE)


class SyslogClassifier:
    """Classify structured syslog messages.

    Expected input format (post-parsing, not raw RFC 5424 bytes):
      {
        "device_id": "router1",
        "device_ip": "10.0.0.1",
        "facility": 23,
        "severity": 5,
        "timestamp": "2026-04-21T12:00:01Z",
        "hostname": "router1",
        "program": "BGP",
        "message": "%BGP-5-ADJCHANGE: neighbor 10.0.0.2 Up"
      }
    """

    def classify(self, log: dict[str, Any]) -> ChangeEvent | None:
        device_id: str = str(log.get("device_id", log.get("hostname", log.get("device_ip", "unknown"))))
        message: str = str(log.get("message", ""))
        ts_str: str | None = log.get("timestamp")
        detected_at = _parse_ts(ts_str) if ts_str else _utc_now()

        for pattern, kind in _SYSLOG_PATTERNS:
            match = pattern.search(message)
            if match:
                iface_match = _IFACE_RE.search(message)
                ip_match = _IP_RE.search(message)
                return _make(
                    kind, EventSource.SYSLOG, device_id,
                    interface_name=iface_match.group(0) if iface_match else None,
                    neighbor_device_id=ip_match.group(0) if ip_match and kind in (
                        ChangeKind.BGP_SESSION_ESTABLISHED,
                        ChangeKind.BGP_SESSION_DROPPED,
                        ChangeKind.OSPF_ADJACENCY_UP,
                        ChangeKind.OSPF_ADJACENCY_DOWN,
                    ) else None,
                    detected_at=detected_at,
                    raw=log,
                    human_summary=f"{device_id}: {message[:120]}",
                )

        return _make(
            ChangeKind.SYSLOG_UNCLASSIFIED, EventSource.SYSLOG, device_id,
            severity=ChangeSeverity.INFO,
            detected_at=detected_at,
            raw=log,
            human_summary=f"{device_id}: {message[:80]}",
        )


# ---------------------------------------------------------------------------
# SSH Poll Diff Classifier
# ---------------------------------------------------------------------------

class SshPollClassifier:
    """Produce ChangeEvents from diffs between consecutive SSH poll results.

    The SSH connector runs lightweight targeted commands (show interface status,
    show lldp neighbors, show bgp summary) at a configured interval. This
    classifier diffs two consecutive result sets and emits one ChangeEvent per
    detected state transition.

    This is the fallback for devices that do not support gNMI/NETCONF/SNMP traps.
    It is less real-time (limited by poll interval) but still far better than
    full snapshot collection — it only queries state fields, not full configs.
    """

    def diff_interfaces(
        self,
        device_id: str,
        previous: dict[str, dict[str, Any]],
        current: dict[str, dict[str, Any]],
        flap_tracker: dict[str, FlapWindow] | None = None,
    ) -> list[ChangeEvent]:
        """Diff two interface status dicts: {iface_name: {oper_state, admin_state, ...}}"""
        events: list[ChangeEvent] = []
        now = _utc_now()
        flap: dict[str, FlapWindow] = flap_tracker or {}

        all_ifaces = set(previous.keys()) | set(current.keys())
        for iface in all_ifaces:
            prev = previous.get(iface, {})
            curr = current.get(iface, {})

            prev_oper = str(prev.get("oper_state", "unknown")).lower()
            curr_oper = str(curr.get("oper_state", "unknown")).lower()

            if prev_oper == curr_oper:
                continue

            if curr_oper == "up":
                kind = ChangeKind.INTERFACE_UP
            elif curr_oper in ("down", "lower_layer_down", "not_present"):
                key = f"{device_id}:{iface}"
                if key not in flap:
                    flap[key] = FlapWindow(device_id=device_id, object_key=iface)
                flap[key].record_transition(now)
                kind = ChangeKind.INTERFACE_FLAP if flap[key].is_flapping else ChangeKind.INTERFACE_DOWN
            else:
                kind = ChangeKind.INTERFACE_DOWN

            events.append(_make(
                kind, EventSource.SSH_POLL_DIFF, device_id,
                interface_name=iface,
                previous_value=prev_oper,
                current_value=curr_oper,
                field_path="interface.oper_state",
                detected_at=now,
                raw={"previous": prev, "current": curr},
                human_summary=f"{device_id} {iface}: {prev_oper} → {curr_oper}",
            ))

        return events

    def diff_neighbors(
        self,
        device_id: str,
        previous: dict[str, dict[str, Any]],
        current: dict[str, dict[str, Any]],
    ) -> list[ChangeEvent]:
        """Diff two LLDP neighbor dicts: {local_iface: {remote_host, remote_iface, ...}}"""
        events: list[ChangeEvent] = []
        now = _utc_now()

        all_ports = set(previous.keys()) | set(current.keys())
        for port in all_ports:
            prev_n = previous.get(port)
            curr_n = current.get(port)

            if prev_n is None and curr_n is not None:
                events.append(_make(
                    ChangeKind.NEIGHBOR_DISCOVERED, EventSource.SSH_POLL_DIFF, device_id,
                    interface_name=port,
                    neighbor_device_id=str(curr_n.get("remote_hostname", "")),
                    neighbor_interface=str(curr_n.get("remote_interface", "")),
                    current_value=curr_n,
                    detected_at=now,
                    raw=curr_n,
                    human_summary=f"{device_id} port {port}: new neighbor {curr_n.get('remote_hostname')}",
                ))
            elif prev_n is not None and curr_n is None:
                events.append(_make(
                    ChangeKind.NEIGHBOR_LOST, EventSource.SSH_POLL_DIFF, device_id,
                    interface_name=port,
                    neighbor_device_id=str(prev_n.get("remote_hostname", "")),
                    previous_value=prev_n,
                    detected_at=now,
                    raw=prev_n,
                    human_summary=f"{device_id} port {port}: lost neighbor {prev_n.get('remote_hostname')}",
                ))
            elif prev_n and curr_n:
                prev_host = str(prev_n.get("remote_hostname", ""))
                curr_host = str(curr_n.get("remote_hostname", ""))
                if prev_host != curr_host:
                    events.append(_make(
                        ChangeKind.NEIGHBOR_CHANGED, EventSource.SSH_POLL_DIFF, device_id,
                        interface_name=port,
                        neighbor_device_id=curr_host,
                        previous_value=prev_host,
                        current_value=curr_host,
                        detected_at=now,
                        raw={"previous": prev_n, "current": curr_n},
                        human_summary=f"{device_id} port {port}: neighbor changed from {prev_host} to {curr_host}",
                    ))

        return events

    def diff_bgp_sessions(
        self,
        device_id: str,
        previous: dict[str, str],
        current: dict[str, str],
        flap_tracker: dict[str, FlapWindow] | None = None,
    ) -> list[ChangeEvent]:
        """Diff two BGP session state dicts: {peer_addr: state_string}"""
        events: list[ChangeEvent] = []
        now = _utc_now()
        flap: dict[str, FlapWindow] = flap_tracker or {}

        all_peers = set(previous.keys()) | set(current.keys())
        for peer in all_peers:
            prev_state = previous.get(peer, "").upper()
            curr_state = current.get(peer, "").upper()
            if prev_state == curr_state:
                continue

            if curr_state == "ESTABLISHED":
                kind = ChangeKind.BGP_SESSION_ESTABLISHED
            else:
                key = f"{device_id}:bgp:{peer}"
                if key not in flap:
                    flap[key] = FlapWindow(device_id=device_id, object_key=f"bgp:{peer}")
                flap[key].record_transition(now)
                kind = ChangeKind.BGP_PEER_FLAP if flap[key].is_flapping else ChangeKind.BGP_SESSION_DROPPED

            events.append(_make(
                kind, EventSource.SSH_POLL_DIFF, device_id,
                neighbor_device_id=peer,
                previous_value=prev_state,
                current_value=curr_state,
                field_path="bgp.session_state",
                detected_at=now,
                raw={"peer": peer, "previous": prev_state, "current": curr_state},
                human_summary=f"{device_id} BGP {peer}: {prev_state} → {curr_state}",
            ))

        return events


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_ts(ts: str) -> datetime:
    try:
        return datetime.fromisoformat(ts.rstrip("Z")).replace(tzinfo=timezone.utc)
    except (ValueError, AttributeError):
        return _utc_now()


from netobserv.streaming.conflict.integration import evidence_record_from_change_event as evidence_record_from_streaming_event


def classify_to_evidence(event: ChangeEvent):
    """Convert a classified ChangeEvent into an EvidenceRecord when applicable.

    This is the Phase 3 bridge toward moving arbitration earlier in the pipeline.
    Unsupported event kinds return None.
    """
    return evidence_record_from_streaming_event(event)


def classify_many_to_evidence(events: list[ChangeEvent]):
    """Convert a batch of classified ChangeEvents into EvidenceRecords where supported."""
    evidence_records = []
    passthrough_events = []
    for event in events:
        evidence = classify_to_evidence(event)
        if evidence is None:
            passthrough_events.append(event)
        else:
            evidence_records.append(evidence)
    return evidence_records, passthrough_events
