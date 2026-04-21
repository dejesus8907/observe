"""Primary normalizer — converts ParsedRecords into CanonicalObjects."""

from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Any

from netobserv.models.canonical import (
    CanonicalARPEntry,
    CanonicalBGPSession,
    CanonicalDevice,
    CanonicalInterface,
    CanonicalMACEntry,
    CanonicalNeighborRelationship,
    CanonicalObject,
    CanonicalRoute,
    CanonicalVLAN,
    DataQualityMetadata,
)
from netobserv.models.enums import (
    BGPSessionState,
    DeviceStatus,
    InterfaceAdminState,
    InterfaceOperState,
    InterfaceType,
    NormalizationWarning,
    RouteProtocol,
    VLANStatus,
)
from netobserv.observability.logging import get_logger
from netobserv.parsers.base import ParsedRecord

logger = get_logger("normalizers.canonical")


class CanonicalNormalizer:
    """
    Converts ParsedRecord objects to CanonicalObjects.

    Handles field normalization, enum translation, missing-field defaults,
    and data quality scoring.
    """

    def normalize(
        self,
        parsed_records: list[ParsedRecord],
        snapshot_id: str | None = None,
    ) -> list[CanonicalObject]:
        results: list[CanonicalObject] = []
        for record in parsed_records:
            try:
                obj = self._dispatch(record, snapshot_id)
                if obj is not None:
                    results.append(obj)  # type: ignore[arg-type]
            except Exception as exc:
                logger.warning(
                    "Normalization failed for record",
                    record_type=record.record_type,
                    error=str(exc),
                )
        return results

    def _dispatch(
        self, record: ParsedRecord, snapshot_id: str | None
    ) -> CanonicalObject | None:
        dispatch = {
            "device": self._normalize_device,
            "interface": self._normalize_interface,
            "neighbor": self._normalize_neighbor,
            "route": self._normalize_route,
            "vlan": self._normalize_vlan,
            "bgp_session": self._normalize_bgp_session,
            "arp": self._normalize_arp,
            "mac": self._normalize_mac,
        }
        fn = dispatch.get(record.record_type)
        if fn is None:
            logger.debug("No normalizer for record type", record_type=record.record_type)
            return None
        return fn(record, snapshot_id)  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Device
    # ------------------------------------------------------------------

    def _normalize_device(
        self, record: ParsedRecord, snapshot_id: str | None
    ) -> CanonicalDevice:
        d = record.data
        warnings: list[NormalizationWarning] = []

        hostname = d.get("hostname") or d.get("name") or ""
        if not hostname:
            warnings.append(NormalizationWarning.MISSING_FIELD)

        status_raw = str(d.get("status", "unknown")).lower()
        try:
            status = DeviceStatus(status_raw)
        except ValueError:
            status = DeviceStatus.UNKNOWN
            warnings.append(NormalizationWarning.ENUM_TRANSLATION_FALLBACK)

        quality = DataQualityMetadata(
            source_system=record.source_system,
            source_timestamp=datetime.utcnow(),
            completeness_score=self._completeness(
                d, ["hostname", "vendor", "model", "software_version"]
            ),
            parsing_confidence=record.parsing_confidence,
            normalization_warnings=warnings,
        )

        return CanonicalDevice(
            device_id=str(uuid.uuid4()),
            hostname=hostname,
            management_ip=d.get("management_ip"),
            vendor=d.get("vendor"),
            model=d.get("model"),
            serial_number=d.get("serial_number"),
            software_version=d.get("software_version"),
            device_type=d.get("device_type"),
            device_role=d.get("device_role"),
            site=d.get("site"),
            region=d.get("region"),
            status=status,
            namespace=d.get("namespace"),
            platform=d.get("platform"),
            tags=d.get("tags") or [],
            custom_fields=d.get("custom_fields") or {},
            source_system=record.source_system,
            source_timestamp=datetime.utcnow(),
            snapshot_id=snapshot_id,
            quality=quality,
        )

    # ------------------------------------------------------------------
    # Interface
    # ------------------------------------------------------------------

    def _normalize_interface(
        self, record: ParsedRecord, snapshot_id: str | None
    ) -> CanonicalInterface:
        d = record.data
        warnings: list[NormalizationWarning] = []

        device_id = d.get("device_id") or d.get("device_hostname") or ""
        name = d.get("name") or ""
        normalized_name = self._normalize_interface_name(name)

        admin_raw = str(d.get("admin_state", "unknown")).lower()
        oper_raw = str(d.get("oper_state", "unknown")).lower()

        try:
            admin_state = InterfaceAdminState(admin_raw)
        except ValueError:
            admin_state = InterfaceAdminState.UNKNOWN
            warnings.append(NormalizationWarning.ENUM_TRANSLATION_FALLBACK)

        try:
            oper_state = InterfaceOperState(oper_raw)
        except ValueError:
            oper_state = InterfaceOperState.UNKNOWN
            warnings.append(NormalizationWarning.ENUM_TRANSLATION_FALLBACK)

        if_type = self._infer_interface_type(normalized_name)

        # Ensure ip_addresses is a list
        ip_raw = d.get("ip_addresses")
        if isinstance(ip_raw, str) and ip_raw:
            ip_addresses = [ip_raw]
        elif isinstance(ip_raw, list):
            ip_addresses = ip_raw
        else:
            ip_addresses = []

        # Access VLAN
        access_vlan = d.get("access_vlan")
        if access_vlan is not None:
            try:
                access_vlan = int(access_vlan)
            except (ValueError, TypeError):
                access_vlan = None

        quality = DataQualityMetadata(
            source_system=record.source_system,
            source_timestamp=datetime.utcnow(),
            completeness_score=self._completeness(d, ["name", "admin_state", "oper_state"]),
            parsing_confidence=record.parsing_confidence,
            normalization_warnings=warnings,
        )

        return CanonicalInterface(
            device_id=device_id,
            name=name,
            normalized_name=normalized_name,
            type=if_type,
            admin_state=admin_state,
            oper_state=oper_state,
            mtu=d.get("mtu"),
            speed=d.get("speed") or d.get("bandwidth"),
            description=d.get("description"),
            mac_address=d.get("mac_address"),
            ip_addresses=ip_addresses,
            vrf=d.get("vrf"),
            vlan_mode=d.get("vlan_mode") or d.get("mode"),
            access_vlan=access_vlan,
            trunk_vlans=d.get("trunk_vlans") or [],
            lag_parent=d.get("lag_parent"),
            is_enabled=bool(d.get("enabled", d.get("admin_state", "up") != "down")),
            duplex=d.get("duplex"),
            source_timestamp=datetime.utcnow(),
            snapshot_id=snapshot_id,
            quality=quality,
        )

    # ------------------------------------------------------------------
    # Neighbor
    # ------------------------------------------------------------------

    def _normalize_neighbor(
        self, record: ParsedRecord, snapshot_id: str | None
    ) -> CanonicalNeighborRelationship:
        d = record.data
        return CanonicalNeighborRelationship(
            local_device_id=d.get("local_hostname", d.get("local_device_id", "")),
            local_interface=d.get("local_interface", ""),
            remote_device_id=d.get("remote_hostname") or d.get("remote_device_id"),
            remote_hostname=d.get("remote_hostname"),
            remote_interface=d.get("remote_interface"),
            remote_management_ip=d.get("remote_management_ip"),
            remote_platform=d.get("remote_platform"),
            protocol=d.get("protocol", "lldp"),
            snapshot_id=snapshot_id,
            quality=DataQualityMetadata(
                source_system=record.source_system,
                parsing_confidence=record.parsing_confidence,
            ),
        )

    # ------------------------------------------------------------------
    # Route
    # ------------------------------------------------------------------

    def _normalize_route(
        self, record: ParsedRecord, snapshot_id: str | None
    ) -> CanonicalRoute:
        d = record.data
        proto_raw = str(d.get("protocol", "unknown")).lower()
        try:
            protocol = RouteProtocol(proto_raw)
        except ValueError:
            protocol = RouteProtocol.UNKNOWN

        return CanonicalRoute(
            device_id=d.get("device_hostname", d.get("device_id", "")),
            prefix=d.get("prefix", d.get("network", "")),
            next_hop=d.get("next_hop"),
            next_hop_interface=d.get("next_hop_interface"),
            protocol=protocol,
            metric=d.get("metric"),
            preference=d.get("preference"),
            vrf=d.get("vrf"),
            is_active=bool(d.get("is_active", True)),
            age=d.get("age"),
            snapshot_id=snapshot_id,
            quality=DataQualityMetadata(
                source_system=record.source_system,
                parsing_confidence=record.parsing_confidence,
            ),
        )

    # ------------------------------------------------------------------
    # VLAN
    # ------------------------------------------------------------------

    def _normalize_vlan(
        self, record: ParsedRecord, snapshot_id: str | None
    ) -> CanonicalVLAN:
        d = record.data
        status_raw = str(d.get("status", "unknown")).lower()
        try:
            status = VLANStatus(status_raw)
        except ValueError:
            status = VLANStatus.UNKNOWN

        vid = d.get("vid")
        if vid is None:
            vid = d.get("vlan_id") or 0
        try:
            vid = int(vid)
        except (ValueError, TypeError):
            vid = 0

        return CanonicalVLAN(
            vid=vid,
            name=d.get("name"),
            site=d.get("site"),
            namespace=d.get("namespace"),
            status=status,
            snapshot_id=snapshot_id,
            quality=DataQualityMetadata(
                source_system=record.source_system,
                parsing_confidence=record.parsing_confidence,
            ),
        )

    # ------------------------------------------------------------------
    # BGP Session
    # ------------------------------------------------------------------

    def _normalize_bgp_session(
        self, record: ParsedRecord, snapshot_id: str | None
    ) -> CanonicalBGPSession:
        d = record.data
        state_raw = str(d.get("state", "unknown")).lower()
        try:
            state = BGPSessionState(state_raw)
        except ValueError:
            state = BGPSessionState.UNKNOWN

        return CanonicalBGPSession(
            device_id=d.get("device_hostname", d.get("device_id", "")),
            local_address=d.get("local_address"),
            remote_address=d.get("remote_address", ""),
            local_asn=d.get("local_asn"),
            remote_asn=d.get("remote_asn") or d.get("peerAsn"),
            state=state,
            vrf=d.get("vrf"),
            peer_group=d.get("peer_group"),
            description=d.get("description"),
            uptime=d.get("uptime"),
            prefixes_received=d.get("prefixes_received"),
            prefixes_sent=d.get("prefixes_sent"),
            snapshot_id=snapshot_id,
            quality=DataQualityMetadata(
                source_system=record.source_system,
                parsing_confidence=record.parsing_confidence,
            ),
        )

    # ------------------------------------------------------------------
    # ARP
    # ------------------------------------------------------------------

    def _normalize_arp(
        self, record: ParsedRecord, snapshot_id: str | None
    ) -> CanonicalARPEntry:
        d = record.data
        return CanonicalARPEntry(
            device_id=d.get("device_hostname", d.get("device_id", "")),
            interface=d.get("interface"),
            ip_address=d.get("ip_address", ""),
            mac_address=d.get("mac_address"),
            vrf=d.get("vrf"),
            state=d.get("state", "reachable"),
            age=d.get("age"),
            snapshot_id=snapshot_id,
            quality=DataQualityMetadata(
                source_system=record.source_system,
                parsing_confidence=record.parsing_confidence,
            ),
        )

    # ------------------------------------------------------------------
    # MAC
    # ------------------------------------------------------------------

    def _normalize_mac(
        self, record: ParsedRecord, snapshot_id: str | None
    ) -> CanonicalMACEntry:
        d = record.data
        vlan_id = d.get("vlan_id")
        if vlan_id is not None:
            try:
                vlan_id = int(vlan_id)
            except (ValueError, TypeError):
                vlan_id = None

        return CanonicalMACEntry(
            device_id=d.get("device_hostname", d.get("device_id", "")),
            mac_address=d.get("mac_address", d.get("mac", "")),
            vlan_id=vlan_id,
            interface=d.get("interface"),
            type=d.get("type", "dynamic"),
            age=d.get("age"),
            snapshot_id=snapshot_id,
            quality=DataQualityMetadata(
                source_system=record.source_system,
                parsing_confidence=record.parsing_confidence,
            ),
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _completeness(data: dict[str, Any], required_fields: list[str]) -> float:
        filled = sum(1 for f in required_fields if data.get(f))
        return filled / len(required_fields) if required_fields else 1.0

    @staticmethod
    def _normalize_interface_name(name: str) -> str:
        """Expand common abbreviations for consistent comparison."""
        abbr_map = {
            r"^Gi(\d)": "GigabitEthernet\\1",
            r"^Te(\d)": "TenGigabitEthernet\\1",
            r"^Fa(\d)": "FastEthernet\\1",
            r"^Et(\d)": "Ethernet\\1",
            r"^Lo(\d)": "Loopback\\1",
            r"^Po(\d)": "Port-channel\\1",
            r"^Tu(\d)": "Tunnel\\1",
            r"^Ma(\d)": "Management\\1",
            r"^Vl(\d)": "Vlan\\1",
        }
        for pattern, replacement in abbr_map.items():
            expanded = re.sub(pattern, replacement, name, count=1)
            if expanded != name:
                return expanded
        return name

    @staticmethod
    def _infer_interface_type(name: str) -> InterfaceType:
        nl = name.lower()
        if "loopback" in nl or nl.startswith("lo"):
            return InterfaceType.LOOPBACK
        if "tunnel" in nl or nl.startswith("tu"):
            return InterfaceType.TUNNEL
        if "port-channel" in nl or "lag" in nl or "bond" in nl or "po" in nl:
            return InterfaceType.LAG
        if "management" in nl or nl.startswith("mgmt") or nl.startswith("ma"):
            return InterfaceType.MANAGEMENT
        if "vlan" in nl or nl.startswith("vl"):
            return InterfaceType.VIRTUAL
        if any(
            x in nl
            for x in ("ethernet", "gigabit", "tengigabit", "fastethernet", "hundredgige")
        ):
            return InterfaceType.ETHERNET
        return InterfaceType.UNKNOWN
