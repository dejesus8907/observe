"""NetBox intended-state parser — converts NetBox API data to canonical ParsedRecords."""

from __future__ import annotations

from typing import Any

from netobserv.connectors.base import RawCollectionResult
from netobserv.parsers.base import ParsedRecord


class NetBoxIntendedStateParser:
    """
    Parses NetBox REST API response data (intended state).

    NetBox responses use a standard pagination envelope with 'results' list.
    Each result is a dict with nested objects for site, role, type, etc.
    """

    platform = "netbox"

    def parse(self, raw_result: RawCollectionResult) -> list[ParsedRecord]:
        records: list[ParsedRecord] = []
        data = raw_result.data

        if data.get("source") != "netbox":
            # Not a NetBox payload
            return records

        for device in data.get("devices", []):
            records.append(self._parse_device(device))

        for iface in data.get("interfaces", []):
            records.append(self._parse_interface(iface))

        for vlan in data.get("vlans", []):
            records.append(self._parse_vlan(vlan))

        for ip in data.get("ip_addresses", []):
            records.append(self._parse_ip_address(ip))

        for cable in data.get("cables", []):
            records.append(self._parse_cable(cable))

        return records

    def _parse_device(self, d: dict[str, Any]) -> ParsedRecord:
        return ParsedRecord(
            record_type="device",
            source_system="netbox",
            source_platform="netbox",
            data={
                "device_id": str(d.get("id", "")),
                "hostname": d.get("name", ""),
                "management_ip": self._extract_ip(d.get("primary_ip")),
                "vendor": self._nested_name(d.get("device_type", {}).get("manufacturer")),
                "model": self._nested_name(d.get("device_type")),
                "serial_number": d.get("serial", ""),
                "device_type": self._nested_name(d.get("device_type")),
                "device_role": self._nested_name(d.get("device_role") or d.get("role")),
                "site": self._nested_name(d.get("site")),
                "status": self._nested_value(d.get("status"), "value"),
                "tags": [t.get("slug", t.get("name", "")) for t in d.get("tags", [])],
                "custom_fields": d.get("custom_fields", {}),
                "intended": True,
            },
            raw=d,
            parsing_confidence=1.0,
        )

    def _parse_interface(self, i: dict[str, Any]) -> ParsedRecord:
        return ParsedRecord(
            record_type="interface",
            source_system="netbox",
            source_platform="netbox",
            data={
                "interface_id": str(i.get("id", "")),
                "device_hostname": self._nested_name(i.get("device")),
                "device_id": str(i.get("device", {}).get("id", "")),
                "name": i.get("name", ""),
                "type": self._nested_value(i.get("type"), "value"),
                "description": i.get("description", ""),
                "mac_address": i.get("mac_address", ""),
                "mtu": i.get("mtu"),
                "mode": self._nested_value(i.get("mode"), "value"),
                "access_vlan": self._nested_id(i.get("untagged_vlan")),
                "trunk_vlans": [
                    v.get("vid") for v in i.get("tagged_vlans", []) if v.get("vid")
                ],
                "lag_parent": self._nested_name(i.get("lag")),
                "enabled": i.get("enabled", True),
                "intended": True,
            },
            raw=i,
            parsing_confidence=1.0,
        )

    def _parse_vlan(self, v: dict[str, Any]) -> ParsedRecord:
        return ParsedRecord(
            record_type="vlan",
            source_system="netbox",
            source_platform="netbox",
            data={
                "vlan_id": str(v.get("id", "")),
                "vid": v.get("vid"),
                "name": v.get("name", ""),
                "site": self._nested_name(v.get("site")),
                "status": self._nested_value(v.get("status"), "value"),
                "intended": True,
            },
            raw=v,
            parsing_confidence=1.0,
        )

    def _parse_ip_address(self, ip: dict[str, Any]) -> ParsedRecord:
        return ParsedRecord(
            record_type="ip_address",
            source_system="netbox",
            source_platform="netbox",
            data={
                "address": ip.get("address", ""),
                "status": self._nested_value(ip.get("status"), "value"),
                "dns_name": ip.get("dns_name", ""),
                "family": ip.get("family", {}).get("value", 4),
                "device_id": self._extract_assigned_object_id(ip),
                "intended": True,
            },
            raw=ip,
            parsing_confidence=1.0,
        )

    def _parse_cable(self, c: dict[str, Any]) -> ParsedRecord:
        """NetBox cable records map to intended topology edges."""
        a_term = c.get("a_terminations", [{}])[0] if c.get("a_terminations") else {}
        b_term = c.get("b_terminations", [{}])[0] if c.get("b_terminations") else {}
        return ParsedRecord(
            record_type="neighbor",
            source_system="netbox",
            source_platform="netbox",
            data={
                "local_interface_id": str(a_term.get("object_id", "")),
                "remote_interface_id": str(b_term.get("object_id", "")),
                "local_interface": a_term.get("object", {}).get("name", ""),
                "remote_interface": b_term.get("object", {}).get("name", ""),
                "local_hostname": a_term.get("object", {})
                .get("device", {})
                .get("name", ""),
                "remote_hostname": b_term.get("object", {})
                .get("device", {})
                .get("name", ""),
                "protocol": "netbox_cable",
                "intended": True,
            },
            raw=c,
            parsing_confidence=1.0,
        )

    @staticmethod
    def _nested_name(obj: Any) -> str | None:
        if isinstance(obj, dict):
            return obj.get("name") or obj.get("display") or str(obj.get("id", "")) or None
        return None

    @staticmethod
    def _nested_value(obj: Any, key: str) -> str | None:
        if isinstance(obj, dict):
            return obj.get(key)
        return None

    @staticmethod
    def _nested_id(obj: Any) -> Any:
        if isinstance(obj, dict):
            return obj.get("vid") or obj.get("id")
        return None

    @staticmethod
    def _extract_ip(obj: Any) -> str | None:
        if isinstance(obj, dict):
            addr = obj.get("address", "")
            # Strip CIDR prefix
            return addr.split("/")[0] if addr else None
        return None

    @staticmethod
    def _extract_assigned_object_id(ip: dict[str, Any]) -> str | None:
        assigned = ip.get("assigned_object")
        if isinstance(assigned, dict):
            device = assigned.get("device", {})
            if isinstance(device, dict):
                return str(device.get("id", ""))
        return None
