"""Cisco IOS/IOS-XE parser (text output — no JSON mode available by default)."""

from __future__ import annotations

import re
from typing import Any

from netobserv.connectors.base import RawCollectionResult
from netobserv.parsers.base import ParsedRecord


_IF_STATUS_RE = re.compile(
    r"^(?P<name>\S+)\s+is\s+(?P<admin>[\w ]+),\s+line protocol is\s+(?P<oper>\w+)",
    re.IGNORECASE,
)
_IF_DESC_RE = re.compile(r"^\s+Description:\s+(.+)$", re.IGNORECASE)
_IF_MTU_RE = re.compile(r"MTU\s+(\d+)", re.IGNORECASE)
_IF_BW_RE = re.compile(r"BW\s+(\d+)\s+Kbit", re.IGNORECASE)
_IF_MAC_RE = re.compile(r"address is\s+([\w.]+)", re.IGNORECASE)
_IF_IP_RE = re.compile(r"Internet address is\s+(\S+)", re.IGNORECASE)

_LLDP_NEIGHBOR_RE = re.compile(
    r"Device ID:\s*(?P<device_id>\S+).*?"
    r"Port ID:\s*(?P<port_id>\S+).*?"
    r"Local Intf:\s*(?P<local_intf>\S+)",
    re.DOTALL | re.IGNORECASE,
)


def _normalize_if_name(name: str) -> str:
    abbr_map = {
        "Gi": "GigabitEthernet",
        "Te": "TenGigabitEthernet",
        "Fa": "FastEthernet",
        "Hu": "HundredGigE",
        "Fo": "FortyGigabitEthernet",
        "Ma": "Management",
        "Lo": "Loopback",
        "Po": "Port-channel",
        "Tu": "Tunnel",
        "Vl": "Vlan",
    }
    for abbr, full in abbr_map.items():
        if name.startswith(abbr) and not name.startswith(full):
            return full + name[len(abbr):]
    return name


class IOSParser:
    """Parser for Cisco IOS / IOS-XE text output."""

    platform = "ios"

    def parse(self, raw_result: RawCollectionResult) -> list[ParsedRecord]:
        records: list[ParsedRecord] = []
        target = raw_result.target

        # Device facts from raw text
        facts = raw_result.raw_device_facts
        hostname = target.hostname
        model = ""
        version = ""
        serial = ""

        if isinstance(facts, dict):
            raw_text = facts.get("raw", "")
            if raw_text:
                model_m = re.search(r"Cisco\s+(\S+)\s+processor", raw_text, re.IGNORECASE)
                version_m = re.search(r"Version\s+([\d.()a-zA-Z]+)", raw_text)
                serial_m = re.search(r"Processor board ID\s+(\S+)", raw_text, re.IGNORECASE)
                model = model_m.group(1) if model_m else ""
                version = version_m.group(1) if version_m else ""
                serial = serial_m.group(1) if serial_m else ""

        records.append(
            ParsedRecord(
                record_type="device",
                source_system="ssh",
                source_platform="ios",
                data={
                    "hostname": hostname,
                    "management_ip": target.management_ip,
                    "vendor": "Cisco",
                    "model": model,
                    "software_version": version,
                    "serial_number": serial,
                },
                raw=facts,
                parsing_confidence=0.8 if model else 0.4,
            )
        )

        # Interfaces from raw text
        for iface_raw in raw_result.raw_interfaces:
            raw_text = iface_raw.get("raw", "") if isinstance(iface_raw, dict) else str(iface_raw)
            if raw_text:
                records.extend(self._parse_interfaces_text(hostname, raw_text))

        # LLDP neighbors from raw text
        for neighbor_raw in raw_result.raw_neighbors:
            raw_text = (
                neighbor_raw.get("raw", "") if isinstance(neighbor_raw, dict) else str(neighbor_raw)
            )
            if raw_text:
                records.extend(self._parse_lldp_text(hostname, raw_text))

        return records

    def _parse_interfaces_text(
        self, hostname: str, text: str
    ) -> list[ParsedRecord]:
        records: list[ParsedRecord] = []
        current: dict[str, Any] | None = None

        for line in text.splitlines():
            status_match = _IF_STATUS_RE.match(line)
            if status_match:
                if current:
                    records.append(
                        ParsedRecord(
                            record_type="interface",
                            source_system="ssh",
                            source_platform="ios",
                            data=current,
                            parsing_confidence=0.8,
                        )
                    )
                admin_raw = status_match.group("admin").strip().lower()
                oper_raw = status_match.group("oper").strip().lower()
                current = {
                    "device_hostname": hostname,
                    "name": _normalize_if_name(status_match.group("name")),
                    "admin_state": "up" if "up" in admin_raw else "down",
                    "oper_state": "up" if oper_raw == "up" else "down",
                    "ip_addresses": [],
                }
                continue

            if current is None:
                continue

            desc_m = _IF_DESC_RE.match(line)
            if desc_m:
                current["description"] = desc_m.group(1).strip()
                continue

            mtu_m = _IF_MTU_RE.search(line)
            if mtu_m:
                current["mtu"] = int(mtu_m.group(1))

            bw_m = _IF_BW_RE.search(line)
            if bw_m:
                current["speed"] = int(bw_m.group(1))

            mac_m = _IF_MAC_RE.search(line)
            if mac_m:
                current["mac_address"] = mac_m.group(1)

            ip_m = _IF_IP_RE.search(line)
            if ip_m:
                current["ip_addresses"].append(ip_m.group(1))  # type: ignore[union-attr]

        if current:
            records.append(
                ParsedRecord(
                    record_type="interface",
                    source_system="ssh",
                    source_platform="ios",
                    data=current,
                    parsing_confidence=0.8,
                )
            )
        return records

    def _parse_lldp_text(self, hostname: str, text: str) -> list[ParsedRecord]:
        records: list[ParsedRecord] = []
        # Split into neighbor blocks
        blocks = re.split(r"-{10,}", text)
        for block in blocks:
            device_id = ""
            port_id = ""
            local_intf = ""
            mgmt_ip = ""

            device_m = re.search(r"Device ID:\s*(\S+)", block)
            port_m = re.search(r"Port ID \(.*?\):\s*(\S+)", block)
            local_m = re.search(r"Local Intf:\s*(\S+)", block)
            mgmt_m = re.search(r"Management address.*?(\d{1,3}(?:\.\d{1,3}){3})", block)

            if device_m:
                device_id = device_m.group(1)
            if port_m:
                port_id = port_m.group(1)
            if local_m:
                local_intf = local_m.group(1)
            if mgmt_m:
                mgmt_ip = mgmt_m.group(1)

            if device_id and local_intf:
                records.append(
                    ParsedRecord(
                        record_type="neighbor",
                        source_system="ssh",
                        source_platform="ios",
                        data={
                            "local_hostname": hostname,
                            "local_interface": _normalize_if_name(local_intf),
                            "remote_hostname": device_id,
                            "remote_interface": port_id,
                            "remote_management_ip": mgmt_ip,
                            "protocol": "lldp",
                        },
                        parsing_confidence=0.75,
                    )
                )
        return records
