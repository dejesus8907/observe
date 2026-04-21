"""Arista EOS-specific parser (eAPI JSON output)."""

from __future__ import annotations

import re
from typing import Any

from netobserv.connectors.base import RawCollectionResult
from netobserv.parsers.base import ParsedRecord


def _normalize_if_name(name: str) -> str:
    """Normalize Arista interface name abbreviations to full form."""
    mapping = {
        "Et": "Ethernet",
        "Gi": "GigabitEthernet",
        "Te": "TenGigabitEthernet",
        "Fo": "FortyGigabitEthernet",
        "Hu": "HundredGigE",
        "Ma": "Management",
        "Lo": "Loopback",
        "Po": "Port-Channel",
        "Vl": "Vlan",
        "Tu": "Tunnel",
    }
    for abbr, full in mapping.items():
        if name.startswith(abbr) and not name.startswith(full):
            return full + name[len(abbr):]
    return name


class EOSParser:
    """Parser for Arista EOS eAPI JSON responses."""

    platform = "eos"

    def parse(self, raw_result: RawCollectionResult) -> list[ParsedRecord]:
        records: list[ParsedRecord] = []
        target = raw_result.target

        # Device facts
        facts = raw_result.raw_device_facts
        if facts:
            records.append(
                ParsedRecord(
                    record_type="device",
                    source_system="eos_eapi",
                    source_platform="eos",
                    data={
                        "hostname": target.hostname,
                        "management_ip": target.management_ip,
                        "vendor": "Arista",
                        "model": facts.get("modelName", ""),
                        "software_version": facts.get("version", ""),
                        "serial_number": facts.get("serialNumber", ""),
                        "platform": facts.get("systemMacAddress", ""),
                    },
                    raw=facts,
                )
            )

        # Interfaces
        # EOS returns interfaces as a dict: {"interfaces": {"Ethernet1": {...}}}
        for iface_raw in raw_result.raw_interfaces:
            # If it came through as a dict of interfaces
            if "interfaces" in iface_raw:
                for iface_name, iface_data in iface_raw["interfaces"].items():
                    records.append(
                        self._parse_interface(target.hostname, iface_name, iface_data)
                    )
            elif "name" in iface_raw or "interfaceType" in iface_raw:
                name = iface_raw.get("name", iface_raw.get("interfaceStatus", ""))
                records.append(self._parse_interface(target.hostname, name, iface_raw))

        # LLDP neighbors
        for neighbor_raw in raw_result.raw_neighbors:
            lldp_neighbors = neighbor_raw.get("lldpNeighbors", [])
            for n in lldp_neighbors:
                records.append(
                    ParsedRecord(
                        record_type="neighbor",
                        source_system="eos_eapi",
                        source_platform="eos",
                        data={
                            "local_hostname": target.hostname,
                            "local_interface": _normalize_if_name(
                                n.get("port", "")
                            ),
                            "remote_hostname": n.get("neighborDevice", ""),
                            "remote_interface": n.get("neighborPort", ""),
                            "protocol": "lldp",
                        },
                        raw=n,
                    )
                )

        # BGP sessions
        for bgp_raw in raw_result.raw_bgp_sessions:
            peers = bgp_raw.get("vrfs", {})
            for vrf_name, vrf_data in peers.items():
                for peer_addr, peer_data in vrf_data.get("peers", {}).items():
                    records.append(
                        ParsedRecord(
                            record_type="bgp_session",
                            source_system="eos_eapi",
                            source_platform="eos",
                            data={
                                "device_hostname": target.hostname,
                                "remote_address": peer_addr,
                                "remote_asn": peer_data.get("peerAsn"),
                                "state": peer_data.get("peerState", "unknown").lower(),
                                "vrf": vrf_name if vrf_name != "default" else None,
                                "uptime": peer_data.get("upDownTime"),
                                "prefixes_received": peer_data.get("prefixReceived"),
                            },
                            raw=peer_data,
                        )
                    )

        # ARP entries
        for arp_raw in raw_result.raw_arp_entries:
            for entry in arp_raw.get("ipV4Neighbors", []):
                records.append(
                    ParsedRecord(
                        record_type="arp",
                        source_system="eos_eapi",
                        source_platform="eos",
                        data={
                            "device_hostname": target.hostname,
                            "ip_address": entry.get("address", ""),
                            "mac_address": entry.get("hwAddress", ""),
                            "interface": entry.get("interface", ""),
                            "age": entry.get("age"),
                        },
                        raw=entry,
                    )
                )

        return records

    def _parse_interface(
        self, hostname: str, name: str, data: dict[str, Any]
    ) -> ParsedRecord:
        line_protocol = data.get("lineProtocolStatus", "").lower()
        interface_status = data.get("interfaceStatus", "").lower()

        admin_state = "down" if "disabled" in interface_status else "up"
        oper_state = "up" if line_protocol == "up" else "down"

        ip_addresses: list[str] = []
        addr_info = data.get("interfaceAddress", {})
        if addr_info:
            primary = addr_info.get("primaryIp", {})
            if primary.get("address") and primary["address"] != "0.0.0.0":
                mask = primary.get("maskLen", 32)
                ip_addresses.append(f"{primary['address']}/{mask}")

        return ParsedRecord(
            record_type="interface",
            source_system="eos_eapi",
            source_platform="eos",
            data={
                "device_hostname": hostname,
                "name": _normalize_if_name(name),
                "description": data.get("description", ""),
                "admin_state": admin_state,
                "oper_state": oper_state,
                "mtu": data.get("mtu"),
                "mac_address": data.get("physicalAddress", ""),
                "speed": data.get("bandwidth"),
                "ip_addresses": ip_addresses,
                "vrf": data.get("forwardingModel"),
            },
            raw=data,
        )
