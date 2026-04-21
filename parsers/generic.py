"""Generic parser — handles raw structured data without platform-specific logic."""

from __future__ import annotations

from typing import Any

from netobserv.connectors.base import RawCollectionResult
from netobserv.parsers.base import ParsedRecord


class GenericParser:
    """Passthrough parser that wraps raw data into ParsedRecord objects."""

    platform = "generic"

    def parse(self, raw_result: RawCollectionResult) -> list[ParsedRecord]:
        records: list[ParsedRecord] = []
        target = raw_result.target
        source = target.platform or "unknown"

        if raw_result.raw_device_facts:
            records.append(
                ParsedRecord(
                    record_type="device",
                    source_system=raw_result.connector_type,
                    source_platform=source,
                    data={
                        "hostname": target.hostname,
                        "management_ip": target.management_ip,
                        **raw_result.raw_device_facts,
                    },
                    raw=raw_result.raw_device_facts,
                )
            )
        else:
            records.append(
                ParsedRecord(
                    record_type="device",
                    source_system=raw_result.connector_type,
                    source_platform=source,
                    data={
                        "hostname": target.hostname,
                        "management_ip": target.management_ip,
                    },
                    parsing_confidence=0.5,
                    warnings=["No device facts collected"],
                )
            )

        for iface in raw_result.raw_interfaces:
            records.append(
                ParsedRecord(
                    record_type="interface",
                    source_system=raw_result.connector_type,
                    source_platform=source,
                    data={"device_hostname": target.hostname, **iface},
                    raw=iface,
                )
            )

        for neighbor in raw_result.raw_neighbors:
            records.append(
                ParsedRecord(
                    record_type="neighbor",
                    source_system=raw_result.connector_type,
                    source_platform=source,
                    data={"local_hostname": target.hostname, **neighbor},
                    raw=neighbor,
                )
            )

        for route in raw_result.raw_routes:
            records.append(
                ParsedRecord(
                    record_type="route",
                    source_system=raw_result.connector_type,
                    source_platform=source,
                    data={"device_hostname": target.hostname, **route},
                    raw=route,
                )
            )

        for vlan in raw_result.raw_vlans:
            records.append(
                ParsedRecord(
                    record_type="vlan",
                    source_system=raw_result.connector_type,
                    source_platform=source,
                    data={"device_hostname": target.hostname, **vlan},
                    raw=vlan,
                )
            )

        for session in raw_result.raw_bgp_sessions:
            records.append(
                ParsedRecord(
                    record_type="bgp_session",
                    source_system=raw_result.connector_type,
                    source_platform=source,
                    data={"device_hostname": target.hostname, **session},
                    raw=session,
                )
            )

        for entry in raw_result.raw_arp_entries:
            records.append(
                ParsedRecord(
                    record_type="arp",
                    source_system=raw_result.connector_type,
                    source_platform=source,
                    data={"device_hostname": target.hostname, **entry},
                    raw=entry,
                )
            )

        for entry in raw_result.raw_mac_entries:
            records.append(
                ParsedRecord(
                    record_type="mac",
                    source_system=raw_result.connector_type,
                    source_platform=source,
                    data={"device_hostname": target.hostname, **entry},
                    raw=entry,
                )
            )

        return records
