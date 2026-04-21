"""IP range inventory source — generates targets from CIDR notation."""

from __future__ import annotations

import ipaddress
from typing import Any

from netobserv.inventory_sources.base import InventoryTarget
from netobserv.observability.logging import get_logger

logger = get_logger("inventory_sources.ip_range")


class IPRangeInventorySource:
    """
    Generates InventoryTarget objects for each address in an IP range.

    Example usage:
        source = IPRangeInventorySource(
            ranges=["10.0.0.0/24"],
            transport="snmp",
            credential_id="snmp-ro",
        )
    """

    source_type = "ip_range"

    def __init__(
        self,
        ranges: list[str],
        *,
        transport: str = "snmp",
        credential_id: str | None = None,
        platform: str | None = None,
        site: str | None = None,
        skip_network_address: bool = True,
        skip_broadcast_address: bool = True,
        max_hosts: int = 1024,
    ) -> None:
        self.ranges = ranges
        self.default_transport = transport
        self.credential_id = credential_id
        self.platform = platform
        self.site = site
        self.skip_network_address = skip_network_address
        self.skip_broadcast_address = skip_broadcast_address
        self.max_hosts = max_hosts

    async def load_targets(self) -> list[InventoryTarget]:
        targets: list[InventoryTarget] = []
        for cidr in self.ranges:
            try:
                network = ipaddress.ip_network(cidr, strict=False)
                hosts = list(network.hosts())
                if len(hosts) > self.max_hosts:
                    logger.warning(
                        "IP range exceeds max_hosts — truncating",
                        cidr=cidr,
                        total=len(hosts),
                        max_hosts=self.max_hosts,
                    )
                    hosts = hosts[: self.max_hosts]

                for ip in hosts:
                    ip_str = str(ip)
                    targets.append(
                        InventoryTarget(
                            hostname=ip_str,
                            management_ip=ip_str,
                            transport=self.default_transport,
                            platform=self.platform,
                            credential_id=self.credential_id,
                            site=self.site,
                            source="ip_range",
                            metadata={"cidr": cidr},
                        )
                    )
            except ValueError as exc:
                logger.error("Invalid CIDR range", cidr=cidr, error=str(exc))

        logger.info(
            "Generated IP range inventory",
            ranges=self.ranges,
            count=len(targets),
        )
        return targets
