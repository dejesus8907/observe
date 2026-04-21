"""Connector registry — maps transport types to connector implementations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from netobserv.connectors.base import BaseConnector, DiscoveryTarget
from netobserv.connectors.ssh import SSHConnector
from netobserv.connectors.rest import RESTConnector
from netobserv.connectors.snmp import SNMPConnector


class ConnectorRegistry:
    """
    Manages connector instances and resolves which connector to use
    for a given discovery target.
    """

    def __init__(self) -> None:
        self._connectors: dict[str, BaseConnector] = {
            "ssh": SSHConnector(),
            "rest": RESTConnector(),
            "snmp": SNMPConnector(),
        }

    def register(self, transport: str, connector: BaseConnector) -> None:
        """Register a custom connector for a given transport type."""
        self._connectors[transport] = connector

    def resolve(self, target: DiscoveryTarget) -> BaseConnector:
        """Return the appropriate connector for a discovery target."""
        transport = target.transport.lower()
        connector = self._connectors.get(transport)
        if not connector:
            raise ValueError(
                f"No connector registered for transport '{transport}'. "
                f"Available: {list(self._connectors.keys())}"
            )
        return connector

    def available_transports(self) -> list[str]:
        return list(self._connectors.keys())
