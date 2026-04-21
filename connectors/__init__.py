"""Discovery connector implementations."""

from netobserv.connectors.base import (
    DiscoveryConnector,
    DiscoveryTarget,
    CollectionContext,
    RawCollectionResult,
    ConnectorCredential,
    ConnectorError,
    ConnectorTimeoutError,
    ConnectorAuthError,
)
from netobserv.connectors.ssh import SSHConnector
from netobserv.connectors.rest import RESTConnector
from netobserv.connectors.snmp import SNMPConnector
from netobserv.connectors.registry import ConnectorRegistry

__all__ = [
    "DiscoveryConnector",
    "DiscoveryTarget",
    "CollectionContext",
    "RawCollectionResult",
    "ConnectorCredential",
    "ConnectorError",
    "ConnectorTimeoutError",
    "ConnectorAuthError",
    "SSHConnector",
    "RESTConnector",
    "SNMPConnector",
    "ConnectorRegistry",
]
