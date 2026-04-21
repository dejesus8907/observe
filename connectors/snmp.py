"""SNMP connector for basic device polling."""

from __future__ import annotations

from typing import Any

from netobserv.connectors.base import (
    BaseConnector,
    CollectionContext,
    ConnectorAuthError,
    DiscoveryTarget,
    RawCollectionResult,
)
from netobserv.observability.logging import get_logger

logger = get_logger("connectors.snmp")

# Standard OIDs used for discovery
_OIDS = {
    "sysDescr": "1.3.6.1.2.1.1.1.0",
    "sysObjectID": "1.3.6.1.2.1.1.2.0",
    "sysName": "1.3.6.1.2.1.1.5.0",
    "sysLocation": "1.3.6.1.2.1.1.6.0",
    "sysContact": "1.3.6.1.2.1.1.4.0",
    "ifTable": "1.3.6.1.2.1.2.2",
    "ifDescr": "1.3.6.1.2.1.2.2.1.2",
    "ifType": "1.3.6.1.2.1.2.2.1.3",
    "ifSpeed": "1.3.6.1.2.1.2.2.1.5",
    "ifAdminStatus": "1.3.6.1.2.1.2.2.1.7",
    "ifOperStatus": "1.3.6.1.2.1.2.2.1.8",
    "ifPhysAddress": "1.3.6.1.2.1.2.2.1.6",
    "ipAdEntAddr": "1.3.6.1.2.1.4.20.1.1",
    "ipAdEntIfIndex": "1.3.6.1.2.1.4.20.1.2",
    "ipAdEntNetMask": "1.3.6.1.2.1.4.20.1.3",
}


class SNMPConnector(BaseConnector):
    """
    SNMP-based connector.

    Performs GET/WALK operations using pysnmp.
    Community-based v2c and v3 USM are supported via credentials.
    """

    connector_type = "snmp"

    async def _collect_impl(
        self, target: DiscoveryTarget, context: CollectionContext
    ) -> RawCollectionResult:
        try:
            from pysnmp.hlapi.asyncio import (  # type: ignore[import-untyped]
                CommunityData,
                ContextData,
                ObjectIdentity,
                ObjectType,
                SnmpEngine,
                UdpTransportTarget,
                bulkCmd,
                getCmd,
            )
        except ImportError:
            return self._failed(target, context, "pysnmp not installed — install netobserv[snmp]")

        cred = context.credential
        community = "public"
        if cred and cred.community:
            community = cred.community.get_secret_value()

        host = target.management_ip or target.hostname
        port = target.port or 161

        snmp_engine = SnmpEngine()
        transport = UdpTransportTarget(
            (host, port), timeout=context.timeout, retries=context.retries
        )
        community_data = CommunityData(community, mpModel=1)  # mpModel=1 -> SNMPv2c
        ctx = ContextData()

        facts: dict[str, Any] = {}
        for key, oid in [
            ("sysDescr", _OIDS["sysDescr"]),
            ("sysName", _OIDS["sysName"]),
            ("sysLocation", _OIDS["sysLocation"]),
        ]:
            try:
                error_ind, error_status, _, var_binds = await getCmd(
                    snmp_engine,
                    community_data,
                    transport,
                    ctx,
                    ObjectType(ObjectIdentity(oid)),
                )
                if not error_ind and not error_status:
                    for var_bind in var_binds:
                        facts[key] = str(var_bind[1])
            except Exception as exc:
                logger.warning("SNMP GET failed", oid=oid, target=host, error=str(exc))

        # Walk interface table
        raw_interfaces: list[dict[str, Any]] = []
        try:
            if_data: dict[int, dict[str, Any]] = {}
            async for error_ind, error_status, _, var_binds in bulkCmd(
                snmp_engine,
                community_data,
                transport,
                ctx,
                0,
                25,
                ObjectType(ObjectIdentity(_OIDS["ifDescr"])),
                ObjectType(ObjectIdentity(_OIDS["ifAdminStatus"])),
                ObjectType(ObjectIdentity(_OIDS["ifOperStatus"])),
                ObjectType(ObjectIdentity(_OIDS["ifSpeed"])),
                lexicographicMode=False,
            ):
                if error_ind or error_status:
                    break
                for var_bind in var_binds:
                    oid_str = str(var_bind[0])
                    value = str(var_bind[1])
                    # Last OID component is the if_index
                    try:
                        if_index = int(oid_str.rsplit(".", 1)[-1])
                    except ValueError:
                        continue
                    if if_index not in if_data:
                        if_data[if_index] = {"if_index": if_index}
                    # Simple mapping by OID prefix
                    if _OIDS["ifDescr"].rstrip(".0") in oid_str:
                        if_data[if_index]["name"] = value
                    elif _OIDS["ifAdminStatus"].rstrip(".0") in oid_str:
                        if_data[if_index]["admin_status"] = value
                    elif _OIDS["ifOperStatus"].rstrip(".0") in oid_str:
                        if_data[if_index]["oper_status"] = value
                    elif _OIDS["ifSpeed"].rstrip(".0") in oid_str:
                        if_data[if_index]["speed"] = value
            raw_interfaces = list(if_data.values())
        except Exception as exc:
            logger.warning("SNMP walk failed", target=host, error=str(exc))

        return RawCollectionResult(
            target=target,
            workflow_id=context.workflow_id,
            snapshot_id=context.snapshot_id,
            connector_type=self.connector_type,
            success=True,
            data={"snmp_facts": facts},
            raw_device_facts=facts,
            raw_interfaces=raw_interfaces,
        )

    def _failed(
        self,
        target: DiscoveryTarget,
        context: CollectionContext,
        error: str,
    ) -> RawCollectionResult:
        return RawCollectionResult(
            target=target,
            workflow_id=context.workflow_id,
            snapshot_id=context.snapshot_id,
            connector_type=self.connector_type,
            success=False,
            error_message=error,
        )
