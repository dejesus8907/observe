"""REST/HTTP connector for API-driven device collection."""

from __future__ import annotations

from typing import Any

import httpx

from netobserv.connectors.base import (
    BaseConnector,
    CollectionContext,
    ConnectorAuthError,
    ConnectorConnectionError,
    DiscoveryTarget,
    RawCollectionResult,
)
from netobserv.observability.logging import get_logger

logger = get_logger("connectors.rest")

# Mapping from platform -> (base_url_template, endpoints)
_PLATFORM_ENDPOINTS: dict[str, dict[str, str]] = {
    "eos": {
        "base": "https://{host}/command-api",
        "method": "eapi",  # special: EOS eAPI
    },
    "nxos": {
        "base": "https://{host}/ins",
        "method": "nxapi",
    },
    "netbox": {
        "base": "{host}/api",
        "devices": "/dcim/devices/",
        "interfaces": "/dcim/interfaces/",
        "vlans": "/ipam/vlans/",
        "prefixes": "/ipam/prefixes/",
        "ip_addresses": "/ipam/ip-addresses/",
        "cables": "/dcim/cables/",
    },
}


class RESTConnector(BaseConnector):
    """
    REST/HTTP-based connector.

    Supports both vendor REST APIs (EOS eAPI, NX-OS NX-API)
    and source-of-truth APIs (NetBox).
    """

    connector_type = "rest"

    async def _collect_impl(
        self, target: DiscoveryTarget, context: CollectionContext
    ) -> RawCollectionResult:
        platform = (target.platform or "generic").lower()
        cred = context.credential

        headers: dict[str, str] = {"Content-Type": "application/json"}
        if cred and cred.api_token:
            headers["Authorization"] = f"Token {cred.api_token.get_secret_value()}"

        verify_ssl = cred.verify_ssl if cred else True
        base_host = target.management_ip or target.hostname

        if platform in ("eos",):
            return await self._collect_eapi(
                target, context, base_host, headers, verify_ssl
            )
        elif platform in ("netbox",):
            return await self._collect_netbox(
                target, context, base_host, headers, verify_ssl
            )
        else:
            return await self._collect_generic(
                target, context, base_host, headers, verify_ssl
            )

    async def _collect_eapi(
        self,
        target: DiscoveryTarget,
        context: CollectionContext,
        host: str,
        headers: dict[str, str],
        verify_ssl: bool,
    ) -> RawCollectionResult:
        url = f"https://{host}/command-api"
        cred = context.credential

        auth: tuple[str, str] | None = None
        if cred and cred.username and cred.password:
            auth = (cred.username, cred.password.get_secret_value())

        commands = [
            "show version",
            "show interfaces",
            "show lldp neighbors detail",
            "show ip route",
            "show vlan",
            "show ip bgp summary",
            "show ip arp",
            "show mac address-table",
        ]
        payload = {
            "jsonrpc": "2.0",
            "method": "runCmds",
            "params": {"version": 1, "cmds": commands, "format": "json"},
            "id": "netobserv-discovery",
        }

        async with httpx.AsyncClient(verify=verify_ssl, timeout=context.timeout) as client:
            try:
                resp = await client.post(url, json=payload, headers=headers, auth=auth)
                resp.raise_for_status()
                data = resp.json()
                results = data.get("result", [])
                keys = [
                    "version", "interfaces", "lldp_neighbors", "routes",
                    "vlans", "bgp_summary", "arp", "mac_table",
                ]
                parsed = dict(zip(keys, results, strict=False))
                return RawCollectionResult(
                    target=target,
                    workflow_id=context.workflow_id,
                    snapshot_id=context.snapshot_id,
                    connector_type=self.connector_type,
                    success=True,
                    data=parsed,
                    raw_device_facts=parsed.get("version", {}),
                    raw_interfaces=self._extract_list(parsed.get("interfaces")),
                    raw_neighbors=self._extract_list(parsed.get("lldp_neighbors")),
                    raw_routes=self._extract_list(parsed.get("routes")),
                    raw_vlans=self._extract_list(parsed.get("vlans")),
                    raw_bgp_sessions=self._extract_list(parsed.get("bgp_summary")),
                    raw_arp_entries=self._extract_list(parsed.get("arp")),
                    raw_mac_entries=self._extract_list(parsed.get("mac_table")),
                )
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code in (401, 403):
                    raise ConnectorAuthError(
                        str(exc), connector=self.connector_type, target=target.hostname
                    ) from exc
                raise ConnectorConnectionError(
                    str(exc), connector=self.connector_type, target=target.hostname
                ) from exc

    async def _collect_netbox(
        self,
        target: DiscoveryTarget,
        context: CollectionContext,
        host: str,
        headers: dict[str, str],
        verify_ssl: bool,
    ) -> RawCollectionResult:
        """Collect intended-state data from NetBox."""
        base_url = host.rstrip("/")
        if not base_url.startswith("http"):
            base_url = f"https://{base_url}"

        async with httpx.AsyncClient(
            base_url=f"{base_url}/api",
            headers=headers,
            verify=verify_ssl,
            timeout=context.timeout,
        ) as client:
            devices = await self._paginate(client, "/dcim/devices/")
            interfaces = await self._paginate(client, "/dcim/interfaces/")
            vlans = await self._paginate(client, "/ipam/vlans/")
            ip_addresses = await self._paginate(client, "/ipam/ip-addresses/")
            cables = await self._paginate(client, "/dcim/cables/")

            return RawCollectionResult(
                target=target,
                workflow_id=context.workflow_id,
                snapshot_id=context.snapshot_id,
                connector_type=self.connector_type,
                success=True,
                data={
                    "source": "netbox",
                    "devices": devices,
                    "interfaces": interfaces,
                    "vlans": vlans,
                    "ip_addresses": ip_addresses,
                    "cables": cables,
                },
            )

    async def _collect_generic(
        self,
        target: DiscoveryTarget,
        context: CollectionContext,
        host: str,
        headers: dict[str, str],
        verify_ssl: bool,
    ) -> RawCollectionResult:
        """Generic REST probe — returns raw HTTP response data."""
        url = f"https://{host}/api/v1/system"
        async with httpx.AsyncClient(verify=verify_ssl, timeout=context.timeout) as client:
            try:
                resp = await client.get(url, headers=headers)
                return RawCollectionResult(
                    target=target,
                    workflow_id=context.workflow_id,
                    snapshot_id=context.snapshot_id,
                    connector_type=self.connector_type,
                    success=True,
                    data={"raw": resp.json() if resp.is_success else resp.text},
                )
            except Exception as exc:
                return RawCollectionResult(
                    target=target,
                    workflow_id=context.workflow_id,
                    snapshot_id=context.snapshot_id,
                    connector_type=self.connector_type,
                    success=False,
                    error_message=str(exc),
                    error_class=type(exc).__name__,
                )

    @staticmethod
    async def _paginate(
        client: httpx.AsyncClient, endpoint: str, limit: int = 1000
    ) -> list[dict[str, Any]]:
        """Paginate through a NetBox-style paginated API endpoint."""
        results: list[dict[str, Any]] = []
        url: str | None = f"{endpoint}?limit={limit}"
        while url:
            resp = await client.get(url)
            resp.raise_for_status()
            body = resp.json()
            results.extend(body.get("results", []))
            # Next page URL is relative in NetBox
            next_url = body.get("next")
            if next_url:
                # Strip base_url prefix since client already has it
                from urllib.parse import urlparse
                parsed = urlparse(next_url)
                url = parsed.path + (f"?{parsed.query}" if parsed.query else "")
            else:
                url = None
        return results

    @staticmethod
    def _extract_list(data: Any) -> list[dict[str, Any]]:
        if isinstance(data, list):
            return data  # type: ignore[return-value]
        if isinstance(data, dict):
            for v in data.values():
                if isinstance(v, list):
                    return v  # type: ignore[return-value]
            return [data]
        return []
