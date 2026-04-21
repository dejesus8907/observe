"""NetBox inventory source — loads device list from NetBox API."""

from __future__ import annotations

from typing import Any

import httpx

from netobserv.inventory_sources.base import InventoryTarget
from netobserv.observability.logging import get_logger

logger = get_logger("inventory_sources.netbox")


class NetBoxInventorySource:
    """Loads discovery targets from a NetBox instance via REST API."""

    source_type = "netbox"

    def __init__(
        self,
        url: str,
        token: str,
        *,
        site: str | None = None,
        tenant: str | None = None,
        tag: str | None = None,
        platform: str | None = None,
        status: str = "active",
        verify_ssl: bool = True,
        default_transport: str = "ssh",
        default_credential_id: str | None = None,
        timeout: int = 30,
    ) -> None:
        self.url = url.rstrip("/")
        self.token = token
        self.filter_site = site
        self.filter_tenant = tenant
        self.filter_tag = tag
        self.filter_platform = platform
        self.filter_status = status
        self.verify_ssl = verify_ssl
        self.default_transport = default_transport
        self.default_credential_id = default_credential_id
        self.timeout = timeout

    async def load_targets(self) -> list[InventoryTarget]:
        params: dict[str, str] = {"limit": "1000"}
        if self.filter_site:
            params["site"] = self.filter_site
        if self.filter_tenant:
            params["tenant"] = self.filter_tenant
        if self.filter_tag:
            params["tag"] = self.filter_tag
        if self.filter_platform:
            params["platform"] = self.filter_platform
        if self.filter_status:
            params["status"] = self.filter_status

        headers = {
            "Authorization": f"Token {self.token}",
            "Accept": "application/json",
        }

        all_devices: list[dict[str, Any]] = []
        url: str | None = f"{self.url}/api/dcim/devices/"

        async with httpx.AsyncClient(
            verify=self.verify_ssl, timeout=self.timeout, headers=headers
        ) as client:
            while url:
                try:
                    resp = await client.get(url, params=params if url.endswith("/") else {})
                    resp.raise_for_status()
                    body = resp.json()
                    all_devices.extend(body.get("results", []))
                    next_url = body.get("next")
                    url = next_url
                    params = {}  # subsequent pages have params embedded in URL
                except Exception as exc:
                    logger.error(
                        "Failed to load NetBox inventory",
                        url=url,
                        error=str(exc),
                    )
                    break

        targets = [self._device_to_target(d) for d in all_devices]
        logger.info(
            "Loaded NetBox inventory",
            url=self.url,
            count=len(targets),
        )
        return targets

    def _device_to_target(self, device: dict[str, Any]) -> InventoryTarget:
        primary_ip = device.get("primary_ip") or {}
        if isinstance(primary_ip, dict):
            mgmt_ip_raw = primary_ip.get("address", "")
            mgmt_ip = mgmt_ip_raw.split("/")[0] if mgmt_ip_raw else None
        else:
            mgmt_ip = None

        platform_obj = device.get("platform") or {}
        platform = platform_obj.get("slug") if isinstance(platform_obj, dict) else None

        site_obj = device.get("site") or {}
        site = site_obj.get("slug") if isinstance(site_obj, dict) else None

        tags = [
            t.get("slug", t.get("name", ""))
            for t in device.get("tags", [])
        ]

        return InventoryTarget(
            hostname=device.get("name", ""),
            management_ip=mgmt_ip,
            transport=self.default_transport,
            platform=platform,
            credential_id=self.default_credential_id,
            site=site,
            tags=tags,
            source="netbox",
            metadata={
                "netbox_id": device.get("id"),
                "device_role": (device.get("device_role") or device.get("role") or {}).get("slug"),
                "device_type": (device.get("device_type") or {}).get("slug"),
                "status": (device.get("status") or {}).get("value"),
            },
        )
