"""SSH connector using asyncssh / Netmiko for device collection."""

from __future__ import annotations

import json
from typing import Any

from netobserv.connectors.base import (
    BaseConnector,
    CollectionContext,
    ConnectorAuthError,
    ConnectorConnectionError,
    DiscoveryTarget,
    RawCollectionResult,
)
from netobserv.observability.logging import get_logger

logger = get_logger("connectors.ssh")


class SSHConnector(BaseConnector):
    """
    SSH-based connector.

    Uses asyncssh for low-level SSH and delegates command dispatch
    to platform-specific command sets. Falls back gracefully to a
    generic command set when the platform is unknown.
    """

    connector_type = "ssh"

    # Platform-specific command sets. Keys are platform hints.
    _PLATFORM_COMMANDS: dict[str, dict[str, str]] = {
        "eos": {
            "facts": "show version | json",
            "interfaces": "show interfaces | json",
            "neighbors": "show lldp neighbors detail | json",
            "routes": "show ip route | json",
            "vlans": "show vlan | json",
            "bgp": "show ip bgp summary | json",
            "arp": "show ip arp | json",
            "mac": "show mac address-table | json",
        },
        "nxos": {
            "facts": "show version | json",
            "interfaces": "show interface | json",
            "neighbors": "show lldp neighbors detail | json",
            "routes": "show ip route | json",
            "vlans": "show vlan | json",
            "bgp": "show bgp ipv4 unicast summary | json",
            "arp": "show ip arp | json",
            "mac": "show mac address-table | json",
        },
        "ios": {
            "facts": "show version",
            "interfaces": "show interfaces",
            "neighbors": "show lldp neighbors detail",
            "routes": "show ip route",
            "vlans": "show vlan brief",
            "arp": "show ip arp",
            "mac": "show mac address-table",
        },
        "iosxr": {
            "facts": "show version",
            "interfaces": "show interfaces",
            "neighbors": "show lldp neighbors detail",
            "routes": "show route",
            "bgp": "show bgp summary",
            "arp": "show arp",
        },
        "junos": {
            "facts": "show version | display json",
            "interfaces": "show interfaces | display json",
            "neighbors": "show lldp neighbors | display json",
            "routes": "show route | display json",
            "bgp": "show bgp summary | display json",
            "arp": "show arp | display json",
        },
    }

    _DEFAULT_COMMANDS: dict[str, str] = {
        "facts": "show version",
        "interfaces": "show interfaces",
        "neighbors": "show lldp neighbors",
        "routes": "show ip route",
        "arp": "show ip arp",
    }

    async def _collect_impl(
        self, target: DiscoveryTarget, context: CollectionContext
    ) -> RawCollectionResult:
        """
        Attempt to connect via SSH and run discovery commands.

        If asyncssh is not installed or the connection fails, return a
        gracefully-failed result rather than raising.
        """
        try:
            import asyncssh  # type: ignore[import-untyped]
        except ImportError:
            return self._failed_result(
                target, context, "asyncssh not installed — install netobserv[ssh]"
            )

        if not context.credential:
            return self._failed_result(
                target, context, "No credential provided for SSH target"
            )

        platform = target.platform or "generic"
        commands = self._PLATFORM_COMMANDS.get(platform, self._DEFAULT_COMMANDS)

        connect_kwargs: dict[str, Any] = {
            "host": target.management_ip or target.hostname,
            "port": target.port or 22,
            "username": context.credential.username,
            "known_hosts": None,  # disable host checking — configure properly in prod
        }
        if context.credential.password:
            connect_kwargs["password"] = context.credential.password.get_secret_value()
        if context.credential.private_key:
            connect_kwargs["client_keys"] = [
                context.credential.private_key.get_secret_value()
            ]

        try:
            async with asyncssh.connect(**connect_kwargs) as conn:
                data: dict[str, Any] = {}
                raw_interfaces: list[dict[str, Any]] = []
                raw_neighbors: list[dict[str, Any]] = []
                raw_routes: list[dict[str, Any]] = []
                raw_vlans: list[dict[str, Any]] = []
                raw_bgp: list[dict[str, Any]] = []
                raw_arp: list[dict[str, Any]] = []
                raw_mac: list[dict[str, Any]] = []
                raw_facts: dict[str, Any] = {}

                for cmd_key, cmd_str in commands.items():
                    try:
                        result = await conn.run(cmd_str, check=True)
                        output = result.stdout.strip() if result.stdout else ""
                        parsed = self._try_parse_json(output)

                        if cmd_key == "facts":
                            raw_facts = parsed or {"raw": output}
                        elif cmd_key == "interfaces":
                            raw_interfaces = self._coerce_list(parsed, output)
                        elif cmd_key == "neighbors":
                            raw_neighbors = self._coerce_list(parsed, output)
                        elif cmd_key == "routes":
                            raw_routes = self._coerce_list(parsed, output)
                        elif cmd_key == "vlans":
                            raw_vlans = self._coerce_list(parsed, output)
                        elif cmd_key == "bgp":
                            raw_bgp = self._coerce_list(parsed, output)
                        elif cmd_key == "arp":
                            raw_arp = self._coerce_list(parsed, output)
                        elif cmd_key == "mac":
                            raw_mac = self._coerce_list(parsed, output)
                    except Exception as exc:
                        logger.warning(
                            "Command failed",
                            command=cmd_str,
                            target=target.hostname,
                            error=str(exc),
                        )
                        data[f"error_{cmd_key}"] = str(exc)

                return RawCollectionResult(
                    target=target,
                    workflow_id=context.workflow_id,
                    snapshot_id=context.snapshot_id,
                    connector_type=self.connector_type,
                    success=True,
                    data=data,
                    raw_device_facts=raw_facts,
                    raw_interfaces=raw_interfaces,
                    raw_neighbors=raw_neighbors,
                    raw_routes=raw_routes,
                    raw_vlans=raw_vlans,
                    raw_bgp_sessions=raw_bgp,
                    raw_arp_entries=raw_arp,
                    raw_mac_entries=raw_mac,
                )

        except asyncssh.DisconnectError as exc:
            raise ConnectorConnectionError(
                str(exc), connector=self.connector_type, target=target.hostname
            ) from exc
        except asyncssh.PermissionDenied as exc:
            raise ConnectorAuthError(
                str(exc), connector=self.connector_type, target=target.hostname
            ) from exc

    def _failed_result(
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

    @staticmethod
    def _try_parse_json(text: str) -> Any:
        try:
            return json.loads(text)
        except (json.JSONDecodeError, ValueError):
            return None

    @staticmethod
    def _coerce_list(parsed: Any, raw_text: str) -> list[dict[str, Any]]:
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            # Some vendors wrap lists in a dict key
            for v in parsed.values():
                if isinstance(v, list):
                    return v  # type: ignore[return-value]
            return [parsed]
        return [{"raw": raw_text}]
