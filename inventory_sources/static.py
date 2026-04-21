"""Static file inventory source — reads YAML or JSON host lists."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from netobserv.inventory_sources.base import InventoryTarget
from netobserv.observability.logging import get_logger

logger = get_logger("inventory_sources.static")


class StaticFileInventorySource:
    """
    Loads discovery targets from a YAML or JSON file.

    Expected YAML format:
    ---
    defaults:
      transport: ssh
      credential_id: default-ssh

    hosts:
      - hostname: router1
        management_ip: 10.0.0.1
        platform: eos
        site: dc1
      - hostname: switch1
        management_ip: 10.0.0.2
        platform: ios
        transport: snmp
        credential_id: snmp-ro
    """

    source_type = "static_file"

    def __init__(self, file_path: str | Path) -> None:
        self.file_path = Path(file_path)

    async def load_targets(self) -> list[InventoryTarget]:
        if not self.file_path.exists():
            logger.warning("Inventory file not found", path=str(self.file_path))
            return []

        try:
            content = self.file_path.read_text()
            if self.file_path.suffix in (".yaml", ".yml"):
                data = yaml.safe_load(content)
            elif self.file_path.suffix == ".json":
                data = json.loads(content)
            else:
                logger.error("Unsupported inventory file format", path=str(self.file_path))
                return []
        except Exception as exc:
            logger.error(
                "Failed to load inventory file",
                path=str(self.file_path),
                error=str(exc),
            )
            return []

        if not isinstance(data, dict):
            logger.error("Invalid inventory format — expected a dict", path=str(self.file_path))
            return []

        defaults: dict[str, Any] = data.get("defaults", {})
        hosts: list[dict[str, Any]] = data.get("hosts", [])

        targets = []
        for host in hosts:
            merged = {**defaults, **host}
            hostname = merged.get("hostname") or merged.get("name") or ""
            if not hostname:
                logger.warning("Skipping host without hostname", raw=merged)
                continue

            targets.append(
                InventoryTarget(
                    hostname=hostname,
                    management_ip=merged.get("management_ip") or merged.get("ip"),
                    transport=merged.get("transport", "ssh"),
                    port=merged.get("port"),
                    platform=merged.get("platform"),
                    credential_id=merged.get("credential_id"),
                    site=merged.get("site"),
                    tags=merged.get("tags", []),
                    source="static_file",
                    metadata={k: v for k, v in merged.items()},
                )
            )

        logger.info(
            "Loaded static inventory",
            file=str(self.file_path),
            count=len(targets),
        )
        return targets
