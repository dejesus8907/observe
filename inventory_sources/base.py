"""Base types and protocol for inventory sources."""

from __future__ import annotations

import uuid
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, Field


class InventoryTarget(BaseModel):
    """A resolved target ready for discovery."""

    target_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    hostname: str
    management_ip: str | None = None
    transport: str = "ssh"
    port: int | None = None
    platform: str | None = None
    credential_id: str | None = None
    site: str | None = None
    tags: list[str] = Field(default_factory=list)
    source: str = "unknown"
    metadata: dict[str, Any] = Field(default_factory=dict)


@runtime_checkable
class InventorySource(Protocol):
    """Protocol that all inventory sources must satisfy."""

    source_type: str

    async def load_targets(self) -> list[InventoryTarget]:
        """Return all discovery targets from this source."""
        ...
