"""Base types and protocol for parsers."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, Field

from netobserv.connectors.base import RawCollectionResult


class ParsedRecord(BaseModel):
    """A single structured record extracted from raw collection output."""

    record_type: str  # device / interface / neighbor / route / vlan / bgp / arp / mac
    source_system: str
    source_platform: str | None = None
    data: dict[str, Any] = Field(default_factory=dict)
    raw: Any = None
    parsing_confidence: float = 1.0
    warnings: list[str] = Field(default_factory=list)


@runtime_checkable
class SourceParser(Protocol):
    """Protocol that all parsers must satisfy."""

    platform: str

    def parse(self, raw_result: RawCollectionResult) -> list[ParsedRecord]:
        """Extract structured ParsedRecord instances from a raw collection result."""
        ...
