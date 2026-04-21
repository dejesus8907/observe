"""Normalizer protocol and base utilities."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from netobserv.models.canonical import CanonicalObject
from netobserv.parsers.base import ParsedRecord


@runtime_checkable
class RecordNormalizer(Protocol):
    """Protocol that all normalizers must satisfy."""

    def normalize(self, parsed_records: list[ParsedRecord]) -> list[CanonicalObject]:
        """Convert a list of ParsedRecords into canonical objects."""
        ...
