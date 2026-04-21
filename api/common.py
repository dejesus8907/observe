"""Shared API helpers.

This module keeps route handlers small and consistent. The API used to repeat the
same weak patterns in every router: mutable request defaults, ad-hoc timestamp
serialization, inconsistent 404 handling, and hand-wavy snapshot resolution.
Those shortcuts make the public surface drift away from the actual backend
behavior.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, TypeVar

from fastapi import HTTPException, status

T = TypeVar("T")


MAX_LIST_LIMIT = 200
DEFAULT_LIST_LIMIT = 20


def serialize_datetime(value: datetime | None) -> str | None:
    """Return an ISO-8601 string for API responses."""
    return value.isoformat() if value else None


def clamp_limit(limit: int, *, default: int = DEFAULT_LIST_LIMIT, max_limit: int = MAX_LIST_LIMIT) -> int:
    """Clamp caller-provided list limits to a safe bounded range."""
    if limit <= 0:
        return default
    return min(limit, max_limit)


def require_resource(resource: T | None, detail: str) -> T:
    """Raise a consistent 404 when a repository lookup misses."""
    if resource is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=detail)
    return resource


async def resolve_latest_snapshot_id(snapshot_repo: Any, *, scope: str | None = None) -> str | None:
    """Resolve the newest snapshot ID, optionally scoped.

    This is intentionally explicit instead of silently picking arbitrary data in
    each route.
    """
    snapshots = await snapshot_repo.list_recent(limit=1, scope=scope)
    if not snapshots:
        return None
    return snapshots[0].id
