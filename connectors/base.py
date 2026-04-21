"""Base types and protocol interface for all discovery connectors."""

from __future__ import annotations

import asyncio
from abc import abstractmethod
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, Field, SecretStr


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class ConnectorError(Exception):
    """Base exception for all connector errors."""

    def __init__(self, message: str, connector: str = "", target: str = "") -> None:
        super().__init__(message)
        self.connector = connector
        self.target = target


class ConnectorTimeoutError(ConnectorError):
    """Collection timed out."""


class ConnectorAuthError(ConnectorError):
    """Authentication or authorization failure."""


class ConnectorConnectionError(ConnectorError):
    """Network-level connection failure."""


class ConnectorParseError(ConnectorError):
    """Failure to parse device response."""


# ---------------------------------------------------------------------------
# Value objects
# ---------------------------------------------------------------------------


class ConnectorCredential(BaseModel):
    """Credential bundle for device access. Values are kept secret."""

    credential_id: str
    name: str
    transport: str  # ssh / rest / snmp
    username: str | None = None
    password: SecretStr | None = None
    private_key: SecretStr | None = None
    api_token: SecretStr | None = None
    community: SecretStr | None = None  # SNMP community
    snmp_version: str = "v2c"
    verify_ssl: bool = True
    extra: dict[str, Any] = Field(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True}


class DiscoveryTarget(BaseModel):
    """A single device/host to be discovered."""

    target_id: str
    hostname: str
    management_ip: str | None = None
    transport: str = "ssh"  # ssh / rest / snmp
    port: int | None = None
    credential_id: str | None = None
    platform: str | None = None  # vendor platform hint
    site: str | None = None
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class CollectionContext(BaseModel):
    """Runtime context passed to every collector call."""

    workflow_id: str
    snapshot_id: str
    timeout: int = 30
    retries: int = 3
    backoff_base: float = 2.0
    read_only: bool = True
    credential: ConnectorCredential | None = None
    extras: dict[str, Any] = Field(default_factory=dict)


class RawCollectionResult(BaseModel):
    """Raw (vendor-specific) data collected from a single device."""

    target: DiscoveryTarget
    workflow_id: str
    snapshot_id: str
    connector_type: str
    collected_at: datetime = Field(default_factory=datetime.utcnow)
    success: bool = True
    error_message: str | None = None
    error_class: str | None = None
    elapsed_seconds: float | None = None

    # Raw data keyed by data type: interfaces, routes, neighbors, etc.
    data: dict[str, Any] = Field(default_factory=dict)

    # Structured sub-collections
    raw_interfaces: list[dict[str, Any]] = Field(default_factory=list)
    raw_routes: list[dict[str, Any]] = Field(default_factory=list)
    raw_neighbors: list[dict[str, Any]] = Field(default_factory=list)
    raw_vlans: list[dict[str, Any]] = Field(default_factory=list)
    raw_vrfs: list[dict[str, Any]] = Field(default_factory=list)
    raw_bgp_sessions: list[dict[str, Any]] = Field(default_factory=list)
    raw_arp_entries: list[dict[str, Any]] = Field(default_factory=list)
    raw_mac_entries: list[dict[str, Any]] = Field(default_factory=list)
    raw_device_facts: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Protocol interface
# ---------------------------------------------------------------------------


@runtime_checkable
class DiscoveryConnector(Protocol):
    """Protocol that all connectors must satisfy."""

    connector_type: str

    async def collect(
        self, target: DiscoveryTarget, context: CollectionContext
    ) -> RawCollectionResult:
        """Collect raw vendor-specific data from a single target."""
        ...

    async def test_connectivity(
        self, target: DiscoveryTarget, context: CollectionContext
    ) -> bool:
        """Return True if the target is reachable and credentials are valid."""
        ...


# ---------------------------------------------------------------------------
# Base implementation with retry/backoff logic
# ---------------------------------------------------------------------------


class BaseConnector:
    """Base class with retry and backoff for connector implementations."""

    connector_type: str = "base"

    async def collect(
        self, target: DiscoveryTarget, context: CollectionContext
    ) -> RawCollectionResult:
        import time

        last_exc: Exception | None = None
        for attempt in range(context.retries + 1):
            start = time.monotonic()
            try:
                result = await asyncio.wait_for(
                    self._collect_impl(target, context),
                    timeout=context.timeout,
                )
                result.elapsed_seconds = time.monotonic() - start
                return result
            except asyncio.TimeoutError as exc:
                last_exc = ConnectorTimeoutError(
                    f"Timeout after {context.timeout}s on attempt {attempt + 1}",
                    connector=self.connector_type,
                    target=target.hostname,
                )
            except ConnectorAuthError:
                # Don't retry auth failures
                raise
            except Exception as exc:
                last_exc = exc
                if attempt < context.retries:
                    backoff = context.backoff_base ** attempt
                    await asyncio.sleep(backoff)

        elapsed = 0.0
        return RawCollectionResult(
            target=target,
            workflow_id=context.workflow_id,
            snapshot_id=context.snapshot_id,
            connector_type=self.connector_type,
            success=False,
            error_message=str(last_exc),
            error_class=type(last_exc).__name__ if last_exc else "UnknownError",
        )

    async def _collect_impl(
        self, target: DiscoveryTarget, context: CollectionContext
    ) -> RawCollectionResult:
        raise NotImplementedError

    async def test_connectivity(
        self, target: DiscoveryTarget, context: CollectionContext
    ) -> bool:
        try:
            result = await self.collect(target, context)
            return result.success
        except Exception:
            return False
