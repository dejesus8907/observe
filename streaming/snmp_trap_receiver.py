"""SNMP trap receiver — ingest pipeline for SNMP v1/v2c/v3 traps.

SNMP traps are device-initiated notifications sent to a well-known port (UDP 162).
They are the legacy equivalent of gNMI ON_CHANGE for older devices. In DoD networks,
many devices — including legacy routers, switches, and OOB management hardware —
will never support gNMI. SNMP trap coverage is not optional.

Architecture
------------
- An asyncio UDP server listens on the configured trap port (default 162, configurable
  for non-root deployment to e.g. 1162)
- Raw trap PDUs are decoded using pysnmp
- Decoded varbinds are passed to SnmpTrapClassifier to produce ChangeEvents
- Events are emitted into the shared event queue

Security notes for DoD deployment:
- SNMPv3 with authPriv (SHA + AES) should be used wherever available
- v2c community strings must be treated as secrets and rotated
- This receiver is read-only — it only listens for traps, never issues GETs/SETs
- Source IP filtering can be configured to reject traps from unexpected sources

pysnmp usage notes:
  asyncio integration uses NotificationReceiver from pysnmp.hlapi.asyncio
  We parse OIDs against a locally maintained MIB subset rather than fetching
  MIBs from the network (which would be inappropriate in a DoD air-gap context)
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from netobserv.streaming.classifier import SnmpTrapClassifier
from netobserv.streaming.adapter_helpers import AdapterObservation, observation_from_change_event, publish_observation
from netobserv.streaming.evidence_mapper import evidence_record_from_change_event
from netobserv.streaming.events import (
    ChangeEvent,
    ChangeKind,
    ChangeSeverity,
    EventSource,
)

logger = logging.getLogger("streaming.snmp_trap")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class SnmpTrapReceiverConfig:
    """Configuration for the SNMP trap UDP listener."""

    listen_host: str = "0.0.0.0"
    listen_port: int = 1162          # 162 requires root; 1162 is safer for service accounts
    community: str = "public"        # v1/v2c only
    # SNMPv3 USM credentials
    v3_username: str | None = None
    v3_auth_key: str | None = None
    v3_priv_key: str | None = None
    v3_auth_protocol: str = "SHA"    # SHA | SHA-256 | SHA-384 | SHA-512
    v3_priv_protocol: str = "AES"    # AES | AES-256
    # Source filter: only accept traps from these IPs (empty = accept all)
    allowed_sources: list[str] = field(default_factory=list)
    # IP → device_id mapping (populated from inventory)
    source_ip_to_device_id: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# SNMP Trap Receiver
# ---------------------------------------------------------------------------


class SnmpTrapReceiver:
    """Async UDP server that receives, decodes, and classifies SNMP traps.

    When pysnmp is available, uses its asyncio notification receiver.
    When unavailable, logs a clear error and continues without trap coverage.
    """

    def __init__(
        self,
        config: SnmpTrapReceiverConfig,
        event_bus: Any,
    ) -> None:
        self._config = config
        self._event_bus = event_bus
        self._classifier = SnmpTrapClassifier()
        self._transport: Any | None = None
        self._server_task: asyncio.Task[None] | None = None
        self._running = False
        self._trap_count = 0
        self._error_count = 0

    def _to_observation(self, event: ChangeEvent) -> AdapterObservation:
        """Return the primary adapter output."""
        return observation_from_change_event(event)

    async def start(self) -> None:
        self._running = True
        self._server_task = asyncio.create_task(
            self._run_server(),
            name="snmp-trap-receiver",
        )
        logger.info(
            "SNMP trap receiver starting",
            extra={"host": self._config.listen_host, "port": self._config.listen_port},
        )

    async def stop(self) -> None:
        self._running = False
        if self._server_task:
            self._server_task.cancel()
            await asyncio.gather(self._server_task, return_exceptions=True)
        logger.info("SNMP trap receiver stopped")

    async def _run_server(self) -> None:
        try:
            from pysnmp.hlapi.asyncio import SnmpEngine, CommunityData, UdpTransportTarget  # type: ignore[import-untyped]
            from pysnmp.entity import engine, config as snmp_config
            from pysnmp.carrier.asyncio.dgram import udp
            from pysnmp.entity.rfc3413 import ntfrcv
        except ImportError:
            logger.error(
                "pysnmp not installed — SNMP trap receiver unavailable. "
                "Install with: pip install pysnmp"
            )
            # Keep the task alive so stop() doesn't raise
            while self._running:
                await asyncio.sleep(60)
            return

        snmp_engine = SnmpEngine()

        # Configure transport
        snmp_config.addTransport(
            snmp_engine,
            udp.domainName,
            udp.UdpAsyncioTransport().openServerMode(
                (self._config.listen_host, self._config.listen_port)
            ),
        )

        # Configure community strings (v1/v2c)
        snmp_config.addV1System(snmp_engine, "trap-receiver", self._config.community)

        # Configure SNMPv3 if credentials are provided
        if self._config.v3_username:
            from pysnmp.hlapi.asyncio import UsmUserData  # type: ignore[import-untyped]
            from pysnmp.proto.secmod.rfc3826 import priv as rfc3826priv  # type: ignore[import-untyped]

            snmp_config.addV3User(
                snmp_engine,
                self._config.v3_username,
                authKey=self._config.v3_auth_key or "",
                privKey=self._config.v3_priv_key or "",
            )

        # Register notification receiver callback
        def _notification_cb(
            snmp_engine_ref: Any,
            state_reference: Any,
            context_engine_id: Any,
            context_name: Any,
            var_binds: Any,
            cb_ctx: Any,
        ) -> None:
            asyncio.get_event_loop().call_soon(
                self._handle_trap, snmp_engine_ref, var_binds, state_reference
            )

        ntfrcv.NotificationReceiver(snmp_engine, _notification_cb)
        snmp_engine.transportDispatcher.jobStarted(1)

        logger.info(
            "SNMP trap receiver listening",
            extra={"host": self._config.listen_host, "port": self._config.listen_port},
        )

        try:
            while self._running:
                await asyncio.sleep(0.1)
                snmp_engine.transportDispatcher.runDispatcher(0)
        except asyncio.CancelledError:
            pass
        finally:
            snmp_engine.transportDispatcher.closeDispatcher()

    def _handle_trap(
        self,
        snmp_engine: Any,
        var_binds: Any,
        state_reference: Any,
    ) -> None:
        """Synchronous callback — decode trap and schedule emit."""
        self._trap_count += 1
        try:
            # Extract source IP from transport information
            source_ip = "unknown"
            try:
                transport_domain, transport_address = snmp_engine.msgAndPduDsp.getTransportInfo(state_reference)
                source_ip = str(transport_address[0])
            except Exception:
                pass

            # Source filter
            if self._config.allowed_sources and source_ip not in self._config.allowed_sources:
                logger.debug("Rejecting trap from unauthorized source %s", source_ip)
                return

            # Resolve device_id from source IP
            device_id = self._config.source_ip_to_device_id.get(source_ip, source_ip)

            # Convert varbinds to a flat dict
            varbind_dict: dict[str, Any] = {}
            trap_oid = ""
            for name, val in var_binds:
                oid_str = str(name)
                varbind_dict[oid_str] = str(val)
                if "snmpTrapOID" in oid_str or oid_str == "1.3.6.1.6.3.1.1.4.1.0":
                    trap_oid = str(val)

            trap_payload: dict[str, Any] = {
                "device_id": device_id,
                "source_ip": source_ip,
                "trap_oid": trap_oid,
                "varbinds": varbind_dict,
                "timestamp": _utc_now().isoformat(),
            }

            event = self._classifier.classify(trap_payload)
            if event:
                asyncio.ensure_future(self._emit(event))

        except Exception as exc:
            self._error_count += 1
            logger.error("Failed to process SNMP trap: %s", exc)

    async def _emit(self, event: ChangeEvent) -> None:
        """Publish through event bus using evidence-first contract."""
        evidence = evidence_record_from_change_event(event)
        try:
            await self._event_bus.publish_adapter_observation(
                evidence=evidence,
                source_event=event,
                fallback_event=event,
            )
        except Exception as exc:
            logger.warning("SNMP event publish failed: %s", exc)

    @property
    def stats(self) -> dict[str, int]:
        return {"traps_received": self._trap_count, "errors": self._error_count}
