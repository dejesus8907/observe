"""gNMI streaming telemetry collector.

gNMI (gRPC Network Management Interface) is the modern, low-latency telemetry
transport for network devices. It replaces SNMP polling with server-initiated
subscription streams — the device pushes state changes as they occur.

This collector manages one gNMI subscription stream per device. It supports:
  - ON_CHANGE subscriptions: device pushes a notification the instant a
    subscribed leaf changes value. Sub-second latency for link events.
  - SAMPLE subscriptions: device pushes values at a configured interval.
    Used for counters and gauges that do not have meaningful change semantics.

The collector uses grpcio and the pygnmi library (or raw grpc stubs).
If gNMI libraries are unavailable, it degrades gracefully to no-op and logs
a clear error — it does not crash the engine.

OpenConfig paths subscribed by default:
  /interfaces/interface/state/oper-status          (ON_CHANGE)
  /interfaces/interface/config/description          (ON_CHANGE)
  /bgp/neighbors/neighbor/state/session-state       (ON_CHANGE)
  /network-instances/network-instance/protocols/
    protocol/ospf/areas/area/interfaces/interface/
    state/adjacency-state                            (ON_CHANGE)
  /lldp/interfaces/interface/neighbors/neighbor     (ON_CHANGE)
  /interfaces/interface/state/counters              (SAMPLE, 30s)

Device vendors that support gNMI:
  Arista EOS 4.22+, Cisco IOS-XR 6.5+, Cisco NX-OS 9.3+, Junos 18.3+
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, AsyncIterator

from netobserv.streaming.classifier import GnmiClassifier
from netobserv.streaming.adapter_helpers import AdapterObservation, observation_from_change_event, publish_observation
from netobserv.streaming.evidence_mapper import evidence_record_from_change_event
from netobserv.streaming.events import (
    ChangeEvent,
    ChangeKind,
    ChangeSeverity,
    EventSource,
    FlapWindow,
)

logger = logging.getLogger("streaming.gnmi")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# gNMI subscription configuration
# ---------------------------------------------------------------------------


@dataclass
class GnmiSubscription:
    """A single gNMI subscription path."""

    path: str
    mode: str = "ON_CHANGE"    # ON_CHANGE | SAMPLE | TARGET_DEFINED
    sample_interval_ns: int = 30_000_000_000   # 30s in nanoseconds (SAMPLE mode only)


# Default subscriptions covering the events we care about
DEFAULT_SUBSCRIPTIONS: list[GnmiSubscription] = [
    GnmiSubscription("/interfaces/interface/state/oper-status", mode="ON_CHANGE"),
    GnmiSubscription("/interfaces/interface/config/description", mode="ON_CHANGE"),
    GnmiSubscription("/bgp/neighbors/neighbor/state/session-state", mode="ON_CHANGE"),
    GnmiSubscription("/lldp/interfaces/interface/neighbors/neighbor", mode="ON_CHANGE"),
    GnmiSubscription("/network-instances/network-instance/protocols/protocol/"
                     "ospf/areas/area/interfaces/interface/state/adjacency-state",
                     mode="ON_CHANGE"),
    GnmiSubscription("/interfaces/interface/state/counters",
                     mode="SAMPLE", sample_interval_ns=30_000_000_000),
]


@dataclass
class GnmiTargetConfig:
    """Connection configuration for a single gNMI target (device)."""

    device_id: str
    hostname: str
    port: int = 6030     # Arista default; Cisco uses 57400; Junos uses 32767
    username: str = "netobserv"
    password: str | None = None
    tls_cert_path: str | None = None
    tls_key_path: str | None = None
    tls_ca_path: str | None = None
    insecure: bool = False           # Only for lab/test — never in DoD production
    subscriptions: list[GnmiSubscription] = field(default_factory=lambda: list(DEFAULT_SUBSCRIPTIONS))
    reconnect_delay_initial: float = 5.0
    reconnect_delay_max: float = 120.0
    reconnect_backoff: float = 2.0


# ---------------------------------------------------------------------------
# gNMI Collector
# ---------------------------------------------------------------------------


class GnmiCollector:
    """Manages gNMI subscription streams for all registered devices.

    Each device gets one asyncio.Task running a persistent gRPC stream.
    Notifications are decoded, passed to GnmiClassifier, and emitted as
    ChangeEvents into the provided event queue.
    """

    def __init__(self, event_bus: Any) -> None:
        self._event_bus = event_bus
        self._targets: dict[str, GnmiTargetConfig] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._running = False
        self._flap_tracker: dict[str, FlapWindow] = {}

    def add_target(self, config: GnmiTargetConfig) -> None:
        self._targets[config.device_id] = config
        if self._running:
            self._start_task(config.device_id)

    def remove_target(self, device_id: str) -> None:
        task = self._tasks.pop(device_id, None)
        if task:
            task.cancel()
        self._targets.pop(device_id, None)

    def _to_observation(self, event: ChangeEvent) -> AdapterObservation:
        """Return the primary adapter output."""
        return observation_from_change_event(event)

    async def start(self) -> None:
        self._running = True
        for device_id in list(self._targets.keys()):
            self._start_task(device_id)

    async def stop(self) -> None:
        self._running = False
        for task in self._tasks.values():
            task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        self._tasks.clear()

    def _start_task(self, device_id: str) -> None:
        task = asyncio.create_task(
            self._device_stream_loop(device_id),
            name=f"gnmi-{device_id}",
        )
        self._tasks[device_id] = task

    async def _device_stream_loop(self, device_id: str) -> None:
        """Maintain a gNMI stream to one device, reconnecting on failure."""
        cfg = self._targets[device_id]
        classifier = GnmiClassifier(flap_tracker=self._flap_tracker)
        delay = cfg.reconnect_delay_initial

        while self._running:
            try:
                await self._stream_device(cfg, classifier)
                delay = cfg.reconnect_delay_initial  # reset on clean exit
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self._emit(ChangeEvent(
                    kind=ChangeKind.TELEMETRY_SESSION_LOST,
                    severity=ChangeSeverity.MEDIUM,
                    source=EventSource.GNMI,
                    device_id=device_id,
                    device_hostname=cfg.hostname,
                    raw_payload={"error": str(exc)},
                    human_summary=f"gNMI stream lost to {cfg.hostname}: {exc}",
                ))
                logger.warning("gNMI stream lost for %s, retrying in %.0fs: %s", device_id, delay, exc)
                await asyncio.sleep(delay)
                delay = min(delay * cfg.reconnect_backoff, cfg.reconnect_delay_max)

    async def _stream_device(
        self,
        cfg: GnmiTargetConfig,
        classifier: GnmiClassifier,
    ) -> None:
        """Open a gNMI subscription stream and process notifications."""
        try:
            from pygnmi.client import gNMIclient  # type: ignore[import-untyped]
        except ImportError:
            logger.error(
                "pygnmi not installed — gNMI streaming unavailable. "
                "Install with: pip install pygnmi"
            )
            # Back off for a long time rather than spin-looping on import failure
            await asyncio.sleep(cfg.reconnect_delay_max)
            return

        target = (cfg.hostname, cfg.port)
        tls_params: dict[str, Any] = {}
        if cfg.insecure:
            tls_params["insecure"] = True
        elif cfg.tls_ca_path:
            tls_params["path_root"] = cfg.tls_ca_path
            if cfg.tls_cert_path:
                tls_params["path_cert"] = cfg.tls_cert_path
            if cfg.tls_key_path:
                tls_params["path_key"] = cfg.tls_key_path

        # Build subscription list for pygnmi format
        subscriptions = [
            (sub.path, sub.sample_interval_ns if sub.mode == "SAMPLE" else 0)
            for sub in cfg.subscriptions
        ]

        # pygnmi is a synchronous library — wrap in executor for async compat
        loop = asyncio.get_event_loop()

        def _run_stream() -> None:
            with gNMIclient(
                target=target,
                username=cfg.username,
                password=cfg.password or "",
                **tls_params,
            ) as gc:
                for notification in gc.subscribe(
                    subscribe={
                        "subscription": [
                            {"path": path, "mode": sub.mode}
                            for sub, (path, _) in zip(cfg.subscriptions, subscriptions)
                        ],
                        "mode": "STREAM",
                        "encoding": "JSON_IETF",
                    }
                ):
                    if not self._running:
                        break
                    self._process_notification_sync(notification, cfg, classifier)

        # Run in thread executor so it doesn't block the event loop
        await loop.run_in_executor(None, _run_stream)

    def _process_notification_sync(
        self,
        notification: dict[str, Any],
        cfg: GnmiTargetConfig,
        classifier: GnmiClassifier,
    ) -> None:
        """Process one gNMI notification synchronously (called from executor thread)."""
        updates = notification.get("update", {}).get("update", [])
        ts_ns: int = notification.get("update", {}).get("timestamp", 0)

        for update in updates:
            path = update.get("path", "")
            val = update.get("val", {})

            raw_notif = {
                "device_id": cfg.device_id,
                "prefix": notification.get("update", {}).get("prefix", ""),
                "path": path,
                "val": val,
                "timestamp_ns": ts_ns,
            }

            event = classifier.classify(raw_notif)
            if event:
                # Schedule the emit on the event loop from this thread
                asyncio.get_event_loop().call_soon_threadsafe(
                    lambda e=event: asyncio.ensure_future(self._emit(e))
                )

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
            logger.warning("gNMI event publish failed: %s", exc)

    @property
    def active_stream_count(self) -> int:
        return sum(1 for t in self._tasks.values() if not t.done())
