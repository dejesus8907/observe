"""Persistent async SSH session pool with bounded concurrency and backpressure.

Architecture changes from v5:
- Semaphore-based concurrency ceiling (max_concurrent_sessions)
- Worker-pool pattern: devices are queued and processed by a bounded pool
  of worker tasks rather than one-task-per-device
- Overload detection: when queue depth exceeds threshold, polling intervals
  are dynamically lengthened (graceful degradation)
- Connection batching: workers acquire sessions from a pool, poll, release
- Configurable max-connections ceiling with queue-based admission
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from netobserv.streaming.classifier import SshPollClassifier
from netobserv.streaming.adapter_helpers import (
    AdapterObservation,
    observation_from_change_event,
    publish_observation,
)
from netobserv.streaming.events import (
    ChangeEvent,
    ChangeKind,
    ChangeSeverity,
    EventSource,
    FlapWindow,
    default_severity,
)

logger = logging.getLogger("streaming.session_pool")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class SessionConfig:
    """Per-device SSH session configuration."""

    device_id: str
    hostname: str
    port: int = 22
    username: str = "netobserv"
    password: str | None = None
    private_key: str | None = None
    platform: str = "generic"

    # Polling
    poll_interval_seconds: float = 30.0
    keepalive_interval_seconds: float = 60.0

    # Reconnect backoff
    reconnect_initial_delay: float = 5.0
    reconnect_max_delay: float = 300.0
    reconnect_backoff_factor: float = 2.0

    # Command sets per platform
    poll_commands: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.poll_commands:
            self.poll_commands = _default_commands(self.platform)


@dataclass
class PoolConfig:
    """Pool-level configuration for concurrency and overload control."""
    max_concurrent_sessions: int = 200
    max_worker_count: int = 50
    max_devices: int = 2000
    overload_queue_threshold: int = 100
    overload_interval_multiplier: float = 2.0
    max_overload_interval_seconds: float = 120.0
    connection_acquire_timeout_seconds: float = 30.0
    batch_poll_size: int = 10
    # Adaptive degradation thresholds (fraction of max_devices)
    degradation_warn_threshold: float = 0.70
    degradation_critical_threshold: float = 0.90


def _default_commands(platform: str) -> dict[str, str]:
    commands: dict[str, dict[str, str]] = {
        "eos": {
            "interfaces": "show interfaces status | json",
            "neighbors": "show lldp neighbors detail | json",
            "bgp": "show ip bgp summary | json",
        },
        "nxos": {
            "interfaces": "show interface brief | json",
            "neighbors": "show lldp neighbors detail | json",
            "bgp": "show bgp ipv4 unicast summary | json",
        },
        "ios": {
            "interfaces": "show interfaces",
            "neighbors": "show lldp neighbors",
            "bgp": "show ip bgp summary",
        },
        "iosxr": {
            "interfaces": "show interfaces brief",
            "neighbors": "show lldp neighbors",
            "bgp": "show bgp summary",
        },
        "junos": {
            "interfaces": "show interfaces terse | display json",
            "neighbors": "show lldp neighbors | display json",
            "bgp": "show bgp summary | display json",
        },
        "generic": {
            "interfaces": "show interfaces",
            "neighbors": "show lldp neighbors",
        },
    }
    return commands.get(platform, commands["generic"])


# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------


@dataclass
class SessionState:
    """Mutable runtime state for one device session."""

    config: SessionConfig
    connected: bool = False
    last_connected_at: datetime | None = None
    last_poll_at: datetime | None = None
    reconnect_attempts: int = 0
    reconnect_delay: float = 0.0

    # Previous poll results for diffing
    prev_interfaces: dict[str, dict[str, Any]] = field(default_factory=dict)
    prev_neighbors: dict[str, dict[str, Any]] = field(default_factory=dict)
    prev_bgp: dict[str, str] = field(default_factory=dict)

    # Flap tracking
    flap_tracker: dict[str, FlapWindow] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Connection wrapper
# ---------------------------------------------------------------------------


class _ManagedConnection:
    """Wraps an asyncssh connection with lifecycle tracking."""

    def __init__(self, conn: Any, device_id: str) -> None:
        self.conn = conn
        self.device_id = device_id
        self.created_at = _utc_now()
        self.last_used_at = _utc_now()
        self.use_count = 0

    async def run(self, cmd: str) -> Any:
        self.last_used_at = _utc_now()
        self.use_count += 1
        return await self.conn.run(cmd, check=False)

    async def close(self) -> None:
        try:
            self.conn.close()
            await self.conn.wait_closed()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Session Pool
# ---------------------------------------------------------------------------


class PersistentSessionPool:
    """Manages long-lived SSH sessions with bounded concurrency.

    Uses a worker-pool pattern: N workers process devices from a shared
    queue, bounded by a connection semaphore.

    Usage:
        pool = PersistentSessionPool(event_queue)
        pool.add_device(session_config)
        await pool.start()
        ...
        await pool.stop()
    """

    def __init__(
        self,
        event_queue: asyncio.Queue[ChangeEvent],
        pool_config: PoolConfig | None = None,
    ) -> None:
        self._queue = event_queue
        self._pool_config = pool_config or PoolConfig()
        self._sessions: dict[str, SessionState] = {}
        self._connections: dict[str, _ManagedConnection] = {}
        self._running = False
        self._classifier = SshPollClassifier()

        # Concurrency control
        self._connection_semaphore = asyncio.Semaphore(
            self._pool_config.max_concurrent_sessions
        )
        self._work_queue: asyncio.Queue[str] = asyncio.Queue()
        self._workers: list[asyncio.Task[None]] = []
        self._scheduler_task: asyncio.Task[None] | None = None

        # Overload tracking
        self._overloaded = False
        self._effective_interval_multiplier = 1.0

        # Metrics
        self._polls_completed = 0
        self._polls_failed = 0
        self._connections_opened = 0
        self._connections_failed = 0
        self._overload_activations = 0

    def add_device(self, config: SessionConfig) -> None:
        """Register a device for persistent polling.

        Raises ValueError if max_devices would be exceeded.
        """
        if config.device_id in self._sessions:
            return
        if len(self._sessions) >= self._pool_config.max_devices:
            raise ValueError(
                f"Cannot add device {config.device_id}: pool at max_devices "
                f"limit ({self._pool_config.max_devices})"
            )
        self._sessions[config.device_id] = SessionState(config=config)

    def remove_device(self, device_id: str) -> None:
        """Unregister a device and close its connection."""
        self._sessions.pop(device_id, None)
        conn = self._connections.pop(device_id, None)
        if conn:
            asyncio.create_task(conn.close())

    def _to_observation(self, event: ChangeEvent) -> AdapterObservation:
        return observation_from_change_event(event)

    async def start(self) -> None:
        self._running = True
        # Start worker pool
        worker_count = min(
            self._pool_config.max_worker_count,
            max(1, len(self._sessions)),
        )
        for i in range(worker_count):
            task = asyncio.create_task(
                self._worker_loop(i), name=f"ssh-worker-{i}"
            )
            self._workers.append(task)
        # Start scheduler
        self._scheduler_task = asyncio.create_task(
            self._scheduler_loop(), name="ssh-scheduler"
        )
        logger.info(
            "Session pool started",
            extra={
                "workers": worker_count,
                "devices": len(self._sessions),
                "max_concurrent": self._pool_config.max_concurrent_sessions,
            },
        )

    async def stop(self) -> None:
        self._running = False
        if self._scheduler_task:
            self._scheduler_task.cancel()
        for task in self._workers:
            task.cancel()
        await asyncio.gather(
            *self._workers,
            *([] if not self._scheduler_task else [self._scheduler_task]),
            return_exceptions=True,
        )
        self._workers.clear()
        # Close all connections
        for conn in self._connections.values():
            await conn.close()
        self._connections.clear()

    async def _scheduler_loop(self) -> None:
        """Periodically enqueue devices that are due for polling."""
        while self._running:
            try:
                now = _utc_now()
                self._update_overload_state()
                for device_id, state in self._sessions.items():
                    interval = state.config.poll_interval_seconds * self._effective_interval_multiplier
                    if state.last_poll_at is None or (now - state.last_poll_at).total_seconds() >= interval:
                        try:
                            self._work_queue.put_nowait(device_id)
                        except asyncio.QueueFull:
                            pass
                # Sleep for a fraction of the shortest poll interval
                min_interval = min(
                    (s.config.poll_interval_seconds for s in self._sessions.values()),
                    default=30.0,
                )
                await asyncio.sleep(min(min_interval / 4.0, 5.0))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("Scheduler error: %s", exc, exc_info=True)
                await asyncio.sleep(5.0)

    async def _worker_loop(self, worker_id: int) -> None:
        """Worker that processes devices from the work queue."""
        while self._running:
            try:
                device_id = await asyncio.wait_for(
                    self._work_queue.get(), timeout=5.0
                )
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                raise

            if device_id not in self._sessions:
                continue

            try:
                await self._poll_device(device_id)
                self._polls_completed += 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._polls_failed += 1
                logger.debug(
                    "Poll failed for %s: %s", device_id, exc
                )

    async def _poll_device(self, device_id: str) -> None:
        """Acquire a connection (or create one), poll, handle errors."""
        state = self._sessions.get(device_id)
        if not state:
            return

        conn = self._connections.get(device_id)

        # Acquire semaphore before opening new connection
        if conn is None:
            try:
                await asyncio.wait_for(
                    self._connection_semaphore.acquire(),
                    timeout=self._pool_config.connection_acquire_timeout_seconds,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "Connection semaphore timeout for %s — pool at capacity",
                    device_id,
                )
                return

            try:
                conn = await self._open_connection(state)
                if conn is None:
                    self._connection_semaphore.release()
                    return
                self._connections[device_id] = conn
                self._connections_opened += 1
            except Exception:
                self._connection_semaphore.release()
                self._connections_failed += 1
                raise

        try:
            await self._poll_once(conn, state)
            state.last_poll_at = _utc_now()
            state.reconnect_attempts = 0
        except Exception as exc:
            # Connection died — close, release semaphore, schedule reconnect
            await self._handle_connection_failure(device_id, state, exc)

    async def _open_connection(self, state: SessionState) -> _ManagedConnection | None:
        """Open a new SSH connection."""
        try:
            import asyncssh
        except ImportError:
            logger.error("asyncssh not installed — SSH streaming unavailable")
            return None

        cfg = state.config
        connect_kwargs: dict[str, Any] = {
            "host": cfg.hostname,
            "port": cfg.port,
            "username": cfg.username,
            "known_hosts": None,
        }
        if cfg.password:
            connect_kwargs["password"] = cfg.password
        if cfg.private_key:
            connect_kwargs["client_keys"] = [cfg.private_key]

        conn = await asyncssh.connect(**connect_kwargs)
        state.connected = True
        state.last_connected_at = _utc_now()

        await self._emit(ChangeEvent(
            kind=ChangeKind.TELEMETRY_SESSION_RESTORED,
            severity=ChangeSeverity.INFO,
            source=EventSource.SSH_POLL_DIFF,
            device_id=cfg.device_id,
            device_hostname=cfg.hostname,
            human_summary=f"SSH streaming session established to {cfg.hostname}",
        ))

        return _ManagedConnection(conn, cfg.device_id)

    async def _handle_connection_failure(
        self, device_id: str, state: SessionState, exc: Exception
    ) -> None:
        """Close failed connection, release semaphore, emit event."""
        old_conn = self._connections.pop(device_id, None)
        if old_conn:
            await old_conn.close()
            self._connection_semaphore.release()

        if state.connected:
            state.connected = False
            await self._emit(ChangeEvent(
                kind=ChangeKind.TELEMETRY_SESSION_LOST,
                severity=ChangeSeverity.MEDIUM,
                source=EventSource.SSH_POLL_DIFF,
                device_id=device_id,
                device_hostname=state.config.hostname,
                raw_payload={"error": str(exc)},
                human_summary=f"SSH session lost to {state.config.hostname}: {exc}",
            ))

        state.reconnect_attempts += 1
        delay = min(
            state.config.reconnect_initial_delay
            * (state.config.reconnect_backoff_factor ** (state.reconnect_attempts - 1)),
            state.config.reconnect_max_delay,
        )
        state.reconnect_delay = delay
        # Delay next poll by pushing last_poll_at forward
        state.last_poll_at = _utc_now()

    def _update_overload_state(self) -> None:
        """Check queue depth and adjust polling intervals if overloaded."""
        depth = self._work_queue.qsize()
        threshold = self._pool_config.overload_queue_threshold

        if depth > threshold and not self._overloaded:
            self._overloaded = True
            self._effective_interval_multiplier = self._pool_config.overload_interval_multiplier
            self._overload_activations += 1
            logger.warning(
                "SSH pool overloaded (queue=%d > %d), slowing poll intervals by %.1fx",
                depth, threshold, self._effective_interval_multiplier,
            )
        elif depth <= threshold // 2 and self._overloaded:
            self._overloaded = False
            self._effective_interval_multiplier = 1.0
            logger.info("SSH pool overload cleared, restoring normal poll intervals")

    async def _poll_once(self, conn: _ManagedConnection, state: SessionState) -> None:
        """Run one polling cycle: collect, diff, emit events."""
        cfg = state.config

        if "interfaces" in cfg.poll_commands:
            try:
                result = await conn.run(cfg.poll_commands["interfaces"])
                curr_interfaces = self._parse_interfaces(result.stdout or "", cfg.platform)
                events = self._classifier.diff_interfaces(
                    cfg.device_id, state.prev_interfaces, curr_interfaces, state.flap_tracker
                )
                for event in events:
                    await self._emit(event)
                state.prev_interfaces = curr_interfaces
            except Exception as exc:
                logger.debug("Interface poll failed", extra={"device": cfg.device_id, "error": str(exc)})

        if "neighbors" in cfg.poll_commands:
            try:
                result = await conn.run(cfg.poll_commands["neighbors"])
                curr_neighbors = self._parse_neighbors(result.stdout or "", cfg.platform)
                events = self._classifier.diff_neighbors(
                    cfg.device_id, state.prev_neighbors, curr_neighbors
                )
                for event in events:
                    await self._emit(event)
                state.prev_neighbors = curr_neighbors
            except Exception as exc:
                logger.debug("Neighbor poll failed", extra={"device": cfg.device_id, "error": str(exc)})

        if "bgp" in cfg.poll_commands:
            try:
                result = await conn.run(cfg.poll_commands["bgp"])
                curr_bgp = self._parse_bgp(result.stdout or "", cfg.platform)
                events = self._classifier.diff_bgp_sessions(
                    cfg.device_id, state.prev_bgp, curr_bgp, state.flap_tracker
                )
                for event in events:
                    await self._emit(event)
                state.prev_bgp = curr_bgp
            except Exception as exc:
                logger.debug("BGP poll failed", extra={"device": cfg.device_id, "error": str(exc)})

    async def _emit(self, event: ChangeEvent) -> None:
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.warning("Event queue full — dropping event", extra={"kind": event.kind.value})

    # ------------------------------------------------------------------
    # Platform-specific parsers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_interfaces(output: str, platform: str) -> dict[str, dict[str, Any]]:
        import json, re
        result: dict[str, dict[str, Any]] = {}
        try:
            data = json.loads(output)
            if platform in ("eos",):
                interfaces = data.get("interfaceStatuses", data.get("interfaces", {}))
                for name, info in interfaces.items():
                    oper = str(info.get("lineProtocolStatus", info.get("operStatus", "unknown"))).lower()
                    result[name] = {"oper_state": "up" if "up" in oper else "down"}
            elif platform in ("nxos",):
                for row in data.get("TABLE_interface", {}).get("ROW_interface", []):
                    name = row.get("interface", "")
                    state = str(row.get("state", "down")).lower()
                    result[name] = {"oper_state": "up" if state == "up" else "down"}
        except (json.JSONDecodeError, AttributeError):
            for match in re.finditer(r"(\S+)\s+is\s+(up|down)", output, re.IGNORECASE):
                result[match.group(1)] = {"oper_state": match.group(2).lower()}
        return result

    @staticmethod
    def _parse_neighbors(output: str, platform: str) -> dict[str, dict[str, Any]]:
        import json, re
        result: dict[str, dict[str, Any]] = {}
        try:
            data = json.loads(output)
            if platform in ("eos",):
                for n in data.get("lldpNeighbors", []):
                    port = n.get("port", "")
                    result[port] = {
                        "remote_hostname": n.get("neighborDevice", ""),
                        "remote_interface": n.get("neighborPort", ""),
                    }
        except (json.JSONDecodeError, AttributeError):
            current_port: str | None = None
            for line in output.splitlines():
                port_m = re.match(r"^Local\s+Intf:\s+(\S+)", line, re.IGNORECASE)
                dev_m = re.match(r"^\s+System Name:\s+(\S+)", line, re.IGNORECASE)
                if port_m:
                    current_port = port_m.group(1)
                if dev_m and current_port:
                    result[current_port] = {"remote_hostname": dev_m.group(1), "remote_interface": ""}
        return result

    @staticmethod
    def _parse_bgp(output: str, platform: str) -> dict[str, str]:
        import json, re
        result: dict[str, str] = {}
        try:
            data = json.loads(output)
            if platform in ("eos",):
                for vrf_data in data.get("vrfs", {}).values():
                    for peer_addr, peer_info in vrf_data.get("peers", {}).items():
                        result[peer_addr] = str(peer_info.get("peerState", "unknown")).upper()
        except (json.JSONDecodeError, AttributeError):
            for match in re.finditer(r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s+\d+\s+\d+\s+(\w+)", output):
                result[match.group(1)] = match.group(2).upper()
        return result

    @property
    def active_session_count(self) -> int:
        return sum(1 for s in self._sessions.values() if s.connected)

    @property
    def total_device_count(self) -> int:
        return len(self._sessions)

    @property
    def stats(self) -> dict[str, Any]:
        device_ratio = len(self._sessions) / max(self._pool_config.max_devices, 1)
        if device_ratio >= self._pool_config.degradation_critical_threshold:
            degradation_level = "critical"
        elif device_ratio >= self._pool_config.degradation_warn_threshold:
            degradation_level = "warning"
        else:
            degradation_level = "normal"
        return {
            "active_sessions": self.active_session_count,
            "total_devices": self.total_device_count,
            "max_devices": self._pool_config.max_devices,
            "device_utilization": round(device_ratio, 3),
            "degradation_level": degradation_level,
            "polls_completed": self._polls_completed,
            "polls_failed": self._polls_failed,
            "connections_opened": self._connections_opened,
            "connections_failed": self._connections_failed,
            "overloaded": self._overloaded,
            "overload_activations": self._overload_activations,
            "effective_interval_multiplier": self._effective_interval_multiplier,
            "work_queue_depth": self._work_queue.qsize(),
            "connection_semaphore_available": self._connection_semaphore._value,
            "max_concurrent_sessions": self._pool_config.max_concurrent_sessions,
        }
