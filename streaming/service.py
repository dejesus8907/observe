"""Lifecycle wrapper for the streaming engine."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone

from netobserv.streaming.engine import StreamingEngine


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class StreamingServiceConfig:
    heartbeat_interval_seconds: float = 30.0
    emit_health_log: bool = False

    def __post_init__(self) -> None:
        if self.heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat_interval_seconds must be > 0")


class StreamingService:
    """Small lifecycle/service wrapper around the streaming engine.

    It does not implement its own collectors; it coordinates engine start/stop
    and optionally runs a heartbeat loop so startup wiring has one object to own.
    """

    def __init__(self, engine: StreamingEngine, config: StreamingServiceConfig | None = None) -> None:
        self._engine = engine
        self._config = config or StreamingServiceConfig()
        self._running = False
        self._heartbeat_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        await self._engine.start()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(), name="streaming-service-heartbeat")

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            await asyncio.gather(self._heartbeat_task, return_exceptions=True)
        await self._engine.stop()

    async def _heartbeat_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._config.heartbeat_interval_seconds)
            if self._config.emit_health_log:
                _ = self._engine.get_bus_stats()

    @property
    def is_running(self) -> bool:
        return self._running and self._engine.is_running
