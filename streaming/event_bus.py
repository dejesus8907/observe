
"""Enhanced in-process async event bus with priority dispatch, fast-path handling,
and explicit truth-normalization boundaries.

Design notes:
- Priority queue: fresher / more authoritative sources are dispatched first.
- Explicit fast-path: urgent topology events can bypass the standard queue.
- Overload policy: when full, the bus can evict a lower-priority queued item to
  admit a higher-priority incoming event.
- Truth normalization boundary: conflict resolution converts noisy observations
  into resolved assertions before topology mutation.
- Correlation policy: by default correlation runs on resolved / normalized events
  only. This is intentional and configurable.
"""

from __future__ import annotations

import asyncio
import hashlib
import heapq
import inspect
import logging
from collections import defaultdict
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from netobserv.streaming.conflict import (
    ConflictResolutionFacade,
    EvidenceRecord,
    ResolutionState,
    ResolvedAssertion,
    synthesize_change_event_from_assertion,
)
from netobserv.streaming.conflict.integration import (
    evidence_record_from_change_event as evidence_record_from_streaming_event,
)
from netobserv.streaming.correlation_integration import CorrelationIntegrationService
from netobserv.streaming.events import ChangeEvent, ChangeKind
from netobserv.streaming.topology_patcher import TopologyDelta, TopologyPatcher

logger = logging.getLogger("streaming.event_bus")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


AsyncEventCallback = Callable[[ChangeEvent], Coroutine[Any, Any, None]]
AsyncDeltaCallback = Callable[[TopologyDelta], Coroutine[Any, Any, None]]
AsyncResolvedAssertionCallback = Callable[[ResolvedAssertion], Coroutine[Any, Any, None]]
AsyncEvidenceCallback = Callable[[EvidenceRecord], Coroutine[Any, Any, None]]
AsyncCorrelationCallback = Callable[[Any, Any], Coroutine[Any, Any, None]]


_SOURCE_PRIORITY: dict[str, int] = {
    "gnmi": 0,
    "gnmi_stream": 0,
    "EventSource.GNMI": 0,
    "netconf": 1,
    "netconf_notification": 1,
    "EventSource.NETCONF_NOTIFICATION": 1,
    "EventSource.NETCONF": 1,
    "snmp_trap": 2,
    "EventSource.SNMP_TRAP": 2,
    "syslog": 3,
    "syslog_structured": 3,
    "EventSource.SYSLOG": 3,
    "ssh_poll_diff": 4,
    "EventSource.SSH_POLL_DIFF": 4,
    "periodic_poll": 5,
    "manual_import": 6,
}
_DEFAULT_PRIORITY = 3

_FAST_PATH_KIND_NAMES = {
    "INTERFACE_DOWN",
    "INTERFACE_UP",
    "NEIGHBOR_DISCOVERED",
    "NEIGHBOR_LOST",
    "DEVICE_DOWN",
    "DEVICE_UP",
    "BGP_SESSION_DOWN",
    "BGP_SESSION_UP",
}


def _event_priority(event: ChangeEvent) -> int:
    """Return dispatch priority for an event (lower = higher priority)."""
    source_str = str(event.source) if hasattr(event, "source") else ""
    return _SOURCE_PRIORITY.get(source_str, _DEFAULT_PRIORITY)


def _default_fast_path(event: ChangeEvent) -> bool:
    kind_name = getattr(getattr(event, "kind", None), "name", "")
    return _event_priority(event) <= 1 or kind_name in _FAST_PATH_KIND_NAMES


@dataclass(order=True)
class _PrioritizedEvent:
    """Wrapper for priority queue ordering."""
    priority: int
    sequence: int
    event: ChangeEvent = field(compare=False)


@dataclass
class _DispatchEnvelope:
    original_event: ChangeEvent
    resolved_event: ChangeEvent
    resolved_assertion: ResolvedAssertion | None
    topology_blocked: bool
    evidence: EvidenceRecord | None = None
    from_fast_path: bool = False
    from_evidence_path: bool = False


class BackpressureState:
    """Tracks backpressure level for the event bus."""
    NONE = "none"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class EventBus:
    """Routes ChangeEvents from ingest sources to subscribers.

    Core properties:
    - Priority dispatch by source type.
    - Optional fast-path queue for urgent topology events.
    - Conflict-resolution / truth-normalization before topology mutation.
    - Priority-aware eviction under queue-full conditions.
    - Explicit loop ownership for thread-safe publish_sync().
    """

    _BP_LOW = 0.50
    _BP_MEDIUM = 0.70
    _BP_HIGH = 0.85
    _BP_CRITICAL = 0.95

    def __init__(
        self,
        dedup_window_seconds: float = 5.0,
        subscriber_timeout_seconds: float = 2.0,
        queue_max_size: int = 10_000,
        conflict_window_seconds: float = 30.0,
        conflict_resolution_enabled: bool = True,
        conflict_max_records_per_key: int = 16,
        *,
        topology_timeout_seconds: float | None = None,
        persistence_timeout_seconds: float | None = None,
        broadcast_timeout_seconds: float | None = None,
        correlation_timeout_seconds: float | None = None,
        evidence_timeout_seconds: float | None = None,
        delta_timeout_seconds: float | None = None,
        correlation_requires_resolution: bool = True,
        fast_path_enabled: bool = True,
        fast_path_queue_max_size: int = 1_000,
    ) -> None:
        self._dedup_window = dedup_window_seconds
        self._subscriber_timeout = subscriber_timeout_seconds
        self._topology_timeout = topology_timeout_seconds or subscriber_timeout_seconds
        self._persistence_timeout = persistence_timeout_seconds or subscriber_timeout_seconds
        self._broadcast_timeout = broadcast_timeout_seconds or subscriber_timeout_seconds
        self._correlation_timeout = correlation_timeout_seconds or subscriber_timeout_seconds
        self._evidence_timeout = evidence_timeout_seconds or subscriber_timeout_seconds
        self._delta_timeout = delta_timeout_seconds or subscriber_timeout_seconds

        self._queue_max_size = queue_max_size
        self._queue: asyncio.PriorityQueue[_PrioritizedEvent] = asyncio.PriorityQueue(maxsize=queue_max_size)

        self._fast_path_enabled = fast_path_enabled
        self._fast_path_queue_max_size = fast_path_queue_max_size
        self._fast_path_queue: asyncio.PriorityQueue[_PrioritizedEvent] = asyncio.PriorityQueue(
            maxsize=fast_path_queue_max_size
        )

        self._sequence_counter = 0
        self._subscribers: dict[ChangeKind | None, list[AsyncEventCallback]] = {}
        self._delta_subscribers: list[AsyncDeltaCallback] = []
        self._patcher: TopologyPatcher | None = None
        self._broadcast: AsyncEventCallback | None = None
        self._persist: AsyncEventCallback | None = None
        self._persist_resolution: AsyncResolvedAssertionCallback | None = None
        self._persist_evidence: AsyncEvidenceCallback | None = None
        self._persist_correlation: AsyncCorrelationCallback | None = None
        self._correlation = CorrelationIntegrationService()
        self._dedup_cache: dict[str, datetime] = {}
        self._running = False
        self._dispatch_task: asyncio.Task[None] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

        self._conflict_resolution_enabled = conflict_resolution_enabled
        self._conflict_window_seconds = conflict_window_seconds
        self._conflict_max_records_per_key = conflict_max_records_per_key
        self._conflict_facade = ConflictResolutionFacade()
        self._evidence_window: dict[tuple[str, str, str], list[EvidenceRecord]] = defaultdict(list)

        self._correlation_requires_resolution = correlation_requires_resolution

        self._events_received = 0
        self._events_deduplicated = 0
        self._events_dispatched = 0
        self._dispatch_errors = 0
        self._events_disputed = 0
        self._events_dropped_backpressure = 0
        self._events_evicted_for_priority = 0
        self._events_skipped_correlation = 0
        self._fast_path_dispatched = 0
        self._fast_path_enqueued = 0

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def subscribe(self, kind: ChangeKind | None, callback: AsyncEventCallback) -> None:
        if kind not in self._subscribers:
            self._subscribers[kind] = []
        self._subscribers[kind].append(callback)

    def subscribe_delta(self, callback: AsyncDeltaCallback) -> None:
        self._delta_subscribers.append(callback)

    def set_patcher(self, patcher: TopologyPatcher) -> None:
        self._patcher = patcher

    def set_broadcast(self, broadcast_fn: AsyncEventCallback) -> None:
        self._broadcast = broadcast_fn

    def set_persist(self, persist_fn: AsyncEventCallback) -> None:
        self._persist = persist_fn

    def set_resolution_persist(self, persist_fn: AsyncResolvedAssertionCallback) -> None:
        self._persist_resolution = persist_fn

    def set_evidence_persist(self, persist_fn: AsyncEvidenceCallback) -> None:
        self._persist_evidence = persist_fn

    def set_correlation_persist(self, persist_fn: AsyncCorrelationCallback) -> None:
        self._persist_correlation = persist_fn

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        if self._running:
            return
        self._loop = asyncio.get_running_loop()
        self._running = True
        self._dispatch_task = asyncio.create_task(self._dispatch_loop(), name="event-bus-dispatch")
        logger.info("Event bus started")

    async def stop(self) -> None:
        self._running = False
        if self._dispatch_task is not None:
            self._dispatch_task.cancel()
            await asyncio.gather(self._dispatch_task, return_exceptions=True)
            self._dispatch_task = None
        logger.info("Event bus stopped", extra=self.stats)

    # ------------------------------------------------------------------
    # Publish API
    # ------------------------------------------------------------------

    async def publish(self, event: ChangeEvent, *, fast_path: bool | None = None) -> None:
        self._events_received += 1

        if event.dedup_key:
            if self._is_duplicate(event):
                self._events_deduplicated += 1
                return

        if fast_path is None:
            fast_path = self._fast_path_enabled and _default_fast_path(event)

        if fast_path:
            accepted = self._try_enqueue_fast_path(event)
            if accepted:
                if event.dedup_key:
                    self._record_dedup(event)
                self._fast_path_enqueued += 1
                return
            logger.warning("Fast-path queue saturated; falling back to standard queue for %s", event.event_id)

        # Under severe pressure, pre-emptively shed low-value events.
        bp = self.backpressure_level
        priority = _event_priority(event)
        if bp == BackpressureState.CRITICAL and priority >= 4:
            self._events_dropped_backpressure += 1
            logger.warning(
                "Dropping low-priority event under critical backpressure: %s from %s",
                event.kind.value,
                event.device_id,
            )
            return

        accepted = self._try_enqueue_standard(event)
        if accepted and event.dedup_key:
            self._record_dedup(event)

    async def publish_fast(self, event: ChangeEvent) -> None:
        """Explicit API for fast-path publishing."""
        await self.publish(event, fast_path=True)

    def publish_sync(self, event: ChangeEvent, *, fast_path: bool | None = None) -> None:
        """Thread-safe sync bridge onto the event bus loop."""
        if self._loop is None:
            raise RuntimeError("Event bus must be started before publish_sync() can be used")

        def _schedule() -> None:
            asyncio.create_task(self.publish(event, fast_path=fast_path))

        self._loop.call_soon_threadsafe(_schedule)

    # ------------------------------------------------------------------
    # Queue admission / backpressure
    # ------------------------------------------------------------------

    @property
    def backpressure_level(self) -> str:
        if self._queue_max_size <= 0:
            return BackpressureState.NONE
        ratio = self._queue.qsize() / self._queue_max_size
        if ratio >= self._BP_CRITICAL:
            return BackpressureState.CRITICAL
        if ratio >= self._BP_HIGH:
            return BackpressureState.HIGH
        if ratio >= self._BP_MEDIUM:
            return BackpressureState.MEDIUM
        if ratio >= self._BP_LOW:
            return BackpressureState.LOW
        return BackpressureState.NONE

    def _next_sequence(self) -> int:
        self._sequence_counter += 1
        return self._sequence_counter

    def _try_enqueue_fast_path(self, event: ChangeEvent) -> bool:
        if not self._fast_path_enabled:
            return False
        item = _PrioritizedEvent(priority=_event_priority(event), sequence=self._next_sequence(), event=event)
        try:
            self._fast_path_queue.put_nowait(item)
            return True
        except asyncio.QueueFull:
            self._events_dropped_backpressure += 1
            return False

    def _try_enqueue_standard(self, event: ChangeEvent) -> bool:
        item = _PrioritizedEvent(priority=_event_priority(event), sequence=self._next_sequence(), event=event)
        try:
            self._queue.put_nowait(item)
            return True
        except asyncio.QueueFull:
            if self._evict_worst_for_better(item):
                return True
            self._events_dropped_backpressure += 1
            logger.warning("Event bus queue full — dropping %s from %s", event.kind.value, event.device_id)
            return False

    def _evict_worst_for_better(self, incoming: _PrioritizedEvent) -> bool:
        """Best-effort priority-aware eviction.

        If the standard queue is full and the incoming event has a higher priority
        (lower numeric priority) than the currently worst queued item, evict the
        worst item and admit the incoming event.
        """
        heap = getattr(self._queue, "_queue", None)
        if not isinstance(heap, list) or not heap:
            return False

        worst_index = max(range(len(heap)), key=lambda idx: (heap[idx].priority, heap[idx].sequence))
        worst_item = heap[worst_index]
        if (incoming.priority, incoming.sequence) >= (worst_item.priority, worst_item.sequence):
            return False

        evicted = heap.pop(worst_index)
        heapq.heapify(heap)
        heapq.heappush(heap, incoming)
        self._events_evicted_for_priority += 1
        self._events_dropped_backpressure += 1
        logger.warning(
            "Evicted queued event %s from %s to admit higher-priority event %s from %s",
            evicted.event.kind.value,
            evicted.event.device_id,
            incoming.event.kind.value,
            incoming.event.device_id,
        )
        return True

    # ------------------------------------------------------------------
    # Evidence / conflict handling
    # ------------------------------------------------------------------

    def _prune_evidence_window(self, now: datetime) -> None:
        cutoff = now - timedelta(seconds=self._conflict_window_seconds)
        dead_keys: list[tuple[str, str, str]] = []
        for key, records in list(self._evidence_window.items()):
            kept = [r for r in records if r.observed_at >= cutoff]
            if kept:
                self._evidence_window[key] = kept[-self._conflict_max_records_per_key :]
            else:
                dead_keys.append(key)
        for key in dead_keys:
            self._evidence_window.pop(key, None)

    def _adapt_event_from_resolution(self, event: ChangeEvent, resolved: ResolvedAssertion) -> ChangeEvent:
        return synthesize_change_event_from_assertion(resolved, event)

    def _resolve_event_for_topology(self, event: ChangeEvent) -> _DispatchEnvelope:
        if not self._conflict_resolution_enabled:
            return _DispatchEnvelope(
                original_event=event,
                resolved_event=event,
                resolved_assertion=None,
                topology_blocked=False,
            )

        evidence = evidence_record_from_streaming_event(event)
        if evidence is None:
            return _DispatchEnvelope(
                original_event=event,
                resolved_event=event,
                resolved_assertion=None,
                topology_blocked=False,
            )

        now = event.received_at
        self._prune_evidence_window(now)
        key = (evidence.subject_type.value, evidence.subject_id, evidence.field_name)
        bucket = self._evidence_window[key]
        bucket.append(evidence)
        if len(bucket) > self._conflict_max_records_per_key:
            del bucket[0 : len(bucket) - self._conflict_max_records_per_key]

        resolved = self._conflict_facade.resolve_group(bucket, now=now)
        adapted = self._adapt_event_from_resolution(event, resolved)
        topology_blocked = resolved.resolution_state == ResolutionState.DISPUTED
        if topology_blocked:
            self._events_disputed += 1

        return _DispatchEnvelope(
            original_event=event,
            resolved_event=adapted,
            resolved_assertion=resolved,
            topology_blocked=topology_blocked,
            evidence=evidence,
        )

    # ------------------------------------------------------------------
    # Dispatch loop
    # ------------------------------------------------------------------

    async def _dispatch_loop(self) -> None:
        while self._running:
            try:
                item = self._get_next_item_nonblocking()
                if item is None:
                    item = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                    queue_ref = self._queue
                else:
                    queue_ref = self._fast_path_queue

                envelope = self._resolve_event_for_topology(item.event)
                envelope.from_fast_path = queue_ref is self._fast_path_queue
                if envelope.from_fast_path:
                    self._fast_path_dispatched += 1

                await self._dispatch_envelope(envelope)
                queue_ref.task_done()
            except asyncio.TimeoutError:
                self._prune_dedup_cache()
                self._prune_evidence_window(_utc_now())
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._dispatch_errors += 1
                logger.error("Event dispatch error: %s", exc, exc_info=True)

    def _get_next_item_nonblocking(self) -> _PrioritizedEvent | None:
        if not self._fast_path_enabled:
            return None
        try:
            return self._fast_path_queue.get_nowait()
        except asyncio.QueueEmpty:
            return None

    async def _dispatch_event(self, event: ChangeEvent) -> None:
        envelope = self._resolve_event_for_topology(event)
        await self._dispatch_envelope(envelope)

    async def _dispatch_resolved_assertion(self, resolved_assertion: ResolvedAssertion, resolved_event: ChangeEvent) -> None:
        envelope = _DispatchEnvelope(
            original_event=resolved_event,
            resolved_event=resolved_event,
            resolved_assertion=resolved_assertion,
            topology_blocked=resolved_assertion.resolution_state == ResolutionState.DISPUTED,
            from_evidence_path=True,
        )
        if envelope.topology_blocked:
            self._events_disputed += 1
        await self._dispatch_envelope(envelope)

    async def _dispatch_envelope(self, envelope: _DispatchEnvelope) -> None:
        self._events_dispatched += 1
        tasks: list[Coroutine[Any, Any, Any]] = []

        resolved_event = envelope.resolved_event
        resolved_assertion = envelope.resolved_assertion

        if self._patcher and not envelope.topology_blocked:
            if resolved_assertion is not None:
                tasks.append(self._run_patcher_resolved(resolved_assertion, resolved_event))
            else:
                tasks.append(self._run_patcher(resolved_event))

        cluster_id: str | None = None
        decision: Any | None = None
        if self._should_correlate(envelope):
            decision = self._correlation.correlate_event(resolved_event)
            cluster_id = getattr(decision, "cluster_id", None)
            if self._persist_correlation is not None:
                tasks.append(
                    self._run_correlation_callback(
                        self._persist_correlation,
                        decision,
                        cluster_id,
                        "persist_correlation",
                    )
                )
        else:
            self._events_skipped_correlation += 1

        if self._persist is not None:
            tasks.append(self._run_event_callback(self._persist, resolved_event, "persist", self._persistence_timeout))

        if self._persist_resolution is not None and resolved_assertion is not None:
            if cluster_id:
                tasks.append(
                    self._run_resolution_callback_with_cluster(
                        self._persist_resolution,
                        resolved_assertion,
                        cluster_id,
                        "persist_resolution",
                    )
                )
            else:
                tasks.append(
                    self._run_resolution_callback(
                        self._persist_resolution,
                        resolved_assertion,
                        "persist_resolution",
                    )
                )

        if self._broadcast is not None:
            tasks.append(self._run_event_callback(self._broadcast, resolved_event, "broadcast", self._broadcast_timeout))

        for kind_key in (resolved_event.kind, None):
            for callback in self._subscribers.get(kind_key, []):
                tasks.append(
                    self._run_event_callback(
                        callback,
                        resolved_event,
                        getattr(callback, "__name__", "subscriber"),
                        self._subscriber_timeout,
                    )
                )

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _should_correlate(self, envelope: _DispatchEnvelope) -> bool:
        if envelope.resolved_assertion is not None:
            return True
        if self._correlation_requires_resolution:
            return False
        return True

    # ------------------------------------------------------------------
    # Sinks / callbacks
    # ------------------------------------------------------------------

    async def _run_patcher_resolved(self, assertion: ResolvedAssertion, source_event: ChangeEvent) -> None:
        assert self._patcher is not None
        try:
            deltas = await asyncio.wait_for(
                self._patcher.apply_resolved_assertion(assertion, source_event=source_event),
                timeout=self._topology_timeout,
            )
            await self._fanout_deltas(deltas)
        except asyncio.TimeoutError:
            self._dispatch_errors += 1
            logger.warning("Resolved topology patcher timed out for event %s", source_event.event_id)
        except Exception as exc:
            self._dispatch_errors += 1
            logger.error("Resolved topology patcher error: %s", exc, exc_info=True)

    async def _run_patcher(self, event: ChangeEvent) -> None:
        if self._patcher is None:
            return
        try:
            deltas = await asyncio.wait_for(self._patcher.apply(event), timeout=self._topology_timeout)
            await self._fanout_deltas(deltas)
        except asyncio.TimeoutError:
            self._dispatch_errors += 1
            logger.warning("Topology patcher timed out for event %s", event.event_id)
        except Exception as exc:
            self._dispatch_errors += 1
            logger.error("Topology patcher error: %s", exc, exc_info=True)

    async def _fanout_deltas(self, deltas: list[TopologyDelta]) -> None:
        for delta in deltas:
            for callback in self._delta_subscribers:
                try:
                    await asyncio.wait_for(callback(delta), timeout=self._delta_timeout)
                except asyncio.TimeoutError:
                    self._dispatch_errors += 1
                    logger.warning("Delta subscriber timed out")
                except Exception as exc:
                    self._dispatch_errors += 1
                    logger.warning("Delta subscriber failed: %s", exc)

    async def _run_event_callback(
        self,
        callback: AsyncEventCallback,
        event: ChangeEvent,
        name: str,
        timeout_seconds: float,
    ) -> None:
        try:
            await asyncio.wait_for(callback(event), timeout=timeout_seconds)
        except asyncio.TimeoutError:
            self._dispatch_errors += 1
            logger.warning("Callback %s timed out for event %s", name, event.event_id)
        except Exception as exc:
            self._dispatch_errors += 1
            logger.error("Callback %s error: %s", name, exc)

    async def _run_resolution_callback(
        self,
        callback: AsyncResolvedAssertionCallback,
        assertion: ResolvedAssertion,
        name: str,
    ) -> None:
        try:
            await asyncio.wait_for(callback(assertion), timeout=self._persistence_timeout)
        except asyncio.TimeoutError:
            self._dispatch_errors += 1
            logger.warning("Resolved assertion callback %s timed out", name)
        except Exception as exc:
            self._dispatch_errors += 1
            logger.warning("Resolved assertion callback %s failed: %s", name, exc)

    async def _run_resolution_callback_with_cluster(
        self,
        callback: Any,
        assertion: ResolvedAssertion,
        cluster_id: str,
        name: str,
    ) -> None:
        try:
            result = callback(assertion, cluster_id=cluster_id)
            if inspect.isawaitable(result):
                await asyncio.wait_for(result, timeout=self._persistence_timeout)
        except TypeError:
            await self._run_resolution_callback(callback, assertion, name)
        except asyncio.TimeoutError:
            self._dispatch_errors += 1
            logger.warning("Resolved assertion callback %s timed out", name)
        except Exception as exc:
            self._dispatch_errors += 1
            logger.warning("Resolved assertion callback %s failed: %s", name, exc)

    async def _run_correlation_callback(self, callback: Any, decision: Any, cluster_id: str | None, name: str) -> None:
        try:
            await asyncio.wait_for(callback(decision, cluster_id), timeout=self._correlation_timeout)
        except asyncio.TimeoutError:
            self._dispatch_errors += 1
            logger.warning("Correlation callback %s timed out", name)
        except Exception as exc:
            self._dispatch_errors += 1
            logger.warning("Correlation callback %s failed: %s", name, exc)

    async def _run_evidence_callback(self, callback: AsyncEvidenceCallback, evidence: EvidenceRecord, name: str) -> None:
        try:
            await asyncio.wait_for(callback(evidence), timeout=self._evidence_timeout)
        except asyncio.TimeoutError:
            self._dispatch_errors += 1
            logger.warning("Evidence callback %s timed out", name)
        except Exception as exc:
            self._dispatch_errors += 1
            logger.warning("Evidence callback %s failed: %s", name, exc)

    # ------------------------------------------------------------------
    # Deduplication
    # ------------------------------------------------------------------

    def _is_duplicate(self, event: ChangeEvent) -> bool:
        if not event.dedup_key:
            return False
        key = self._dedup_hash(event)
        last_seen = self._dedup_cache.get(key)
        if last_seen is None:
            return False
        elapsed = (_utc_now() - last_seen).total_seconds()
        return elapsed < self._dedup_window

    def _record_dedup(self, event: ChangeEvent) -> None:
        if event.dedup_key:
            self._dedup_cache[self._dedup_hash(event)] = _utc_now()

    def _dedup_hash(self, event: ChangeEvent) -> str:
        raw = f"{event.kind.value}:{event.dedup_key}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def _prune_dedup_cache(self) -> None:
        now = _utc_now()
        stale = [
            key
            for key, ts in self._dedup_cache.items()
            if (now - ts).total_seconds() > self._dedup_window * 2
        ]
        for key in stale:
            self._dedup_cache.pop(key, None)

    # ------------------------------------------------------------------
    # Evidence-native API
    # ------------------------------------------------------------------

    async def publish_evidence_record(self, evidence: EvidenceRecord, *, source_event: ChangeEvent) -> None:
        if self._persist_evidence is not None:
            await self._run_evidence_callback(self._persist_evidence, evidence, "persist_evidence")

        now = source_event.received_at
        self._prune_evidence_window(now)
        key = (evidence.subject_type.value, evidence.subject_id, evidence.field_name)
        bucket = self._evidence_window[key]
        bucket.append(evidence)
        if len(bucket) > self._conflict_max_records_per_key:
            del bucket[0 : len(bucket) - self._conflict_max_records_per_key]

        resolved_assertion = self._conflict_facade.resolve_group(bucket, now=now)
        resolved_event = self._adapt_event_from_resolution(source_event, resolved_assertion)
        await self._dispatch_resolved_assertion(resolved_assertion, resolved_event)

    async def publish_evidence_records(self, evidence_records: list[EvidenceRecord], *, source_event: ChangeEvent) -> None:
        for evidence in evidence_records:
            await self.publish_evidence_record(evidence, source_event=source_event)

    async def publish_adapter_observation(
        self,
        *,
        evidence: EvidenceRecord | None,
        fallback_event: ChangeEvent | None = None,
        source_event: ChangeEvent | None = None,
    ) -> None:
        if evidence is not None:
            if source_event is None:
                source_event = fallback_event
            if source_event is None:
                raise ValueError("source_event or fallback_event is required when publishing evidence")
            await self.publish_evidence_record(evidence, source_event=source_event)
            return

        if fallback_event is None:
            raise ValueError("fallback_event is required when no evidence is available")
        await self.publish(fallback_event)

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def stats(self) -> dict[str, int | str]:
        return {
            "events_received": self._events_received,
            "events_deduplicated": self._events_deduplicated,
            "events_dispatched": self._events_dispatched,
            "dispatch_errors": self._dispatch_errors,
            "events_disputed": self._events_disputed,
            "events_dropped_backpressure": self._events_dropped_backpressure,
            "events_evicted_for_priority": self._events_evicted_for_priority,
            "events_skipped_correlation": self._events_skipped_correlation,
            "fast_path_enqueued": self._fast_path_enqueued,
            "fast_path_dispatched": self._fast_path_dispatched,
            "queue_size": self._queue.qsize(),
            "fast_path_queue_size": self._fast_path_queue.qsize(),
            "dedup_cache_size": len(self._dedup_cache),
            "backpressure_level": self.backpressure_level,
            "correlation_requires_resolution": str(self._correlation_requires_resolution).lower(),
        }
