"""Incremental topology patcher — applies ChangeEvents as surgical edge diffs.

The existing topology builder does a full graph rebuild from scratch on every
discovery run. That is correct for snapshots and completely wrong for streaming.
When a link goes down, you do not re-discover 500 devices to update one edge.

This patcher receives ChangeEvents from the event bus and applies minimal,
targeted mutations to the live topology graph:
  - INTERFACE_DOWN on a device → mark affected edges as degraded
  - NEIGHBOR_LOST → remove or downgrade topology edge
  - NEIGHBOR_DISCOVERED → create new edge with streaming evidence
  - BGP_SESSION_DROPPED → update BGP peer edge state
  - MAC_MOVE → flag affected L2 edges for conflict review

Design
------
The patcher operates on an in-memory edge index keyed by (source_id, target_id).
Writes are async-safe via an asyncio.Lock. The patcher also persists delta
records to the database for operator audit and temporal queries.

A TopologyDelta captures:
  - which edge changed
  - what changed (added / removed / degraded / restored)
  - which ChangeEvent caused it
  - the timestamp
  - the new edge state

This delta stream is the foundation for the "dynamic, real-time bird's-eye view"
claim. The WebSocket push API watches for deltas and streams them to clients.
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from netobserv.streaming.conflict import ResolvedAssertion
from netobserv.streaming.events import ChangeEvent, ChangeKind

try:
    from netobserv.observability.logging import get_logger
    logger = get_logger("streaming.topology_patcher")
except ImportError:
    import logging
    logger = logging.getLogger("streaming.topology_patcher")  # type: ignore[assignment]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _new_id() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Delta model
# ---------------------------------------------------------------------------


class EdgeDeltaKind(str, Enum):
    ADDED = "added"
    REMOVED = "removed"
    DEGRADED = "degraded"           # edge exists but link is down
    RESTORED = "restored"           # edge recovered from degraded
    CONFIDENCE_CHANGED = "confidence_changed"
    CONFLICT_FLAGGED = "conflict_flagged"


@dataclass(frozen=True)
class LiveEdgeState:
    """Current state of a topology edge in the live graph."""

    edge_id: str
    source_node_id: str
    target_node_id: str
    source_interface: str | None
    target_interface: str | None
    edge_type: str
    confidence_score: float
    is_degraded: bool = False
    conflict_flags: list[str] = field(default_factory=list)
    last_seen: datetime = field(default_factory=_utc_now)
    last_event_id: str | None = None

    def with_update(self, **kwargs: Any) -> "LiveEdgeState":
        """Return a new LiveEdgeState with updated fields."""
        current = {
            "edge_id": self.edge_id,
            "source_node_id": self.source_node_id,
            "target_node_id": self.target_node_id,
            "source_interface": self.source_interface,
            "target_interface": self.target_interface,
            "edge_type": self.edge_type,
            "confidence_score": self.confidence_score,
            "is_degraded": self.is_degraded,
            "conflict_flags": list(self.conflict_flags),
            "last_seen": self.last_seen,
            "last_event_id": self.last_event_id,
        }
        current.update(kwargs)
        return LiveEdgeState(**current)  # type: ignore[arg-type]


@dataclass(frozen=True)
class TopologyDelta:
    """An immutable record of one topology change caused by one ChangeEvent."""

    delta_id: str = field(default_factory=_new_id)
    change_event_id: str = ""
    change_kind: EdgeDeltaKind = EdgeDeltaKind.ADDED
    edge_id: str = ""
    source_node_id: str = ""
    target_node_id: str = ""
    source_interface: str | None = None
    target_interface: str | None = None
    edge_type: str = ""
    previous_state: dict[str, Any] = field(default_factory=dict)
    new_state: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=_utc_now)
    human_summary: str = ""


# ---------------------------------------------------------------------------
# Topology Patcher
# ---------------------------------------------------------------------------


class TopologyPatcher:
    """Applies ChangeEvents as incremental edge mutations on the live topology.

    Thread/task safety: all mutations are serialized through an asyncio.Lock.
    The live graph is stored in memory; persisting deltas to DB is handled by
    calling code (the engine passes deltas to a repository).

    Initialization
    --------------
    Optionally seed the patcher with an existing snapshot's edge list so
    the live graph starts from known good state rather than empty.
    """

    def __init__(self, delta_queue: asyncio.Queue[TopologyDelta]) -> None:
        self._delta_queue = delta_queue
        self._lock = asyncio.Lock()
        # Primary edge index: edge_id → LiveEdgeState
        self._edges: dict[str, LiveEdgeState] = {}
        # Lookup: (source_id, target_id) → list of edge_ids (supports parallel links)
        self._pair_index: dict[tuple[str, str], list[str]] = {}
        # Interface index: (device_id, interface_name) → list of edge_ids
        self._iface_index: dict[tuple[str, str], list[str]] = {}

    def seed_from_snapshot(self, edges: list[dict[str, Any]]) -> None:
        """Populate the live graph from snapshot edges at startup.

        Call this synchronously before start() if you want the patcher to
        start from a known baseline rather than an empty graph.
        """
        for e in edges:
            state = LiveEdgeState(
                edge_id=e.get("id", _new_id()),
                source_node_id=e.get("source_node_id", e.get("source", "")),
                target_node_id=e.get("target_node_id", e.get("target", "")),
                source_interface=e.get("source_interface"),
                target_interface=e.get("target_interface"),
                edge_type=e.get("edge_type", e.get("type", "physical")),
                confidence_score=float(e.get("confidence_score", e.get("confidence", 0.5))),
            )
            self._store_edge(state)

    def _store_edge(self, state: LiveEdgeState) -> None:
        self._edges[state.edge_id] = state
        pair_key = _pair(state.source_node_id, state.target_node_id)
        if pair_key not in self._pair_index:
            self._pair_index[pair_key] = []
        if state.edge_id not in self._pair_index[pair_key]:
            self._pair_index[pair_key].append(state.edge_id)
        if state.source_interface:
            ikey = (state.source_node_id, state.source_interface)
            if ikey not in self._iface_index:
                self._iface_index[ikey] = []
            if state.edge_id not in self._iface_index[ikey]:
                self._iface_index[ikey].append(state.edge_id)
        if state.target_interface:
            ikey = (state.target_node_id, state.target_interface)
            if ikey not in self._iface_index:
                self._iface_index[ikey] = []
            if state.edge_id not in self._iface_index[ikey]:
                self._iface_index[ikey].append(state.edge_id)

    def _snapshot_indexes(self) -> tuple[dict, dict, dict]:
        """Capture current state for rollback on exception."""
        return (
            dict(self._edges),
            {k: list(v) for k, v in self._pair_index.items()},
            {k: list(v) for k, v in self._iface_index.items()},
        )

    def _get_pair_edge_id(self, pair_key: tuple[str, str]) -> str | None:
        """Get the first edge_id for a device pair, or None."""
        ids = self._pair_index.get(pair_key, [])
        return ids[0] if ids else None

    def _get_pair_edge_ids(self, pair_key: tuple[str, str]) -> list[str]:
        """Get all edge_ids for a device pair."""
        return list(self._pair_index.get(pair_key, []))

    def _find_matching_edge(self, pair_key: tuple[str, str], interface_name: str | None = None) -> str | None:
        """Find the best matching edge for a pair, optionally filtered by interface."""
        ids = self._pair_index.get(pair_key, [])
        if not ids:
            return None
        if not interface_name:
            return ids[0]
        # Prefer edge that matches the interface
        for eid in ids:
            edge = self._edges.get(eid)
            if edge and (edge.source_interface == interface_name or edge.target_interface == interface_name):
                return eid
        return ids[0]  # fallback to first

    def _restore_indexes(self, snapshot: tuple[dict, dict, dict]) -> None:
        """Roll back to snapshot state."""
        self._edges, self._pair_index, self._iface_index = snapshot

    async def apply(self, event: ChangeEvent) -> list[TopologyDelta]:
        """Apply a ChangeEvent to the live topology. Returns produced deltas.

        Mutations are atomic: if an exception occurs mid-dispatch, indexes
        are rolled back to their pre-mutation state.
        """
        async with self._lock:
            snapshot = self._snapshot_indexes()
            try:
                deltas = self._dispatch(event)
            except Exception:
                self._restore_indexes(snapshot)
                raise
        for delta in deltas:
            try:
                self._delta_queue.put_nowait(delta)
            except asyncio.QueueFull:
                logger.warning("Topology delta queue full, dropping delta %s", delta.delta_id)
        return deltas

    async def apply_resolved_assertion(
        self,
        assertion: ResolvedAssertion,
        *,
        source_event: ChangeEvent,
    ) -> list[TopologyDelta]:
        """Apply a resolved assertion to the live topology.

        Mutations are atomic: rolled back on exception.
        """
        async with self._lock:
            snapshot = self._snapshot_indexes()
            try:
                deltas = self._dispatch_resolved_assertion(assertion, source_event)
            except Exception:
                self._restore_indexes(snapshot)
                raise
        for delta in deltas:
            try:
                self._delta_queue.put_nowait(delta)
            except asyncio.QueueFull:
                logger.warning("Topology delta queue full, dropping delta %s", delta.delta_id)
        return deltas

    def _dispatch_resolved_assertion(self, assertion: ResolvedAssertion, source_event: ChangeEvent) -> list[TopologyDelta]:
        field_key = f"{assertion.subject_type.value}.{assertion.field_name}"
        synthetic = source_event.model_copy(
            update={
                "current_value": assertion.resolved_value,
                "confidence": float(assertion.confidence),
                "is_confirmed": assertion.resolution_state.value == "confirmed",
            }
        )
        if field_key == "interface.oper_status":
            synthetic = synthetic.model_copy(
                update={
                    "kind": ChangeKind.INTERFACE_UP if str(assertion.resolved_value).lower() == "up" else ChangeKind.INTERFACE_DOWN,
                    "field_path": "interface.oper_status",
                }
            )
            return self._dispatch(synthetic)
        if field_key == "edge.exists":
            synthetic = synthetic.model_copy(
                update={
                    "kind": ChangeKind.NEIGHBOR_DISCOVERED if bool(assertion.resolved_value) else ChangeKind.NEIGHBOR_LOST,
                    "field_path": "edge.exists",
                }
            )
            return self._dispatch(synthetic)
        if field_key == "bgp_session.session_state" or field_key == "bgp.session_state":
            synthetic = synthetic.model_copy(
                update={
                    "kind": ChangeKind.BGP_SESSION_ESTABLISHED if str(assertion.resolved_value).lower() == "established" else ChangeKind.BGP_SESSION_DROPPED,
                    "field_path": "bgp.session_state",
                }
            )
            return self._dispatch(synthetic)
        return []

    def _dispatch(self, event: ChangeEvent) -> list[TopologyDelta]:
        """Route event to the appropriate mutation handler."""
        handlers = {
            ChangeKind.INTERFACE_DOWN: self._on_interface_down,
            ChangeKind.INTERFACE_UP: self._on_interface_up,
            ChangeKind.INTERFACE_FLAP: self._on_interface_down,
            ChangeKind.NEIGHBOR_DISCOVERED: self._on_neighbor_discovered,
            ChangeKind.NEIGHBOR_LOST: self._on_neighbor_lost,
            ChangeKind.NEIGHBOR_CHANGED: self._on_neighbor_changed,
            ChangeKind.BGP_SESSION_DROPPED: self._on_bgp_dropped,
            ChangeKind.BGP_SESSION_ESTABLISHED: self._on_bgp_established,
            ChangeKind.BGP_PEER_FLAP: self._on_bgp_dropped,
            ChangeKind.MAC_MOVE: self._on_mac_move,
            ChangeKind.TOPOLOGY_ANOMALY: self._on_topology_anomaly,
        }
        handler = handlers.get(event.kind)
        if handler:
            return handler(event)
        return []

    # ------------------------------------------------------------------
    # Interface handlers
    # ------------------------------------------------------------------

    def _on_interface_down(self, event: ChangeEvent) -> list[TopologyDelta]:
        if not event.interface_name:
            return []
        deltas: list[TopologyDelta] = []
        ikey = (event.device_id, event.interface_name)
        for edge_id in self._iface_index.get(ikey, []):
            edge = self._edges.get(edge_id)
            if edge and not edge.is_degraded:
                updated = edge.with_update(
                    is_degraded=True,
                    last_seen=event.detected_at,
                    last_event_id=event.event_id,
                )
                self._edges[edge_id] = updated
                deltas.append(TopologyDelta(
                    change_event_id=event.event_id,
                    change_kind=EdgeDeltaKind.DEGRADED,
                    edge_id=edge_id,
                    source_node_id=edge.source_node_id,
                    target_node_id=edge.target_node_id,
                    source_interface=edge.source_interface,
                    target_interface=edge.target_interface,
                    edge_type=edge.edge_type,
                    previous_state={"is_degraded": False},
                    new_state={"is_degraded": True},
                    human_summary=f"Edge {edge.source_node_id}↔{edge.target_node_id} degraded: {event.device_id} {event.interface_name} down",
                ))
        return deltas

    def _on_interface_up(self, event: ChangeEvent) -> list[TopologyDelta]:
        if not event.interface_name:
            return []
        deltas: list[TopologyDelta] = []
        ikey = (event.device_id, event.interface_name)
        for edge_id in self._iface_index.get(ikey, []):
            edge = self._edges.get(edge_id)
            if edge and edge.is_degraded:
                updated = edge.with_update(
                    is_degraded=False,
                    last_seen=event.detected_at,
                    last_event_id=event.event_id,
                )
                self._edges[edge_id] = updated
                deltas.append(TopologyDelta(
                    change_event_id=event.event_id,
                    change_kind=EdgeDeltaKind.RESTORED,
                    edge_id=edge_id,
                    source_node_id=edge.source_node_id,
                    target_node_id=edge.target_node_id,
                    source_interface=edge.source_interface,
                    target_interface=edge.target_interface,
                    edge_type=edge.edge_type,
                    previous_state={"is_degraded": True},
                    new_state={"is_degraded": False},
                    human_summary=f"Edge {edge.source_node_id}↔{edge.target_node_id} restored: {event.device_id} {event.interface_name} up",
                ))
        return deltas

    # ------------------------------------------------------------------
    # Neighbor handlers
    # ------------------------------------------------------------------

    def _on_neighbor_discovered(self, event: ChangeEvent) -> list[TopologyDelta]:
        if not event.neighbor_device_id:
            return []
        pair_key = _pair(event.device_id, event.neighbor_device_id)
        existing_ids = self._get_pair_edge_ids(pair_key)
        if existing_ids:
            # Check for a matching edge (prefer interface match)
            for edge_id in existing_ids:
                edge = self._edges.get(edge_id)
                if edge and edge.is_degraded:
                    if not event.interface_name or edge.source_interface == event.interface_name or edge.target_interface == event.interface_name:
                        return self._restore_edge(edge, event)
            return []

        # New edge
        new_edge = LiveEdgeState(
            edge_id=_new_id(),
            source_node_id=event.device_id,
            target_node_id=event.neighbor_device_id,
            source_interface=event.interface_name,
            target_interface=event.neighbor_interface,
            edge_type="l2_adjacency",
            confidence_score=0.75,
            last_seen=event.detected_at,
            last_event_id=event.event_id,
        )
        self._store_edge(new_edge)
        return [TopologyDelta(
            change_event_id=event.event_id,
            change_kind=EdgeDeltaKind.ADDED,
            edge_id=new_edge.edge_id,
            source_node_id=new_edge.source_node_id,
            target_node_id=new_edge.target_node_id,
            source_interface=new_edge.source_interface,
            target_interface=new_edge.target_interface,
            edge_type=new_edge.edge_type,
            new_state={"confidence_score": 0.75, "source": "lldp_streaming"},
            human_summary=f"New neighbor: {event.device_id} ({event.interface_name}) ↔ {event.neighbor_device_id} ({event.neighbor_interface})",
        )]

    def _on_neighbor_lost(self, event: ChangeEvent) -> list[TopologyDelta]:
        if not event.neighbor_device_id:
            return []
        pair_key = _pair(event.device_id, event.neighbor_device_id)
        edge_id = self._find_matching_edge(pair_key, event.interface_name)
        if not edge_id:
            return []
        edge = self._edges[edge_id]
        # Mark as degraded rather than removing (the edge may recover)
        if not edge.is_degraded:
            updated = edge.with_update(
                is_degraded=True,
                last_seen=event.detected_at,
                last_event_id=event.event_id,
            )
            self._edges[edge_id] = updated
            return [TopologyDelta(
                change_event_id=event.event_id,
                change_kind=EdgeDeltaKind.REMOVED,
                edge_id=edge_id,
                source_node_id=edge.source_node_id,
                target_node_id=edge.target_node_id,
                source_interface=edge.source_interface,
                target_interface=edge.target_interface,
                edge_type=edge.edge_type,
                previous_state={"is_degraded": False},
                new_state={"is_degraded": True},
                human_summary=f"Neighbor lost: {event.device_id} ({event.interface_name}) ↔ {event.neighbor_device_id}",
            )]
        return []

    def _on_neighbor_changed(self, event: ChangeEvent) -> list[TopologyDelta]:
        """Port now connects to a different device — flag conflict."""
        deltas = self._on_neighbor_lost(event)
        if event.neighbor_device_id:
            deltas += self._on_neighbor_discovered(event)
        return deltas

    def _restore_edge(self, edge: LiveEdgeState, event: ChangeEvent) -> list[TopologyDelta]:
        updated = edge.with_update(
            is_degraded=False,
            last_seen=event.detected_at,
            last_event_id=event.event_id,
        )
        self._edges[edge.edge_id] = updated
        return [TopologyDelta(
            change_event_id=event.event_id,
            change_kind=EdgeDeltaKind.RESTORED,
            edge_id=edge.edge_id,
            source_node_id=edge.source_node_id,
            target_node_id=edge.target_node_id,
            edge_type=edge.edge_type,
            new_state={"is_degraded": False},
            human_summary=f"Edge restored: {edge.source_node_id}↔{edge.target_node_id}",
        )]

    # ------------------------------------------------------------------
    # BGP handlers
    # ------------------------------------------------------------------

    def _on_bgp_dropped(self, event: ChangeEvent) -> list[TopologyDelta]:
        if not event.neighbor_device_id:
            return []
        pair_key = _pair(event.device_id, event.neighbor_device_id)
        edge_id = self._get_pair_edge_id(pair_key)
        if not edge_id:
            return []
        edge = self._edges[edge_id]
        if not edge.is_degraded:
            updated = edge.with_update(
                is_degraded=True,
                last_seen=event.detected_at,
                last_event_id=event.event_id,
            )
            self._edges[edge_id] = updated
            return [TopologyDelta(
                change_event_id=event.event_id,
                change_kind=EdgeDeltaKind.DEGRADED,
                edge_id=edge_id,
                source_node_id=edge.source_node_id,
                target_node_id=edge.target_node_id,
                edge_type="bgp_peer",
                new_state={"is_degraded": True},
                human_summary=f"BGP session dropped: {event.device_id} ↔ {event.neighbor_device_id}",
            )]
        return []

    def _on_bgp_established(self, event: ChangeEvent) -> list[TopologyDelta]:
        if not event.neighbor_device_id:
            return []
        pair_key = _pair(event.device_id, event.neighbor_device_id)
        edge_id = self._get_pair_edge_id(pair_key)
        if edge_id and self._edges[edge_id].is_degraded:
            return self._restore_edge(self._edges[edge_id], event)
        if not edge_id:
            new_edge = LiveEdgeState(
                edge_id=_new_id(),
                source_node_id=event.device_id,
                target_node_id=event.neighbor_device_id,
                source_interface=None,
                target_interface=None,
                edge_type="bgp_peer",
                confidence_score=0.8,
                last_seen=event.detected_at,
                last_event_id=event.event_id,
            )
            self._store_edge(new_edge)
            return [TopologyDelta(
                change_event_id=event.event_id,
                change_kind=EdgeDeltaKind.ADDED,
                edge_id=new_edge.edge_id,
                source_node_id=event.device_id,
                target_node_id=event.neighbor_device_id,
                edge_type="bgp_peer",
                new_state={"confidence_score": 0.8},
                human_summary=f"BGP session established: {event.device_id} ↔ {event.neighbor_device_id}",
            )]
        return []

    # ------------------------------------------------------------------
    # MAC move / anomaly handlers
    # ------------------------------------------------------------------

    def _on_mac_move(self, event: ChangeEvent) -> list[TopologyDelta]:
        """A MAC move is a potential security event — flag all L2 edges on the source device."""
        deltas: list[TopologyDelta] = []
        for edge in self._edges.values():
            if (edge.source_node_id == event.device_id or edge.target_node_id == event.device_id) \
                    and edge.edge_type in ("l2_adjacency", "physical"):
                flag = f"mac_move:{event.mac_address or 'unknown'}"
                if flag not in edge.conflict_flags:
                    updated = edge.with_update(
                        conflict_flags=[*edge.conflict_flags, flag],
                        last_event_id=event.event_id,
                    )
                    self._edges[edge.edge_id] = updated
                    deltas.append(TopologyDelta(
                        change_event_id=event.event_id,
                        change_kind=EdgeDeltaKind.CONFLICT_FLAGGED,
                        edge_id=edge.edge_id,
                        source_node_id=edge.source_node_id,
                        target_node_id=edge.target_node_id,
                        edge_type=edge.edge_type,
                        new_state={"conflict_flags": updated.conflict_flags},
                        human_summary=f"MAC move detected on {event.device_id}: MAC {event.mac_address} — L2 edges flagged",
                    ))
        return deltas

    def _on_topology_anomaly(self, event: ChangeEvent) -> list[TopologyDelta]:
        """Generic topology anomaly — flag all edges for this device."""
        deltas: list[TopologyDelta] = []
        for edge in self._edges.values():
            if edge.source_node_id == event.device_id or edge.target_node_id == event.device_id:
                flag = "topology_anomaly"
                if flag not in edge.conflict_flags:
                    updated = edge.with_update(
                        conflict_flags=[*edge.conflict_flags, flag],
                        last_event_id=event.event_id,
                    )
                    self._edges[edge.edge_id] = updated
                    deltas.append(TopologyDelta(
                        change_event_id=event.event_id,
                        change_kind=EdgeDeltaKind.CONFLICT_FLAGGED,
                        edge_id=edge.edge_id,
                        source_node_id=edge.source_node_id,
                        target_node_id=edge.target_node_id,
                        edge_type=edge.edge_type,
                        new_state={"conflict_flags": updated.conflict_flags},
                        human_summary=f"Topology anomaly flagged for {event.device_id}",
                    ))
        return deltas

    # ------------------------------------------------------------------
    # Query interface
    # ------------------------------------------------------------------

    def get_live_edges(self) -> list[LiveEdgeState]:
        return list(self._edges.values())

    def get_degraded_edges(self) -> list[LiveEdgeState]:
        return [e for e in self._edges.values() if e.is_degraded]

    def get_conflicted_edges(self) -> list[LiveEdgeState]:
        return [e for e in self._edges.values() if e.conflict_flags]

    def get_neighbors(self, device_id: str) -> list[LiveEdgeState]:
        return [
            e for e in self._edges.values()
            if e.source_node_id == device_id or e.target_node_id == device_id
        ]

    def summary(self) -> dict[str, int]:
        edges = list(self._edges.values())
        return {
            "total_edges": len(edges),
            "degraded_edges": sum(1 for e in edges if e.is_degraded),
            "conflicted_edges": sum(1 for e in edges if e.conflict_flags),
            "healthy_edges": sum(1 for e in edges if not e.is_degraded and not e.conflict_flags),
        }

    def validate_invariants(self) -> dict[str, Any]:
        """Return structural and semantic invariants for churn-safety validation."""
        missing_pair_refs = 0
        dangling_iface_refs = 0
        orphan_pair_entries = 0

        # Check every edge has a valid entry in the pair_index
        for edge_id, edge in self._edges.items():
            pair_key = _pair(edge.source_node_id, edge.target_node_id)
            pair_list = self._pair_index.get(pair_key, [])
            if edge_id not in pair_list:
                missing_pair_refs += 1

        # Check pair_index doesn't reference deleted edges
        for pair_key, edge_ids in self._pair_index.items():
            for eid in edge_ids:
                if eid not in self._edges:
                    orphan_pair_entries += 1

        # Check iface_index doesn't reference deleted edges
        for edge_ids in self._iface_index.values():
            for edge_id in edge_ids:
                if edge_id not in self._edges:
                    dangling_iface_refs += 1

        is_consistent = (missing_pair_refs == 0 and
                         dangling_iface_refs == 0 and
                         orphan_pair_entries == 0)
        return {
            "missing_pair_refs": missing_pair_refs,
            "dangling_iface_refs": dangling_iface_refs,
            "orphan_pair_entries": orphan_pair_entries,
            "duplicate_pair_targets": 0,  # no longer applicable with list-based index
            "is_consistent": is_consistent,
            "edge_count": len(self._edges),
            "pair_index_count": len(self._pair_index),
            "iface_index_count": len(self._iface_index),
        }

    def validate_semantic_correctness(
        self, expected_states: dict[str, dict[str, Any]]
    ) -> dict[str, Any]:
        """Validate that edge states match expected ground truth.

        Args:
            expected_states: {edge_pair_key: {"oper_state": "up"|"down", "exists": True|False}}

        Returns:
            Dict with mismatches and correctness score.
        """
        mismatches: list[dict[str, Any]] = []
        checked = 0

        for pair_key_str, expected in expected_states.items():
            parts = pair_key_str.split("<->") if "<->" in pair_key_str else pair_key_str.split(":")
            if len(parts) == 2:
                pk = _pair(parts[0].strip(), parts[1].strip())
            else:
                continue

            edge_id = self._get_pair_edge_id(pk)
            edge = self._edges.get(edge_id) if edge_id else None
            checked += 1

            if expected.get("exists", True) is False:
                if edge is not None and not edge.is_degraded:
                    mismatches.append({
                        "pair": pair_key_str,
                        "field": "exists",
                        "expected": False,
                        "actual": True,
                        "detail": "Edge should not exist but is present and healthy",
                    })
                continue

            if edge is None:
                if expected.get("exists", True) is True:
                    mismatches.append({
                        "pair": pair_key_str,
                        "field": "exists",
                        "expected": True,
                        "actual": False,
                        "detail": "Edge should exist but is missing",
                    })
                continue

            for field_name, expected_value in expected.items():
                if field_name == "exists":
                    continue
                actual_value = getattr(edge, field_name, None)
                if actual_value is None:
                    actual_value = edge.metadata.get(field_name)
                if actual_value != expected_value:
                    mismatches.append({
                        "pair": pair_key_str,
                        "field": field_name,
                        "expected": expected_value,
                        "actual": actual_value,
                    })

        correctness = (checked - len(mismatches)) / max(checked, 1)
        return {
            "checked": checked,
            "mismatches": len(mismatches),
            "mismatch_details": mismatches,
            "correctness_score": correctness,
            "is_semantically_correct": len(mismatches) == 0,
        }

    def snapshot_state(self) -> dict[str, dict[str, Any]]:
        """Capture the current state of all edges for convergence testing."""
        result: dict[str, dict[str, Any]] = {}
        for edge_id, edge in self._edges.items():
            pk = _pair(edge.source_node_id, edge.target_node_id)
            key = f"{pk[0]}<->{pk[1]}"
            result[key] = {
                "edge_id": edge_id,
                "is_degraded": edge.is_degraded,
                "conflict_flags": list(edge.conflict_flags) if edge.conflict_flags else [],
                "confidence_score": edge.confidence_score,
                "source_interface": edge.source_interface,
                "target_interface": edge.target_interface,
            }
        return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pair(a: str, b: str) -> tuple[str, str]:
    """Canonical undirected pair key (sorted so A↔B == B↔A)."""
    return (min(a, b), max(a, b))
