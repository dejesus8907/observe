"""Causal graph for topology-aware, multi-hop event correlation.

This replaces the flat cause-effect pair lookup with a proper directed
acyclic graph (DAG) that models known failure propagation paths through
network topology.  The graph supports:

- Multi-hop chain detection (interface_down on sw1 → neighbor_lost on sw1
  → bgp_session_dropped on sw2 → route_withdrawn on sw3)
- Topology-distance weighting (closer hops score higher)
- Temporal ordering enforcement (cause must precede effect)
- Cross-domain fusion (combining interface/LLDP/BGP/routing domains)
"""

from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any


# ---------------------------------------------------------------------------
# Propagation rule definitions
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PropagationRule:
    """A known causal relationship between two event kinds."""
    cause_kind: str
    effect_kind: str
    max_delay_seconds: float = 15.0
    base_confidence: float = 0.85
    same_device_required: bool = False
    shared_interface_required: bool = False
    neighbor_device_allowed: bool = True


# The canonical set of known propagation rules.  Order matters —
# earlier rules in a chain produce higher confidence.
PROPAGATION_RULES: list[PropagationRule] = [
    # Layer 1/2 → Layer 2 adjacency
    PropagationRule("interface_down", "neighbor_lost",
                    max_delay_seconds=10.0, base_confidence=0.92,
                    same_device_required=True, shared_interface_required=True),
    PropagationRule("interface_down", "interface_flap",
                    max_delay_seconds=5.0, base_confidence=0.80,
                    same_device_required=True, shared_interface_required=True),

    # Layer 2 adjacency → Layer 3 control-plane
    PropagationRule("neighbor_lost", "bgp_session_dropped",
                    max_delay_seconds=15.0, base_confidence=0.88,
                    neighbor_device_allowed=True),
    PropagationRule("interface_down", "bgp_session_dropped",
                    max_delay_seconds=20.0, base_confidence=0.85,
                    same_device_required=True),
    PropagationRule("interface_down", "bgp_session_dropped",
                    max_delay_seconds=20.0, base_confidence=0.78,
                    same_device_required=False, neighbor_device_allowed=True),

    # Layer 3 control-plane → routing
    PropagationRule("bgp_session_dropped", "route_withdrawn",
                    max_delay_seconds=30.0, base_confidence=0.82,
                    neighbor_device_allowed=True),

    # Device-level failures
    PropagationRule("device_reachability_lost", "interface_down",
                    max_delay_seconds=5.0, base_confidence=0.90,
                    same_device_required=True),
    PropagationRule("device_reachability_lost", "bgp_session_dropped",
                    max_delay_seconds=15.0, base_confidence=0.85,
                    same_device_required=True),
    PropagationRule("device_reachability_lost", "neighbor_lost",
                    max_delay_seconds=10.0, base_confidence=0.87,
                    neighbor_device_allowed=True),

    # Cross-domain: LLDP flap → BGP
    PropagationRule("neighbor_lost", "route_withdrawn",
                    max_delay_seconds=45.0, base_confidence=0.72,
                    neighbor_device_allowed=True),

    # MAC/STP domain
    PropagationRule("interface_down", "mac_move",
                    max_delay_seconds=10.0, base_confidence=0.65,
                    neighbor_device_allowed=True),
]

# Pre-index rules by cause kind for fast lookup
_RULES_BY_CAUSE: dict[str, list[PropagationRule]] = defaultdict(list)
for _r in PROPAGATION_RULES:
    _RULES_BY_CAUSE[_r.cause_kind].append(_r)


# ---------------------------------------------------------------------------
# Topology adjacency model
# ---------------------------------------------------------------------------

@dataclass
class TopologyAdjacency:
    """Lightweight topology adjacency for causal graph reasoning.

    This is seeded from the topology patcher's live edge state and
    updated incrementally as edges are added/removed.
    """
    # device_id -> set of neighbor device_ids
    _neighbors: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    # (device_id, interface) -> neighbor device_id
    _interface_peers: dict[tuple[str, str], str] = field(default_factory=dict)

    def add_edge(self, device_a: str, device_b: str,
                 interface_a: str = "", interface_b: str = "") -> None:
        self._neighbors[device_a].add(device_b)
        self._neighbors[device_b].add(device_a)
        if interface_a:
            self._interface_peers[(device_a, interface_a)] = device_b
        if interface_b:
            self._interface_peers[(device_b, interface_b)] = device_a

    def remove_edge(self, device_a: str, device_b: str,
                    interface_a: str = "", interface_b: str = "") -> None:
        self._neighbors.get(device_a, set()).discard(device_b)
        self._neighbors.get(device_b, set()).discard(device_a)
        if interface_a:
            self._interface_peers.pop((device_a, interface_a), None)
        if interface_b:
            self._interface_peers.pop((device_b, interface_b), None)

    def are_neighbors(self, device_a: str, device_b: str) -> bool:
        return device_b in self._neighbors.get(device_a, set())

    def get_neighbors(self, device_id: str) -> set[str]:
        return set(self._neighbors.get(device_id, set()))

    def get_interface_peer(self, device_id: str, interface: str) -> str | None:
        return self._interface_peers.get((device_id, interface))

    def shortest_path_length(self, device_a: str, device_b: str, max_hops: int = 5) -> int | None:
        """BFS shortest path.  Returns None if unreachable within max_hops."""
        if device_a == device_b:
            return 0
        visited: set[str] = {device_a}
        queue: deque[tuple[str, int]] = deque([(device_a, 0)])
        while queue:
            current, depth = queue.popleft()
            if depth >= max_hops:
                continue
            for neighbor in self._neighbors.get(current, set()):
                if neighbor == device_b:
                    return depth + 1
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, depth + 1))
        return None

    def seed_from_edges(self, edges: list[dict[str, Any]]) -> None:
        """Bulk load from topology patcher edge dicts."""
        for edge in edges:
            src = edge.get("source_node_id", "")
            tgt = edge.get("target_node_id", "")
            src_iface = edge.get("source_interface", "")
            tgt_iface = edge.get("target_interface", "")
            if src and tgt:
                self.add_edge(src, tgt, src_iface, tgt_iface)

    @property
    def device_count(self) -> int:
        return len(self._neighbors)


# ---------------------------------------------------------------------------
# Causal chain detection
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CausalLink:
    """A single link in a causal chain."""
    cause_event_id: str
    effect_event_id: str
    rule: PropagationRule
    confidence: float
    hop_distance: int
    delay_seconds: float


@dataclass
class CausalChain:
    """An ordered chain of causal links from root cause to terminal effect."""
    links: list[CausalLink] = field(default_factory=list)

    @property
    def depth(self) -> int:
        return len(self.links)

    @property
    def root_event_id(self) -> str | None:
        return self.links[0].cause_event_id if self.links else None

    @property
    def terminal_event_id(self) -> str | None:
        return self.links[-1].effect_event_id if self.links else None

    @property
    def total_confidence(self) -> float:
        if not self.links:
            return 0.0
        conf = 1.0
        for link in self.links:
            conf *= link.confidence
        return conf

    @property
    def total_delay(self) -> float:
        return sum(link.delay_seconds for link in self.links)


class CausalGraphEngine:
    """Performs causal graph reasoning over a stream of events.

    Given a new event and a set of recent events, the engine:
    1. Checks all known propagation rules
    2. Enforces temporal ordering (cause must precede effect)
    3. Validates topology constraints (same device, neighbor, etc.)
    4. Discovers multi-hop chains via BFS over the rule graph
    5. Scores the resulting causal chain with distance decay
    """

    def __init__(
        self,
        topology: TopologyAdjacency | None = None,
        max_chain_depth: int = 4,
        distance_decay_factor: float = 0.85,
    ) -> None:
        self.topology = topology or TopologyAdjacency()
        self.max_chain_depth = max_chain_depth
        self.distance_decay_factor = distance_decay_factor

    def find_causal_links(
        self,
        event: dict,
        candidate_causes: list[dict],
    ) -> list[CausalLink]:
        """Find all direct causal links from candidate_causes to event."""
        event_kind = self._normalize_kind(event.get("kind", ""))
        event_device = str(event.get("device_id", "") or "")
        event_iface = str(event.get("interface_name", "") or "")
        event_neighbor = str(event.get("neighbor_device_id", "") or "")
        event_time = self._coerce_dt(event.get("received_at"))

        links: list[CausalLink] = []

        for cause in candidate_causes:
            cause_kind = self._normalize_kind(cause.get("kind", ""))
            cause_device = str(cause.get("device_id", "") or "")
            cause_iface = str(cause.get("interface_name", "") or "")
            cause_neighbor = str(cause.get("neighbor_device_id", "") or "")
            cause_time = self._coerce_dt(cause.get("received_at"))
            cause_id = str(cause.get("event_id", ""))

            # Temporal ordering: cause must precede or equal effect
            delay = (event_time - cause_time).total_seconds()
            if delay < -1.0:  # 1s tolerance for clock skew
                continue

            for rule in _RULES_BY_CAUSE.get(cause_kind, []):
                if rule.effect_kind != event_kind:
                    continue
                if abs(delay) > rule.max_delay_seconds:
                    continue
                if not self._check_topology_constraint(
                    rule, cause_device, cause_iface, cause_neighbor,
                    event_device, event_iface, event_neighbor,
                ):
                    continue

                hop_dist = self._compute_hop_distance(
                    cause_device, event_device
                )
                confidence = self._link_confidence(rule, delay, hop_dist)
                links.append(CausalLink(
                    cause_event_id=cause_id,
                    effect_event_id=str(event.get("event_id", "")),
                    rule=rule,
                    confidence=confidence,
                    hop_distance=hop_dist,
                    delay_seconds=max(0.0, delay),
                ))

        # Sort by confidence descending
        links.sort(key=lambda l: l.confidence, reverse=True)
        return links

    def find_causal_chains(
        self,
        event: dict,
        all_events: dict[str, dict],
    ) -> list[CausalChain]:
        """BFS backward from event through all_events to discover causal chains."""
        event_id = str(event.get("event_id", ""))
        chains: list[CausalChain] = []
        # BFS: (current_event, chain_so_far)
        queue: deque[tuple[dict, CausalChain]] = deque()

        candidates = [e for eid, e in all_events.items() if eid != event_id]
        direct_links = self.find_causal_links(event, candidates)

        for link in direct_links:
            chain = CausalChain(links=[link])
            if chain.depth >= self.max_chain_depth:
                chains.append(chain)
                continue
            cause_event = all_events.get(link.cause_event_id)
            if cause_event:
                queue.append((cause_event, chain))
            else:
                chains.append(chain)

        visited: set[str] = {event_id}
        while queue:
            current_event, current_chain = queue.popleft()
            current_id = str(current_event.get("event_id", ""))
            if current_id in visited:
                chains.append(current_chain)
                continue
            visited.add(current_id)

            if current_chain.depth >= self.max_chain_depth:
                chains.append(current_chain)
                continue

            upstream_candidates = [
                e for eid, e in all_events.items()
                if eid != current_id and eid not in visited
            ]
            upstream_links = self.find_causal_links(current_event, upstream_candidates)

            if not upstream_links:
                chains.append(current_chain)
                continue

            # Follow the best upstream link (avoid exponential blowup)
            best = upstream_links[0]
            extended = CausalChain(links=[best] + current_chain.links)
            cause_evt = all_events.get(best.cause_event_id)
            if cause_evt and extended.depth < self.max_chain_depth:
                queue.append((cause_evt, extended))
            else:
                chains.append(extended)

        # Sort by total confidence
        chains.sort(key=lambda c: c.total_confidence, reverse=True)
        return chains

    def _check_topology_constraint(
        self,
        rule: PropagationRule,
        cause_device: str, cause_iface: str, cause_neighbor: str,
        effect_device: str, effect_iface: str, effect_neighbor: str,
    ) -> bool:
        if rule.same_device_required:
            if cause_device != effect_device:
                return False
        if rule.shared_interface_required:
            if not cause_iface or cause_iface != effect_iface:
                return False
        if not rule.same_device_required and cause_device != effect_device:
            # Devices differ — check topology adjacency
            if rule.neighbor_device_allowed:
                if not self._devices_are_related(
                    cause_device, cause_neighbor, effect_device, effect_neighbor
                ):
                    return False
            else:
                return False
        return True

    def _devices_are_related(
        self,
        cause_device: str, cause_neighbor: str,
        effect_device: str, effect_neighbor: str,
    ) -> bool:
        """Check if two devices are topologically related."""
        if cause_device == effect_device:
            return True
        if cause_device and effect_device and self.topology.are_neighbors(cause_device, effect_device):
            return True
        if cause_neighbor and cause_neighbor == effect_device:
            return True
        if effect_neighbor and effect_neighbor == cause_device:
            return True
        # Check 2-hop via topology
        path = self.topology.shortest_path_length(cause_device, effect_device, max_hops=3)
        if path is not None and path <= 3:
            return True
        return False

    def _compute_hop_distance(self, cause_device: str, effect_device: str) -> int:
        if cause_device == effect_device:
            return 0
        path = self.topology.shortest_path_length(cause_device, effect_device, max_hops=5)
        return path if path is not None else 5

    def _link_confidence(self, rule: PropagationRule, delay_seconds: float, hop_distance: int) -> float:
        """Score a single causal link."""
        base = rule.base_confidence
        # Time decay: confidence drops as delay approaches max
        if rule.max_delay_seconds > 0:
            time_factor = 1.0 - 0.3 * (min(delay_seconds, rule.max_delay_seconds) / rule.max_delay_seconds)
        else:
            time_factor = 1.0
        # Distance decay
        dist_factor = self.distance_decay_factor ** hop_distance
        return max(0.0, min(0.99, base * time_factor * dist_factor))

    @staticmethod
    def _normalize_kind(kind: str) -> str:
        return kind.lower().replace("changekind.", "")

    @staticmethod
    def _coerce_dt(value: Any) -> datetime:
        from datetime import timezone
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                pass
        return datetime.now(timezone.utc)
