"""Root-cause ranking engine for incident clusters.

Given a cluster of correlated events, this engine ranks candidate root
causes using a multi-factor scoring model:

- Temporal priority (earlier events score higher)
- Causal chain depth (events that explain more downstream effects score higher)
- Domain authority (L1/L2 events outrank L3/routing events as root causes)
- Source confidence (high-trust sources boost the root cause score)
- Topology centrality (events on devices with more affected neighbors score higher)
"""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .causal_graph import CausalChain, CausalGraphEngine


# Domain layers — lower layers are more likely root causes
_DOMAIN_LAYER: dict[str, int] = {
    "device_reachability_lost": 0,
    "interface_down": 1,
    "interface_flap": 1,
    "neighbor_lost": 2,
    "bgp_session_dropped": 3,
    "route_withdrawn": 4,
    "mac_move": 2,
    "interface_up": 1,
    "bgp_session_established": 3,
    "neighbor_discovered": 2,
}

# Higher layer number = less likely to be root cause
_MAX_LAYER = 5


@dataclass(slots=True)
class RootCauseCandidate:
    """A scored root-cause candidate within an incident cluster."""
    event_id: str
    event_kind: str
    device_id: str
    score: float
    rank: int = 0
    downstream_count: int = 0
    causal_depth: int = 0
    temporal_rank: int = 0
    domain_layer: int = 0
    explanation: str = ""


@dataclass
class RootCauseRanking:
    """Complete ranking of root-cause candidates for an incident cluster."""
    cluster_id: str
    candidates: list[RootCauseCandidate] = field(default_factory=list)
    chains: list[CausalChain] = field(default_factory=list)
    top_root_cause: RootCauseCandidate | None = None

    @property
    def confidence(self) -> float:
        if not self.candidates:
            return 0.0
        if len(self.candidates) == 1:
            return min(0.99, self.candidates[0].score)
        top = self.candidates[0].score
        runner = self.candidates[1].score
        margin = top - runner
        return min(0.99, 0.5 + margin * 2.0)


class RootCauseRanker:
    """Rank root-cause candidates within an event cluster."""

    # Scoring weights (sum to ~1.0 for interpretability)
    TEMPORAL_WEIGHT = 0.25
    CAUSAL_DEPTH_WEIGHT = 0.30
    DOMAIN_WEIGHT = 0.20
    DOWNSTREAM_WEIGHT = 0.15
    CENTRALITY_WEIGHT = 0.10

    def __init__(self, causal_engine: CausalGraphEngine | None = None) -> None:
        self.causal_engine = causal_engine or CausalGraphEngine()

    def rank(
        self,
        cluster_id: str,
        events: dict[str, dict],
    ) -> RootCauseRanking:
        """Rank all events in a cluster as root-cause candidates."""
        if not events:
            return RootCauseRanking(cluster_id=cluster_id)

        # 1. Discover causal chains for every event
        all_chains: list[CausalChain] = []
        chains_by_root: dict[str, list[CausalChain]] = defaultdict(list)
        downstream_count: Counter[str] = Counter()

        for event_id, event in events.items():
            event_chains = self.causal_engine.find_causal_chains(event, events)
            for chain in event_chains:
                all_chains.append(chain)
                root_id = chain.root_event_id
                if root_id:
                    chains_by_root[root_id].append(chain)
                    # Every event in the chain except the root is "downstream"
                    for link in chain.links:
                        downstream_count[root_id] += 1

        # 2. Temporal ordering
        time_sorted = sorted(
            events.items(),
            key=lambda item: self._event_time(item[1]),
        )
        temporal_rank: dict[str, int] = {}
        for idx, (eid, _) in enumerate(time_sorted):
            temporal_rank[eid] = idx

        # 3. Score each candidate
        n_events = len(events)
        candidates: list[RootCauseCandidate] = []

        for event_id, event in events.items():
            kind = self._normalize_kind(event.get("kind", ""))
            device = str(event.get("device_id", "") or "")
            layer = _DOMAIN_LAYER.get(kind, _MAX_LAYER)

            # Temporal score: earlier = higher
            t_rank = temporal_rank.get(event_id, n_events)
            temporal_score = 1.0 - (t_rank / max(n_events, 1))

            # Causal depth score: how many chains root at this event
            root_chains = chains_by_root.get(event_id, [])
            max_depth = max((c.depth for c in root_chains), default=0)
            depth_score = min(1.0, max_depth / max(self.causal_engine.max_chain_depth, 1))

            # Downstream score: how many events are downstream of this one
            ds_count = downstream_count.get(event_id, 0)
            downstream_score = min(1.0, ds_count / max(n_events - 1, 1))

            # Domain layer score: lower layer = higher score
            domain_score = 1.0 - (layer / _MAX_LAYER)

            # Centrality: how many unique devices are in this event's chains
            affected_devices: set[str] = set()
            for chain in root_chains:
                for link in chain.links:
                    eff_evt = events.get(link.effect_event_id, {})
                    d = str(eff_evt.get("device_id", "") or "")
                    if d:
                        affected_devices.add(d)
            centrality_score = min(1.0, len(affected_devices) / max(n_events, 1))

            total = (
                self.TEMPORAL_WEIGHT * temporal_score
                + self.CAUSAL_DEPTH_WEIGHT * depth_score
                + self.DOMAIN_WEIGHT * domain_score
                + self.DOWNSTREAM_WEIGHT * downstream_score
                + self.CENTRALITY_WEIGHT * centrality_score
            )

            explanation_parts = [
                f"temporal={temporal_score:.2f}(rank {t_rank}/{n_events})",
                f"causal_depth={depth_score:.2f}(max_depth={max_depth})",
                f"domain={domain_score:.2f}(layer={layer})",
                f"downstream={downstream_score:.2f}(count={ds_count})",
                f"centrality={centrality_score:.2f}(devices={len(affected_devices)})",
            ]

            candidates.append(RootCauseCandidate(
                event_id=event_id,
                event_kind=kind,
                device_id=device,
                score=total,
                downstream_count=ds_count,
                causal_depth=max_depth,
                temporal_rank=t_rank,
                domain_layer=layer,
                explanation="; ".join(explanation_parts),
            ))

        # Sort by score descending
        candidates.sort(key=lambda c: c.score, reverse=True)
        for idx, c in enumerate(candidates):
            c.rank = idx + 1

        ranking = RootCauseRanking(
            cluster_id=cluster_id,
            candidates=candidates,
            chains=all_chains[:20],  # cap stored chains
            top_root_cause=candidates[0] if candidates else None,
        )
        return ranking

    @staticmethod
    def _event_time(event: dict) -> datetime:
        val = event.get("received_at") or event.get("detected_at")
        if isinstance(val, datetime):
            return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
        if isinstance(val, str):
            try:
                return datetime.fromisoformat(val.replace("Z", "+00:00"))
            except ValueError:
                pass
        return datetime.now(timezone.utc)

    @staticmethod
    def _normalize_kind(kind: str) -> str:
        return kind.lower().replace("changekind.", "")
