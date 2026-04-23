"""Event correlator with causal graph reasoning and root-cause ranking.

This correlator replaces the flat heuristic bonus model with a proper
causal graph engine that:
- Enforces temporal ordering (cause precedes effect)
- Detects multi-hop failure chains through topology
- Performs Bayesian-style root-cause ranking
- Supports cross-domain correlation (L1/L2/L3/routing)
"""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from .causal_graph import CausalGraphEngine, TopologyAdjacency
from .models import (
    CorrelationCluster,
    CorrelationDecision,
    CorrelationLink,
    CorrelationRole,
    CorrelationState,
    CorrelationType,
)
from .policy import CorrelationPolicy
from .root_cause_ranker import RootCauseRanker, RootCauseRanking


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class EventCorrelator:
    """Correlate events into incident clusters using causal graph reasoning."""

    def __init__(
        self,
        policy: CorrelationPolicy | None = None,
        topology: TopologyAdjacency | None = None,
        causal_engine: CausalGraphEngine | None = None,
        ranker: RootCauseRanker | None = None,
    ) -> None:
        self.policy = policy or CorrelationPolicy()
        self.topology = topology or TopologyAdjacency()
        self.causal_engine = causal_engine or CausalGraphEngine(topology=self.topology)
        self.ranker = ranker or RootCauseRanker(causal_engine=self.causal_engine)
        self._clusters: dict[str, CorrelationCluster] = {}
        self._events: dict[str, dict] = {}

    def set_topology(self, topology: TopologyAdjacency) -> None:
        """Update the topology adjacency model (called when topology changes)."""
        self.topology = topology
        self.causal_engine.topology = topology

    def correlate(self, event: dict, *, now: datetime | None = None) -> CorrelationDecision:
        now = self._coerce_dt(now or event.get("received_at") or _utc_now())
        event_id = str(event["event_id"])
        self._events[event_id] = dict(event)

        self._expire_old_clusters(now)

        best = self._find_best_cluster(event, now=now)
        if best is None:
            cluster = self._new_cluster(event, now=now)
            self._clusters[cluster.cluster_id] = cluster
            return CorrelationDecision(
                cluster_id=cluster.cluster_id,
                event_id=event_id,
                role=CorrelationRole.ROOT,
                is_new_cluster=True,
                confidence=1.0,
                linked_event_ids=[],
                explanation="Opened new cluster — no causal or heuristic match found.",
            )

        cluster, confidence, links, explanation = best
        role = self._role_for_event(confidence, event, cluster)
        cluster.updated_at = now
        cluster.event_ids.append(event_id)
        self._append_cluster_dimensions(cluster, event)
        cluster.links.extend(links)

        if role == CorrelationRole.ROOT:
            if cluster.root_event_id is None:
                cluster.root_event_id = event_id
            else:
                # Update root if this event is temporally earlier
                current_root = self._events.get(cluster.root_event_id)
                if current_root:
                    current_root_time = self._coerce_dt(current_root.get("received_at"))
                    event_time = self._coerce_dt(event.get("received_at"))
                    if event_time < current_root_time:
                        cluster.root_event_id = event_id

        if (now - cluster.opened_at).total_seconds() >= self.policy.max_window_seconds / 2.0:
            cluster.state = CorrelationState.STABLE

        return CorrelationDecision(
            cluster_id=cluster.cluster_id,
            event_id=event_id,
            role=role,
            is_new_cluster=False,
            confidence=confidence,
            linked_event_ids=[link.target_event_id for link in links],
            explanation=explanation,
        )

    def correlate_and_return_cluster(self, event: dict, *, now: datetime | None = None):
        decision = self.correlate(event, now=now)
        return decision, self._clusters[decision.cluster_id]

    def rank_root_causes(self, cluster_id: str) -> RootCauseRanking:
        """Run root-cause ranking on a specific cluster."""
        cluster = self._clusters.get(cluster_id)
        if not cluster:
            return RootCauseRanking(cluster_id=cluster_id)
        cluster_events = {
            eid: self._events[eid]
            for eid in cluster.event_ids
            if eid in self._events
        }
        return self.ranker.rank(cluster_id, cluster_events)

    def _find_best_cluster(
        self, event: dict, *, now: datetime
    ) -> tuple[CorrelationCluster, float, list[CorrelationLink], str] | None:
        candidates: list[tuple[CorrelationCluster, float, list[CorrelationLink], str]] = []
        for cluster in self._clusters.values():
            if cluster.state == CorrelationState.EXPIRED:
                continue
            age = (now - cluster.updated_at).total_seconds()
            if age > self.policy.max_window_seconds:
                continue
            score, links, reasons = self._score_cluster(event, cluster, now=now)
            if score >= self.policy.minimum_link_confidence:
                candidates.append((cluster, score, links, "; ".join(reasons)))

        if not candidates:
            return None
        candidates.sort(key=lambda item: item[1], reverse=True)

        # Merge: if multiple clusters match, merge the lower-scoring ones
        # into the best one. This prevents cluster fragmentation.
        if len(candidates) > 1:
            best_cluster = candidates[0][0]
            for other_cluster, _, _, _ in candidates[1:]:
                self._merge_clusters(best_cluster, other_cluster, now=now)

        return candidates[0]

    def _merge_clusters(self, target: CorrelationCluster, source: CorrelationCluster, *, now: datetime) -> None:
        """Merge source cluster into target with dedup and root revalidation."""
        # Union event_ids (dedup)
        target_event_set = set(target.event_ids)
        for eid in source.event_ids:
            if eid not in target_event_set:
                target.event_ids.append(eid)
                target_event_set.add(eid)

        # Union dimension lists (dedup)
        for attr in ("device_ids", "interface_ids", "subject_keys", "kinds"):
            target_list = getattr(target, attr)
            target_set = set(target_list)
            for key in getattr(source, attr):
                if key not in target_set:
                    target_list.append(key)
                    target_set.add(key)

        # Merge links with dedup by (source_event_id, target_event_id) pair
        existing_link_pairs = {
            (link.source_event_id, link.target_event_id) for link in target.links
        }
        for link in source.links:
            pair = (link.source_event_id, link.target_event_id)
            if pair not in existing_link_pairs:
                target.links.append(link)
                existing_link_pairs.add(pair)

        # Re-validate root: find the temporally earliest event across all merged events
        earliest_id = None
        earliest_time = None
        for eid in target.event_ids:
            evt = self._events.get(eid)
            if evt:
                t = self._coerce_dt(evt.get("received_at"))
                if earliest_time is None or t < earliest_time:
                    earliest_time = t
                    earliest_id = eid
        if earliest_id:
            target.root_event_id = earliest_id

        target.updated_at = now
        # Mark source as expired and record merge
        source.state = CorrelationState.EXPIRED
        source.metadata["merged_into"] = target.cluster_id

    def _prune_expired_clusters(self) -> None:
        """Remove expired clusters that have been merged or timed out,
        to prevent unbounded memory growth."""
        dead_ids = [
            cid for cid, c in self._clusters.items()
            if c.state == CorrelationState.EXPIRED
            and "merged_into" in c.metadata
        ]
        for cid in dead_ids:
            del self._clusters[cid]

    def _score_cluster(
        self, event: dict, cluster: CorrelationCluster, *, now: datetime
    ) -> tuple[float, list[CorrelationLink], list[str]]:
        score = 0.10
        reasons: list[str] = []
        links: list[CorrelationLink] = []
        event_id = str(event["event_id"])
        event_kind = str(event.get("kind", ""))
        device_id = str(event.get("device_id", "") or "")
        interface_name = str(event.get("interface_name", "") or "")
        neighbor_device = str(event.get("neighbor_device_id", "") or "")
        subject_key = self._subject_key(event)

        # --- Heuristic bonuses (retained for non-causal signals) ---
        if subject_key in cluster.subject_keys:
            score += self.policy.same_subject_bonus
            reasons.append("same_subject")
        if device_id and device_id in cluster.device_ids:
            score += self.policy.same_device_bonus
            reasons.append("same_device")
        if interface_name and interface_name in cluster.interface_ids:
            score += self.policy.same_interface_bonus
            reasons.append("same_interface")
        if neighbor_device and neighbor_device in cluster.device_ids:
            score += self.policy.topology_neighbor_bonus
            reasons.append("topology_neighbor")

        # Topology-confirmed adjacency: if the topology model confirms
        # this device is a neighbor of any device in the cluster, boost score.
        if device_id and self.topology.device_count > 0:
            for cluster_device in cluster.device_ids:
                if cluster_device != device_id and self.topology.are_neighbors(device_id, cluster_device):
                    score += self.policy.topology_neighbor_bonus
                    reasons.append(f"topology_confirmed_neighbor:{cluster_device}")
                    break

        # --- Causal graph reasoning ---
        cluster_events = {
            eid: self._events[eid]
            for eid in cluster.event_ids[-12:]
            if eid in self._events
        }
        causal_links = self.causal_engine.find_causal_links(event, list(cluster_events.values()))
        if causal_links:
            best_causal = causal_links[0]
            causal_bonus = min(0.40, best_causal.confidence * 0.45)
            score += causal_bonus
            reasons.append(f"causal:{best_causal.rule.cause_kind}->{best_causal.rule.effect_kind}"
                           f"(conf={best_causal.confidence:.2f},hops={best_causal.hop_distance},"
                           f"delay={best_causal.delay_seconds:.1f}s)")
            links.append(CorrelationLink(
                source_event_id=event_id,
                target_event_id=best_causal.cause_event_id,
                correlation_type=CorrelationType.CAUSE_EFFECT,
                confidence=best_causal.confidence,
                reason=(f"{best_causal.rule.cause_kind}->{best_causal.rule.effect_kind} "
                        f"within {best_causal.delay_seconds:.1f}s, "
                        f"{best_causal.hop_distance} hops"),
            ))

            # Multi-hop chain bonus
            chains = self.causal_engine.find_causal_chains(event, self._events)
            if chains and chains[0].depth > 1:
                chain_bonus = min(0.15, chains[0].total_confidence * 0.18)
                score += chain_bonus
                reasons.append(f"multi_hop_chain(depth={chains[0].depth},"
                               f"conf={chains[0].total_confidence:.2f})")

        # --- Legacy pair-based fallback ---
        if not causal_links:
            most_recent_event_ids = list(reversed(cluster.event_ids[-6:]))
            for target_event_id in most_recent_event_ids:
                target = self._events.get(target_event_id)
                if target is None:
                    continue
                target_kind = str(target.get("kind", ""))
                target_device = str(target.get("device_id", "") or "")
                target_time = self._coerce_dt(target.get("received_at", now))
                dt = abs((now - target_time).total_seconds())
                if dt > self.policy.max_window_seconds:
                    continue

                corr_type = self._infer_correlation_type(
                    event_kind, target_kind, device_id, target_device
                )
                if corr_type == CorrelationType.CAUSE_EFFECT and dt <= self.policy.cause_effect_window_seconds:
                    score += self.policy.cause_effect_bonus
                    reasons.append(f"legacy_cause_effect:{target_kind}")
                    links.append(CorrelationLink(
                        source_event_id=event_id,
                        target_event_id=target_event_id,
                        correlation_type=CorrelationType.CAUSE_EFFECT,
                        confidence=min(0.99, self.policy.cause_effect_bonus + 0.45),
                        reason=f"{target_kind}->{event_kind} within {dt:.1f}s (legacy)",
                    ))
                    break

                if corr_type == CorrelationType.SAME_SUBJECT and dt <= self.policy.max_window_seconds:
                    links.append(CorrelationLink(
                        source_event_id=event_id,
                        target_event_id=target_event_id,
                        correlation_type=CorrelationType.SAME_SUBJECT,
                        confidence=min(0.95, self.policy.same_subject_bonus + 0.35),
                        reason="Shared subject key and time cohort",
                    ))

        # Derivative penalty
        if event_kind.endswith("_flap") or event_kind.endswith("_changed"):
            score = max(0.0, score - self.policy.derivative_penalty)
            reasons.append("derivative_penalty")

        score = max(0.0, min(0.99, score))
        if not reasons:
            reasons.append("temporal_cohort")
        return score, links, reasons

    _CAUSE_EFFECT_KIND_PAIRS = {
        ("interface_down", "neighbor_lost"),
        ("interface_down", "bgp_session_dropped"),
        ("neighbor_lost", "bgp_session_dropped"),
        ("device_reachability_lost", "bgp_session_dropped"),
        ("bgp_session_dropped", "route_withdrawn"),
    }

    def _infer_correlation_type(
        self, source_kind: str, target_kind: str,
        source_device: str, target_device: str,
    ) -> CorrelationType:
        sk = source_kind.lower().replace("changekind.", "")
        tk = target_kind.lower().replace("changekind.", "")
        if (tk, sk) in self._CAUSE_EFFECT_KIND_PAIRS:
            return CorrelationType.CAUSE_EFFECT
        if source_device and target_device and source_device == target_device:
            return CorrelationType.SAME_SUBJECT
        return CorrelationType.TEMPORAL_COHORT

    def _new_cluster(self, event: dict, *, now: datetime) -> CorrelationCluster:
        event_id = str(event["event_id"])
        cluster = CorrelationCluster(
            cluster_id=f"cluster-{uuid4().hex[:12]}",
            opened_at=now,
            updated_at=now,
            state=CorrelationState.OPEN,
            root_event_id=event_id,
            event_ids=[event_id],
            device_ids=[], kinds=[], subject_keys=[], interface_ids=[],
            metadata={"created_reason": "no-candidate-passed-threshold"},
        )
        self._append_cluster_dimensions(cluster, event)
        return cluster

    def _append_cluster_dimensions(self, cluster: CorrelationCluster, event: dict) -> None:
        for attr, key in [("subject_keys", self._subject_key(event)),
                          ("device_ids", str(event.get("device_id", "") or "")),
                          ("interface_ids", str(event.get("interface_name", "") or "")),
                          ("kinds", str(event.get("kind", "") or ""))]:
            lst = getattr(cluster, attr)
            if key and key not in lst:
                lst.append(key)

    def _subject_key(self, event: dict) -> str:
        field = str(event.get("field_path", "") or "")
        device = str(event.get("device_id", "") or "")
        iface = str(event.get("interface_name", "") or "")
        neighbor = str(event.get("neighbor_device_id", "") or "")
        if field or device or iface or neighbor:
            return f"{field}|{device}|{iface}|{neighbor}"
        return str(event.get("event_id", ""))

    def _role_for_event(self, confidence: float, event: dict, cluster: CorrelationCluster) -> CorrelationRole:
        kind = str(event.get("kind", "")).lower().replace("changekind.", "")
        event_time = self._coerce_dt(event.get("received_at"))
        if cluster.root_event_id and cluster.root_event_id in self._events:
            root_time = self._coerce_dt(self._events[cluster.root_event_id].get("received_at"))
            if event_time < root_time:
                return CorrelationRole.ROOT
        if confidence >= self.policy.root_threshold:
            return CorrelationRole.PRIMARY
        if confidence >= self.policy.derivative_threshold or kind.endswith("_flap"):
            return CorrelationRole.DERIVATIVE
        return CorrelationRole.SUPPORTING

    def _expire_old_clusters(self, now: datetime) -> None:
        for cluster in self._clusters.values():
            if cluster.state == CorrelationState.EXPIRED:
                continue
            if (now - cluster.updated_at).total_seconds() > self.policy.max_window_seconds:
                cluster.state = CorrelationState.EXPIRED
        # Prune merged clusters to prevent memory leak
        self._prune_expired_clusters()
        # Bound event memory
        if len(self._events) > 5000:
            keep: set[str] = set()
            for c in self._clusters.values():
                if c.state != CorrelationState.EXPIRED:
                    keep.update(c.event_ids)
            self._events = {k: v for k, v in self._events.items() if k in keep}

    @staticmethod
    def _coerce_dt(value) -> datetime:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return _utc_now()
        return _utc_now()
