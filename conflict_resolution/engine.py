"""Conflict Resolution Engine.

This is the top-level orchestrator.  It receives ``ConflictRecord`` objects in
DETECTED state and drives them to a terminal state (RESOLVED, PRESERVED, or
STALE) using a configurable pipeline of strategies.

Resolution pipeline (applied in order)
---------------------------------------
1. **AUTHORITATIVE_OVERRIDE** — if any candidate comes exclusively from an
   AUTHORITATIVE source, it wins immediately without further evaluation.
2. **Expiry check** — if all evidence across all candidates has decayed past
   the expiry threshold the conflict is marked STALE.
3. **PRESERVE_ALL gate** — if the engine is configured with
   ``default_strategy=PRESERVE_ALL``, all non-expired candidates are kept.
4. **Strategy dispatch** — the configured strategy (HIGHEST_TRUST,
   MOST_RECENT, HIGHEST_CONFIDENCE, MAJORITY_VOTE, or MANUAL) is applied to
   select a winner among the surviving (non-expired) candidates.
5. If no winner can be determined (e.g. a tie that the strategy cannot break),
   the conflict is PRESERVED rather than silently dropped.

Topology edge wiring
--------------------
``resolve_topology_edges()`` is a convenience method that accepts the list of
``CanonicalTopologyEdge`` objects produced by ``TopologyBuilder._reconcile_edges``
and re-runs them through the engine, enriching each edge's
``confidence_score`` and ``conflict_flags`` with the resolution outcome.

This keeps the engine independent of the topology subsystem while still
allowing the builder to delegate conflict resolution to this module.
"""

from __future__ import annotations

import logging
from collections import Counter
from datetime import datetime
from typing import Any, Optional, Sequence

from netobserv.conflict_resolution.decay import EvidenceDecayModel, default_decay_for_tier
from netobserv.conflict_resolution.models import ConflictCandidate, ConflictRecord
from netobserv.conflict_resolution.scoring import (
    CandidateScore,
    ConfidenceScorer,
    EvidenceEntry,
    ScoringConfig,
)
from netobserv.conflict_resolution.source_trust import SourceTrustRegistry
from netobserv.models.enums import (
    ConflictState,
    EdgeConfidence,
    ResolutionStrategy,
    SourceTrustTier,
)

logger = logging.getLogger("netobserv.conflict_resolution.engine")


# ---------------------------------------------------------------------------
# EngineConfig
# ---------------------------------------------------------------------------


class EngineConfig:
    """Configuration bundle for the ConflictResolutionEngine.

    Parameters
    ----------
    default_strategy:
        The fall-through strategy used when no AUTHORITATIVE source is present
        and no override is specified per-conflict.
    preserve_on_tie:
        When True, a tie (two candidates with identical composite scores) is
        PRESERVED rather than broken arbitrarily.
    min_score_to_resolve:
        Winning candidates must achieve at least this composite score.  Below
        this threshold the conflict is PRESERVED (not silently dropped).
    expiry_threshold:
        Effective weight below which individual evidence items are considered
        expired.  Forwarded to scoring and decay models.
    authoritative_override_enabled:
        If True, an AUTHORITATIVE source wins unconditionally (step 1 of the
        pipeline).  Set False in unit tests to isolate strategy logic.
    """

    def __init__(
        self,
        default_strategy: ResolutionStrategy = ResolutionStrategy.HIGHEST_CONFIDENCE,
        preserve_on_tie: bool = True,
        min_score_to_resolve: float = 0.20,
        expiry_threshold: float = 0.05,
        authoritative_override_enabled: bool = True,
    ) -> None:
        self.default_strategy = default_strategy
        self.preserve_on_tie = preserve_on_tie
        self.min_score_to_resolve = min_score_to_resolve
        self.expiry_threshold = expiry_threshold
        self.authoritative_override_enabled = authoritative_override_enabled


# ---------------------------------------------------------------------------
# ResolutionResult
# ---------------------------------------------------------------------------


class ResolutionResult:
    """Outcome summary returned from a batch resolution run."""

    def __init__(self) -> None:
        self.resolved: list[ConflictRecord] = []
        self.preserved: list[ConflictRecord] = []
        self.stale: list[ConflictRecord] = []
        self.suppressed: list[ConflictRecord] = []
        self.errors: list[tuple[ConflictRecord, Exception]] = []

    @property
    def total(self) -> int:
        return (
            len(self.resolved)
            + len(self.preserved)
            + len(self.stale)
            + len(self.suppressed)
            + len(self.errors)
        )

    def summary(self) -> dict[str, int]:
        return {
            "resolved": len(self.resolved),
            "preserved": len(self.preserved),
            "stale": len(self.stale),
            "suppressed": len(self.suppressed),
            "errors": len(self.errors),
            "total": self.total,
        }


# ---------------------------------------------------------------------------
# ConflictResolutionEngine
# ---------------------------------------------------------------------------


class ConflictResolutionEngine:
    """Drives ConflictRecord objects from DETECTED to a terminal state.

    The engine is intentionally stateless between calls — it does not cache
    past decisions.  All state lives in the ConflictRecord objects passed to
    it.  This makes it safe to run the engine multiple times on the same set
    of conflicts as evidence ages.
    """

    def __init__(
        self,
        config: Optional[EngineConfig] = None,
        trust_registry: Optional[SourceTrustRegistry] = None,
        scoring_config: Optional[ScoringConfig] = None,
        decay_model: Optional[EvidenceDecayModel] = None,
    ) -> None:
        self._config = config or EngineConfig()
        self._trust = trust_registry or SourceTrustRegistry()
        self._scorer = ConfidenceScorer(
            trust_registry=self._trust,
            config=scoring_config or ScoringConfig(),
            decay_model=decay_model,
        )

    # ------------------------------------------------------------------
    # Primary entry points
    # ------------------------------------------------------------------

    def resolve(
        self,
        conflict: ConflictRecord,
        strategy: Optional[ResolutionStrategy] = None,
        reference_time: Optional[datetime] = None,
    ) -> ConflictRecord:
        """Resolve a single conflict in-place and return it.

        Parameters
        ----------
        conflict:
            The record to resolve.  Must be in DETECTED or EVALUATING state.
        strategy:
            Override the default strategy for this specific conflict.
        reference_time:
            Inject "now" for deterministic scoring in tests.
        """
        if conflict.is_terminal:
            return conflict  # Already settled — idempotent

        effective_strategy = strategy or self._config.default_strategy
        conflict.transition(ConflictState.EVALUATING, reason="engine.resolve() called")

        try:
            self._run_resolution(conflict, effective_strategy, reference_time)
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Conflict resolution failed for conflict_id=%s: %s",
                conflict.conflict_id,
                exc,
            )
            conflict.suppress(reason=f"engine error: {exc}")

        return conflict

    def resolve_many(
        self,
        conflicts: Sequence[ConflictRecord],
        strategy: Optional[ResolutionStrategy] = None,
        reference_time: Optional[datetime] = None,
    ) -> ResolutionResult:
        """Resolve a batch of conflicts.  Errors are caught per-record."""
        result = ResolutionResult()
        for conflict in conflicts:
            if conflict.is_terminal:
                continue
            try:
                self.resolve(conflict, strategy=strategy, reference_time=reference_time)
            except Exception as exc:  # noqa: BLE001
                result.errors.append((conflict, exc))
                continue

            if conflict.state == ConflictState.RESOLVED:
                result.resolved.append(conflict)
            elif conflict.state == ConflictState.PRESERVED:
                result.preserved.append(conflict)
            elif conflict.state == ConflictState.STALE:
                result.stale.append(conflict)
            else:
                result.suppressed.append(conflict)

        summary = result.summary()
        logger.info(
            "Batch resolution complete — resolved=%d preserved=%d stale=%d suppressed=%d errors=%d total=%d",
            summary["resolved"],
            summary["preserved"],
            summary["stale"],
            summary["suppressed"],
            summary["errors"],
            summary["total"],
        )
        return result

    def preserve(
        self,
        conflict: ConflictRecord,
        reason: str = "explicit preserve requested",
    ) -> ConflictRecord:
        """Explicitly preserve all candidates in a conflict without resolution."""
        if conflict.is_terminal:
            return conflict
        conflict.transition(ConflictState.EVALUATING, reason="preserve() called")
        candidate_ids = [c.candidate_id for c in conflict.candidates]
        conflict.mark_preserved(candidate_ids, reason=reason)
        return conflict

    # ------------------------------------------------------------------
    # Topology edge convenience
    # ------------------------------------------------------------------

    def resolve_topology_edges(
        self,
        edge_groups: dict[str, list[Any]],  # group_key → list[CanonicalTopologyEdge]
        snapshot_id: Optional[str] = None,
        reference_time: Optional[datetime] = None,
    ) -> dict[str, Any]:
        """Resolve conflicts across topology edge groups.

        Accepts the same ``groups`` dict that ``TopologyBuilder._reconcile_edges``
        works with (keyed by canonical pair key, values are lists of
        ``CanonicalTopologyEdge``).

        For each group with multiple edges (i.e. a conflict), a
        ``ConflictRecord`` is created, scored, and resolved.  The winning
        edge's ``confidence_score`` is boosted and non-winning edges have
        their ``conflict_flags`` updated.

        Returns a dict mapping group_key → winning CanonicalTopologyEdge.
        """
        result: dict[str, Any] = {}

        for group_key, edges in edge_groups.items():
            if len(edges) == 1:
                result[group_key] = edges[0]
                continue

            # Build a ConflictRecord for this edge group
            record = ConflictRecord(
                object_type="topology_edge",
                object_id=group_key,
                field_name="evidence_source",
                snapshot_id=snapshot_id,
            )

            for edge in edges:
                for src in (edge.evidence_source or ["unknown"]):
                    obs_at = getattr(edge, "created_at", None)
                    candidate = ConflictCandidate(
                        value=edge,
                        source_names=[src],
                        observed_at=obs_at,
                        composite_score=edge.confidence_score,
                        trust_tier=self._trust.tier_of(src).value,
                    )
                    record.candidates.append(candidate)

            self.resolve(record, reference_time=reference_time)

            if record.state == ConflictState.RESOLVED and record.winner:
                winning_edge = record.winner.value
                # Boost winner score slightly for surviving resolution
                winning_edge = winning_edge.model_copy(
                    update={
                        "confidence_score": min(winning_edge.confidence_score + 0.02, 1.0),
                        "evidence_source": list(
                            dict.fromkeys(
                                src
                                for c in record.candidates
                                for src in c.source_names
                            )
                        ),
                    }
                )
                result[group_key] = winning_edge
            elif record.state in (ConflictState.PRESERVED, ConflictState.STALE):
                # On preserve/stale: keep the highest-scoring edge, flag it
                best = max(edges, key=lambda e: e.confidence_score)
                best = best.model_copy(
                    update={
                        "confidence": EdgeConfidence.CONFLICT,
                        "conflict_flags": list(
                            set(best.conflict_flags or [])
                            | {record.state.value, "multi_source_conflict"}
                        ),
                    }
                )
                result[group_key] = best
            else:
                result[group_key] = max(edges, key=lambda e: e.confidence_score)

        return result

    # ------------------------------------------------------------------
    # Internal resolution pipeline
    # ------------------------------------------------------------------

    def _run_resolution(
        self,
        conflict: ConflictRecord,
        strategy: ResolutionStrategy,
        reference_time: Optional[datetime],
    ) -> None:
        """Execute the resolution pipeline for a single conflict."""

        # Stage 1: AUTHORITATIVE_OVERRIDE
        if self._config.authoritative_override_enabled:
            auth_winner = self._try_authoritative_override(conflict)
            if auth_winner is not None:
                conflict.mark_resolved(
                    auth_winner.candidate_id,
                    ResolutionStrategy.AUTHORITATIVE_OVERRIDE,
                    reason="authoritative source overrides all others",
                )
                return

        # Stage 2: Score all candidates
        scored = self._score_candidates(conflict, reference_time)

        # Stage 3: Expiry check
        non_expired = [c for c in scored.values() if not c.all_expired]
        if not non_expired:
            conflict.mark_stale()
            return

        # Stage 4: PRESERVE_ALL gate
        if strategy == ResolutionStrategy.PRESERVE_ALL:
            conflict.mark_preserved(
                [c.candidate_key for c in non_expired],
                reason="strategy=PRESERVE_ALL",
            )
            return

        # Stage 5: Strategy dispatch
        winner_key = self._dispatch_strategy(strategy, conflict, scored, reference_time)

        if winner_key is None:
            if self._config.preserve_on_tie:
                conflict.mark_preserved(
                    [c.candidate_key for c in non_expired],
                    reason="tie — no clear winner",
                )
            else:
                # Arbitrary tiebreak: highest composite score by sort order
                winner_key = max(non_expired, key=lambda c: c.composite_score).candidate_key
                conflict.mark_resolved(winner_key, strategy, reason="tiebreak by score")
            return

        winning_score = scored[winner_key].composite_score
        if winning_score < self._config.min_score_to_resolve:
            conflict.mark_preserved(
                [c.candidate_key for c in non_expired],
                reason=(
                    f"winner score {winning_score:.3f} < "
                    f"min_score_to_resolve {self._config.min_score_to_resolve:.3f}"
                ),
            )
            return

        conflict.mark_resolved(winner_key, strategy, reason=f"strategy={strategy.value}")

    # ------------------------------------------------------------------
    # Strategy implementations
    # ------------------------------------------------------------------

    def _try_authoritative_override(
        self, conflict: ConflictRecord
    ) -> Optional[ConflictCandidate]:
        """Return a candidate if it is exclusively sourced from AUTHORITATIVE."""
        auth_candidates = [
            c
            for c in conflict.candidates
            if c.source_names
            and all(self._trust.is_authoritative(s) for s in c.source_names)
        ]
        if len(auth_candidates) == 1:
            return auth_candidates[0]
        # Multiple authoritative candidates — cannot override; fall through
        return None

    def _score_candidates(
        self,
        conflict: ConflictRecord,
        reference_time: Optional[datetime],
    ) -> dict[str, CandidateScore]:
        """Score every candidate in the conflict using the ConfidenceScorer."""
        scores: dict[str, CandidateScore] = {}
        for candidate in conflict.candidates:
            entries = [
                EvidenceEntry(
                    source_name=src,
                    observed_at=candidate.observed_at,
                    agrees=True,
                )
                for src in candidate.source_names
            ]
            cs = self._scorer.score(candidate.candidate_id, entries, reference_time)
            candidate.composite_score = cs.composite_score
            candidate.decay_label = (
                cs.decayed_weights[0].freshness_label if cs.decayed_weights else "unknown"
            )
            candidate.trust_tier = (
                self._trust.tier_of(candidate.source_names[0]).value
                if candidate.source_names
                else "unknown"
            )
            scores[candidate.candidate_id] = cs
        return scores

    def _dispatch_strategy(
        self,
        strategy: ResolutionStrategy,
        conflict: ConflictRecord,
        scores: dict[str, CandidateScore],
        reference_time: Optional[datetime],
    ) -> Optional[str]:
        """Return the winning candidate_id, or None on an unresolvable tie."""
        if strategy == ResolutionStrategy.HIGHEST_CONFIDENCE:
            return self._strategy_highest_confidence(scores)
        if strategy == ResolutionStrategy.HIGHEST_TRUST:
            return self._strategy_highest_trust(conflict)
        if strategy == ResolutionStrategy.MOST_RECENT:
            return self._strategy_most_recent(conflict)
        if strategy == ResolutionStrategy.MAJORITY_VOTE:
            return self._strategy_majority_vote(conflict)
        if strategy == ResolutionStrategy.MANUAL:
            return None  # Must be supplied externally
        if strategy == ResolutionStrategy.DECAY_WINNER:
            return self._strategy_decay_winner(conflict, scores)
        return self._strategy_highest_confidence(scores)

    def _strategy_highest_confidence(
        self, scores: dict[str, CandidateScore]
    ) -> Optional[str]:
        if not scores:
            return None
        best_key = max(scores, key=lambda k: scores[k].composite_score)
        best_score = scores[best_key].composite_score
        # Tie detection
        tied = [k for k, s in scores.items() if abs(s.composite_score - best_score) < 1e-6]
        return best_key if len(tied) == 1 else None

    def _strategy_highest_trust(self, conflict: ConflictRecord) -> Optional[str]:
        """Pick the candidate whose sources have the best (lowest) rank."""
        if not conflict.candidates:
            return None

        def best_rank(c: ConflictCandidate) -> int:
            if not c.source_names:
                return 999
            return min(self._trust.rank_of(s) for s in c.source_names)

        ranked = sorted(conflict.candidates, key=best_rank)
        if len(ranked) >= 2 and best_rank(ranked[0]) == best_rank(ranked[1]):
            return None   # Tie
        return ranked[0].candidate_id

    def _strategy_most_recent(self, conflict: ConflictRecord) -> Optional[str]:
        """Pick the candidate with the most recent observed_at timestamp."""
        with_ts = [c for c in conflict.candidates if c.observed_at is not None]
        if not with_ts:
            return None
        ranked = sorted(with_ts, key=lambda c: c.observed_at, reverse=True)  # type: ignore[arg-type]
        if len(ranked) >= 2 and ranked[0].observed_at == ranked[1].observed_at:
            return None
        return ranked[0].candidate_id

    def _strategy_majority_vote(self, conflict: ConflictRecord) -> Optional[str]:
        """Pick the candidate value agreed upon by the most distinct sources."""
        # Count source votes per candidate_id
        vote_counts: Counter[str] = Counter()
        for c in conflict.candidates:
            vote_counts[c.candidate_id] += len(c.source_names)

        if not vote_counts:
            return None
        most_common = vote_counts.most_common(2)
        if len(most_common) >= 2 and most_common[0][1] == most_common[1][1]:
            return None  # Tie
        return most_common[0][0]

    def _strategy_decay_winner(
        self,
        conflict: ConflictRecord,
        scores: dict[str, CandidateScore],
    ) -> Optional[str]:
        """Last surviving (non-expired) candidate with highest score wins."""
        surviving = {k: s for k, s in scores.items() if not s.all_expired}
        if not surviving:
            return None
        return max(surviving, key=lambda k: surviving[k].composite_score)
