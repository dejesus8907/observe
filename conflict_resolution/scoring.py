"""Composite Confidence Scorer.

Combines source trust weight, freshness/decay, and corroboration count into a
single composite confidence score in [0.0, 1.0].

Scoring formula
---------------
    composite = clamp(trust_component + corroboration_bonus + penalty)

Where:

    trust_component   = Σ (effective_weight_i × source_vote_i) / normaliser
                        — weighted average of all contributing source weights
                        after decay is applied.

    corroboration_bonus = k_agree × log(1 + n_agreeing)
                        — log-scaled bonus for additional independent sources
                        that agree on the same value. Diminishing returns.

    penalty           = k_conflict × n_conflicting
                        — linear penalty for each source that actively
                        disagrees with the candidate value.

Constants k_agree and k_conflict are tunable via ScoringConfig.

This module is purposefully stateless — the scorer holds only configuration
and performs no I/O.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Sequence

from netobserv.conflict_resolution.decay import DecayedWeight, EvidenceDecayModel, default_decay_for_tier
from netobserv.conflict_resolution.source_trust import SourceTrustRegistry


@dataclass
class ScoringConfig:
    """Tunable parameters for the composite confidence scorer."""

    # Log-scale bonus coefficient for corroborating sources
    corroboration_k: float = 0.08
    # Linear penalty per conflicting source
    conflict_penalty_k: float = 0.12
    # Minimum score any candidate can receive (prevents zero from a single bad read)
    floor: float = 0.01
    # Maximum score ceiling (leaves headroom for manual overrides to stand out)
    ceiling: float = 1.0


@dataclass
class EvidenceEntry:
    """A single piece of evidence contributing to a candidate value."""

    source_name: str
    observed_at: Optional[datetime] = None
    # Optional pre-computed raw weight override (skips trust registry look-up)
    raw_weight_override: Optional[float] = None
    # Whether this entry agrees with (True) or contradicts (False) the candidate
    agrees: bool = True


@dataclass
class CandidateScore:
    """Full scoring result for a single candidate value."""

    candidate_key: str           # Stable identifier for the candidate value
    composite_score: float       # Final score in [floor, ceiling]
    trust_component: float       # Weighted trust sum (pre-bonus/penalty)
    corroboration_bonus: float
    conflict_penalty: float
    supporting_sources: list[str] = field(default_factory=list)
    conflicting_sources: list[str] = field(default_factory=list)
    decayed_weights: list[DecayedWeight] = field(default_factory=list)
    all_expired: bool = False    # True if every piece of evidence has decayed away

    def __lt__(self, other: "CandidateScore") -> bool:
        return self.composite_score < other.composite_score


class ConfidenceScorer:
    """Scores candidate values using trust, freshness, and corroboration.

    Usage::

        registry = SourceTrustRegistry()
        scorer = ConfidenceScorer(registry)

        entries = [
            EvidenceEntry("lldp",          observed_at=datetime(2026, 4, 21, 10, 0)),
            EvidenceEntry("netbox_cable",  observed_at=datetime(2026, 4, 21,  8, 0)),
            EvidenceEntry("bgp_session",   observed_at=datetime(2026, 4, 21,  9, 0), agrees=False),
        ]
        score = scorer.score("edge:dev-a→dev-b", entries)
        print(score.composite_score)
    """

    def __init__(
        self,
        trust_registry: Optional[SourceTrustRegistry] = None,
        config: Optional[ScoringConfig] = None,
        decay_model: Optional[EvidenceDecayModel] = None,
    ) -> None:
        self._registry = trust_registry or SourceTrustRegistry()
        self._config = config or ScoringConfig()
        self._decay_model = decay_model  # If None, use per-tier defaults

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def score(
        self,
        candidate_key: str,
        evidence: Sequence[EvidenceEntry],
        reference_time: Optional[datetime] = None,
    ) -> CandidateScore:
        """Compute the composite confidence score for *candidate_key*.

        Parameters
        ----------
        candidate_key:
            A stable string that identifies the candidate (e.g. edge id,
            field value string, or object fingerprint).
        evidence:
            All evidence entries for this candidate.  Entries with
            ``agrees=False`` are counted as contradictions.
        reference_time:
            The reference "now" for decay computation.  Defaults to
            ``datetime.utcnow()``.  Inject an explicit value in tests.
        """
        if not evidence:
            return CandidateScore(
                candidate_key=candidate_key,
                composite_score=self._config.floor,
                trust_component=0.0,
                corroboration_bonus=0.0,
                conflict_penalty=0.0,
                all_expired=True,
            )

        now = reference_time or datetime.utcnow()

        agreeing: list[EvidenceEntry] = [e for e in evidence if e.agrees]
        conflicting: list[EvidenceEntry] = [e for e in evidence if not e.agrees]

        decayed_weights: list[DecayedWeight] = []
        weight_sum = 0.0
        weight_normaliser = 0.0

        for entry in agreeing:
            profile = self._registry.get(entry.source_name)
            raw_w = (
                entry.raw_weight_override
                if entry.raw_weight_override is not None
                else profile.base_weight
            )
            decay_model = self._decay_model or default_decay_for_tier(profile.tier.value)
            dw = decay_model.decay(raw_w, entry.observed_at, reference_time=now)
            decayed_weights.append(dw)
            if not dw.is_expired:
                weight_sum += dw.effective_weight
                weight_normaliser += profile.base_weight  # normalise against ideal weight

        all_expired = all(dw.is_expired for dw in decayed_weights) if decayed_weights else True

        trust_component = (
            weight_sum / weight_normaliser if weight_normaliser > 0 else 0.0
        )

        n_agreeing = sum(1 for dw in decayed_weights if not dw.is_expired)
        corroboration_bonus = self._config.corroboration_k * math.log(1 + n_agreeing)

        conflict_penalty = self._config.conflict_penalty_k * len(conflicting)

        composite = trust_component + corroboration_bonus - conflict_penalty
        composite = max(self._config.floor, min(self._config.ceiling, composite))

        return CandidateScore(
            candidate_key=candidate_key,
            composite_score=composite,
            trust_component=trust_component,
            corroboration_bonus=corroboration_bonus,
            conflict_penalty=conflict_penalty,
            supporting_sources=[e.source_name for e in agreeing],
            conflicting_sources=[e.source_name for e in conflicting],
            decayed_weights=decayed_weights,
            all_expired=all_expired,
        )

    def score_many(
        self,
        candidates: dict[str, Sequence[EvidenceEntry]],
        reference_time: Optional[datetime] = None,
    ) -> dict[str, CandidateScore]:
        """Score multiple candidates and return a dict keyed by candidate_key."""
        return {
            key: self.score(key, entries, reference_time)
            for key, entries in candidates.items()
        }

    def winner(
        self,
        candidates: dict[str, Sequence[EvidenceEntry]],
        reference_time: Optional[datetime] = None,
    ) -> Optional[str]:
        """Return the candidate_key with the highest composite score, or None."""
        if not candidates:
            return None
        scores = self.score_many(candidates, reference_time)
        return max(scores, key=lambda k: scores[k].composite_score)
