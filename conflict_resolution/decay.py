"""Evidence Decay Model.

Evidence does not stay valid indefinitely.  The older a data point is, the
less weight it should carry in a conflict resolution decision.  This module
provides:

1. ``EvidenceDecayModel`` — configurable decay curves per decay strategy.
2. ``DecayedWeight``        — the post-decay scalar plus diagnostic metadata.
3. ``EvidenceAgeClassifier`` — human-readable freshness bands (fresh / stale /
   expired) used by the ConflictRecord state machine.

Decay strategies
----------------
EXPONENTIAL (default)
    weight(t) = exp(-λ·t),  λ = ln(2) / half_life
    Smooth, continuous decay.  Evidence at t=half_life is exactly 0.5×.

LINEAR
    weight(t) = max(0, 1 - t / ttl)
    Uniform erosion; drops to 0 at exactly TTL.

STEP
    weight(t) = 1.0 if t ≤ ttl else 0.0
    Binary: either fully fresh or fully expired.  Useful for hard deadlines.

LOGARITHMIC
    weight(t) = max(0, 1 - log(1 + t/scale) / log(1 + ttl/scale))
    Slow initial decay then rapid drop-off — good for long-lived source-of-truth
    records whose authority erodes quickly once they become outdated.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional


class DecayStrategy(str, Enum):
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    STEP = "step"
    LOGARITHMIC = "logarithmic"


@dataclass(frozen=True)
class DecayedWeight:
    """Result returned by the decay model for a single evidence observation."""

    raw_weight: float         # Weight before decay (from trust registry)
    decay_factor: float       # Multiplier in [0.0, 1.0] produced by the decay curve
    effective_weight: float   # raw_weight × decay_factor
    age_seconds: float        # Age of the evidence at evaluation time
    is_expired: bool          # True if effective_weight fell below expiry_threshold
    freshness_label: str      # "fresh" | "aging" | "stale" | "expired"

    def __float__(self) -> float:
        return self.effective_weight


class EvidenceDecayModel:
    """Applies time-based decay to a raw trust weight.

    Parameters
    ----------
    strategy:
        The decay curve to apply.
    half_life_seconds:
        For EXPONENTIAL — the time at which weight reaches 50% of raw.
        For STEP — acts as the hard TTL boundary.
        For LINEAR — the full TTL (weight reaches 0 at this age).
        For LOGARITHMIC — the inflection scale parameter.
    expiry_threshold:
        Effective weights below this value are marked ``is_expired=True``.
        The caller (engine) may discard expired evidence entirely.
    fresh_threshold:
        Evidence with decay_factor ≥ this value is labelled "fresh".
    aging_threshold:
        Evidence with decay_factor ≥ this (and < fresh_threshold) is "aging".
        Below aging_threshold but above expiry → "stale".
    """

    def __init__(
        self,
        strategy: DecayStrategy = DecayStrategy.EXPONENTIAL,
        half_life_seconds: float = 1800.0,   # 30 min default
        expiry_threshold: float = 0.05,
        fresh_threshold: float = 0.80,
        aging_threshold: float = 0.40,
    ) -> None:
        if half_life_seconds <= 0:
            raise ValueError("half_life_seconds must be > 0")
        if not (0.0 <= expiry_threshold < aging_threshold < fresh_threshold <= 1.0):
            raise ValueError(
                "thresholds must satisfy 0 ≤ expiry < aging < fresh ≤ 1"
            )
        self.strategy = strategy
        self.half_life_seconds = half_life_seconds
        self.expiry_threshold = expiry_threshold
        self.fresh_threshold = fresh_threshold
        self.aging_threshold = aging_threshold

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def decay(
        self,
        raw_weight: float,
        observed_at: Optional[datetime],
        reference_time: Optional[datetime] = None,
    ) -> DecayedWeight:
        """Apply decay to *raw_weight* based on the age of *observed_at*.

        Parameters
        ----------
        raw_weight:
            Pre-decay trust weight (e.g. from SourceTrustProfile.base_weight).
        observed_at:
            When the evidence was collected.  ``None`` ⟹ age = half_life
            (conservative: treat as half-decayed).
        reference_time:
            The "now" used to compute age.  Defaults to ``datetime.utcnow()``.
            Explicit injection makes scoring deterministic in tests.
        """
        now = reference_time or datetime.utcnow()
        if observed_at is None:
            age_seconds = self.half_life_seconds
        else:
            age_seconds = max(0.0, (now - observed_at).total_seconds())

        decay_factor = self._compute_decay(age_seconds)
        effective = max(0.0, raw_weight * decay_factor)
        is_expired = effective < self.expiry_threshold

        if decay_factor >= self.fresh_threshold:
            label = "fresh"
        elif decay_factor >= self.aging_threshold:
            label = "aging"
        elif not is_expired:
            label = "stale"
        else:
            label = "expired"

        return DecayedWeight(
            raw_weight=raw_weight,
            decay_factor=decay_factor,
            effective_weight=effective,
            age_seconds=age_seconds,
            is_expired=is_expired,
            freshness_label=label,
        )

    def is_expired_evidence(
        self, observed_at: Optional[datetime], reference_time: Optional[datetime] = None
    ) -> bool:
        """Convenience predicate — True if evidence age crosses expiry threshold."""
        return self.decay(1.0, observed_at, reference_time).is_expired

    # ------------------------------------------------------------------
    # Decay curve implementations
    # ------------------------------------------------------------------

    def _compute_decay(self, age_seconds: float) -> float:
        if self.strategy == DecayStrategy.EXPONENTIAL:
            return self._exponential(age_seconds)
        if self.strategy == DecayStrategy.LINEAR:
            return self._linear(age_seconds)
        if self.strategy == DecayStrategy.STEP:
            return self._step(age_seconds)
        if self.strategy == DecayStrategy.LOGARITHMIC:
            return self._logarithmic(age_seconds)
        raise ValueError(f"Unknown decay strategy: {self.strategy}")

    def _exponential(self, age: float) -> float:
        lam = math.log(2) / self.half_life_seconds
        return math.exp(-lam * age)

    def _linear(self, age: float) -> float:
        return max(0.0, 1.0 - age / self.half_life_seconds)

    def _step(self, age: float) -> float:
        return 1.0 if age <= self.half_life_seconds else 0.0

    def _logarithmic(self, age: float) -> float:
        scale = self.half_life_seconds
        numerator = math.log(1.0 + age / scale)
        denominator = math.log(2.0)   # log(1 + 1) at age == scale → 0.5
        return max(0.0, 1.0 - numerator / denominator)


# ---------------------------------------------------------------------------
# Convenience: per-source-tier decay model defaults
# ---------------------------------------------------------------------------

def default_decay_for_tier(tier_name: str) -> EvidenceDecayModel:
    """Return a sensible decay model for each SourceTrustTier.

    Authoritative sources decay slowly (logarithmic); live device telemetry
    uses exponential with a short half-life; inferred sources use a step
    function with a hard TTL.
    """
    profiles = {
        "authoritative": EvidenceDecayModel(
            strategy=DecayStrategy.LOGARITHMIC,
            half_life_seconds=43200.0,    # 12 h
            expiry_threshold=0.05,
        ),
        "first_party": EvidenceDecayModel(
            strategy=DecayStrategy.EXPONENTIAL,
            half_life_seconds=900.0,      # 15 min
            expiry_threshold=0.05,
        ),
        "second_party": EvidenceDecayModel(
            strategy=DecayStrategy.EXPONENTIAL,
            half_life_seconds=1800.0,     # 30 min
            expiry_threshold=0.05,
        ),
        "third_party": EvidenceDecayModel(
            strategy=DecayStrategy.LINEAR,
            half_life_seconds=7200.0,     # 2 h full TTL
            expiry_threshold=0.05,
        ),
        "untrusted": EvidenceDecayModel(
            strategy=DecayStrategy.STEP,
            half_life_seconds=1800.0,     # 30-min hard cut-off
            expiry_threshold=0.05,
        ),
    }
    return profiles.get(
        tier_name.lower(),
        EvidenceDecayModel(),   # exponential 30-min default
    )
