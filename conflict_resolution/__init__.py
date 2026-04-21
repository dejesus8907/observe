"""Conflict Resolution Engine for NetObserv.

Public surface
--------------
Engine:
    ConflictResolutionEngine    — top-level orchestrator
    EngineConfig                — strategy and threshold configuration
    ResolutionResult            — batch resolution outcome summary

Models:
    ConflictRecord              — conflict state-machine object
    ConflictCandidate           — a single competing value + provenance
    StateTransition             — immutable audit event

Source trust:
    SourceTrustRegistry         — maps source names to trust profiles
    SourceTrustProfile          — per-source tier + weight + freshness TTL

Evidence decay:
    EvidenceDecayModel          — applies time-based decay curves
    DecayedWeight               — result of a single decay calculation
    DecayStrategy               — EXPONENTIAL | LINEAR | STEP | LOGARITHMIC
    default_decay_for_tier      — convenience factory by SourceTrustTier name

Scoring:
    ConfidenceScorer            — composite trust + freshness + corroboration scorer
    CandidateScore              — full score breakdown for a single candidate
    EvidenceEntry               — a single piece of evidence feeding the scorer
    ScoringConfig               — tunable scorer parameters
"""

from netobserv.conflict_resolution.decay import (
    DecayStrategy,
    DecayedWeight,
    EvidenceDecayModel,
    default_decay_for_tier,
)
from netobserv.conflict_resolution.engine import (
    ConflictResolutionEngine,
    EngineConfig,
    ResolutionResult,
)
from netobserv.conflict_resolution.models import (
    ConflictCandidate,
    ConflictRecord,
    StateTransition,
)
from netobserv.conflict_resolution.scoring import (
    CandidateScore,
    ConfidenceScorer,
    EvidenceEntry,
    ScoringConfig,
)
from netobserv.conflict_resolution.source_trust import (
    SourceTrustProfile,
    SourceTrustRegistry,
)

__all__ = [
    # Engine
    "ConflictResolutionEngine",
    "EngineConfig",
    "ResolutionResult",
    # Models
    "ConflictRecord",
    "ConflictCandidate",
    "StateTransition",
    # Source trust
    "SourceTrustRegistry",
    "SourceTrustProfile",
    # Decay
    "EvidenceDecayModel",
    "DecayedWeight",
    "DecayStrategy",
    "default_decay_for_tier",
    # Scoring
    "ConfidenceScorer",
    "CandidateScore",
    "EvidenceEntry",
    "ScoringConfig",
]
