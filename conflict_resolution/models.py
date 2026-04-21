"""Conflict Resolution data models.

Defines the ConflictRecord — the central state-machine object that moves
through detection → evaluation → resolution/preservation.

State machine transitions
-------------------------
    DETECTED  →  EVALUATING   (engine picks it up)
    EVALUATING → RESOLVED     (a winner was chosen)
    EVALUATING → PRESERVED    (explicitly kept as multi-valued)
    EVALUATING → STALE        (all evidence decayed past usability)
    DETECTED   → SUPPRESSED   (operator ack with no action)
    PRESERVED  → STALE        (preserved evidence later decays)

Each transition is recorded in the ``history`` list for full audit trail.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from netobserv.models.enums import ConflictState, ResolutionStrategy


def _now() -> datetime:
    return datetime.utcnow()


def _new_id() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# ConflictCandidate
# ---------------------------------------------------------------------------


@dataclass
class ConflictCandidate:
    """One of the competing values in a conflict.

    Each candidate represents a distinct assertion about the same field/edge
    made by one or more sources.
    """

    candidate_id: str = field(default_factory=_new_id)
    value: Any = None                    # The actual asserted value
    source_names: list[str] = field(default_factory=list)
    observed_at: Optional[datetime] = None
    composite_score: float = 0.0         # Filled in by ConfidenceScorer
    trust_tier: str = "unknown"
    decay_label: str = "unknown"         # fresh / aging / stale / expired
    is_winner: bool = False              # Set by engine after resolution


# ---------------------------------------------------------------------------
# StateTransition
# ---------------------------------------------------------------------------


@dataclass
class StateTransition:
    """An immutable record of a single state-machine transition."""

    from_state: ConflictState
    to_state: ConflictState
    timestamp: datetime = field(default_factory=_now)
    actor: str = "engine"                # "engine" | "operator" | "decay"
    reason: str = ""


# ---------------------------------------------------------------------------
# ConflictRecord
# ---------------------------------------------------------------------------


@dataclass
class ConflictRecord:
    """The central state-machine object for a single detected conflict.

    A conflict arises when two or more sources assert different values for the
    same field on the same object (e.g. two sources disagree on which remote
    port a cable is connected to).

    Fields
    ------
    conflict_id:
        Unique identifier.  Stable across re-evaluations within the same
        snapshot.
    object_type:
        Canonical object class (e.g. ``"topology_edge"``, ``"device"``,
        ``"interface"``).
    object_id:
        Identifier of the object in conflict (e.g. edge_id, device_id).
    field_name:
        The specific field where the disagreement was detected, or ``None``
        for whole-object conflicts (e.g. duplicate identity).
    candidates:
        All competing values plus their provenance.
    state:
        Current life-cycle state (DETECTED → EVALUATING → RESOLVED / …).
    strategy:
        The algorithm that was (or will be) used to resolve this conflict.
    resolved_candidate_id:
        ID of the winning candidate after resolution.  ``None`` if
        PRESERVED or not yet resolved.
    preserved_candidate_ids:
        IDs of all candidates intentionally kept (PRESERVE_ALL strategy).
    snapshot_id:
        The discovery snapshot this conflict belongs to.
    history:
        Ordered list of all state transitions for audit purposes.
    metadata:
        Free-form dict for caller-supplied annotations.
    """

    conflict_id: str = field(default_factory=_new_id)
    object_type: str = ""
    object_id: str = ""
    field_name: Optional[str] = None
    candidates: list[ConflictCandidate] = field(default_factory=list)
    state: ConflictState = ConflictState.DETECTED
    strategy: Optional[ResolutionStrategy] = None
    resolved_candidate_id: Optional[str] = None
    preserved_candidate_ids: list[str] = field(default_factory=list)
    snapshot_id: Optional[str] = None
    detected_at: datetime = field(default_factory=_now)
    resolved_at: Optional[datetime] = None
    history: list[StateTransition] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # State machine helpers
    # ------------------------------------------------------------------

    def transition(
        self,
        to_state: ConflictState,
        *,
        actor: str = "engine",
        reason: str = "",
    ) -> None:
        """Transition to *to_state* and record the event in history."""
        self.history.append(
            StateTransition(
                from_state=self.state,
                to_state=to_state,
                actor=actor,
                reason=reason,
            )
        )
        self.state = to_state
        if to_state in (
            ConflictState.RESOLVED,
            ConflictState.PRESERVED,
            ConflictState.STALE,
            ConflictState.SUPPRESSED,
        ):
            self.resolved_at = _now()

    def mark_resolved(
        self, candidate_id: str, strategy: ResolutionStrategy, reason: str = ""
    ) -> None:
        """Apply a resolution: mark *candidate_id* as the winner."""
        self.strategy = strategy
        self.resolved_candidate_id = candidate_id
        for c in self.candidates:
            c.is_winner = c.candidate_id == candidate_id
        self.transition(ConflictState.RESOLVED, reason=reason)

    def mark_preserved(
        self, candidate_ids: list[str], reason: str = ""
    ) -> None:
        """Preserve all *candidate_ids* as a multi-valued truth."""
        self.strategy = ResolutionStrategy.PRESERVE_ALL
        self.preserved_candidate_ids = list(candidate_ids)
        self.transition(ConflictState.PRESERVED, reason=reason)

    def mark_stale(self, reason: str = "all evidence decayed") -> None:
        self.strategy = ResolutionStrategy.DECAY_WINNER
        self.transition(ConflictState.STALE, actor="decay", reason=reason)

    def suppress(self, operator: str = "operator", reason: str = "") -> None:
        self.transition(ConflictState.SUPPRESSED, actor=operator, reason=reason)

    # ------------------------------------------------------------------
    # Convenience accessors
    # ------------------------------------------------------------------

    @property
    def is_terminal(self) -> bool:
        return self.state in (
            ConflictState.RESOLVED,
            ConflictState.PRESERVED,
            ConflictState.STALE,
            ConflictState.SUPPRESSED,
        )

    @property
    def winner(self) -> Optional[ConflictCandidate]:
        if not self.resolved_candidate_id:
            return None
        for c in self.candidates:
            if c.candidate_id == self.resolved_candidate_id:
                return c
        return None

    @property
    def preserved(self) -> list[ConflictCandidate]:
        ids = set(self.preserved_candidate_ids)
        return [c for c in self.candidates if c.candidate_id in ids]

    def candidate_count(self) -> int:
        return len(self.candidates)

    def source_count(self) -> int:
        seen: set[str] = set()
        for c in self.candidates:
            seen.update(c.source_names)
        return len(seen)

    def summary(self) -> str:
        n = len(self.candidates)
        src = self.source_count()
        return (
            f"ConflictRecord({self.conflict_id[:8]}…) "
            f"[{self.object_type}/{self.field_name or 'whole'}] "
            f"state={self.state.value} candidates={n} sources={src}"
        )
