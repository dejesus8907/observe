from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta

from .models import EvidenceRecord
from .policy_profiles import get_stabilization_min_transitions, get_stabilization_window


@dataclass(slots=True)
class StabilizationDecision:
    churn_detected: bool
    stability_penalty: float
    transition_count: int
    window_seconds: float


class ConflictStabilizer:
    """Detect oscillation/churn and damp confidence.

    This is intentionally simple in Phase 4: it does not override the resolver,
    but it can penalize confidence for rapidly oscillating fields so the system
    stops pretending flapping state is clean truth.
    """

    def __init__(self, *, window_seconds: float = 30.0, min_transitions: int = 3, penalty_per_extra_transition: float = 0.08) -> None:
        self.window_seconds = window_seconds
        self.min_transitions = min_transitions
        self.penalty_per_extra_transition = penalty_per_extra_transition

    def evaluate(self, records: list[EvidenceRecord], *, now: datetime) -> StabilizationDecision:
        if records:
            field_key = f"{records[0].subject_type.value}.{records[0].field_name}"
            window_seconds = get_stabilization_window(field_key, self.window_seconds)
            min_transitions = get_stabilization_min_transitions(field_key, self.min_transitions)
        else:
            field_key = ""
            window_seconds = self.window_seconds
            min_transitions = self.min_transitions
        cutoff = now - timedelta(seconds=window_seconds)
        recent = [r for r in sorted(records, key=lambda r: r.observed_at) if r.observed_at >= cutoff]
        if len(recent) < 2:
            return StabilizationDecision(False, 0.0, 0, window_seconds)

        transitions = 0
        last_value = None
        for record in recent:
            value = repr(record.asserted_value)
            if last_value is not None and value != last_value:
                transitions += 1
            last_value = value

        if transitions < min_transitions - 1:
            return StabilizationDecision(False, 0.0, transitions, window_seconds)

        extra = max(0, transitions - (min_transitions - 1))
        penalty = min(0.40, 0.15 + extra * self.penalty_per_extra_transition)
        return StabilizationDecision(True, penalty, transitions, window_seconds)
