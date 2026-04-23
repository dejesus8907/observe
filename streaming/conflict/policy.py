from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class ConflictResolutionPolicy:
    freshness_decay_floor: float = 0.05
    likely_threshold: float = 0.65
    confirmed_threshold: float = 0.85
    dispute_margin: float = 0.12
    corroboration_bonus: float = 0.08
    oscillation_window_seconds: float = 30.0
    oscillation_min_transitions: int = 3
    max_confidence: float = 0.99


DefaultConflictResolutionPolicy = ConflictResolutionPolicy()
