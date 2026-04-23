from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, Any

class CorrelationType(str, Enum):
    SAME_SUBJECT = "same_subject"
    TOPOLOGY_NEIGHBOR = "topology_neighbor"
    CAUSE_EFFECT = "cause_effect"
    TEMPORAL_COHORT = "temporal_cohort"
    DERIVATIVE_SIGNAL = "derivative_signal"

class CorrelationRole(str, Enum):
    ROOT = "root"
    PRIMARY = "primary"
    DERIVATIVE = "derivative"
    SUPPORTING = "supporting"
    UNKNOWN = "unknown"

class CorrelationState(str, Enum):
    OPEN = "open"
    STABLE = "stable"
    CLOSED = "closed"
    EXPIRED = "expired"

@dataclass(slots=True)
class CorrelationLink:
    source_event_id: str
    target_event_id: str
    correlation_type: CorrelationType
    confidence: float
    reason: str

@dataclass(slots=True)
class CorrelationDecision:
    cluster_id: str
    event_id: str
    role: CorrelationRole
    is_new_cluster: bool
    confidence: float
    linked_event_ids: list[str] = field(default_factory=list)
    explanation: str = ""

@dataclass(slots=True)
class CorrelationCluster:
    cluster_id: str
    opened_at: datetime
    updated_at: datetime
    state: CorrelationState
    root_event_id: Optional[str]
    event_ids: list[str] = field(default_factory=list)
    subject_keys: list[str] = field(default_factory=list)
    device_ids: list[str] = field(default_factory=list)
    interface_ids: list[str] = field(default_factory=list)
    kinds: list[str] = field(default_factory=list)
    links: list[CorrelationLink] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
