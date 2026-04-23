from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional


class SubjectType(str, Enum):
    DEVICE = "device"
    INTERFACE = "interface"
    EDGE = "edge"
    BGP_SESSION = "bgp_session"
    ROUTE = "route"
    MAC_BINDING = "mac_binding"
    ARP_MAPPING = "arp_mapping"


class SourceHealthState(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class ResolutionState(str, Enum):
    CONFIRMED = "confirmed"
    LIKELY = "likely"
    DISPUTED = "disputed"
    STALE = "stale"
    WITHDRAWN = "withdrawn"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"


class ConflictType(str, Enum):
    NONE = "none"
    DIRECT_CONTRADICTION = "direct_contradiction"
    TEMPORAL_TRANSITION = "temporal_transition"
    PARTIAL_OBSERVATION = "partial_observation"
    STALE_DISAGREEMENT = "stale_disagreement"
    OSCILLATION = "oscillation"


@dataclass(slots=True)
class EvidenceRecord:
    evidence_id: str
    subject_type: SubjectType
    subject_id: str
    field_name: str
    asserted_value: Any
    source_type: str
    source_instance: str
    observed_at: datetime
    ingested_at: datetime
    confidence_hint: float = 0.8
    freshness_ttl_seconds: float = 30.0
    collector_latency_ms: Optional[float] = None
    source_health: SourceHealthState = SourceHealthState.UNKNOWN
    causal_event_id: Optional[str] = None
    topology_scope: Optional[str] = None
    site_scope: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def is_expired(self, now: datetime) -> bool:
        return (now - self.observed_at).total_seconds() > self.freshness_ttl_seconds


@dataclass(slots=True)
class ResolutionExplanation:
    summary: str
    winning_evidence_ids: list[str] = field(default_factory=list)
    rejected_evidence_ids: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ResolvedAssertion:
    subject_type: SubjectType
    subject_id: str
    field_name: str
    resolved_value: Any
    resolution_state: ResolutionState
    confidence: float
    conflict_type: ConflictType
    observed_at: datetime
    stale_after: Optional[datetime]
    last_authoritative_source: Optional[str]
    contributing_evidence_ids: list[str] = field(default_factory=list)
    rejected_evidence_ids: list[str] = field(default_factory=list)
    disputed: bool = False
    explanation: Optional[ResolutionExplanation] = None
