from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from math import exp

from .models import EvidenceRecord
from .policy import ConflictResolutionPolicy
from .trust import DefaultTrustModel, TrustContext
from .policy_profiles import get_ttl_seconds, hard_delete_requires_direct_evidence


@dataclass(slots=True)
class ScoredEvidence:
    evidence: EvidenceRecord
    source_score: float
    freshness_weight: float
    corroboration_weight: float
    total_score: float


class EvidenceScorer:
    def __init__(
        self,
        trust_model: DefaultTrustModel | None = None,
        policy: ConflictResolutionPolicy | None = None,
    ) -> None:
        self.trust_model = trust_model or DefaultTrustModel()
        self.policy = policy or ConflictResolutionPolicy()

    def freshness_weight(self, evidence: EvidenceRecord, now: datetime) -> float:
        field_key = f"{evidence.subject_type.value}.{evidence.field_name}"
        policy_ttl = get_ttl_seconds(field_key, evidence.freshness_ttl_seconds)
        ttl = max(policy_ttl, 1.0)
        age = max((now - evidence.observed_at).total_seconds(), 0.0)
        if age >= ttl:
            return self.policy.freshness_decay_floor
        normalized = age / ttl
        return max(self.policy.freshness_decay_floor, exp(-2.2 * normalized))

    def score(
        self,
        evidence: EvidenceRecord,
        *,
        context: TrustContext,
        corroborating_count: int,
        now: datetime,
    ) -> ScoredEvidence:
        source_score = self.trust_model.source_score(evidence, context)
        freshness = self.freshness_weight(evidence, now)
        corroboration = 1.0 + max(0, corroborating_count - 1) * self.policy.corroboration_bonus
        total = min(
            self.policy.max_confidence,
            max(0.0, source_score * freshness * corroboration * max(0.0, min(1.0, evidence.confidence_hint))),
        )
        field_key = f"{evidence.subject_type.value}.{evidence.field_name}"
        if hard_delete_requires_direct_evidence(field_key) and evidence.asserted_value is False and evidence.source_type not in {"gnmi_stream", "netconf_notification"}:
            total = min(total, 0.54)
        return ScoredEvidence(
            evidence=evidence,
            source_score=source_score,
            freshness_weight=freshness,
            corroboration_weight=corroboration,
            total_score=total,
        )
