from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

from .models import (
    ConflictType,
    EvidenceRecord,
    ResolvedAssertion,
    ResolutionExplanation,
    ResolutionState,
)
from .policy import ConflictResolutionPolicy
from .scoring import EvidenceScorer
from .stabilizer import ConflictStabilizer
from .trust import DefaultTrustModel, TrustContext
from .policy_profiles import derivative_bias_from_interface_failure, get_confirmed_threshold, get_dispute_margin, get_likely_threshold


class ConflictResolver:
    def __init__(
        self,
        trust_model: DefaultTrustModel | None = None,
        policy: ConflictResolutionPolicy | None = None,
        scorer: EvidenceScorer | None = None,
        stabilizer: ConflictStabilizer | None = None,
    ) -> None:
        self.policy = policy or ConflictResolutionPolicy()
        self.trust_model = trust_model or DefaultTrustModel()
        self.scorer = scorer or EvidenceScorer(self.trust_model, self.policy)
        self.stabilizer = stabilizer or ConflictStabilizer(
            window_seconds=self.policy.oscillation_window_seconds,
            min_transitions=self.policy.oscillation_min_transitions,
        )

    def resolve(
        self,
        evidence_records: list[EvidenceRecord],
        *,
        now: datetime | None = None,
    ) -> ResolvedAssertion:
        if not evidence_records:
            raise ValueError("resolve() requires at least one EvidenceRecord")

        now = now or max(r.ingested_at for r in evidence_records)
        first = evidence_records[0]
        self._validate_homogeneous_target(evidence_records)

        valid_records = [r for r in evidence_records if not r.is_expired(now) and r.source_health.value != "unhealthy"]
        if not valid_records:
            return ResolvedAssertion(
                subject_type=first.subject_type,
                subject_id=first.subject_id,
                field_name=first.field_name,
                resolved_value=None,
                resolution_state=ResolutionState.STALE,
                confidence=0.0,
                conflict_type=ConflictType.STALE_DISAGREEMENT,
                observed_at=now,
                stale_after=now,
                last_authoritative_source=None,
                contributing_evidence_ids=[],
                rejected_evidence_ids=[r.evidence_id for r in evidence_records],
                disputed=True,
                explanation=ResolutionExplanation(
                    summary="All evidence was expired or unhealthy.",
                    rejected_evidence_ids=[r.evidence_id for r in evidence_records],
                ),
            )

        context = TrustContext(subject_type=first.subject_type, field_name=first.field_name)
        grouped = defaultdict(list)
        for record in valid_records:
            grouped[self._value_key(record.asserted_value)].append(record)

        total_by_group = {}
        for value_key, records in grouped.items():
            scored = [self.scorer.score(r, context=context, corroborating_count=len(records), now=now) for r in records]
            total_by_group[value_key] = sum(s.total_score for s in scored)

        sorted_groups = sorted(total_by_group.items(), key=lambda item: item[1], reverse=True)
        winning_key, winning_score = sorted_groups[0]
        runner_up_score = sorted_groups[1][1] if len(sorted_groups) > 1 else 0.0
        margin = winning_score - runner_up_score

        winning_records = grouped[winning_key]
        winner_latest = max(r.observed_at for r in winning_records)

        conflict_type = self._classify_conflict(valid_records, grouped, now=now)
        stabilization = self.stabilizer.evaluate(valid_records, now=now)
        if stabilization.churn_detected:
            # Apply penalty to ALL groups: full to current winner, half to others.
            # Then do a full global re-sort to find the true post-penalty winner.
            penalized_groups: list[tuple[str, float]] = []
            for gkey, gscore in sorted_groups:
                if gkey == winning_key:
                    penalized_groups.append((gkey, max(0.0, gscore - stabilization.stability_penalty)))
                else:
                    penalized_groups.append((gkey, max(0.0, gscore - stabilization.stability_penalty / 2.0)))

            penalized_groups.sort(key=lambda item: item[1], reverse=True)
            sorted_groups = penalized_groups
            winning_key, winning_score = sorted_groups[0]
            runner_up_score = sorted_groups[1][1] if len(sorted_groups) > 1 else 0.0
            winning_records = grouped[winning_key]
            winner_latest = max(r.observed_at for r in winning_records)

        field_key = f"{first.subject_type.value}.{first.field_name}"
        if derivative_bias_from_interface_failure(field_key):
            for rec in valid_records:
                if isinstance(rec.metadata, dict) and rec.metadata.get("upstream_interface_failure"):
                    winning_score = min(winning_score, 0.84)
        resolution_state = self._resolution_state(
            field_key=field_key,
            winning_score=winning_score,
            runner_up_score=runner_up_score,
            margin=winning_score - runner_up_score,
            conflict_type=conflict_type,
        )

        contributing_ids = [r.evidence_id for r in winning_records]
        rejected_ids = [r.evidence_id for r in valid_records if r.evidence_id not in contributing_ids]
        last_authoritative_source = max(winning_records, key=lambda r: r.observed_at).source_type
        stale_after = winner_latest + timedelta(seconds=max(r.freshness_ttl_seconds for r in winning_records))

        return ResolvedAssertion(
            subject_type=first.subject_type,
            subject_id=first.subject_id,
            field_name=first.field_name,
            resolved_value=winning_records[0].asserted_value,
            resolution_state=resolution_state,
            confidence=self._confidence_from_state(winning_score, resolution_state),
            conflict_type=conflict_type,
            observed_at=winner_latest,
            stale_after=stale_after,
            last_authoritative_source=last_authoritative_source,
            contributing_evidence_ids=contributing_ids,
            rejected_evidence_ids=rejected_ids,
            disputed=resolution_state == ResolutionState.DISPUTED,
            explanation=ResolutionExplanation(
                summary=self._explanation_summary(resolution_state, conflict_type, winning_records[0].asserted_value, winning_score, runner_up_score),
                winning_evidence_ids=contributing_ids,
                rejected_evidence_ids=rejected_ids,
                notes=[
                    f"winning_score={winning_score:.3f}",
                    f"runner_up_score={runner_up_score:.3f}",
                    f"margin={margin:.3f}",
                    f"conflict_type={conflict_type.value}",
                    f"churn_detected={stabilization.churn_detected}",
                    f"stability_penalty={stabilization.stability_penalty:.3f}",
                ],
            ),
        )

    def _validate_homogeneous_target(self, records: list[EvidenceRecord]) -> None:
        first = (records[0].subject_type, records[0].subject_id, records[0].field_name)
        for r in records[1:]:
            if (r.subject_type, r.subject_id, r.field_name) != first:
                raise ValueError("All evidence records must target the same subject_type, subject_id, and field_name")

    def _value_key(self, value: Any) -> str:
        return repr(value)

    def _classify_conflict(self, records, grouped, *, now: datetime) -> ConflictType:
        if len(grouped) == 1:
            return ConflictType.NONE
        if self._detect_oscillation(records, now=now):
            return ConflictType.OSCILLATION
        latest_by_group = {k: max(r.observed_at for r in v) for k, v in grouped.items()}
        latest_times = sorted(latest_by_group.values())
        if latest_times and (latest_times[-1] - latest_times[0]).total_seconds() > 5:
            return ConflictType.TEMPORAL_TRANSITION
        fresh_groups = 0
        for _, records_for_value in grouped.items():
            freshest = max(records_for_value, key=lambda r: r.observed_at)
            if not freshest.is_expired(now):
                fresh_groups += 1
        if fresh_groups > 1:
            return ConflictType.DIRECT_CONTRADICTION
        return ConflictType.STALE_DISAGREEMENT

    def _detect_oscillation(self, records, *, now: datetime) -> bool:
        ordered = sorted(records, key=lambda r: r.observed_at)
        cutoff = now - timedelta(seconds=self.policy.oscillation_window_seconds)
        recent = [r for r in ordered if r.observed_at >= cutoff]
        if len(recent) < self.policy.oscillation_min_transitions:
            return False
        transitions = 0
        last_key = None
        for r in recent:
            key = self._value_key(r.asserted_value)
            if last_key is not None and key != last_key:
                transitions += 1
            last_key = key
        return transitions >= self.policy.oscillation_min_transitions - 1

    def _resolution_state(self, *, field_key: str, winning_score: float, runner_up_score: float, margin: float, conflict_type: ConflictType) -> ResolutionState:
        dispute_margin = get_dispute_margin(field_key, self.policy.dispute_margin)
        confirmed_threshold = get_confirmed_threshold(field_key, self.policy.confirmed_threshold)
        likely_threshold = get_likely_threshold(field_key, self.policy.likely_threshold)
        if winning_score <= 0:
            return ResolutionState.INSUFFICIENT_EVIDENCE
        if conflict_type in {ConflictType.DIRECT_CONTRADICTION, ConflictType.OSCILLATION} and margin < dispute_margin:
            return ResolutionState.DISPUTED
        if winning_score >= confirmed_threshold and margin >= dispute_margin:
            return ResolutionState.CONFIRMED
        if winning_score >= likely_threshold:
            return ResolutionState.LIKELY
        if runner_up_score > 0:
            return ResolutionState.DISPUTED
        return ResolutionState.INSUFFICIENT_EVIDENCE

    def _confidence_from_state(self, winning_score: float, state: ResolutionState) -> float:
        if state == ResolutionState.CONFIRMED:
            return min(0.99, max(0.90, winning_score))
        if state == ResolutionState.LIKELY:
            return min(0.89, max(0.65, winning_score))
        if state == ResolutionState.DISPUTED:
            return min(0.64, max(0.35, winning_score))
        if state == ResolutionState.STALE:
            return 0.10
        return max(0.0, min(0.34, winning_score))

    def _explanation_summary(self, state: ResolutionState, conflict_type: ConflictType, winning_value: Any, winning_score: float, runner_up_score: float) -> str:
        return (
            f"Resolved value {winning_value!r} as {state.value} "
            f"under {conflict_type.value}; winning score {winning_score:.3f}, "
            f"runner-up {runner_up_score:.3f}."
        )
