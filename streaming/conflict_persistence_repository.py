from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from netobserv.streaming.conflict.models import (
    ConflictType,
    EvidenceRecord,
    ResolvedAssertion,
    ResolutionState,
    SourceHealthState,
    SubjectType,
)
from netobserv.streaming.repository import StreamingRepository
from netobserv.streaming.topology_patcher import EdgeDeltaKind, TopologyDelta


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_datetime(value: Any, *, fallback: datetime | None = None) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            pass
    return fallback or _utc_now()


def _to_subject_type(value: Any) -> SubjectType:
    text = str(value or SubjectType.DEVICE.value)
    try:
        return SubjectType(text)
    except ValueError:
        return SubjectType.DEVICE


def _to_resolution_state(value: Any) -> ResolutionState:
    text = str(value or ResolutionState.INSUFFICIENT_EVIDENCE.value)
    try:
        return ResolutionState(text)
    except ValueError:
        return ResolutionState.INSUFFICIENT_EVIDENCE


def _to_conflict_type(value: Any) -> ConflictType:
    text = str(value or ConflictType.NONE.value)
    try:
        return ConflictType(text)
    except ValueError:
        return ConflictType.NONE


def _to_source_health(value: Any) -> SourceHealthState:
    text = str(value or SourceHealthState.UNKNOWN.value)
    try:
        return SourceHealthState(text)
    except ValueError:
        return SourceHealthState.UNKNOWN


class EvidenceRepository:
    async def append_evidence(self, evidence: Dict[str, Any]) -> None:
        ...

    async def list_evidence_for_subject(self, subject_id: str) -> List[Dict[str, Any]]:
        ...


class ResolvedAssertionRepository:
    async def append_resolved_assertion(self, assertion: Dict[str, Any]) -> None:
        ...

    async def get_current_assertion(self, subject_id: str, field_name: str) -> Optional[Dict[str, Any]]:
        ...


class TopologyRepository:
    async def append_delta(self, delta: Dict[str, Any]) -> None:
        ...

    async def get_current_topology(self) -> Dict[str, Any]:
        ...


class ConflictPersistenceRepository(
    EvidenceRepository,
    ResolvedAssertionRepository,
    TopologyRepository,
):
    """Concrete persistence adapter for conflict/query model surfaces."""

    def __init__(self, repository: StreamingRepository | None = None) -> None:
        self._repository = repository or StreamingRepository()

    async def append_evidence(self, evidence: Dict[str, Any]) -> None:
        subject_type = _to_subject_type(evidence.get("subject_type"))
        source_health = _to_source_health(evidence.get("source_health"))
        record = EvidenceRecord(
            evidence_id=str(evidence.get("evidence_id") or f"evidence:{_utc_now().timestamp()}"),
            subject_type=subject_type,
            subject_id=str(evidence.get("subject_id") or ""),
            field_name=str(evidence.get("field_name") or ""),
            asserted_value=evidence.get("asserted_value"),
            source_type=str(evidence.get("source_type") or "unknown"),
            source_instance=str(evidence.get("source_instance") or "unknown"),
            observed_at=_to_datetime(evidence.get("observed_at")),
            ingested_at=_to_datetime(evidence.get("ingested_at")),
            confidence_hint=float(evidence.get("confidence_hint", 0.8)),
            freshness_ttl_seconds=float(evidence.get("freshness_ttl_seconds", 30.0)),
            source_health=source_health,
            metadata=dict(evidence.get("metadata") or {}),
        )
        await self._repository.persist_evidence_record(record)

    async def list_evidence_for_subject(self, subject_id: str) -> List[Dict[str, Any]]:
        rows = await self._repository.list_recent_evidence(subject_id=subject_id, limit=500)
        return rows

    async def append_resolved_assertion(self, assertion: Dict[str, Any]) -> None:
        subject_type = _to_subject_type(assertion.get("subject_type"))
        resolved = ResolvedAssertion(
            subject_type=subject_type,
            subject_id=str(assertion.get("subject_id") or ""),
            field_name=str(assertion.get("field_name") or ""),
            resolved_value=assertion.get("resolved_value"),
            resolution_state=_to_resolution_state(assertion.get("resolution_state")),
            confidence=float(assertion.get("confidence", 0.0)),
            conflict_type=_to_conflict_type(assertion.get("conflict_type")),
            observed_at=_to_datetime(assertion.get("observed_at")),
            stale_after=_to_datetime(assertion.get("stale_after"), fallback=None) if assertion.get("stale_after") else None,
            last_authoritative_source=assertion.get("last_authoritative_source"),
            contributing_evidence_ids=list(assertion.get("contributing_evidence_ids") or []),
            rejected_evidence_ids=list(assertion.get("rejected_evidence_ids") or []),
            disputed=bool(assertion.get("disputed", False)),
            explanation=None,
        )
        await self._repository.persist_resolved_assertion(resolved)

    async def get_current_assertion(self, subject_id: str, field_name: str) -> Optional[Dict[str, Any]]:
        return await self._repository.get_latest_resolved_assertion(
            subject_id=subject_id,
            field_name=field_name,
        )

    async def append_delta(self, delta: Dict[str, Any]) -> None:
        change_kind = str(delta.get("change_kind") or EdgeDeltaKind.ADDED.value)
        topo_delta = TopologyDelta(
            delta_id=str(delta.get("delta_id") or f"delta:{_utc_now().timestamp()}"),
            change_event_id=str(delta.get("change_event_id") or "event:unknown"),
            change_kind=EdgeDeltaKind(change_kind),
            edge_id=str(delta.get("edge_id") or "edge:unknown"),
            source_node_id=str(delta.get("source_node_id") or ""),
            target_node_id=str(delta.get("target_node_id") or ""),
            source_interface=delta.get("source_interface"),
            target_interface=delta.get("target_interface"),
            edge_type=str(delta.get("edge_type") or "physical"),
            previous_state=dict(delta.get("previous_state") or {}),
            new_state=dict(delta.get("new_state") or {}),
            created_at=_to_datetime(delta.get("created_at")),
            human_summary=str(delta.get("human_summary") or ""),
        )
        await self._repository.persist_delta(topo_delta)

    async def get_current_topology(self) -> Dict[str, Any]:
        return await self._repository.get_current_topology_with_truth(limit=5000)
