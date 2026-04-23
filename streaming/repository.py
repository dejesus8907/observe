"""Persistence helpers for the streaming subsystem."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from netobserv.streaming.db_models import (
    ResolvedAssertionModel,
    StreamingEventModel,
    StreamingSessionModel,
    TopologyDeltaModel,
)
from netobserv.streaming.conflict import ResolvedAssertion
from netobserv.streaming.events import ChangeEvent, ChangeKind, ChangeSeverity, EventSource
from netobserv.streaming.topology_patcher import TopologyDelta, EdgeDeltaKind
from netobserv.storage.database import RuntimeSessionLocal


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@contextmanager
def streaming_session_scope() -> Iterator[Session]:
    if RuntimeSessionLocal is None:
        raise RuntimeError("RuntimeSessionLocal is not configured")
    session = RuntimeSessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


class StreamingRepository:
    """Durable persistence adapter for streaming events, deltas, and session state.

    This repository intentionally uses the synchronous runtime SQLAlchemy engine
    because the streaming event bus and receivers are latency-sensitive and do
    not need the full async ORM stack already used by the scheduled workflow
    control plane.
    """

    async def persist_event(self, event: ChangeEvent) -> None:
        await asyncio.to_thread(self._persist_event_sync, event)

    def _persist_event_sync(self, event: ChangeEvent) -> None:
        with streaming_session_scope() as session:
            row = StreamingEventModel(
                event_id=event.event_id,
                kind=event.kind.value,
                severity=event.severity.value,
                source=event.source.value,
                device_id=event.device_id,
                device_hostname=event.device_hostname,
                interface_name=event.interface_name,
                detected_at=event.detected_at,
                received_at=event.received_at,
                previous_value=event.previous_value,
                current_value=event.current_value,
                field_path=event.field_path,
                neighbor_device_id=event.neighbor_device_id,
                neighbor_interface=event.neighbor_interface,
                vlan_id=event.vlan_id,
                vrf=event.vrf,
                prefix=event.prefix,
                mac_address=event.mac_address,
                confidence=event.confidence,
                is_confirmed=event.is_confirmed,
                dedup_key=event.dedup_key,
                topology_patch_applied=event.topology_patch_applied,
                topology_delta_id=event.topology_delta_id,
                human_summary=event.human_summary,
                raw_payload=event.raw_payload or {},
            )
            session.merge(row)

    async def persist_delta(self, delta: TopologyDelta) -> None:
        await asyncio.to_thread(self._persist_delta_sync, delta)

    def _persist_delta_sync(self, delta: TopologyDelta) -> None:
        with streaming_session_scope() as session:
            row = TopologyDeltaModel(
                delta_id=delta.delta_id,
                change_event_id=delta.change_event_id,
                change_kind=delta.change_kind.value,
                edge_id=delta.edge_id,
                source_node_id=delta.source_node_id,
                target_node_id=delta.target_node_id,
                source_interface=delta.source_interface,
                target_interface=delta.target_interface,
                edge_type=delta.edge_type,
                previous_state=delta.previous_state,
                new_state=delta.new_state,
                created_at=delta.created_at,
                human_summary=delta.human_summary,
            )
            session.merge(row)
            if delta.change_event_id:
                event_row = session.get(StreamingEventModel, delta.change_event_id)
                if event_row is not None:
                    event_row.topology_patch_applied = True
                    event_row.topology_delta_id = delta.delta_id

    async def upsert_session(
        self,
        *,
        device_id: str,
        session_type: str,
        state: str,
        device_hostname: str | None = None,
        connected_at: datetime | None = None,
        last_event_at: datetime | None = None,
        reconnect_attempts: int = 0,
        error_message: str | None = None,
    ) -> None:
        await asyncio.to_thread(
            self._upsert_session_sync,
            device_id,
            session_type,
            state,
            device_hostname,
            connected_at,
            last_event_at,
            reconnect_attempts,
            error_message,
        )

    def _upsert_session_sync(
        self,
        device_id: str,
        session_type: str,
        state: str,
        device_hostname: str | None,
        connected_at: datetime | None,
        last_event_at: datetime | None,
        reconnect_attempts: int,
        error_message: str | None,
    ) -> None:
        session_id = f"{session_type}:{device_id}"
        with streaming_session_scope() as session:
            row = session.get(StreamingSessionModel, session_id)
            if row is None:
                row = StreamingSessionModel(
                    session_id=session_id,
                    device_id=device_id,
                    device_hostname=device_hostname,
                    session_type=session_type,
                    state=state,
                    connected_at=connected_at,
                    last_event_at=last_event_at,
                    reconnect_attempts=reconnect_attempts,
                    error_message=error_message,
                )
                session.add(row)
            else:
                row.device_hostname = device_hostname or row.device_hostname
                row.state = state
                row.connected_at = connected_at or row.connected_at
                row.last_event_at = last_event_at or row.last_event_at
                row.reconnect_attempts = reconnect_attempts
                row.error_message = error_message
                row.updated_at = datetime.utcnow()

    def list_recent_events(
        self,
        *,
        limit: int = 100,
        kind: str | None = None,
        device_id: str | None = None,
        severity: str | None = None,
    ) -> list[dict[str, Any]]:
        with streaming_session_scope() as session:
            stmt = select(StreamingEventModel).order_by(desc(StreamingEventModel.detected_at)).limit(max(1, min(limit, 500)))
            if kind:
                stmt = stmt.where(StreamingEventModel.kind == kind)
            if device_id:
                stmt = stmt.where(StreamingEventModel.device_id == device_id)
            if severity:
                stmt = stmt.where(StreamingEventModel.severity == severity)
            rows = session.execute(stmt).scalars().all()
            return [
                {
                    "event_id": row.event_id,
                    "kind": row.kind,
                    "severity": row.severity,
                    "source": row.source,
                    "device_id": row.device_id,
                    "device_hostname": row.device_hostname,
                    "interface_name": row.interface_name,
                    "detected_at": row.detected_at.isoformat() if row.detected_at else None,
                    "received_at": row.received_at.isoformat() if row.received_at else None,
                    "human_summary": row.human_summary,
                    "neighbor_device_id": row.neighbor_device_id,
                    "current_value": row.current_value,
                    "previous_value": row.previous_value,
                    "topology_patch_applied": row.topology_patch_applied,
                    "topology_delta_id": row.topology_delta_id,
                }
                for row in rows
            ]

    def list_sessions(self, *, limit: int = 200) -> list[dict[str, Any]]:
        with streaming_session_scope() as session:
            stmt = select(StreamingSessionModel).order_by(desc(StreamingSessionModel.updated_at)).limit(max(1, min(limit, 500)))
            rows = session.execute(stmt).scalars().all()
            return [
                {
                    "session_id": row.session_id,
                    "device_id": row.device_id,
                    "device_hostname": row.device_hostname,
                    "session_type": row.session_type,
                    "state": row.state,
                    "connected_at": row.connected_at.isoformat() if row.connected_at else None,
                    "last_event_at": row.last_event_at.isoformat() if row.last_event_at else None,
                    "reconnect_attempts": row.reconnect_attempts,
                    "error_message": row.error_message,
                    "updated_at": row.updated_at.isoformat() if row.updated_at else None,
                }
                for row in rows
            ]

    def count_events(self) -> int:
        with streaming_session_scope() as session:
            return session.query(StreamingEventModel).count()

    async def persist_resolved_assertion(self, assertion: ResolvedAssertion) -> None:
        await asyncio.to_thread(self._persist_resolved_assertion_sync, assertion)

    def _persist_resolved_assertion_sync(self, assertion: ResolvedAssertion) -> None:
        with streaming_session_scope() as session:
            explanation_payload = {}
            if assertion.explanation is not None:
                explanation_payload = {
                    "summary": assertion.explanation.summary,
                    "winning_evidence_ids": assertion.explanation.winning_evidence_ids,
                    "rejected_evidence_ids": assertion.explanation.rejected_evidence_ids,
                    "notes": assertion.explanation.notes,
                }
            row = ResolvedAssertionModel(
                assertion_id=f"{assertion.subject_type.value}:{assertion.subject_id}:{assertion.field_name}:{assertion.observed_at.isoformat()}",
                subject_type=assertion.subject_type.value,
                subject_id=assertion.subject_id,
                field_name=assertion.field_name,
                resolved_value={"value": assertion.resolved_value},
                resolution_state=assertion.resolution_state.value,
                confidence=float(assertion.confidence),
                conflict_type=assertion.conflict_type.value,
                observed_at=assertion.observed_at,
                stale_after=assertion.stale_after,
                last_authoritative_source=assertion.last_authoritative_source,
                contributing_evidence_ids=list(assertion.contributing_evidence_ids),
                rejected_evidence_ids=list(assertion.rejected_evidence_ids),
                disputed=bool(assertion.disputed),
                explanation=explanation_payload,
            )
            session.merge(row)

    async def list_recent_resolved_assertions(self, limit: int = 100) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._list_recent_resolved_assertions_sync, max(1, min(limit, 1000)))

    def _list_recent_resolved_assertions_sync(self, limit: int) -> list[dict[str, Any]]:
        with streaming_session_scope() as session:
            stmt = (
                select(ResolvedAssertionModel)
                .order_by(desc(ResolvedAssertionModel.observed_at))
                .limit(limit)
            )
            rows = session.execute(stmt).scalars().all()
            return [self._row_to_assertion(row) for row in rows]

    async def get_latest_resolved_assertion(
        self,
        *,
        subject_id: str,
        field_name: str,
        subject_type: str | None = None,
    ) -> dict[str, Any] | None:
        return await asyncio.to_thread(
            self._get_latest_resolved_assertion_sync,
            subject_id,
            field_name,
            subject_type,
        )

    def _get_latest_resolved_assertion_sync(
        self,
        subject_id: str,
        field_name: str,
        subject_type: str | None,
    ) -> dict[str, Any] | None:
        with streaming_session_scope() as session:
            stmt = (
                select(ResolvedAssertionModel)
                .where(
                    ResolvedAssertionModel.subject_id == subject_id,
                    ResolvedAssertionModel.field_name == field_name,
                )
                .order_by(desc(ResolvedAssertionModel.observed_at))
                .limit(1)
            )
            if subject_type:
                stmt = stmt.where(ResolvedAssertionModel.subject_type == subject_type)
            row = session.execute(stmt).scalars().first()
            if row is None:
                return None
            return self._row_to_assertion(row)

    async def list_current_resolved_assertions(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._list_current_resolved_assertions_sync, limit)

    def _list_current_resolved_assertions_sync(self, limit: int | None) -> list[dict[str, Any]]:
        with streaming_session_scope() as session:
            stmt = select(ResolvedAssertionModel).order_by(desc(ResolvedAssertionModel.observed_at))
            rows = session.execute(stmt).scalars().all()
            seen_keys: set[tuple[str, str, str]] = set()
            current_rows: list[dict[str, Any]] = []
            for row in rows:
                key = (row.subject_type, row.subject_id, row.field_name)
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                current_rows.append(self._row_to_assertion(row))
                if limit is not None and len(current_rows) >= limit:
                    break
            return current_rows

    async def persist_evidence_record(self, evidence: Any) -> None:
        payload = {
            "evidence_id": evidence.evidence_id,
            "subject_type": evidence.subject_type.value,
            "subject_id": evidence.subject_id,
            "field_name": evidence.field_name,
            "asserted_value": evidence.asserted_value,
            "source_type": evidence.source_type,
            "source_instance": evidence.source_instance,
            "confidence_hint": evidence.confidence_hint,
            "source_health": evidence.source_health.value,
            "metadata": evidence.metadata,
        }
        synthetic = ChangeEvent(
            event_id=f"evidence:{evidence.evidence_id}",
            kind=ChangeKind.UNKNOWN,
            severity=ChangeSeverity.INFO,
            source=EventSource.SSH_POLL_DIFF if evidence.source_type == "ssh_diff" else EventSource.SYSLOG,
            device_id=evidence.subject_id,
            detected_at=evidence.observed_at,
            received_at=evidence.ingested_at,
            current_value=evidence.asserted_value,
            field_path=f"{evidence.subject_type.value}.{evidence.field_name}",
            raw_payload={"evidence_record": payload},
            human_summary=f"Evidence recorded for {evidence.subject_type.value}.{evidence.field_name}",
        )
        await self.persist_event(synthetic)

    async def persist_delta_with_lineage(
        self,
        delta: Any,
        *,
        source_assertion_id: str | None = None,
        cluster_id: str | None = None,
    ) -> None:
        await asyncio.to_thread(self._persist_delta_with_lineage_sync, delta, source_assertion_id, cluster_id)

    def _persist_delta_with_lineage_sync(
        self,
        delta: Any,
        source_assertion_id: str | None = None,
        cluster_id: str | None = None,
    ) -> None:
        if isinstance(delta, TopologyDelta):
            base = delta
        else:
            payload = dict(getattr(delta, "__dict__", {}))
            base = TopologyDelta(
                delta_id=payload.get("delta_id", "delta:unknown"),
                change_event_id=payload.get("change_event_id", "event:unknown"),
                change_kind=EdgeDeltaKind(payload.get("change_kind", "added")),
                edge_id=payload.get("edge_id", "edge:unknown"),
                source_node_id=payload.get("source_node_id", ""),
                target_node_id=payload.get("target_node_id", ""),
                source_interface=payload.get("source_interface"),
                target_interface=payload.get("target_interface"),
                edge_type=payload.get("edge_type", "unknown"),
                previous_state=payload.get("previous_state", {}) or {},
                new_state=payload.get("new_state", {}) or {},
                created_at=payload.get("created_at") or _utc_now(),
                human_summary=payload.get("human_summary", ""),
            )
        lineage = {
            "source_assertion_id": source_assertion_id,
            "cluster_id": cluster_id,
        }
        patched = base.__class__(
            delta_id=base.delta_id,
            change_event_id=base.change_event_id,
            change_kind=base.change_kind,
            edge_id=base.edge_id,
            source_node_id=base.source_node_id,
            target_node_id=base.target_node_id,
            source_interface=base.source_interface,
            target_interface=base.target_interface,
            edge_type=base.edge_type,
            previous_state=dict(base.previous_state),
            new_state={**dict(base.new_state), "lineage": lineage},
            created_at=base.created_at,
            human_summary=base.human_summary,
        )
        self._persist_delta_sync(patched)

    async def list_recent_evidence(
        self,
        *,
        subject_type: str | None = None,
        subject_id: str | None = None,
        field_name: str | None = None,
        source_type: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        return await asyncio.to_thread(
            self._list_recent_evidence_sync,
            subject_type,
            subject_id,
            field_name,
            source_type,
            max(1, min(limit, 1000)),
        )

    def _list_recent_evidence_sync(
        self,
        subject_type: str | None,
        subject_id: str | None,
        field_name: str | None,
        source_type: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        with streaming_session_scope() as session:
            stmt = (
                select(StreamingEventModel)
                .order_by(desc(StreamingEventModel.received_at))
                .limit(limit)
            )
            rows = session.execute(stmt).scalars().all()
            filtered = []
            for row in rows:
                payload = row.raw_payload or {}
                evidence = payload.get("evidence_record")
                if not evidence:
                    continue
                if subject_type and evidence.get("subject_type") != subject_type:
                    continue
                if subject_id and evidence.get("subject_id") != subject_id:
                    continue
                if field_name and evidence.get("field_name") != field_name:
                    continue
                if source_type and evidence.get("source_type") != source_type:
                    continue
                filtered.append(
                    {
                        "event_id": row.event_id,
                        "received_at": row.received_at.isoformat() if row.received_at else None,
                        "detected_at": row.detected_at.isoformat() if row.detected_at else None,
                        "evidence": evidence,
                    }
                )
            return filtered

    async def list_disputes(
        self,
        *,
        subject_type: str | None = None,
        field_name: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        rows = await self.list_recent_resolved_assertions(limit=max(1, min(limit, 1000)) * 3)
        filtered = []
        for row in rows:
            if row.get("resolution_state") != "disputed":
                continue
            if subject_type and row.get("subject_type") != subject_type:
                continue
            if field_name and row.get("field_name") != field_name:
                continue
            filtered.append(row)
            if len(filtered) >= limit:
                break
        return filtered

    async def list_subject_history(
        self,
        *,
        subject_id: str,
        subject_type: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        evidence = await self.list_recent_evidence(subject_type=subject_type, subject_id=subject_id, limit=limit)
        assertions = await self.list_recent_resolved_assertions(limit=max(1, min(limit, 1000)) * 3)
        assertions = [
            row
            for row in assertions
            if row.get("subject_id") == subject_id and (subject_type is None or row.get("subject_type") == subject_type)
        ][:limit]
        return {
            "subject_id": subject_id,
            "subject_type": subject_type,
            "evidence": evidence,
            "resolved_assertions": assertions,
        }

    async def get_current_topology_with_truth(self, limit: int = 200) -> dict[str, Any]:
        assertions = await self.list_current_resolved_assertions(limit=None)
        edge_assertions = [a for a in assertions if a.get("subject_type") in {"edge", "interface", "bgp_session", "bgp"}]
        nodes = {}
        edges = []
        for row in edge_assertions:
            subj_type = row.get("subject_type")
            subject_id = row.get("subject_id")
            state = row.get("resolution_state")
            confidence = row.get("confidence")
            cluster_id = row.get("cluster_id")
            if subj_type == "edge":
                edges.append(
                    {
                        "subject_id": subject_id,
                        "state": state,
                        "confidence": confidence,
                        "disputed": bool(row.get("disputed")),
                        "cluster_id": cluster_id,
                        "last_authoritative_source": row.get("last_authoritative_source"),
                        "resolved_value": row.get("resolved_value"),
                    }
                )
            else:
                nodes[subject_id] = {
                    "subject_id": subject_id,
                    "subject_type": subj_type,
                    "state": state,
                    "confidence": confidence,
                    "disputed": bool(row.get("disputed")),
                    "cluster_id": cluster_id,
                    "last_authoritative_source": row.get("last_authoritative_source"),
                    "resolved_value": row.get("resolved_value"),
                }
        capped = max(1, min(limit, 5000))
        return {"nodes": list(nodes.values())[:capped], "edges": edges[:capped]}

    @staticmethod
    def _row_to_assertion(row: ResolvedAssertionModel) -> dict[str, Any]:
        return {
            "assertion_id": row.assertion_id,
            "subject_type": row.subject_type,
            "subject_id": row.subject_id,
            "field_name": row.field_name,
            "resolved_value": row.resolved_value,
            "resolution_state": row.resolution_state,
            "confidence": row.confidence,
            "conflict_type": row.conflict_type,
            "observed_at": row.observed_at.isoformat(),
            "stale_after": row.stale_after.isoformat() if row.stale_after else None,
            "last_authoritative_source": row.last_authoritative_source,
            "cluster_id": row.cluster_id,
            "disputed": row.disputed,
            "explanation": row.explanation,
        }
