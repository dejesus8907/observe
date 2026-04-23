from __future__ import annotations
import asyncio
from datetime import datetime, timezone
from typing import Any
from sqlalchemy import desc, select
from netobserv.storage.database import get_runtime_session
from netobserv.streaming.correlation.models import CorrelationCluster, CorrelationDecision, CorrelationRole, CorrelationState
from netobserv.streaming.correlation_db_models import CorrelationClusterModel, CorrelationMembershipModel, CorrelationLinkModel

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)

class CorrelationRepository:
    async def persist_cluster_state(self, *, cluster: CorrelationCluster, decision: CorrelationDecision, assertion_id: str | None = None) -> None:
        await asyncio.to_thread(self._persist_cluster_state_sync, cluster, decision, assertion_id)

    def _persist_cluster_state_sync(self, cluster: CorrelationCluster, decision: CorrelationDecision, assertion_id: str | None = None) -> None:
        session = get_runtime_session()
        try:
            root_event_id, root_confidence = self._score_root_candidate(cluster)
            session.merge(CorrelationClusterModel(
                cluster_id=cluster.cluster_id,
                opened_at=cluster.opened_at,
                updated_at=cluster.updated_at,
                closed_at=None if cluster.state != CorrelationState.CLOSED else cluster.updated_at,
                state=cluster.state.value,
                root_event_id=root_event_id,
                root_assertion_id=assertion_id if decision.role == CorrelationRole.ROOT else None,
                root_confidence=root_confidence,
                cluster_confidence=self._cluster_confidence(cluster),
                summary_json=self._cluster_summary(cluster),
                device_ids_json=list(cluster.device_ids),
                subject_keys_json=list(cluster.subject_keys),
                kind_set_json=list(cluster.kinds),
                metadata_json=dict(cluster.metadata),
            ))
            session.merge(CorrelationMembershipModel(
                membership_id=f"{cluster.cluster_id}:{decision.event_id}",
                cluster_id=cluster.cluster_id,
                event_id=decision.event_id,
                assertion_id=assertion_id,
                role=decision.role.value,
                joined_at=cluster.updated_at,
                confidence=float(decision.confidence),
                explanation_json={"explanation": decision.explanation, "linked_event_ids": list(decision.linked_event_ids), "is_new_cluster": bool(decision.is_new_cluster)},
            ))
            for link in cluster.links:
                session.merge(CorrelationLinkModel(
                    link_id=f"{cluster.cluster_id}:{link.source_event_id}:{link.target_event_id}:{link.correlation_type.value}",
                    cluster_id=cluster.cluster_id,
                    source_event_id=link.source_event_id,
                    target_event_id=link.target_event_id,
                    source_assertion_id=None,
                    target_assertion_id=None,
                    correlation_type=link.correlation_type.value,
                    confidence=float(link.confidence),
                    reason=link.reason,
                    created_at=cluster.updated_at,
                ))
            session.commit()
        finally:
            session.close()

    async def list_clusters(self, *, state: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._list_clusters_sync, state, limit)

    def _list_clusters_sync(self, state: str | None, limit: int) -> list[dict[str, Any]]:
        session = get_runtime_session()
        try:
            stmt = select(CorrelationClusterModel)
            if state:
                stmt = stmt.where(CorrelationClusterModel.state == state)
            stmt = stmt.order_by(desc(CorrelationClusterModel.updated_at)).limit(limit)
            rows = session.execute(stmt).scalars().all()
            return [{"cluster_id": r.cluster_id, "opened_at": r.opened_at.isoformat(), "updated_at": r.updated_at.isoformat(), "closed_at": r.closed_at.isoformat() if r.closed_at else None, "state": r.state, "root_event_id": r.root_event_id, "root_assertion_id": r.root_assertion_id, "root_confidence": r.root_confidence, "cluster_confidence": r.cluster_confidence, "summary": r.summary_json, "device_ids": r.device_ids_json, "subject_keys": r.subject_keys_json, "kind_set": r.kind_set_json, "metadata": r.metadata_json} for r in rows]
        finally:
            session.close()

    async def get_cluster_history(self, cluster_id: str) -> dict[str, Any]:
        return await asyncio.to_thread(self._get_cluster_history_sync, cluster_id)

    def _get_cluster_history_sync(self, cluster_id: str) -> dict[str, Any]:
        session = get_runtime_session()
        try:
            cluster = session.get(CorrelationClusterModel, cluster_id)
            if cluster is None:
                return {"cluster": None, "memberships": [], "links": []}
            members = session.execute(select(CorrelationMembershipModel).where(CorrelationMembershipModel.cluster_id == cluster_id).order_by(CorrelationMembershipModel.joined_at.asc())).scalars().all()
            links = session.execute(select(CorrelationLinkModel).where(CorrelationLinkModel.cluster_id == cluster_id).order_by(CorrelationLinkModel.created_at.asc())).scalars().all()
            return {
                "cluster": {"cluster_id": cluster.cluster_id, "opened_at": cluster.opened_at.isoformat(), "updated_at": cluster.updated_at.isoformat(), "state": cluster.state, "root_event_id": cluster.root_event_id, "root_assertion_id": cluster.root_assertion_id, "root_confidence": cluster.root_confidence, "cluster_confidence": cluster.cluster_confidence, "summary": cluster.summary_json, "device_ids": cluster.device_ids_json, "subject_keys": cluster.subject_keys_json, "kind_set": cluster.kind_set_json, "metadata": cluster.metadata_json},
                "memberships": [{"membership_id": m.membership_id, "cluster_id": m.cluster_id, "event_id": m.event_id, "assertion_id": m.assertion_id, "role": m.role, "joined_at": m.joined_at.isoformat(), "confidence": m.confidence, "explanation": m.explanation_json} for m in members],
                "links": [{"link_id": l.link_id, "cluster_id": l.cluster_id, "source_event_id": l.source_event_id, "target_event_id": l.target_event_id, "correlation_type": l.correlation_type, "confidence": l.confidence, "reason": l.reason, "created_at": l.created_at.isoformat()} for l in links],
            }
        finally:
            session.close()

    async def list_disputed_clusters(self, limit: int = 100) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._list_disputed_clusters_sync, limit)

    def _list_disputed_clusters_sync(self, limit: int) -> list[dict[str, Any]]:
        session = get_runtime_session()
        try:
            rows = session.execute(select(CorrelationClusterModel).order_by(desc(CorrelationClusterModel.updated_at)).limit(limit)).scalars().all()
            rows = [r for r in rows if bool((r.summary_json or {}).get("contains_disputed"))]
            return [{"cluster_id": r.cluster_id, "updated_at": r.updated_at.isoformat(), "summary": r.summary_json, "cluster_confidence": r.cluster_confidence} for r in rows]
        finally:
            session.close()

    def _score_root_candidate(self, cluster: CorrelationCluster) -> tuple[str | None, float]:
        if not cluster.event_ids:
            return None, 0.0
        scores = {eid: 0.0 for eid in cluster.event_ids}
        for idx, eid in enumerate(cluster.event_ids):
            scores[eid] += max(0.0, 0.25 - (idx * 0.03))
        for link in cluster.links:
            if link.source_event_id in scores and link.correlation_type.value == "cause_effect":
                scores[link.source_event_id] += 0.35
                if link.target_event_id in scores:
                    scores[link.target_event_id] -= 0.10
            if link.source_event_id in scores and link.correlation_type.value == "same_subject":
                scores[link.source_event_id] += 0.08
        best = max(scores.items(), key=lambda kv: kv[1])[0]
        return best, min(0.99, max(0.10, scores[best]))

    def _cluster_confidence(self, cluster: CorrelationCluster) -> float:
        if not cluster.links:
            return 0.70 if len(cluster.event_ids) > 1 else 0.55
        avg = sum(l.confidence for l in cluster.links) / len(cluster.links)
        return min(0.99, max(0.35, avg))

    def _cluster_summary(self, cluster: CorrelationCluster) -> dict[str, Any]:
        disputed = cluster.metadata.get("contains_disputed", False)
        return {"event_count": len(cluster.event_ids), "link_count": len(cluster.links), "device_count": len(cluster.device_ids), "subject_count": len(cluster.subject_keys), "contains_disputed": bool(disputed), "dominant_kinds": list(cluster.kinds[:5])}
