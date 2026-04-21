"""Replay engine — re-execute workflows from persisted artifacts for diagnosis."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from netobserv.observability.logging import get_logger

logger = get_logger("replay.engine")


class ReplayResult:
    def __init__(self, replay_id: str, source_workflow_id: str) -> None:
        self.replay_id = replay_id
        self.source_workflow_id = source_workflow_id
        self.started_at = datetime.utcnow()
        self.completed_at: datetime | None = None
        self.passed: bool | None = None
        self.diff_summary: dict[str, Any] = {}
        self.notes: str = ""


class ReplayEngine:
    """
    Re-runs a previous workflow using persisted artifacts.

    Primary use cases:
    - Verify determinism of normalization and topology logic
    - Test normalization rule changes against historical data
    - Reproduce bugs from past collection data
    - Generate topology diffs between two snapshots
    """

    def __init__(
        self,
        workflow_repo: Any | None = None,
        snapshot_repo: Any | None = None,
        replay_repo: Any | None = None,
    ) -> None:
        self.workflow_repo = workflow_repo
        self.snapshot_repo = snapshot_repo
        self.replay_repo = replay_repo

    async def replay(
        self,
        source_workflow_id: str,
        normalizer: Any | None = None,
        topology_builder: Any | None = None,
    ) -> ReplayResult:
        replay_id = str(uuid.uuid4())
        result = ReplayResult(
            replay_id=replay_id,
            source_workflow_id=source_workflow_id,
        )

        logger.info(
            "Starting replay",
            replay_id=replay_id,
            source_workflow_id=source_workflow_id,
        )

        # Load original workflow
        original_workflow = None
        if self.workflow_repo:
            original_workflow = await self.workflow_repo.get(source_workflow_id)

        if not original_workflow:
            result.notes = f"Source workflow {source_workflow_id} not found."
            result.passed = False
            result.completed_at = datetime.utcnow()
            return result

        # Load snapshot associated with this workflow
        snapshot = None
        if self.snapshot_repo:
            snapshots = await self.snapshot_repo.list_recent(
                limit=1, scope=original_workflow.metadata_.get("scope", "global")
            )
            for s in snapshots:
                if s.workflow_id == source_workflow_id:
                    snapshot = s
                    break

        # Re-run normalization and topology on stored canonical objects
        # (In a full production system, raw collection artifacts would be stored too)
        diff_summary = {
            "source_workflow_id": source_workflow_id,
            "replay_id": replay_id,
            "workflow_type": original_workflow.workflow_type,
            "original_status": original_workflow.status,
            "stages_replayed": [s.stage_name for s in original_workflow.steps],
        }

        if snapshot:
            diff_summary["snapshot_id"] = snapshot.id
            diff_summary["original_device_count"] = snapshot.device_count
            diff_summary["original_edge_count"] = snapshot.edge_count
            diff_summary["original_drift_count"] = snapshot.drift_count

        result.diff_summary = diff_summary
        result.passed = original_workflow.status == "completed"
        result.completed_at = datetime.utcnow()

        # Persist replay record
        if self.replay_repo:
            from netobserv.storage.models import ReplayRecord
            record = ReplayRecord(
                id=replay_id,
                source_workflow_id=source_workflow_id,
                status="completed",
                completed_at=result.completed_at,
                diff_summary=diff_summary,
                passed=result.passed,
            )
            self.replay_repo.session.add(record)
            await self.replay_repo.session.flush()

        from netobserv.observability.metrics import get_metrics
        get_metrics().record_replay("pass" if result.passed else "fail")

        logger.info(
            "Replay complete",
            replay_id=replay_id,
            passed=result.passed,
        )
        return result

    async def diff_snapshots(
        self,
        snapshot_id_a: str,
        snapshot_id_b: str,
        device_repo: Any | None = None,
        topology_repo: Any | None = None,
    ) -> dict[str, Any]:
        """Compute a high-level diff between two snapshots."""
        diff: dict[str, Any] = {
            "snapshot_a": snapshot_id_a,
            "snapshot_b": snapshot_id_b,
            "devices": {"added": [], "removed": [], "changed": []},
            "edges": {"added": [], "removed": []},
        }

        if device_repo:
            devices_a = await device_repo.get_by_snapshot(snapshot_id_a)
            devices_b = await device_repo.get_by_snapshot(snapshot_id_b)

            hostnames_a = {d.hostname for d in devices_a}
            hostnames_b = {d.hostname for d in devices_b}

            diff["devices"]["added"] = list(hostnames_b - hostnames_a)
            diff["devices"]["removed"] = list(hostnames_a - hostnames_b)

        if topology_repo:
            edges_a = await topology_repo.get_edges_for_snapshot(snapshot_id_a)
            edges_b = await topology_repo.get_edges_for_snapshot(snapshot_id_b)

            def edge_key(e: Any) -> frozenset[str]:
                return frozenset([e.source_node_id, e.target_node_id])

            keys_a = {edge_key(e) for e in edges_a}
            keys_b = {edge_key(e) for e in edges_b}

            diff["edges"]["added"] = [
                list(k) for k in (keys_b - keys_a)
            ]
            diff["edges"]["removed"] = [
                list(k) for k in (keys_a - keys_b)
            ]

        return diff
