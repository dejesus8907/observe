"""Validation workflow — compares intended vs actual state."""

from __future__ import annotations

from typing import Any

from netobserv.models.canonical import CanonicalDevice, CanonicalInterface, CanonicalVLAN
from netobserv.models.enums import WorkflowStatus
from netobserv.observability.logging import get_logger
from netobserv.sync.planner import DefaultSyncPlanner, SyncPolicy
from netobserv.validation.validator import (
    ActualStateBundle,
    DefaultStateValidator,
    IntendedStateBundle,
)
from netobserv.workflows.manager import WorkflowManager

logger = get_logger("workflows.validation")


class ValidationWorkflow:
    """
    Orchestrates the validation pipeline.

    Stages:
      1. VALIDATION
      2. SYNC_PLAN
      3. REPORTING
      4. COMPLETE
    """

    def __init__(
        self,
        manager: WorkflowManager,
        drift_repo: Any | None = None,
        sync_repo: Any | None = None,
    ) -> None:
        self.manager = manager
        self.drift_repo = drift_repo
        self.sync_repo = sync_repo

    async def run(
        self,
        intended: IntendedStateBundle,
        actual: ActualStateBundle,
        sync_policy: SyncPolicy | None = None,
        snapshot_id: str | None = None,
    ) -> str:
        workflow_id = await self.manager.start_workflow(
            "validation",
            {
                "snapshot_id": snapshot_id,
                "intended_device_count": len(intended.devices),
                "actual_device_count": len(actual.devices),
            },
        )

        try:
            # Validation
            validation_result = await self.manager.execute_stage(
                workflow_id,
                "VALIDATION",
                self._validate,
                intended,
                actual,
                snapshot_id,
                workflow_id,
            )

            drift_records = validation_result.get("drift_records", [])

            # Sync plan
            policy = sync_policy or SyncPolicy()
            sync_result = await self.manager.execute_stage(
                workflow_id,
                "SYNC_PLAN",
                self._build_sync_plan,
                drift_records,
                policy,
                workflow_id,
                snapshot_id,
            )

            # Reporting
            await self.manager.execute_stage(
                workflow_id,
                "REPORTING",
                self._report,
                drift_records,
                sync_result.get("actions", []),
            )

            await self.manager.complete_workflow(workflow_id, WorkflowStatus.COMPLETED)

        except Exception as exc:
            logger.error(
                "Validation workflow failed",
                workflow_id=workflow_id,
                error=str(exc),
                exc_info=True,
            )
            await self.manager.complete_workflow(
                workflow_id, WorkflowStatus.FAILED, error=str(exc)
            )

        return workflow_id

    async def _validate(
        self,
        intended: IntendedStateBundle,
        actual: ActualStateBundle,
        snapshot_id: str | None,
        workflow_id: str | None,
    ) -> dict[str, Any]:
        validator = DefaultStateValidator(
            snapshot_id=snapshot_id, workflow_id=workflow_id
        )
        drift_records = validator.compare(intended, actual)

        if self.drift_repo:
            from netobserv.storage.models import DriftRecord as DriftORM

            orm_records = [
                DriftORM(
                    id=d.drift_id,
                    snapshot_id=d.snapshot_id,
                    workflow_id=d.workflow_id,
                    intended_ref=d.intended_ref,
                    actual_ref=d.actual_ref,
                    object_type=d.object_type,
                    mismatch_type=d.mismatch_type.value,
                    field_name=d.field_name,
                    intended_value=d.intended_value,
                    actual_value=d.actual_value,
                    severity=d.severity.value,
                    explanation=d.explanation,
                    remediation_hint=d.remediation_hint,
                    confidence=d.confidence,
                    evidence=d.evidence,
                )
                for d in drift_records
            ]
            if orm_records:
                await self.drift_repo.save_bulk(orm_records)

        from netobserv.observability.metrics import get_metrics
        metrics = get_metrics()
        for d in drift_records:
            metrics.record_drift(d.mismatch_type.value, d.severity.value)

        return {
            "drift_records": drift_records,
            "drift_count": len(drift_records),
        }

    async def _build_sync_plan(
        self,
        drift_records: list[Any],
        policy: SyncPolicy,
        workflow_id: str,
        snapshot_id: str | None,
    ) -> dict[str, Any]:
        planner = DefaultSyncPlanner(workflow_id=workflow_id, snapshot_id=snapshot_id)
        actions = planner.plan(drift_records, policy)

        from netobserv.observability.metrics import get_metrics
        get_metrics().record_sync_plan(policy.mode.value)

        logger.info(
            "Sync plan created",
            actions=len(actions),
            mode=policy.mode.value,
        )

        return {"actions": actions, "action_count": len(actions)}

    async def _report(
        self, drift_records: list[Any], actions: list[Any]
    ) -> dict[str, Any]:
        by_type: dict[str, int] = {}
        by_severity: dict[str, int] = {}

        for d in drift_records:
            by_type[d.mismatch_type.value] = by_type.get(d.mismatch_type.value, 0) + 1
            by_severity[d.severity.value] = by_severity.get(d.severity.value, 0) + 1

        report = {
            "drift_summary": {
                "total": len(drift_records),
                "by_type": by_type,
                "by_severity": by_severity,
            },
            "sync_plan_summary": {
                "total_actions": len(actions),
            },
        }

        logger.info("Validation report generated", **report["drift_summary"])
        return {"report": report}
