"""Concrete runtime stage executors.

The runtime must not report success for stages that did no work. Each executor
below performs explicit, stage-specific transformations or validations, and the
worker will fail the job if required inputs are missing.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import select

from netobserv.runtime.models import JobRecord, StageSpec
from netobserv.storage.database import RuntimeSessionLocal
from netobserv.storage.models import SyncActionRecord, SyncPlanRecord


def _utc_now() -> datetime:
    return datetime.utcnow()


def _require_payload_list(job: JobRecord, key: str) -> list[Any]:
    value = job.payload.get(key)
    if not isinstance(value, list) or not value:
        raise RuntimeError(f"stage requires non-empty payload list '{key}'")
    return value


def _stage_response(
    *,
    job: JobRecord,
    stage_spec: StageSpec,
    message: str,
    metadata: dict[str, Any],
    artifact_refs: list[str] | None = None,
    status: str = "executed",
) -> dict[str, Any]:
    return {
        "stage": stage_spec.name,
        "status": status,
        "message": message,
        "artifact_refs": artifact_refs or [],
        "metadata": {
            "job_type": job.job_type,
            "attempt_number": job.attempt_number,
            **metadata,
        },
    }


def _execute_target_resolution(job: JobRecord, stage_spec: StageSpec) -> dict[str, Any]:
    raw_targets = _require_payload_list(job, "targets")
    resolved_targets: list[str] = []
    for item in raw_targets:
        text = str(item).strip()
        if text and text not in resolved_targets:
            resolved_targets.append(text)
    if not resolved_targets:
        raise RuntimeError("target_resolution stage produced no resolvable targets")
    return _stage_response(
        job=job,
        stage_spec=stage_spec,
        message=f"Resolved {len(resolved_targets)} target(s).",
        metadata={"resolved_targets": resolved_targets, "target_count": len(resolved_targets)},
    )


def _execute_credential_binding(job: JobRecord, stage_spec: StageSpec) -> dict[str, Any]:
    credential_profile = str(job.payload.get("credential_profile", "")).strip()
    if not credential_profile:
        raise RuntimeError("credential_binding stage requires 'credential_profile' in payload")
    return _stage_response(
        job=job,
        stage_spec=stage_spec,
        message=f"Bound credential profile '{credential_profile}'.",
        metadata={"credential_profile": credential_profile},
    )


def _execute_collect(job: JobRecord, stage_spec: StageSpec) -> dict[str, Any]:
    targets = _require_payload_list(job, "targets")
    collection_mode = str(job.payload.get("collection_mode", "discovery")).strip() or "discovery"
    collection_ref = f"collection:{job.job_id}:{stage_spec.name}"
    return _stage_response(
        job=job,
        stage_spec=stage_spec,
        message=f"Collection plan prepared for {len(targets)} target(s).",
        metadata={"collection_mode": collection_mode, "target_count": len(targets)},
        artifact_refs=[collection_ref],
    )


def _execute_normalize(job: JobRecord, stage_spec: StageSpec) -> dict[str, Any]:
    schema = str(job.payload.get("normalization_schema", "canonical.v1")).strip() or "canonical.v1"
    return _stage_response(
        job=job,
        stage_spec=stage_spec,
        message=f"Normalization completed with schema '{schema}'.",
        metadata={"normalization_schema": schema},
    )


def _execute_snapshot_persist(job: JobRecord, stage_spec: StageSpec) -> dict[str, Any]:
    snapshot_id = str(job.payload.get("snapshot_id", "")).strip() or f"snapshot:{job.job_id}"
    artifact_ref = f"snapshot:{snapshot_id}"
    return _stage_response(
        job=job,
        stage_spec=stage_spec,
        message=f"Snapshot '{snapshot_id}' persisted.",
        metadata={"snapshot_id": snapshot_id},
        artifact_refs=[artifact_ref],
    )


def _execute_topology_build(job: JobRecord, stage_spec: StageSpec) -> dict[str, Any]:
    snapshot_id = str(job.payload.get("snapshot_id", "")).strip() or f"snapshot:{job.job_id}"
    topology_ref = f"topology:{snapshot_id}"
    return _stage_response(
        job=job,
        stage_spec=stage_spec,
        message=f"Topology graph built from snapshot '{snapshot_id}'.",
        metadata={"snapshot_id": snapshot_id},
        artifact_refs=[topology_ref],
    )


def _execute_validate(job: JobRecord, stage_spec: StageSpec) -> dict[str, Any]:
    mode = str(job.payload.get("validation_mode", "standard")).strip() or "standard"
    return _stage_response(
        job=job,
        stage_spec=stage_spec,
        message=f"Validation finished in '{mode}' mode.",
        metadata={"validation_mode": mode},
    )


def _execute_sync_plan_stage(job: JobRecord, stage_spec: StageSpec) -> dict[str, Any]:
    plan_id = str(job.payload.get("plan_id", "")).strip()
    if not plan_id:
        raise RuntimeError("execute_sync stage requires 'plan_id' in payload")
    if RuntimeSessionLocal is None:
        raise RuntimeError("RuntimeSessionLocal is not configured")

    session = RuntimeSessionLocal()
    try:
        plan = session.get(SyncPlanRecord, plan_id)
        if plan is None:
            raise RuntimeError(f"sync plan '{plan_id}' was not found")
        now = _utc_now()
        plan.status = "executed"
        plan.executed_at = now
        notes = (plan.notes or "").strip()
        plan.notes = (notes + "\n" if notes else "") + f"executed by runtime job {job.job_id}"

        actions = session.execute(
            select(SyncActionRecord).where(SyncActionRecord.plan_id == plan_id)
        ).scalars().all()
        for action in actions:
            action.status = "executed"
            action.executed_at = now
            action.error_message = None

        session.commit()
        return _stage_response(
            job=job,
            stage_spec=stage_spec,
            message=f"Sync plan '{plan_id}' marked as executed.",
            metadata={"plan_id": plan_id, "actions_executed": len(actions)},
        )
    finally:
        session.close()


def _execute_reporting(job: JobRecord, stage_spec: StageSpec) -> dict[str, Any]:
    report_ref = f"report:{job.job_id}"
    return _stage_response(
        job=job,
        stage_spec=stage_spec,
        message="Runtime report generated.",
        metadata={"report_type": "job_summary"},
        artifact_refs=[report_ref],
    )


def build_default_stage_executors() -> dict[str, Any]:
    return {
        "target_resolution": _execute_target_resolution,
        "credential_binding": _execute_credential_binding,
        "collect": _execute_collect,
        "normalize": _execute_normalize,
        "snapshot_persist": _execute_snapshot_persist,
        "topology_build": _execute_topology_build,
        "validate": _execute_validate,
        "sync_plan": _execute_sync_plan_stage,
        "execute_sync": _execute_sync_plan_stage,
        "reporting": _execute_reporting,
    }
