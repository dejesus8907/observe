"""Sync planner — generates synchronization action plans from drift records."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel

from netobserv.models.canonical import CanonicalDriftRecord
from netobserv.models.enums import (
    DriftSeverity,
    MismatchType,
    SyncActionType,
    SyncMode,
)
from netobserv.observability.logging import get_logger

logger = get_logger("sync.planner")


class SyncPolicy(BaseModel):
    """Controls what sync actions are permitted."""

    mode: SyncMode = SyncMode.DRY_RUN
    allow_create: bool = True
    allow_update: bool = True
    allow_delete: bool = False  # Safe default: no deletes
    require_approval_for_delete: bool = True
    tag_managed_objects: bool = True
    managed_object_tag: str = "netobserv-managed"
    allowed_object_types: list[str] = field(default_factory=list)
    excluded_object_types: list[str] = field(default_factory=list)
    min_confidence: float = 0.5
    severity_filter: list[str] = field(default_factory=list)  # empty = all


@dataclass
class SyncAction:
    """A single proposed synchronization action."""

    action_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    plan_id: str = ""
    drift_id: str | None = None
    object_type: str = ""
    action_type: SyncActionType = SyncActionType.NOOP
    object_ref: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    requires_approval: bool = False
    explanation: str = ""
    estimated_risk: str = "low"


@dataclass
class SyncPlan:
    """A complete set of synchronization actions for a workflow run."""

    plan_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    workflow_id: str | None = None
    snapshot_id: str | None = None
    mode: SyncMode = SyncMode.DRY_RUN
    policy: SyncPolicy = field(default_factory=SyncPolicy)
    actions: list[SyncAction] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    notes: str = ""

    @property
    def action_count(self) -> int:
        return len(self.actions)

    @property
    def creates(self) -> list[SyncAction]:
        return [a for a in self.actions if a.action_type == SyncActionType.CREATE]

    @property
    def updates(self) -> list[SyncAction]:
        return [a for a in self.actions if a.action_type == SyncActionType.UPDATE]

    @property
    def deletes(self) -> list[SyncAction]:
        return [a for a in self.actions if a.action_type == SyncActionType.DELETE]

    @property
    def skipped(self) -> list[SyncAction]:
        return [a for a in self.actions if a.action_type == SyncActionType.SKIP]


@runtime_checkable
class SyncPlanner(Protocol):
    def plan(
        self,
        drift_records: list[CanonicalDriftRecord],
        policy: SyncPolicy,
    ) -> list[SyncAction]:
        ...


class DefaultSyncPlanner:
    """
    Converts drift records into sync actions based on the policy.

    Mapping:
      MISSING_IN_ACTUAL  -> no action (actual state is read-only source)
      MISSING_IN_INTENDED -> CREATE action (add to source-of-truth)
      STATE_MISMATCH     -> UPDATE action
      IDENTITY_MISMATCH  -> UPDATE action (lower confidence)
    """

    def __init__(
        self,
        workflow_id: str | None = None,
        snapshot_id: str | None = None,
    ) -> None:
        self.workflow_id = workflow_id
        self.snapshot_id = snapshot_id

    def plan(
        self,
        drift_records: list[CanonicalDriftRecord],
        policy: SyncPolicy,
    ) -> list[SyncAction]:
        plan_id = str(uuid.uuid4())
        actions: list[SyncAction] = []

        for drift in drift_records:
            # Apply filters
            if drift.confidence < policy.min_confidence:
                actions.append(
                    SyncAction(
                        plan_id=plan_id,
                        drift_id=drift.drift_id,
                        object_type=drift.object_type,
                        action_type=SyncActionType.SKIP,
                        explanation=f"Low confidence ({drift.confidence:.2f}) — skipped.",
                    )
                )
                continue

            if policy.allowed_object_types and drift.object_type not in policy.allowed_object_types:
                actions.append(
                    SyncAction(
                        plan_id=plan_id,
                        drift_id=drift.drift_id,
                        object_type=drift.object_type,
                        action_type=SyncActionType.SKIP,
                        explanation=f"Object type '{drift.object_type}' not in allowed list.",
                    )
                )
                continue

            if drift.object_type in policy.excluded_object_types:
                actions.append(
                    SyncAction(
                        plan_id=plan_id,
                        drift_id=drift.drift_id,
                        object_type=drift.object_type,
                        action_type=SyncActionType.SKIP,
                        explanation=f"Object type '{drift.object_type}' excluded by policy.",
                    )
                )
                continue

            action = self._drift_to_action(drift, policy, plan_id)
            if action:
                actions.append(action)

        logger.info(
            "Sync plan generated",
            total=len(actions),
            creates=sum(1 for a in actions if a.action_type == SyncActionType.CREATE),
            updates=sum(1 for a in actions if a.action_type == SyncActionType.UPDATE),
            deletes=sum(1 for a in actions if a.action_type == SyncActionType.DELETE),
            skips=sum(1 for a in actions if a.action_type == SyncActionType.SKIP),
        )
        return actions

    def _drift_to_action(
        self,
        drift: CanonicalDriftRecord,
        policy: SyncPolicy,
        plan_id: str,
    ) -> SyncAction | None:
        if drift.mismatch_type == MismatchType.MISSING_IN_INTENDED:
            # Actual has it but intended doesn't → CREATE in SoT
            if not policy.allow_create:
                return SyncAction(
                    plan_id=plan_id,
                    drift_id=drift.drift_id,
                    object_type=drift.object_type,
                    action_type=SyncActionType.SKIP,
                    explanation="CREATE not allowed by policy.",
                )
            return SyncAction(
                plan_id=plan_id,
                drift_id=drift.drift_id,
                object_type=drift.object_type,
                action_type=SyncActionType.CREATE,
                object_ref=drift.actual_ref,
                payload={
                    "actual_value": drift.actual_value,
                    "tags": [policy.managed_object_tag] if policy.tag_managed_objects else [],
                },
                explanation=drift.explanation,
                estimated_risk="low",
            )

        elif drift.mismatch_type in (MismatchType.STATE_MISMATCH, MismatchType.IDENTITY_MISMATCH):
            if not policy.allow_update:
                return SyncAction(
                    plan_id=plan_id,
                    drift_id=drift.drift_id,
                    object_type=drift.object_type,
                    action_type=SyncActionType.SKIP,
                    explanation="UPDATE not allowed by policy.",
                )
            return SyncAction(
                plan_id=plan_id,
                drift_id=drift.drift_id,
                object_type=drift.object_type,
                action_type=SyncActionType.UPDATE,
                object_ref=drift.intended_ref or drift.actual_ref,
                payload={
                    "field": drift.field_name,
                    "from": drift.intended_value,
                    "to": drift.actual_value,
                },
                explanation=drift.explanation,
                estimated_risk=(
                    "medium"
                    if drift.severity in (DriftSeverity.HIGH, DriftSeverity.CRITICAL)
                    else "low"
                ),
            )

        elif drift.mismatch_type == MismatchType.MISSING_IN_ACTUAL:
            # SoT has it, actual doesn't — typically this means device is missing
            # Don't auto-generate delete actions without explicit policy
            if not policy.allow_delete:
                return SyncAction(
                    plan_id=plan_id,
                    drift_id=drift.drift_id,
                    object_type=drift.object_type,
                    action_type=SyncActionType.SKIP,
                    explanation="DELETE not allowed by policy — device missing in actual state requires manual review.",
                )
            requires_approval = policy.require_approval_for_delete
            return SyncAction(
                plan_id=plan_id,
                drift_id=drift.drift_id,
                object_type=drift.object_type,
                action_type=SyncActionType.DELETE,
                object_ref=drift.intended_ref,
                requires_approval=requires_approval,
                explanation=drift.explanation,
                estimated_risk="high",
            )

        return None
