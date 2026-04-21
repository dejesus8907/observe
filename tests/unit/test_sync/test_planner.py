"""Unit tests for the sync planner."""

from __future__ import annotations

import pytest

from netobserv.models.canonical import CanonicalDriftRecord
from netobserv.models.enums import (
    DriftSeverity,
    MismatchType,
    SyncActionType,
    SyncMode,
)
from netobserv.sync.planner import DefaultSyncPlanner, SyncPolicy


@pytest.fixture
def policy_dry_run() -> SyncPolicy:
    return SyncPolicy(mode=SyncMode.DRY_RUN, allow_create=True, allow_update=True, allow_delete=False)


@pytest.fixture
def drift_missing_in_intended() -> CanonicalDriftRecord:
    return CanonicalDriftRecord(
        object_type="device",
        mismatch_type=MismatchType.MISSING_IN_INTENDED,
        severity=DriftSeverity.MEDIUM,
        actual_ref="dev-b",
        explanation="Device found in actual but not in SoT.",
        remediation_hint="Add to NetBox.",
        confidence=1.0,
    )


@pytest.fixture
def drift_state_mismatch() -> CanonicalDriftRecord:
    return CanonicalDriftRecord(
        object_type="device",
        mismatch_type=MismatchType.STATE_MISMATCH,
        severity=DriftSeverity.HIGH,
        intended_ref="dev-a",
        actual_ref="dev-a",
        field_name="status",
        intended_value="planned",
        actual_value="active",
        explanation="Status mismatch.",
        confidence=1.0,
    )


@pytest.fixture
def drift_missing_in_actual() -> CanonicalDriftRecord:
    return CanonicalDriftRecord(
        object_type="device",
        mismatch_type=MismatchType.MISSING_IN_ACTUAL,
        severity=DriftSeverity.CRITICAL,
        intended_ref="dev-c",
        explanation="Device not found.",
        confidence=1.0,
    )


class TestSyncPlannerCreate:
    def test_creates_action_for_missing_in_intended(
        self,
        drift_missing_in_intended: CanonicalDriftRecord,
        policy_dry_run: SyncPolicy,
    ) -> None:
        planner = DefaultSyncPlanner()
        actions = planner.plan([drift_missing_in_intended], policy_dry_run)
        create_actions = [a for a in actions if a.action_type == SyncActionType.CREATE]
        assert len(create_actions) == 1
        assert create_actions[0].object_ref == "dev-b"

    def test_skips_create_when_not_allowed(
        self, drift_missing_in_intended: CanonicalDriftRecord
    ) -> None:
        policy = SyncPolicy(mode=SyncMode.DRY_RUN, allow_create=False)
        planner = DefaultSyncPlanner()
        actions = planner.plan([drift_missing_in_intended], policy)
        create_actions = [a for a in actions if a.action_type == SyncActionType.CREATE]
        skip_actions = [a for a in actions if a.action_type == SyncActionType.SKIP]
        assert len(create_actions) == 0
        assert len(skip_actions) >= 1


class TestSyncPlannerUpdate:
    def test_creates_update_for_state_mismatch(
        self,
        drift_state_mismatch: CanonicalDriftRecord,
        policy_dry_run: SyncPolicy,
    ) -> None:
        planner = DefaultSyncPlanner()
        actions = planner.plan([drift_state_mismatch], policy_dry_run)
        update_actions = [a for a in actions if a.action_type == SyncActionType.UPDATE]
        assert len(update_actions) == 1
        assert update_actions[0].payload["field"] == "status"
        assert update_actions[0].payload["from"] == "planned"
        assert update_actions[0].payload["to"] == "active"

    def test_skips_update_when_not_allowed(
        self, drift_state_mismatch: CanonicalDriftRecord
    ) -> None:
        policy = SyncPolicy(mode=SyncMode.DRY_RUN, allow_update=False)
        planner = DefaultSyncPlanner()
        actions = planner.plan([drift_state_mismatch], policy)
        skip_actions = [a for a in actions if a.action_type == SyncActionType.SKIP]
        assert len(skip_actions) >= 1


class TestSyncPlannerDelete:
    def test_skips_delete_by_default(
        self,
        drift_missing_in_actual: CanonicalDriftRecord,
        policy_dry_run: SyncPolicy,
    ) -> None:
        planner = DefaultSyncPlanner()
        actions = planner.plan([drift_missing_in_actual], policy_dry_run)
        delete_actions = [a for a in actions if a.action_type == SyncActionType.DELETE]
        skip_actions = [a for a in actions if a.action_type == SyncActionType.SKIP]
        assert len(delete_actions) == 0
        assert len(skip_actions) == 1

    def test_creates_delete_when_allowed_with_approval(
        self,
        drift_missing_in_actual: CanonicalDriftRecord,
    ) -> None:
        policy = SyncPolicy(
            mode=SyncMode.DRY_RUN,
            allow_delete=True,
            require_approval_for_delete=True,
        )
        planner = DefaultSyncPlanner()
        actions = planner.plan([drift_missing_in_actual], policy)
        delete_actions = [a for a in actions if a.action_type == SyncActionType.DELETE]
        assert len(delete_actions) == 1
        assert delete_actions[0].requires_approval is True


class TestSyncPlannerFilters:
    def test_low_confidence_is_skipped(self) -> None:
        drift = CanonicalDriftRecord(
            object_type="device",
            mismatch_type=MismatchType.MISSING_IN_INTENDED,
            severity=DriftSeverity.LOW,
            confidence=0.2,
        )
        policy = SyncPolicy(min_confidence=0.5)
        planner = DefaultSyncPlanner()
        actions = planner.plan([drift], policy)
        skip_actions = [a for a in actions if a.action_type == SyncActionType.SKIP]
        assert len(skip_actions) == 1

    def test_excluded_type_is_skipped(self) -> None:
        drift = CanonicalDriftRecord(
            object_type="device",
            mismatch_type=MismatchType.MISSING_IN_INTENDED,
            severity=DriftSeverity.MEDIUM,
            confidence=1.0,
        )
        policy = SyncPolicy(excluded_object_types=["device"])
        planner = DefaultSyncPlanner()
        actions = planner.plan([drift], policy)
        skip_actions = [a for a in actions if a.action_type == SyncActionType.SKIP]
        assert len(skip_actions) == 1
