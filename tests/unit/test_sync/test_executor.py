"""Unit tests for the sync executor."""

from __future__ import annotations

import pytest

from netobserv.models.enums import SyncActionType, SyncMode
from netobserv.sync.executor import SyncExecutionResult, SyncExecutor
from netobserv.sync.planner import SyncAction, SyncPlan, SyncPolicy


class RecordingAdapter:
    def __init__(self, *, fail_on: str | None = None) -> None:
        self.calls: list[tuple[str, str | None, dict | None]] = []
        self.fail_on = fail_on

    async def create(self, object_ref: str | None, payload: dict) -> None:
        self.calls.append(("create", object_ref, payload))
        if self.fail_on == "create":
            raise RuntimeError("create failed")

    async def update(self, object_ref: str | None, payload: dict) -> None:
        self.calls.append(("update", object_ref, payload))
        if self.fail_on == "update":
            raise RuntimeError("update failed")

    async def delete(self, object_ref: str | None) -> None:
        self.calls.append(("delete", object_ref, None))
        if self.fail_on == "delete":
            raise RuntimeError("delete failed")


def _plan(*actions: SyncAction, mode: SyncMode = SyncMode.APPROVED) -> SyncPlan:
    return SyncPlan(
        mode=mode,
        policy=SyncPolicy(mode=mode),
        actions=list(actions),
    )


@pytest.mark.asyncio
async def test_dry_run_skips_everything() -> None:
    executor = SyncExecutor()
    action = SyncAction(object_type="device", action_type=SyncActionType.CREATE, object_ref="dev1")
    result = await executor.execute(_plan(action, mode=SyncMode.DRY_RUN))
    assert isinstance(result, SyncExecutionResult)
    assert result.succeeded == []
    assert len(result.skipped) == 1


@pytest.mark.asyncio
async def test_execute_success_path() -> None:
    executor = SyncExecutor()
    adapter = RecordingAdapter()
    executor.register_adapter("device", adapter)
    actions = [
        SyncAction(object_type="device", action_type=SyncActionType.CREATE, object_ref="dev1", payload={"x": 1}),
        SyncAction(object_type="device", action_type=SyncActionType.UPDATE, object_ref="dev1", payload={"y": 2}),
    ]
    result = await executor.execute(_plan(*actions))
    assert len(result.succeeded) == 2
    assert adapter.calls == [
        ("create", "dev1", {"x": 1}),
        ("update", "dev1", {"y": 2}),
    ]


@pytest.mark.asyncio
async def test_missing_adapter_is_skipped_not_failed() -> None:
    executor = SyncExecutor()
    action = SyncAction(object_type="device", action_type=SyncActionType.CREATE, object_ref="dev1")
    result = await executor.execute(_plan(action))
    assert result.failed == []
    assert len(result.skipped) == 1


@pytest.mark.asyncio
async def test_action_requiring_approval_is_skipped_when_not_approved() -> None:
    executor = SyncExecutor()
    adapter = RecordingAdapter()
    executor.register_adapter("device", adapter)
    action = SyncAction(
        object_type="device",
        action_type=SyncActionType.DELETE,
        object_ref="dev1",
        requires_approval=True,
    )
    result = await executor.execute(_plan(action, mode=SyncMode.SELECTIVE))
    assert result.succeeded == []
    assert len(result.skipped) == 1
    assert adapter.calls == []


@pytest.mark.asyncio
async def test_fail_fast_halts_on_first_failure() -> None:
    executor = SyncExecutor(fail_fast=True)
    adapter = RecordingAdapter(fail_on="update")
    executor.register_adapter("device", adapter)
    actions = [
        SyncAction(object_type="device", action_type=SyncActionType.UPDATE, object_ref="dev1", payload={"a": 1}),
        SyncAction(object_type="device", action_type=SyncActionType.CREATE, object_ref="dev2", payload={"b": 2}),
    ]
    result = await executor.execute(_plan(*actions))
    assert len(result.failed) == 1
    assert result.halted is True
    assert len(result.succeeded) == 0
    assert adapter.calls == [("update", "dev1", {"a": 1})]


@pytest.mark.asyncio
async def test_non_fail_fast_continues_after_failure() -> None:
    executor = SyncExecutor(fail_fast=False)
    adapter = RecordingAdapter(fail_on="update")
    executor.register_adapter("device", adapter)
    actions = [
        SyncAction(object_type="device", action_type=SyncActionType.UPDATE, object_ref="dev1", payload={"a": 1}),
        SyncAction(object_type="device", action_type=SyncActionType.CREATE, object_ref="dev2", payload={"b": 2}),
    ]
    result = await executor.execute(_plan(*actions))
    assert len(result.failed) == 1
    assert result.halted is False
    assert len(result.succeeded) == 1
    assert adapter.calls == [
        ("update", "dev1", {"a": 1}),
        ("create", "dev2", {"b": 2}),
    ]
