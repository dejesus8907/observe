"""Unit tests for the workflow manager."""

from __future__ import annotations

import asyncio
import pytest

from netobserv.models.enums import WorkflowStatus
from netobserv.workflows.manager import WorkflowManager


@pytest.fixture
def manager() -> WorkflowManager:
    return WorkflowManager()


class TestWorkflowManager:
    @pytest.mark.asyncio
    async def test_start_workflow_returns_id(self, manager: WorkflowManager) -> None:
        wf_id = await manager.start_workflow("discovery", {"scope": "dc1"})
        assert wf_id is not None
        assert len(wf_id) == 36  # UUID format

    @pytest.mark.asyncio
    async def test_get_status_running(self, manager: WorkflowManager) -> None:
        wf_id = await manager.start_workflow("discovery", {})
        status = await manager.get_status(wf_id)
        assert status["status"] == "running"
        assert status["workflow_type"] == "discovery"

    @pytest.mark.asyncio
    async def test_execute_stage_success(self, manager: WorkflowManager) -> None:
        wf_id = await manager.start_workflow("discovery", {})

        async def my_stage() -> dict:
            return {"result": 42}

        artifacts = await manager.execute_stage(wf_id, "TEST_STAGE", my_stage)
        assert artifacts["result"] == 42

        status = await manager.get_status(wf_id)
        steps = status["steps"]
        assert len(steps) == 1
        assert steps[0]["stage"] == "TEST_STAGE"
        assert steps[0]["status"] == "completed"

    @pytest.mark.asyncio
    async def test_execute_stage_failure_raises(self, manager: WorkflowManager) -> None:
        wf_id = await manager.start_workflow("discovery", {})

        async def failing_stage() -> None:
            raise ValueError("stage failed on purpose")

        with pytest.raises(RuntimeError, match="stage failed on purpose"):
            await manager.execute_stage(wf_id, "FAIL_STAGE", failing_stage)

    @pytest.mark.asyncio
    async def test_complete_workflow(self, manager: WorkflowManager) -> None:
        wf_id = await manager.start_workflow("discovery", {})
        await manager.complete_workflow(wf_id, WorkflowStatus.COMPLETED)
        status = await manager.get_status(wf_id)
        assert status["status"] == "completed"

    @pytest.mark.asyncio
    async def test_unknown_workflow_returns_error(self, manager: WorkflowManager) -> None:
        status = await manager.get_status("nonexistent-workflow-id")
        assert "error" in status

    @pytest.mark.asyncio
    async def test_concurrent_stages_are_independent(self, manager: WorkflowManager) -> None:
        wf_id_1 = await manager.start_workflow("discovery", {})
        wf_id_2 = await manager.start_workflow("validation", {})

        async def stage_1() -> dict:
            await asyncio.sleep(0.01)
            return {"id": 1}

        async def stage_2() -> dict:
            return {"id": 2}

        results = await asyncio.gather(
            manager.execute_stage(wf_id_1, "S1", stage_1),
            manager.execute_stage(wf_id_2, "S2", stage_2),
        )
        assert results[0]["id"] == 1
        assert results[1]["id"] == 2

        s1 = await manager.get_status(wf_id_1)
        assert len(s1["steps"]) == 1

        s2 = await manager.get_status(wf_id_2)
        assert len(s2["steps"]) == 1
