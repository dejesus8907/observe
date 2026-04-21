"""Durable workflow manager — creates, tracks, and persists workflow state."""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from netobserv.models.enums import WorkflowStage, WorkflowStatus
from netobserv.observability.logging import get_logger
from netobserv.observability.metrics import get_metrics

logger = get_logger("workflows.manager")


@dataclass
class WorkflowStepResult:
    stage: str
    status: str
    started_at: datetime
    completed_at: datetime
    elapsed_seconds: float
    artifacts: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass
class WorkflowContext:
    """Runtime context shared across all stages of a workflow."""

    workflow_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    workflow_type: str = "discovery"
    metadata: dict[str, Any] = field(default_factory=dict)
    status: WorkflowStatus = WorkflowStatus.PENDING
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    current_stage: str | None = None
    steps: list[WorkflowStepResult] = field(default_factory=list)
    artifacts: dict[str, Any] = field(default_factory=dict)
    error_message: str | None = None

    def to_status_dict(self) -> dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "workflow_type": self.workflow_type,
            "status": self.status.value,
            "current_stage": self.current_stage,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error_message": self.error_message,
            "steps": [
                {
                    "stage": s.stage,
                    "status": s.status,
                    "elapsed_seconds": s.elapsed_seconds,
                    "errors": s.errors,
                    "warnings": s.warnings,
                }
                for s in self.steps
            ],
            "metadata": self.metadata,
        }


class WorkflowManager:
    """
    Creates and manages workflow lifecycles.

    The manager is responsible for:
    - Generating workflow IDs
    - Tracking stage transitions
    - Persisting step results
    - Emitting metrics and logs
    - Recovering state after restart (via storage)
    """

    def __init__(
        self,
        workflow_repo: Any | None = None,
    ) -> None:
        self._workflow_repo = workflow_repo
        self._active: dict[str, WorkflowContext] = {}

    async def start_workflow(
        self, workflow_type: str, metadata: dict[str, Any]
    ) -> str:
        """Create and register a new workflow. Returns the workflow_id."""
        ctx = WorkflowContext(
            workflow_type=workflow_type,
            metadata=metadata,
            status=WorkflowStatus.RUNNING,
            started_at=datetime.utcnow(),
        )
        self._active[ctx.workflow_id] = ctx

        if self._workflow_repo:
            await self._workflow_repo.create(workflow_type, metadata)

        get_metrics().inc_active_workflows()
        logger.info(
            "Workflow started",
            workflow_id=ctx.workflow_id,
            workflow_type=workflow_type,
        )
        return ctx.workflow_id

    async def execute_stage(
        self,
        workflow_id: str,
        stage_name: str,
        stage_fn: Callable[..., Coroutine[Any, Any, Any]],
        *args: Any,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Execute a single workflow stage, recording timing and status.

        Returns the stage artifacts dict.
        """
        ctx = self._active.get(workflow_id)
        if not ctx:
            raise ValueError(f"Unknown workflow: {workflow_id}")

        ctx.current_stage = stage_name
        started_at = datetime.utcnow()
        start_time = time.monotonic()
        errors: list[str] = []
        warnings: list[str] = []
        artifacts: dict[str, Any] = {}

        logger.info(
            "Stage started",
            workflow_id=workflow_id,
            stage=stage_name,
        )

        try:
            result = await stage_fn(*args, **kwargs)
            if isinstance(result, dict):
                artifacts = result
            status = "completed"
        except Exception as exc:
            errors.append(str(exc))
            status = "failed"
            logger.error(
                "Stage failed",
                workflow_id=workflow_id,
                stage=stage_name,
                error=str(exc),
                exc_info=True,
            )

        elapsed = time.monotonic() - start_time
        completed_at = datetime.utcnow()

        step = WorkflowStepResult(
            stage=stage_name,
            status=status,
            started_at=started_at,
            completed_at=completed_at,
            elapsed_seconds=elapsed,
            artifacts=artifacts,
            errors=errors,
            warnings=warnings,
        )
        ctx.steps.append(step)

        get_metrics().record_stage_duration(
            stage=stage_name,
            workflow_type=ctx.workflow_type,
            duration=elapsed,
        )

        if self._workflow_repo:
            await self._workflow_repo.add_step(
                workflow_id=workflow_id,
                stage_name=stage_name,
                status=status,
                artifacts=artifacts,
                errors=errors,
                warnings=warnings,
                elapsed_seconds=elapsed,
            )

        logger.info(
            "Stage completed",
            workflow_id=workflow_id,
            stage=stage_name,
            status=status,
            elapsed_seconds=f"{elapsed:.2f}",
        )

        if status == "failed":
            raise RuntimeError(
                f"Stage '{stage_name}' failed: {'; '.join(errors)}"
            )

        return artifacts

    async def complete_workflow(
        self,
        workflow_id: str,
        status: WorkflowStatus = WorkflowStatus.COMPLETED,
        error: str | None = None,
    ) -> None:
        ctx = self._active.get(workflow_id)
        if not ctx:
            return

        ctx.status = status
        ctx.completed_at = datetime.utcnow()
        ctx.error_message = error

        get_metrics().dec_active_workflows()
        get_metrics().record_snapshot(status.value)

        if self._workflow_repo:
            await self._workflow_repo.update_status(
                workflow_id, status.value, error
            )

        logger.info(
            "Workflow completed",
            workflow_id=workflow_id,
            status=status.value,
            elapsed=(
                (ctx.completed_at - ctx.started_at).total_seconds()
                if ctx.started_at
                else None
            ),
        )

    async def get_status(self, workflow_id: str) -> dict[str, Any]:
        ctx = self._active.get(workflow_id)
        if ctx:
            return ctx.to_status_dict()

        if self._workflow_repo:
            record = await self._workflow_repo.get(workflow_id)
            if record:
                return {
                    "workflow_id": record.id,
                    "workflow_type": record.workflow_type,
                    "status": record.status,
                    "created_at": record.created_at.isoformat() if record.created_at else None,
                    "started_at": record.started_at.isoformat() if record.started_at else None,
                    "completed_at": record.completed_at.isoformat() if record.completed_at else None,
                    "error_message": record.error_message,
                    "steps": [
                        {
                            "stage": s.stage_name,
                            "status": s.status,
                            "elapsed_seconds": s.elapsed_seconds,
                        }
                        for s in record.steps
                    ],
                    "metadata": record.metadata_,
                }

        return {"error": f"Workflow {workflow_id} not found"}
