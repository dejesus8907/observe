"""Sync executor — applies sync actions to target systems."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from netobserv.models.enums import SyncActionType, SyncMode
from netobserv.observability.logging import get_logger
from netobserv.sync.planner import SyncAction, SyncPlan, SyncPolicy

logger = get_logger("sync.executor")


class SyncExecutionResult:
    def __init__(self) -> None:
        self.succeeded: list[str] = []
        self.failed: list[tuple[str, str]] = []
        self.skipped: list[str] = []
        self.executed_at: datetime = datetime.utcnow()

    @property
    def total(self) -> int:
        return len(self.succeeded) + len(self.failed) + len(self.skipped)


class SyncExecutor:
    """
    Executes a sync plan against a target system.

    In dry-run mode, no actual changes are made.
    In approved mode, changes are dispatched to the target adapter.

    The executor is extensible: register adapters per object_type.
    """

    def __init__(self) -> None:
        self._adapters: dict[str, Any] = {}

    def register_adapter(self, object_type: str, adapter: Any) -> None:
        """Register a sync adapter for a given object type."""
        self._adapters[object_type] = adapter

    async def execute(
        self, plan: SyncPlan
    ) -> SyncExecutionResult:
        result = SyncExecutionResult()

        if plan.mode == SyncMode.DRY_RUN or plan.mode == SyncMode.VALIDATION_ONLY:
            logger.info(
                "Dry-run / validation-only mode — no changes will be made",
                plan_id=plan.plan_id,
                action_count=plan.action_count,
            )
            result.skipped = [a.action_id for a in plan.actions]
            return result

        for action in plan.actions:
            if action.action_type == SyncActionType.SKIP:
                result.skipped.append(action.action_id)
                continue

            if action.requires_approval and plan.mode != SyncMode.APPROVED:
                logger.warning(
                    "Action requires approval — skipped",
                    action_id=action.action_id,
                    object_type=action.object_type,
                )
                result.skipped.append(action.action_id)
                continue

            adapter = self._adapters.get(action.object_type)
            if not adapter:
                logger.warning(
                    "No adapter for object type — skipped",
                    object_type=action.object_type,
                )
                result.skipped.append(action.action_id)
                continue

            try:
                await self._dispatch_action(adapter, action)
                result.succeeded.append(action.action_id)
                logger.info(
                    "Sync action executed",
                    action_id=action.action_id,
                    action_type=action.action_type.value,
                    object_type=action.object_type,
                )
            except Exception as exc:
                result.failed.append((action.action_id, str(exc)))
                logger.error(
                    "Sync action failed",
                    action_id=action.action_id,
                    error=str(exc),
                )

        return result

    async def _dispatch_action(self, adapter: Any, action: SyncAction) -> None:
        if action.action_type == SyncActionType.CREATE:
            await adapter.create(action.object_ref, action.payload)
        elif action.action_type == SyncActionType.UPDATE:
            await adapter.update(action.object_ref, action.payload)
        elif action.action_type == SyncActionType.DELETE:
            await adapter.delete(action.object_ref)
        else:
            pass  # NOOP
