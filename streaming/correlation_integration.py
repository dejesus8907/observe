from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from netobserv.streaming.correlation import CorrelationFacade


@dataclass(slots=True)
class CorrelatedLineage:
    cluster_id: str
    role: str
    confidence: float
    is_new_cluster: bool
    linked_event_ids: list[str]
    explanation: str


class CorrelationIntegrationService:
    """Bridges resolved streaming views into the durable correlation pipeline."""

    def __init__(self, facade: CorrelationFacade | None = None) -> None:
        self.facade = facade or CorrelationFacade()

    def correlate_event(self, event: Any):
        return self.facade.correlate_event(event)

    def lineage_from_decision(self, decision: Any) -> CorrelatedLineage:
        return CorrelatedLineage(
            cluster_id=decision.cluster_id,
            role=decision.role.value if hasattr(decision.role, "value") else str(decision.role),
            confidence=float(decision.confidence),
            is_new_cluster=bool(decision.is_new_cluster),
            linked_event_ids=list(decision.linked_event_ids),
            explanation=str(decision.explanation),
        )
