from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from netobserv.streaming.conflict import EvidenceRecord
from netobserv.streaming.event_bus import EventBus
from netobserv.streaming.events import ChangeEvent
from netobserv.streaming.evidence_mapper import evidence_record_from_change_event


@dataclass(slots=True)
class AdapterObservation:
    """Primary adapter output.

    Adapters should prefer evidence-native observations. A synthesized
    ChangeEvent remains available as a compatibility view for downstream paths
    that still expect events.
    """

    evidence: EvidenceRecord | None = None
    compatibility_event: ChangeEvent | None = None

    @property
    def is_evidence_native(self) -> bool:
        return self.evidence is not None


def observation_from_change_event(event: ChangeEvent) -> AdapterObservation:
    evidence = evidence_record_from_change_event(event)
    return AdapterObservation(evidence=evidence, compatibility_event=event)


async def publish_observation(event_bus: EventBus, observation: AdapterObservation) -> None:
    if observation.evidence is not None:
        source_event = observation.compatibility_event
        if source_event is None:
            raise ValueError("evidence-native observation requires a compatibility ChangeEvent for now")
        await event_bus.publish_adapter_observation(
            evidence=observation.evidence,
            source_event=source_event,
            fallback_event=source_event,
        )
        return
    if observation.compatibility_event is None:
        raise ValueError("observation requires either evidence or compatibility_event")
    await event_bus.publish(observation.compatibility_event)
