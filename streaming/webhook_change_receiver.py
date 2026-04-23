from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from netobserv.streaming.adapter_helpers import AdapterObservation, publish_observation
from netobserv.streaming.conflict import SubjectType
from netobserv.streaming.evidence_mapper import make_evidence_record, synthesize_fallback_change_event
from netobserv.streaming.event_bus import EventBus
from netobserv.streaming.events import ChangeKind, EventSource


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class WebhookSource:
    source_id: str
    source_name: str
    trust_label: str = "webhook"


class WebhookChangeReceiver:
    """Evidence-first scaffold for REST/webhook-based change adapters.

    This is for external systems that can push structured device/interface/link
    state changes into NetObserv.
    """

    def __init__(self, event_bus: EventBus) -> None:
        self._event_bus = event_bus

    def build_edge_exists_observation(
        self,
        *,
        source: WebhookSource,
        device_id: str,
        interface_name: str,
        neighbor_device_id: str,
        neighbor_interface: str,
        exists: bool,
        observed_at: Optional[datetime] = None,
        raw_payload: dict[str, Any] | None = None,
    ) -> AdapterObservation:
        observed_at = observed_at or _utc_now()
        edge_subject = f"{device_id}:{interface_name}<->{neighbor_device_id}:{neighbor_interface}"
        evidence = make_evidence_record(
            evidence_id=f"webhook:{source.source_id}:{edge_subject}:{observed_at.isoformat()}",
            subject_id=edge_subject,
            subject_type=SubjectType.EDGE,
            field_name="exists",
            asserted_value=bool(exists),
            source_type="manual_import" if source.trust_label == "manual" else "syslog_structured",
            source_instance=source.source_name,
            observed_at=observed_at,
            ingested_at=observed_at,
            confidence_hint=0.75,
            freshness_ttl_seconds=30.0,
            source_health="healthy",
            metadata=raw_payload or {},
        )
        compatibility_event = synthesize_fallback_change_event(
            source=EventSource.SYSLOG,
            kind=ChangeKind.NEIGHBOR_DISCOVERED if bool(exists) else ChangeKind.NEIGHBOR_LOST,
            device_id=device_id,
            interface_name=interface_name,
            neighbor_device_id=neighbor_device_id,
            neighbor_interface=neighbor_interface,
            detected_at=observed_at,
            received_at=observed_at,
            field_path="edge.exists",
            current_value=bool(exists),
            confidence=0.75,
            raw_payload=raw_payload or {},
            human_summary=f"Webhook reported edge {edge_subject} exists={bool(exists)}",
        )
        return AdapterObservation(evidence=evidence, compatibility_event=compatibility_event)

    async def publish_observation(self, observation: AdapterObservation) -> None:
        await publish_observation(self._event_bus, observation)
