from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from netobserv.streaming.adapter_helpers import AdapterObservation, publish_observation
from netobserv.streaming.conflict import EvidenceRecord, SubjectType
from netobserv.streaming.evidence_mapper import make_evidence_record, synthesize_fallback_change_event
from netobserv.streaming.event_bus import EventBus
from netobserv.streaming.events import ChangeKind, EventSource


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class NetconfTarget:
    device_id: str
    host: str
    port: int = 830
    username: str = ""
    password: str = ""
    hostkey_verify: bool = False
    device_hostname: Optional[str] = None


class NetconfNotificationCollector:
    """Evidence-first NETCONF notification collector scaffold.

    This is intentionally a clean scaffold, not fake production completeness.
    It establishes the evidence-first contract for future NETCONF integration.
    """

    def __init__(self, event_bus: EventBus) -> None:
        self._event_bus = event_bus
        self._running = False
        self._tasks: list[asyncio.Task[Any]] = []

    async def start(self) -> None:
        self._running = True

    async def stop(self) -> None:
        self._running = False
        for task in self._tasks:
            task.cancel()
        self._tasks.clear()

    def build_interface_status_observation(
        self,
        *,
        target: NetconfTarget,
        interface_name: str,
        oper_status: str,
        observed_at: datetime | None = None,
        raw_payload: dict[str, Any] | None = None,
    ) -> AdapterObservation:
        observed_at = observed_at or _utc_now()
        evidence = make_evidence_record(
            evidence_id=f"netconf:{target.device_id}:{interface_name}:{observed_at.isoformat()}",
            subject_id=f"{target.device_id}:{interface_name}",
            subject_type=SubjectType.INTERFACE,
            field_name="oper_status",
            asserted_value=oper_status,
            source_type="netconf_notification",
            source_instance=target.device_hostname or target.host,
            observed_at=observed_at,
            ingested_at=observed_at,
            confidence_hint=0.92,
            freshness_ttl_seconds=30.0,
            source_health="healthy",
            metadata=raw_payload or {},
        )
        compatibility_event = synthesize_fallback_change_event(
            source=EventSource.NETCONF_NOTIFICATION if hasattr(EventSource, "NETCONF_NOTIFICATION") else EventSource.GNMI_STREAM,
            kind=ChangeKind.INTERFACE_UP if str(oper_status).lower() == "up" else ChangeKind.INTERFACE_DOWN,
            device_id=target.device_id,
            device_hostname=target.device_hostname,
            interface_name=interface_name,
            detected_at=observed_at,
            received_at=observed_at,
            field_path="interface.oper_status",
            current_value=oper_status,
            confidence=0.92,
            raw_payload=raw_payload or {},
            human_summary=f"NETCONF notification reported {interface_name} {oper_status}",
        )
        return AdapterObservation(evidence=evidence, compatibility_event=compatibility_event)

    async def publish_observation(self, observation: AdapterObservation) -> None:
        await publish_observation(self._event_bus, observation)
