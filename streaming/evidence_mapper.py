# ChangeEvent synthesis is now treated as a downstream compatibility view.
from __future__ import annotations

from datetime import datetime
from typing import Any

from netobserv.streaming.conflict import EvidenceRecord, SourceHealthState, SubjectType
from netobserv.streaming.events import ChangeEvent, ChangeKind, EventSource


def _subject_id_from_event(event: ChangeEvent) -> str:
    if event.field_path and event.field_path.startswith("interface.") and event.interface_name:
        return f"{event.device_id}:{event.interface_name}"
    if event.field_path and event.field_path.startswith("edge."):
        neighbor = event.neighbor_device_id or "unknown-neighbor"
        local_if = event.interface_name or "unknown-if"
        remote_if = event.neighbor_interface or "unknown-remote-if"
        return f"{event.device_id}:{local_if}<->{neighbor}:{remote_if}"
    if event.field_path and event.field_path.startswith("bgp.") and event.neighbor_device_id:
        return f"{event.device_id}:bgp:{event.neighbor_device_id}"
    if event.interface_name:
        return f"{event.device_id}:{event.interface_name}"
    return event.device_id


def _subject_type_from_field_path(field_path: str) -> SubjectType:
    if field_path.startswith("interface."):
        return SubjectType.INTERFACE
    if field_path.startswith("edge."):
        return SubjectType.EDGE
    if field_path.startswith("bgp."):
        return SubjectType.BGP_SESSION
    return SubjectType.DEVICE


def make_evidence_record(
    *,
    evidence_id: str,
    subject_id: str,
    subject_type: SubjectType,
    field_name: str,
    asserted_value: Any,
    source_type: str,
    source_instance: str,
    observed_at: datetime,
    ingested_at: datetime,
    confidence_hint: float = 0.8,
    freshness_ttl_seconds: float = 30.0,
    source_health: str = "unknown",
    metadata: dict[str, Any] | None = None,
) -> EvidenceRecord:
    try:
        health = SourceHealthState(source_health)
    except Exception:
        health = SourceHealthState.UNKNOWN
    return EvidenceRecord(
        evidence_id=evidence_id,
        subject_type=subject_type,
        subject_id=subject_id,
        field_name=field_name,
        asserted_value=asserted_value,
        source_type=source_type,
        source_instance=source_instance,
        observed_at=observed_at,
        ingested_at=ingested_at,
        confidence_hint=float(confidence_hint),
        freshness_ttl_seconds=float(freshness_ttl_seconds),
        source_health=health,
        metadata=metadata or {},
    )


def evidence_record_from_change_event(event: ChangeEvent) -> EvidenceRecord | None:
    field_path = event.field_path
    asserted_value = event.current_value

    if event.kind in {ChangeKind.INTERFACE_UP, ChangeKind.INTERFACE_DOWN, ChangeKind.INTERFACE_FLAP}:
        field_path = "interface.oper_status"
        if event.kind == ChangeKind.INTERFACE_UP:
            asserted_value = "up"
        elif event.kind == ChangeKind.INTERFACE_DOWN:
            asserted_value = "down"
        else:
            asserted_value = event.current_value or "flapping"

    elif event.kind in {ChangeKind.NEIGHBOR_DISCOVERED, ChangeKind.NEIGHBOR_LOST, ChangeKind.NEIGHBOR_CHANGED}:
        field_path = "edge.exists"
        if event.kind == ChangeKind.NEIGHBOR_DISCOVERED:
            asserted_value = True
        elif event.kind == ChangeKind.NEIGHBOR_LOST:
            asserted_value = False
        else:
            asserted_value = event.current_value if event.current_value is not None else True

    elif event.kind in {ChangeKind.BGP_SESSION_ESTABLISHED, ChangeKind.BGP_SESSION_DROPPED, ChangeKind.BGP_PEER_FLAP}:
        field_path = "bgp.session_state"
        if event.kind == ChangeKind.BGP_SESSION_ESTABLISHED:
            asserted_value = "established"
        elif event.kind == ChangeKind.BGP_SESSION_DROPPED:
            asserted_value = "idle"
        else:
            asserted_value = event.current_value or "flapping"

    if not field_path:
        return None

    full_field = field_path
    short_field = field_path.split(".", 1)[1] if "." in field_path else field_path
    return make_evidence_record(
        evidence_id=event.event_id,
        subject_id=_subject_id_from_event(event),
        subject_type=_subject_type_from_field_path(full_field),
        field_name=short_field,
        asserted_value=asserted_value,
        source_type=event.source.value if hasattr(event.source, "value") else str(event.source),
        source_instance=event.device_hostname or event.device_id,
        observed_at=event.detected_at,
        ingested_at=event.received_at,
        confidence_hint=float(event.confidence),
        freshness_ttl_seconds=float(event.raw_payload.get("freshness_ttl_seconds", 30.0)) if isinstance(event.raw_payload, dict) else 30.0,
        source_health=(event.raw_payload.get("source_health", "unknown") if isinstance(event.raw_payload, dict) else "unknown"),
        metadata=dict(event.raw_payload or {}),
    )


def synthesize_fallback_change_event(
    *,
    source: EventSource,
    kind: ChangeKind,
    device_id: str,
    detected_at: datetime,
    received_at: datetime,
    field_path: str,
    current_value: Any,
    interface_name: str | None = None,
    neighbor_device_id: str | None = None,
    neighbor_interface: str | None = None,
    device_hostname: str | None = None,
    confidence: float = 0.8,
    raw_payload: dict[str, Any] | None = None,
    human_summary: str | None = None,
) -> ChangeEvent:
    from netobserv.streaming.events import ChangeSeverity
    return ChangeEvent(
        kind=kind,
        severity=ChangeSeverity.INFO,
        source=source,
        device_id=device_id,
        device_hostname=device_hostname,
        interface_name=interface_name,
        neighbor_device_id=neighbor_device_id,
        neighbor_interface=neighbor_interface,
        detected_at=detected_at,
        received_at=received_at,
        current_value=current_value,
        field_path=field_path,
        confidence=confidence,
        raw_payload=raw_payload or {},
        human_summary=human_summary or f"Fallback synthesized event for {field_path}",
    )
