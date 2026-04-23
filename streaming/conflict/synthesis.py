from __future__ import annotations

from typing import Any

from netobserv.streaming.events import ChangeEvent, ChangeKind, ChangeSeverity, EventSource

from .models import ResolvedAssertion


def synthesize_change_event_from_assertion(assertion: ResolvedAssertion, source_event: ChangeEvent) -> ChangeEvent:
    """Convert a resolved assertion into a canonical ChangeEvent view.

    This keeps the rest of the streaming stack compatible while conflict
    resolution becomes more evidence-native.
    """
    updates: dict[str, Any] = {
        "confidence": float(assertion.confidence),
        "is_confirmed": assertion.resolution_state.value == "confirmed",
        "severity": source_event.severity,
        "source": source_event.source,
        "human_summary": (
            f"{source_event.human_summary or ''} "
            f"[resolved={assertion.resolved_value!r}, state={assertion.resolution_state.value}, confidence={assertion.confidence:.2f}]"
        ).strip(),
    }
    raw = dict(source_event.raw_payload or {})
    raw["conflict_resolution"] = {
        "state": assertion.resolution_state.value,
        "confidence": float(assertion.confidence),
        "conflict_type": assertion.conflict_type.value,
        "contributing_evidence_ids": list(assertion.contributing_evidence_ids),
        "rejected_evidence_ids": list(assertion.rejected_evidence_ids),
    }
    updates["raw_payload"] = raw

    field_key = f"{assertion.subject_type.value}.{assertion.field_name}"
    if field_key == "interface.oper_status":
        updates["field_path"] = "interface.oper_status"
        updates["current_value"] = assertion.resolved_value
        updates["kind"] = ChangeKind.INTERFACE_UP if str(assertion.resolved_value).lower() == "up" else ChangeKind.INTERFACE_DOWN
    elif field_key == "edge.exists":
        updates["field_path"] = "edge.exists"
        updates["current_value"] = bool(assertion.resolved_value)
        updates["kind"] = ChangeKind.NEIGHBOR_DISCOVERED if bool(assertion.resolved_value) else ChangeKind.NEIGHBOR_LOST
    elif field_key == "bgp_session.session_state" or field_key == "bgp.session_state":
        updates["field_path"] = "bgp.session_state"
        updates["current_value"] = assertion.resolved_value
        updates["kind"] = ChangeKind.BGP_SESSION_ESTABLISHED if str(assertion.resolved_value).lower() == "established" else ChangeKind.BGP_SESSION_DROPPED
    return source_event.model_copy(update=updates)
