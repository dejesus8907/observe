from __future__ import annotations

from typing import Any

from .policies.interface_oper_status import POLICY as INTERFACE_OPER_STATUS_POLICY
from .policies.edge_exists import POLICY as EDGE_EXISTS_POLICY
from .policies.bgp_session_state import POLICY as BGP_SESSION_STATE_POLICY

FIELD_POLICIES = {
    "interface.oper_status": INTERFACE_OPER_STATUS_POLICY,
    "edge.exists": EDGE_EXISTS_POLICY,
    "bgp.session_state": BGP_SESSION_STATE_POLICY,
}

# SubjectType enum values may differ from policy key prefixes
# (e.g. SubjectType.BGP_SESSION = "bgp_session" but policy uses "bgp")
_FIELD_KEY_ALIASES = {
    "bgp_session.session_state": "bgp.session_state",
}

def _normalize_field_key(field_key: str) -> str:
    return _FIELD_KEY_ALIASES.get(field_key, field_key)

def get_field_policy(field_key: str) -> dict[str, Any]:
    return FIELD_POLICIES.get(_normalize_field_key(field_key), {})

def get_policy_value(field_key: str, key: str, default: Any = None) -> Any:
    return FIELD_POLICIES.get(_normalize_field_key(field_key), {}).get(key, default)

def get_field_weight(field_key: str, source_type: str, default: float = 0.75) -> float:
    policy = FIELD_POLICIES.get(_normalize_field_key(field_key), {})
    return float(policy.get("field_weights", {}).get(source_type, default))

def get_dispute_margin(field_key: str, default: float = 0.12) -> float:
    return float(get_policy_value(field_key, "dispute_margin", default))

def get_confirmed_threshold(field_key: str, default: float = 0.85) -> float:
    return float(get_policy_value(field_key, "confirmed_threshold", default))

def get_likely_threshold(field_key: str, default: float = 0.65) -> float:
    return float(get_policy_value(field_key, "likely_threshold", default))

def get_ttl_seconds(field_key: str, default: float = 30.0) -> float:
    return float(get_policy_value(field_key, "ttl_seconds", default))

def get_stabilization_window(field_key: str, default: float = 30.0) -> float:
    return float(get_policy_value(field_key, "stabilization_window_seconds", default))

def get_stabilization_min_transitions(field_key: str, default: int = 3) -> int:
    return int(get_policy_value(field_key, "stabilization_min_transitions", default))

def hard_delete_requires_direct_evidence(field_key: str) -> bool:
    return bool(get_policy_value(field_key, "hard_delete_requires_direct_evidence", False))

def derivative_bias_from_interface_failure(field_key: str) -> bool:
    return bool(get_policy_value(field_key, "derivative_bias_from_interface_failure", False))
