from __future__ import annotations

from dataclasses import dataclass

from .models import EvidenceRecord, SourceHealthState, SubjectType
from .policy_profiles import get_field_policy
from .vendor_profile_loader import get_vendor_override


@dataclass(slots=True)
class TrustContext:
    subject_type: SubjectType
    field_name: str


class DefaultTrustModel:
    BASE_TRUST: dict[str, float] = {
        "gnmi_stream": 1.00,
        "netconf_notification": 0.95,
        "snmp_trap": 0.80,
        "syslog_structured": 0.65,
        "ssh_diff": 0.55,
        "periodic_poll": 0.45,
        "manual_import": 0.30,
    }

    FIELD_WEIGHTS: dict[tuple[str, str], float] = {
        ("gnmi_stream", "interface.oper_status"): 1.00,
        ("snmp_trap", "interface.oper_status"): 0.90,
        ("syslog_structured", "interface.oper_status"): 0.70,
        ("ssh_diff", "interface.oper_status"): 0.55,
        ("gnmi_stream", "edge.exists"): 0.95,
        ("syslog_structured", "edge.exists"): 0.60,
        ("ssh_diff", "edge.exists"): 0.70,
        ("gnmi_stream", "bgp.session_state"): 0.95,
        ("syslog_structured", "bgp.session_state"): 0.75,
        ("snmp_trap", "bgp.session_state"): 0.60,
    }

    HEALTH_WEIGHTS: dict[SourceHealthState, float] = {
        SourceHealthState.HEALTHY: 1.0,
        SourceHealthState.DEGRADED: 0.7,
        SourceHealthState.UNHEALTHY: 0.0,
        SourceHealthState.UNKNOWN: 0.85,
    }

    def base_trust(self, evidence: EvidenceRecord) -> float:
        return self.BASE_TRUST.get(evidence.source_type, 0.40)

    def field_weight(self, evidence: EvidenceRecord, context: TrustContext) -> float:
        field_key = f"{context.subject_type.value}.{context.field_name}"
        vendor = None
        if isinstance(evidence.metadata, dict):
            vendor = evidence.metadata.get("vendor")
        override = get_vendor_override(vendor, field_key, evidence.source_type, None)
        if override is not None:
            return float(override)
        policy = get_field_policy(field_key)
        if policy:
            weights = policy.get("field_weights", {})
            if evidence.source_type in weights:
                return float(weights[evidence.source_type])
        key = (evidence.source_type, field_key)
        return self.FIELD_WEIGHTS.get(key, 0.75)

    def health_weight(self, evidence: EvidenceRecord) -> float:
        return self.HEALTH_WEIGHTS.get(evidence.source_health, 0.5)

    def source_score(self, evidence: EvidenceRecord, context: TrustContext) -> float:
        return self.base_trust(evidence) * self.field_weight(evidence, context) * self.health_weight(evidence)
