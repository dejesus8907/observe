POLICY = {
    "field_key": "edge.exists",
    "preferred_sources": ["gnmi_stream", "netconf_notification", "ssh_diff", "syslog_structured"],
    "ttl_seconds": 20.0,
    "dispute_margin": 0.14,
    "confirmed_threshold": 0.88,
    "likely_threshold": 0.68,
    "stabilization_window_seconds": 25.0,
    "stabilization_min_transitions": 2,
    "field_weights": {
        "gnmi_stream": 0.95,
        "netconf_notification": 0.92,
        "ssh_diff": 0.70,
        "syslog_structured": 0.60,
        "snmp_trap": 0.55,
    },
    "hard_delete_requires_direct_evidence": True,
}
