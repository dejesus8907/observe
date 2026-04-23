POLICY = {
    "field_key": "interface.oper_status",
    "preferred_sources": ["gnmi_stream", "netconf_notification", "snmp_trap", "syslog_structured", "ssh_diff"],
    "ttl_seconds": 30.0,
    "dispute_margin": 0.10,
    "confirmed_threshold": 0.85,
    "likely_threshold": 0.65,
    "stabilization_window_seconds": 30.0,
    "stabilization_min_transitions": 3,
    "field_weights": {
        "gnmi_stream": 1.00,
        "netconf_notification": 0.98,
        "snmp_trap": 0.90,
        "syslog_structured": 0.70,
        "ssh_diff": 0.55,
    },
}
