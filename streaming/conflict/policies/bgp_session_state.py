POLICY = {
    "field_key": "bgp.session_state",
    "preferred_sources": ["gnmi_stream", "netconf_notification", "syslog_structured", "snmp_trap"],
    "ttl_seconds": 15.0,
    "dispute_margin": 0.12,
    "confirmed_threshold": 0.86,
    "likely_threshold": 0.66,
    "stabilization_window_seconds": 20.0,
    "stabilization_min_transitions": 2,
    "field_weights": {
        "gnmi_stream": 0.95,
        "netconf_notification": 0.93,
        "syslog_structured": 0.75,
        "snmp_trap": 0.60,
        "ssh_diff": 0.40,
    },
    "derivative_bias_from_interface_failure": True,
}
