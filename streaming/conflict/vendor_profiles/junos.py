PROFILE = {
    "vendor": "junos",
    "overrides": {
        "interface.oper_status": {
            "netconf_notification": 1.00,
            "gnmi_stream": 0.97
        },
        "bgp.session_state": {
            "netconf_notification": 0.96,
            "syslog_structured": 0.78
        }
    }
}
