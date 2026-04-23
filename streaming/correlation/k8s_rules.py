"""Kubernetes causal propagation rules for the correlation engine.

These rules extend the existing network-layer propagation rules with
K8s-aware causal chains:

    NODE_NOT_READY → POD_NOT_READY         (pods on the node fail)
    POD_NOT_READY → SERVICE_ENDPOINTS_EMPTY (service loses backends)
    NODE_NETWORK_UNAVAILABLE → POD_NOT_READY
    POD_CRASH_LOOP → SERVICE_ENDPOINTS_CHANGED
    INTERFACE_DOWN → NODE_NETWORK_UNAVAILABLE  (network→K8s bridge)
    DEVICE_REACHABILITY_LOST → NODE_NOT_READY  (network→K8s bridge)

Usage:
    from netobserv.streaming.correlation.k8s_rules import K8S_PROPAGATION_RULES
    engine = CausalGraphEngine(rules=DEFAULT_RULES + K8S_PROPAGATION_RULES)
"""

from netobserv.streaming.correlation.causal_graph import PropagationRule


# K8s-internal causal chains
K8S_PROPAGATION_RULES = [
    # Node failure → pod failure
    PropagationRule(
        "k8s_node_not_ready", "k8s_pod_not_ready",
        max_delay_seconds=30.0, base_confidence=0.92,
        same_device_required=True,     # pod runs on the same node (device_id)
    ),
    PropagationRule(
        "k8s_node_network_unavailable", "k8s_pod_not_ready",
        max_delay_seconds=30.0, base_confidence=0.88,
        same_device_required=True,
    ),
    PropagationRule(
        "k8s_node_pressure", "k8s_pod_evicted",
        max_delay_seconds=60.0, base_confidence=0.80,
        same_device_required=True,
    ),

    # Pod failure → service degradation
    PropagationRule(
        "k8s_pod_not_ready", "k8s_service_endpoints_changed",
        max_delay_seconds=15.0, base_confidence=0.85,
        same_device_required=False,
    ),
    PropagationRule(
        "k8s_pod_not_ready", "k8s_service_endpoints_empty",
        max_delay_seconds=15.0, base_confidence=0.90,
        same_device_required=False,
    ),
    PropagationRule(
        "k8s_pod_crash_loop", "k8s_service_endpoints_changed",
        max_delay_seconds=30.0, base_confidence=0.82,
        same_device_required=False,
    ),

    # Restart → crash loop
    PropagationRule(
        "k8s_pod_restarted", "k8s_pod_crash_loop",
        max_delay_seconds=120.0, base_confidence=0.75,
        same_device_required=True,
    ),

    # Deployment stall from pod failures
    PropagationRule(
        "k8s_pod_crash_loop", "k8s_deployment_rollout_stalled",
        max_delay_seconds=60.0, base_confidence=0.78,
        same_device_required=False,
    ),
    PropagationRule(
        "k8s_deployment_replica_mismatch", "k8s_deployment_rollout_stalled",
        max_delay_seconds=120.0, base_confidence=0.70,
        same_device_required=False,
    ),

    # ================================================================
    # Cross-layer bridge: network events → K8s symptoms
    # ================================================================

    # Physical link down → node network unavailable
    PropagationRule(
        "interface_down", "k8s_node_network_unavailable",
        max_delay_seconds=15.0, base_confidence=0.82,
        same_device_required=True,     # node is registered as device
    ),

    # Device unreachable → node not ready
    PropagationRule(
        "device_reachability_lost", "k8s_node_not_ready",
        max_delay_seconds=20.0, base_confidence=0.85,
        same_device_required=True,
    ),

    # BGP session drop → node network issues (for nodes with BGP, e.g. Calico)
    PropagationRule(
        "bgp_session_dropped", "k8s_node_network_unavailable",
        max_delay_seconds=30.0, base_confidence=0.72,
        same_device_required=True,
    ),
]
