from datetime import datetime, timezone
from .correlator import EventCorrelator
from .causal_graph import TopologyAdjacency

def _utc_now():
    return datetime.now(timezone.utc)


class CorrelationFacade:
    def __init__(self, correlator=None, topology=None):
        self._topology = topology or TopologyAdjacency()
        self.correlator = correlator or EventCorrelator(topology=self._topology)

    def correlate_event(self, event, *, now=None):
        event_dict = {
            "event_id": event.event_id,
            "kind": str(event.kind),
            "device_id": getattr(event, "device_id", None),
            "interface_name": getattr(event, "interface_name", None),
            "neighbor_device_id": getattr(event, "neighbor_device_id", None),
            "field_path": getattr(event, "field_path", None),
            "received_at": event.received_at,
            "detected_at": getattr(event, "detected_at", None),
        }
        return self.correlator.correlate(event_dict, now=now or event.received_at)

    def rank_root_causes(self, cluster_id):
        return self.correlator.rank_root_causes(cluster_id)

    def update_topology(self, edges):
        """Refresh the topology adjacency from the topology patcher's edge list."""
        self._topology = TopologyAdjacency()
        self._topology.seed_from_edges(edges)
        self.correlator.set_topology(self._topology)
