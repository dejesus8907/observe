"""Topology conflict detection."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any

from netobserv.models.canonical import (
    CanonicalDevice,
    CanonicalNeighborRelationship,
    CanonicalTopologyEdge,
)
from netobserv.observability.logging import get_logger

logger = get_logger("topology.conflict")


@dataclass
class TopologyConflict:
    """A detected conflict in the topology evidence."""

    conflict_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    snapshot_id: str | None = None
    conflict_type: str = "unknown"
    involved_edge_ids: list[str] = field(default_factory=list)
    description: str = ""
    severity: str = "medium"
    evidence: list[str] = field(default_factory=list)


class ConflictDetector:
    """
    Detects topology conflicts across the edge set.

    Conflict types:
    - inconsistent_neighbors: A says it connects to B via Eth1, B doesn't agree
    - duplicate_identity: two edges claim the same interface is connected to two different peers
    - missing_peer: LLDP neighbor references a hostname not in the discovered set
    - impossible_duplex: edge speed/duplex inconsistency
    - unresolved_device: remote hostname couldn't be mapped to a known device
    """

    def detect(
        self,
        edges: list[CanonicalTopologyEdge],
        devices: dict[str, CanonicalDevice],
        neighbors: list[CanonicalNeighborRelationship],
    ) -> list[TopologyConflict]:
        conflicts: list[TopologyConflict] = []
        snapshot_id = edges[0].snapshot_id if edges else None

        conflicts.extend(self._detect_duplicate_interface_claims(edges, snapshot_id))
        conflicts.extend(self._detect_unresolved_devices(neighbors, devices, snapshot_id))
        conflicts.extend(self._detect_inconsistent_neighbors(neighbors, devices, snapshot_id))

        if conflicts:
            logger.info(
                "Topology conflicts detected",
                count=len(conflicts),
                snapshot_id=snapshot_id,
            )

        return conflicts

    def _detect_duplicate_interface_claims(
        self,
        edges: list[CanonicalTopologyEdge],
        snapshot_id: str | None,
    ) -> list[TopologyConflict]:
        """
        Detect when the same local interface is claimed by two different edges
        connecting to different remote devices.
        """
        from collections import defaultdict

        # Map (device, interface) -> list of edges using it
        iface_to_edges: dict[tuple[str, str], list[CanonicalTopologyEdge]] = defaultdict(list)
        for edge in edges:
            if edge.source_interface:
                iface_to_edges[(edge.source_node_id, edge.source_interface)].append(edge)
            if edge.target_interface:
                iface_to_edges[(edge.target_node_id, edge.target_interface)].append(edge)

        conflicts: list[TopologyConflict] = []
        for (device_id, iface_name), iface_edges in iface_to_edges.items():
            # More than one edge using same interface to different peers = conflict
            peers = set()
            for e in iface_edges:
                peer = e.target_node_id if e.source_node_id == device_id else e.source_node_id
                peers.add(peer)

            if len(peers) > 1:
                conflicts.append(
                    TopologyConflict(
                        snapshot_id=snapshot_id,
                        conflict_type="duplicate_interface_claim",
                        involved_edge_ids=[e.edge_id for e in iface_edges],
                        description=(
                            f"Interface {iface_name} on {device_id} "
                            f"is claimed by {len(peers)} different topology edges."
                        ),
                        severity="high",
                        evidence=[f"Connected to: {p}" for p in peers],
                    )
                )

        return conflicts

    def _detect_unresolved_devices(
        self,
        neighbors: list[CanonicalNeighborRelationship],
        devices: dict[str, CanonicalDevice],
        snapshot_id: str | None,
    ) -> list[TopologyConflict]:
        """Flag LLDP/CDP records that reference unknown remote devices."""
        hostname_set = {d.hostname for d in devices.values()}
        conflicts: list[TopologyConflict] = []

        for n in neighbors:
            remote = n.remote_hostname
            if remote and remote not in hostname_set:
                conflicts.append(
                    TopologyConflict(
                        snapshot_id=snapshot_id,
                        conflict_type="unresolved_device",
                        description=(
                            f"LLDP/CDP neighbor '{remote}' on {n.local_device_id} "
                            f"port {n.local_interface} was not discovered."
                        ),
                        severity="low",
                        evidence=[f"Protocol: {n.protocol}", f"Local port: {n.local_interface}"],
                    )
                )

        return conflicts

    def _detect_inconsistent_neighbors(
        self,
        neighbors: list[CanonicalNeighborRelationship],
        devices: dict[str, CanonicalDevice],
        snapshot_id: str | None,
    ) -> list[TopologyConflict]:
        """
        If device A reports device B as neighbor on port X,
        but device B reports device A on a completely different port,
        that may indicate a cabling inconsistency.
        """
        # Build lookup: (local_hostname, remote_hostname) -> local_interface
        forward: dict[tuple[str, str], str] = {}
        conflicts: list[TopologyConflict] = []

        for n in neighbors:
            local = n.local_device_id
            remote = n.remote_hostname or n.remote_device_id or ""
            if not remote:
                continue
            key = (local, remote)
            forward[key] = n.local_interface

        for n in neighbors:
            local = n.local_device_id
            remote = n.remote_hostname or n.remote_device_id or ""
            if not remote:
                continue
            # Check if the reverse record exists and points to the same interfaces
            reverse_key = (remote, local)
            if reverse_key in forward:
                reverse_local_if = forward[reverse_key]
                if n.remote_interface and reverse_local_if:
                    if n.remote_interface != reverse_local_if:
                        conflicts.append(
                            TopologyConflict(
                                snapshot_id=snapshot_id,
                                conflict_type="inconsistent_neighbor_interface",
                                description=(
                                    f"{local} reports {remote}'s port as {n.remote_interface}, "
                                    f"but {remote} reports its own port as {reverse_local_if}."
                                ),
                                severity="medium",
                                evidence=[
                                    f"Forward: {local} -> {n.local_interface} -> {remote} {n.remote_interface}",
                                    f"Reverse: {remote} -> {reverse_local_if} -> {local}",
                                ],
                            )
                        )

        return conflicts
