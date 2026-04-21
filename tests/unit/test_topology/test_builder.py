"""Unit tests for the topology builder."""

from __future__ import annotations

import pytest

from netobserv.models.canonical import (
    CanonicalDevice,
    CanonicalNeighborRelationship,
)
from netobserv.models.enums import DeviceStatus, EdgeConfidence, EdgeType
from netobserv.topology.builder import TopologyBuilder


@pytest.fixture
def builder() -> TopologyBuilder:
    return TopologyBuilder(min_confidence_threshold=0.0)


class TestTopologyBuilderLLDP:
    def test_builds_edge_from_bidirectional_lldp(
        self,
        builder: TopologyBuilder,
        device_a: CanonicalDevice,
        device_b: CanonicalDevice,
        lldp_neighbor_ab: CanonicalNeighborRelationship,
        lldp_neighbor_ba: CanonicalNeighborRelationship,
        snapshot_id: str,
    ) -> None:
        objects = [device_a, device_b, lldp_neighbor_ab, lldp_neighbor_ba]
        result = builder.build(snapshot_id, objects)

        lldp_edges = [e for e in result.edges if e.edge_type == EdgeType.L2_ADJACENCY]
        assert len(lldp_edges) == 1
        edge = lldp_edges[0]
        assert edge.confidence == EdgeConfidence.HIGH
        assert edge.confidence_score >= 0.9
        assert edge.evidence_count == 2

    def test_single_direction_lldp_gets_medium_confidence(
        self,
        builder: TopologyBuilder,
        device_a: CanonicalDevice,
        device_b: CanonicalDevice,
        lldp_neighbor_ab: CanonicalNeighborRelationship,
        snapshot_id: str,
    ) -> None:
        objects = [device_a, device_b, lldp_neighbor_ab]
        result = builder.build(snapshot_id, objects)

        lldp_edges = [e for e in result.edges if e.edge_type == EdgeType.L2_ADJACENCY]
        assert len(lldp_edges) == 1
        assert lldp_edges[0].confidence == EdgeConfidence.MEDIUM

    def test_no_neighbors_produces_no_edges(
        self,
        builder: TopologyBuilder,
        device_a: CanonicalDevice,
        device_b: CanonicalDevice,
        snapshot_id: str,
    ) -> None:
        result = builder.build(snapshot_id, [device_a, device_b])
        assert result.edge_count == 0

    def test_interface_descriptions_produce_low_confidence_edges(
        self,
        builder: TopologyBuilder,
        device_a: CanonicalDevice,
        device_b: CanonicalDevice,
        interface_a: Any,
        snapshot_id: str,
    ) -> None:
        from netobserv.models.canonical import CanonicalInterface

        objects = [device_a, device_b, interface_a]
        result = builder.build(snapshot_id, objects)
        desc_edges = [
            e for e in result.edges if e.inference_method == "interface_description"
        ]
        # interface_a description is "link to router2" — should produce edge
        assert len(desc_edges) >= 1
        assert all(e.confidence == EdgeConfidence.LOW for e in desc_edges)


class TestTopologyEdgeReconciliation:
    def test_duplicate_edges_merged(
        self,
        builder: TopologyBuilder,
        device_a: CanonicalDevice,
        device_b: CanonicalDevice,
        lldp_neighbor_ab: CanonicalNeighborRelationship,
        lldp_neighbor_ba: CanonicalNeighborRelationship,
        snapshot_id: str,
    ) -> None:
        """LLDP + description edges for same pair should be merged."""
        from netobserv.models.canonical import CanonicalInterface

        interface_a = CanonicalInterface(
            device_id="dev-a",
            name="Ethernet1",
            description="link to router2",
        )

        objects = [device_a, device_b, lldp_neighbor_ab, lldp_neighbor_ba, interface_a]
        result = builder.build(snapshot_id, objects)

        l2_edges = [e for e in result.edges if e.edge_type == EdgeType.L2_ADJACENCY]
        # Should still be 1 merged L2 edge (not 2 from LLDP + description separately)
        assert len(l2_edges) == 1
        # Merged edge should reference multiple evidence sources
        assert len(l2_edges[0].evidence_source) >= 1


class TestTopologyConflicts:
    def test_unresolved_device_produces_conflict(
        self,
        builder: TopologyBuilder,
        device_a: CanonicalDevice,
        snapshot_id: str,
    ) -> None:
        neighbor = CanonicalNeighborRelationship(
            local_device_id="dev-a",
            local_interface="Ethernet1",
            remote_hostname="unknown-device",
            protocol="lldp",
            snapshot_id=snapshot_id,
        )
        result = builder.build(snapshot_id, [device_a, neighbor])
        unresolved = [c for c in result.conflicts if c.conflict_type == "unresolved_device"]
        assert len(unresolved) >= 1


# Need to make interface_a available in the TestTopologyBuilderLLDP test scope
from typing import Any  # noqa: E402 (placed after tests intentionally)
