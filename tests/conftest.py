"""Shared pytest fixtures for NetObserv tests."""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from netobserv.models.canonical import (
    CanonicalDevice,
    CanonicalInterface,
    CanonicalNeighborRelationship,
    CanonicalTopologyEdge,
    CanonicalVLAN,
    CanonicalDriftRecord,
    DataQualityMetadata,
)
from netobserv.models.enums import (
    DeviceStatus,
    DriftSeverity,
    EdgeConfidence,
    EdgeType,
    InterfaceAdminState,
    InterfaceOperState,
    InterfaceType,
    MismatchType,
    VLANStatus,
)
from netobserv.connectors.base import (
    CollectionContext,
    ConnectorCredential,
    DiscoveryTarget,
    RawCollectionResult,
)
from netobserv.parsers.base import ParsedRecord


# ---------------------------------------------------------------------------
# Fixture: snapshot_id
# ---------------------------------------------------------------------------


@pytest.fixture
def snapshot_id() -> str:
    return str(uuid.uuid4())


@pytest.fixture
def workflow_id() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Device fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def device_a(snapshot_id: str) -> CanonicalDevice:
    return CanonicalDevice(
        device_id="dev-a",
        hostname="router1",
        management_ip="10.0.0.1",
        vendor="Arista",
        model="DCS-7050CX3-32S",
        software_version="4.28.0F",
        device_role="spine",
        site="dc1",
        status=DeviceStatus.ACTIVE,
        snapshot_id=snapshot_id,
    )


@pytest.fixture
def device_b(snapshot_id: str) -> CanonicalDevice:
    return CanonicalDevice(
        device_id="dev-b",
        hostname="router2",
        management_ip="10.0.0.2",
        vendor="Arista",
        model="DCS-7050CX3-32S",
        software_version="4.28.0F",
        device_role="leaf",
        site="dc1",
        status=DeviceStatus.ACTIVE,
        snapshot_id=snapshot_id,
    )


@pytest.fixture
def device_c(snapshot_id: str) -> CanonicalDevice:
    return CanonicalDevice(
        device_id="dev-c",
        hostname="router3",
        management_ip="10.0.0.3",
        vendor="Cisco",
        model="Catalyst-9300",
        device_role="access",
        site="dc1",
        status=DeviceStatus.ACTIVE,
        snapshot_id=snapshot_id,
    )


# ---------------------------------------------------------------------------
# Interface fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def interface_a(snapshot_id: str) -> CanonicalInterface:
    return CanonicalInterface(
        device_id="dev-a",
        name="Ethernet1",
        normalized_name="Ethernet1",
        type=InterfaceType.ETHERNET,
        admin_state=InterfaceAdminState.UP,
        oper_state=InterfaceOperState.UP,
        mtu=9214,
        speed=100000000,
        description="link to router2",
        snapshot_id=snapshot_id,
    )


@pytest.fixture
def interface_b(snapshot_id: str) -> CanonicalInterface:
    return CanonicalInterface(
        device_id="dev-b",
        name="Ethernet1",
        normalized_name="Ethernet1",
        type=InterfaceType.ETHERNET,
        admin_state=InterfaceAdminState.UP,
        oper_state=InterfaceOperState.UP,
        mtu=9214,
        speed=100000000,
        description="link to router1",
        snapshot_id=snapshot_id,
    )


# ---------------------------------------------------------------------------
# Neighbor fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def lldp_neighbor_ab(snapshot_id: str) -> CanonicalNeighborRelationship:
    """LLDP neighbor: router1 Eth1 -> router2 Eth1"""
    return CanonicalNeighborRelationship(
        local_device_id="dev-a",
        local_interface="Ethernet1",
        remote_device_id="dev-b",
        remote_hostname="router2",
        remote_interface="Ethernet1",
        protocol="lldp",
        snapshot_id=snapshot_id,
    )


@pytest.fixture
def lldp_neighbor_ba(snapshot_id: str) -> CanonicalNeighborRelationship:
    """Reverse LLDP neighbor: router2 Eth1 -> router1 Eth1"""
    return CanonicalNeighborRelationship(
        local_device_id="dev-b",
        local_interface="Ethernet1",
        remote_device_id="dev-a",
        remote_hostname="router1",
        remote_interface="Ethernet1",
        protocol="lldp",
        snapshot_id=snapshot_id,
    )


# ---------------------------------------------------------------------------
# VLAN fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def vlan_100(snapshot_id: str) -> CanonicalVLAN:
    return CanonicalVLAN(
        vid=100,
        name="prod-web",
        site="dc1",
        status=VLANStatus.ACTIVE,
        snapshot_id=snapshot_id,
    )


@pytest.fixture
def vlan_200(snapshot_id: str) -> CanonicalVLAN:
    return CanonicalVLAN(
        vid=200,
        name="prod-db",
        site="dc1",
        status=VLANStatus.ACTIVE,
        snapshot_id=snapshot_id,
    )


# ---------------------------------------------------------------------------
# Topology edge fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def topology_edge_ab(snapshot_id: str) -> CanonicalTopologyEdge:
    return CanonicalTopologyEdge(
        snapshot_id=snapshot_id,
        source_node_id="dev-a",
        target_node_id="dev-b",
        source_interface="Ethernet1",
        target_interface="Ethernet1",
        edge_type=EdgeType.L2_ADJACENCY,
        evidence_source=["lldp"],
        evidence_count=2,
        confidence=EdgeConfidence.HIGH,
        confidence_score=0.9,
        is_direct=True,
        inference_method="lldp_cdp",
    )


# ---------------------------------------------------------------------------
# Drift record fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def drift_device_missing(snapshot_id: str) -> CanonicalDriftRecord:
    return CanonicalDriftRecord(
        snapshot_id=snapshot_id,
        object_type="device",
        mismatch_type=MismatchType.MISSING_IN_ACTUAL,
        severity=DriftSeverity.CRITICAL,
        explanation="Device 'router1' not discovered.",
        remediation_hint="Check reachability.",
        confidence=1.0,
    )


# ---------------------------------------------------------------------------
# Raw collection result fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def raw_result_eos(snapshot_id: str, workflow_id: str) -> RawCollectionResult:
    target = DiscoveryTarget(
        target_id="t1",
        hostname="router1",
        management_ip="10.0.0.1",
        transport="rest",
        platform="eos",
    )
    return RawCollectionResult(
        target=target,
        workflow_id=workflow_id,
        snapshot_id=snapshot_id,
        connector_type="rest",
        success=True,
        raw_device_facts={
            "modelName": "DCS-7050CX3-32S",
            "version": "4.28.0F",
            "serialNumber": "SN123456",
        },
        raw_interfaces=[
            {
                "interfaces": {
                    "Ethernet1": {
                        "lineProtocolStatus": "up",
                        "interfaceStatus": "connected",
                        "description": "link to router2",
                        "mtu": 9214,
                        "bandwidth": 100000000,
                        "physicalAddress": "aa:bb:cc:dd:ee:01",
                        "interfaceAddress": {
                            "primaryIp": {"address": "10.1.1.1", "maskLen": 30}
                        },
                    }
                }
            }
        ],
        raw_neighbors=[
            {
                "lldpNeighbors": [
                    {
                        "port": "Ethernet1",
                        "neighborDevice": "router2",
                        "neighborPort": "Ethernet1",
                    }
                ]
            }
        ],
    )


# ---------------------------------------------------------------------------
# Collection context fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def collection_context(workflow_id: str, snapshot_id: str) -> CollectionContext:
    return CollectionContext(
        workflow_id=workflow_id,
        snapshot_id=snapshot_id,
        timeout=5,
        retries=0,
        read_only=True,
    )
