"""Unit tests for the canonical normalizer."""

from __future__ import annotations

import pytest

from netobserv.models.canonical import (
    CanonicalDevice,
    CanonicalInterface,
    CanonicalNeighborRelationship,
    CanonicalVLAN,
)
from netobserv.models.enums import DeviceStatus, InterfaceAdminState, InterfaceOperState
from netobserv.normalizers.canonical_normalizer import CanonicalNormalizer
from netobserv.parsers.base import ParsedRecord


@pytest.fixture
def normalizer() -> CanonicalNormalizer:
    return CanonicalNormalizer()


class TestDeviceNormalization:
    def test_normalizes_basic_device(self, normalizer: CanonicalNormalizer) -> None:
        record = ParsedRecord(
            record_type="device",
            source_system="ssh",
            data={
                "hostname": "router1",
                "vendor": "Arista",
                "model": "DCS-7050",
                "software_version": "4.28.0F",
                "status": "active",
            },
        )
        results = normalizer.normalize([record], snapshot_id="snap1")
        assert len(results) == 1
        device = results[0]
        assert isinstance(device, CanonicalDevice)
        assert device.hostname == "router1"
        assert device.vendor == "Arista"
        assert device.status == DeviceStatus.ACTIVE

    def test_unknown_status_defaults_to_unknown(self, normalizer: CanonicalNormalizer) -> None:
        record = ParsedRecord(
            record_type="device",
            source_system="ssh",
            data={"hostname": "router1", "status": "bogus_status"},
        )
        results = normalizer.normalize([record])
        assert isinstance(results[0], CanonicalDevice)
        assert results[0].status == DeviceStatus.UNKNOWN

    def test_sets_snapshot_id(self, normalizer: CanonicalNormalizer) -> None:
        record = ParsedRecord(
            record_type="device",
            source_system="ssh",
            data={"hostname": "router1"},
        )
        results = normalizer.normalize([record], snapshot_id="test-snap")
        assert results[0].snapshot_id == "test-snap"  # type: ignore[union-attr]

    def test_completeness_score_reflects_fields(self, normalizer: CanonicalNormalizer) -> None:
        # Only hostname provided
        record = ParsedRecord(
            record_type="device",
            source_system="ssh",
            data={"hostname": "r1"},
        )
        results = normalizer.normalize([record])
        device = results[0]
        assert isinstance(device, CanonicalDevice)
        assert device.quality.completeness_score < 1.0


class TestInterfaceNormalization:
    def test_normalizes_basic_interface(self, normalizer: CanonicalNormalizer) -> None:
        record = ParsedRecord(
            record_type="interface",
            source_system="ssh",
            data={
                "device_hostname": "router1",
                "name": "Ethernet1",
                "admin_state": "up",
                "oper_state": "up",
                "mtu": 9214,
            },
        )
        results = normalizer.normalize([record])
        assert len(results) == 1
        iface = results[0]
        assert isinstance(iface, CanonicalInterface)
        assert iface.name == "Ethernet1"
        assert iface.admin_state == InterfaceAdminState.UP
        assert iface.oper_state == InterfaceOperState.UP
        assert iface.mtu == 9214

    def test_interface_name_normalization(self, normalizer: CanonicalNormalizer) -> None:
        record = ParsedRecord(
            record_type="interface",
            source_system="ssh",
            data={"device_hostname": "r1", "name": "Gi0/1", "admin_state": "up"},
        )
        results = normalizer.normalize([record])
        iface = results[0]
        assert isinstance(iface, CanonicalInterface)
        # Gi0/1 should be expanded
        assert iface.normalized_name == "GigabitEthernet0/1"

    def test_loopback_type_inference(self, normalizer: CanonicalNormalizer) -> None:
        record = ParsedRecord(
            record_type="interface",
            source_system="ssh",
            data={"device_hostname": "r1", "name": "Loopback0"},
        )
        results = normalizer.normalize([record])
        from netobserv.models.enums import InterfaceType
        iface = results[0]
        assert isinstance(iface, CanonicalInterface)
        assert iface.type == InterfaceType.LOOPBACK

    def test_access_vlan_coercion(self, normalizer: CanonicalNormalizer) -> None:
        record = ParsedRecord(
            record_type="interface",
            source_system="ssh",
            data={"device_hostname": "r1", "name": "Eth1", "access_vlan": "100"},
        )
        results = normalizer.normalize([record])
        iface = results[0]
        assert isinstance(iface, CanonicalInterface)
        assert iface.access_vlan == 100


class TestNeighborNormalization:
    def test_normalizes_lldp_neighbor(self, normalizer: CanonicalNormalizer) -> None:
        record = ParsedRecord(
            record_type="neighbor",
            source_system="ssh",
            data={
                "local_hostname": "router1",
                "local_interface": "Ethernet1",
                "remote_hostname": "router2",
                "remote_interface": "Ethernet1",
                "protocol": "lldp",
            },
        )
        results = normalizer.normalize([record])
        assert len(results) == 1
        neighbor = results[0]
        assert isinstance(neighbor, CanonicalNeighborRelationship)
        assert neighbor.local_device_id == "router1"
        assert neighbor.remote_hostname == "router2"
        assert neighbor.protocol == "lldp"


class TestVLANNormalization:
    def test_normalizes_vlan(self, normalizer: CanonicalNormalizer) -> None:
        record = ParsedRecord(
            record_type="vlan",
            source_system="ssh",
            data={"vid": 100, "name": "prod", "status": "active"},
        )
        results = normalizer.normalize([record])
        vlan = results[0]
        assert isinstance(vlan, CanonicalVLAN)
        assert vlan.vid == 100
        assert vlan.name == "prod"

    def test_vid_string_coercion(self, normalizer: CanonicalNormalizer) -> None:
        record = ParsedRecord(
            record_type="vlan",
            source_system="ssh",
            data={"vid": "200"},
        )
        results = normalizer.normalize([record])
        vlan = results[0]
        assert isinstance(vlan, CanonicalVLAN)
        assert vlan.vid == 200

    def test_unknown_record_type_skipped(self, normalizer: CanonicalNormalizer) -> None:
        record = ParsedRecord(
            record_type="banana",
            source_system="ssh",
            data={},
        )
        results = normalizer.normalize([record])
        assert len(results) == 0
