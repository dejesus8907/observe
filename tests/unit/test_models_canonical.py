from netobserv.models.canonical import (
    CanonicalARPEntry,
    CanonicalDevice,
    CanonicalIPAddress,
    CanonicalInterface,
    CanonicalInventorySource,
    CanonicalRoute,
    CanonicalSubnet,
    CanonicalTopologyEdge,
    DataQualityMetadata,
)
from netobserv.models.enums import EdgeConfidence, InventorySourceType, NormalizationWarning


def test_device_tags_are_cleaned_and_deduped() -> None:
    device = CanonicalDevice(
        hostname="  spine-1  ",
        management_ip=" 10.0.0.1 ",
        tags=["core", " core ", "fabric", ""],
    )

    assert device.hostname == "spine-1"
    assert device.management_ip == "10.0.0.1"
    assert device.tags == ["core", "fabric"]


def test_interface_normalizes_addresses_mac_and_vlans() -> None:
    iface = CanonicalInterface(
        device_id="leaf-1",
        name=" Ethernet1 ",
        mac_address="AA-BB-CC-DD-EE-FF",
        ip_addresses=["10.0.0.1/24", " 10.0.0.1/24 ", "2001:db8::1/64"],
        trunk_vlans=[20, "10", "20", "30"],
        allowed_vlans="100",
    )

    assert iface.name == "Ethernet1"
    assert iface.mac_address == "aa:bb:cc:dd:ee:ff"
    assert iface.ip_addresses == ["10.0.0.1/24", "2001:db8::1/64"]
    assert iface.trunk_vlans == [10, 20, 30]
    assert iface.allowed_vlans == [100]


def test_ip_and_subnet_family_are_derived() -> None:
    ip = CanonicalIPAddress(address="2001:db8::1/64")
    subnet = CanonicalSubnet(prefix="10.1.1.8/24")

    assert ip.family == 6
    assert subnet.prefix == "10.1.1.0/24"
    assert subnet.family == 4


def test_edge_populates_evidence_count_and_confidence_score() -> None:
    edge = CanonicalTopologyEdge(
        source_node_id="a",
        target_node_id="b",
        evidence_source=["lldp", "cdp"],
        confidence=EdgeConfidence.HIGH,
    )

    assert edge.evidence_count == 2
    assert edge.confidence_score == 0.95


def test_arp_normalizes_mac_and_family() -> None:
    arp = CanonicalARPEntry(device_id="leaf-1", ip_address="2001:db8::10", mac_address="aabb.ccdd.eeff")

    assert arp.family == 6
    assert arp.mac_address == "aa:bb:cc:dd:ee:ff"


def test_inventory_source_uses_enum_type() -> None:
    source = CanonicalInventorySource(name=" NetBox ", source_type="netbox")
    assert source.name == "NetBox"
    assert source.source_type == InventorySourceType.NETBOX


def test_data_quality_metadata_dedupes_warning_and_references() -> None:
    quality = DataQualityMetadata(
        normalization_warnings=[NormalizationWarning.PARSE_ERROR, "parse_error"],
        unresolved_references=[" Eth1 ", "Eth1", ""],
    )
    assert quality.normalization_warnings == [NormalizationWarning.PARSE_ERROR]
    assert quality.unresolved_references == ["Eth1"]


def test_route_prefix_normalizes_network() -> None:
    route = CanonicalRoute(device_id="leaf-1", prefix="10.2.2.9/24")
    assert route.prefix == "10.2.2.0/24"
