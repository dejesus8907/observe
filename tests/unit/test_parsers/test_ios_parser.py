from netobserv.connectors.base import DiscoveryTarget, RawCollectionResult
from netobserv.parsers.ios import IOSParser


def test_ios_parser_extracts_neighbors_arp_and_mac():
    parser = IOSParser()
    target = DiscoveryTarget(target_id="1", hostname="edge1", platform="ios")
    result = RawCollectionResult(
        target=target,
        workflow_id="wf",
        snapshot_id="snap",
        connector_type="ssh",
        success=True,
        raw_device_facts={"raw": """Cisco C9300 processor
Version 17.09.01
Processor board ID FOO123"""},
        raw_neighbors=[{"raw": """Device ID: spine1
Local Intf: Gi1/0/1
Port ID (outgoing port): Eth1
Management address: 10.0.0.1"""}],
        raw_arp_entries=[{"raw": "Internet 10.0.0.2 3 0011.2233.4455 ARPA Gi1/0/2"}],
        raw_mac_entries=[{"raw": "  10  aabb.ccdd.eeff   DYNAMIC  Gi1/0/3"}],
    )
    records = parser.parse(result)
    types = [r.record_type for r in records]
    assert "device" in types
    assert "neighbor" in types
    assert "arp" in types
    assert "mac" in types
