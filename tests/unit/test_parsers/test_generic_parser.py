from netobserv.connectors.base import DiscoveryTarget, RawCollectionResult
from netobserv.parsers.generic import GenericParser


def test_generic_parser_coerces_non_mapping_entries():
    parser = GenericParser()
    target = DiscoveryTarget(target_id="1", hostname="g1")
    result = RawCollectionResult(
        target=target,
        workflow_id="wf",
        snapshot_id="snap",
        connector_type="rest",
        success=True,
        raw_interfaces=["Ethernet1"],
    )
    records = parser.parse(result)
    iface = [r for r in records if r.record_type == "interface"][0]
    assert iface.data["value"] == "Ethernet1"
