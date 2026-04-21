from netobserv.connectors.base import DiscoveryTarget, RawCollectionResult
from netobserv.parsers.netbox import NetBoxIntendedStateParser


def test_netbox_parser_includes_prefixes_as_routes():
    parser = NetBoxIntendedStateParser()
    target = DiscoveryTarget(target_id="1", hostname="netbox", platform="netbox")
    result = RawCollectionResult(
        target=target,
        workflow_id="wf",
        snapshot_id="snap",
        connector_type="rest",
        success=True,
        data={"source": "netbox", "prefixes": [{"prefix": "10.0.0.0/24", "status": {"value": "active"}}]},
    )
    records = parser.parse(result)
    route = [r for r in records if r.record_type == "route"][0]
    assert route.data["prefix"] == "10.0.0.0/24"
