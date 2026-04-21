from __future__ import annotations

import json

import pytest

from netobserv.inventory_sources.base import InventoryTarget, dedupe_targets
from netobserv.inventory_sources.ip_range import IPRangeInventorySource
from netobserv.inventory_sources.netbox import NetBoxInventorySource
from netobserv.inventory_sources.static import StaticFileInventorySource


@pytest.mark.asyncio
async def test_static_source_accepts_bare_list_and_dedupes(tmp_path):
    payload = [
        {"hostname": "r1", "management_ip": "10.0.0.1", "tags": ["Core", "core"]},
        {"hostname": "r1", "management_ip": "10.0.0.1", "platform": "ios"},
    ]
    path = tmp_path / "hosts.json"
    path.write_text(json.dumps(payload))

    targets = await StaticFileInventorySource(path).load_targets()

    assert len(targets) == 1
    assert targets[0].hostname == "r1"
    assert targets[0].management_ip == "10.0.0.1"
    assert targets[0].platform == "ios"
    assert targets[0].tags == ["core"]


@pytest.mark.asyncio
async def test_ip_range_source_respects_exclusions_and_max_hosts():
    source = IPRangeInventorySource(
        ["10.0.0.0/29"],
        max_hosts=3,
        exclude=["10.0.0.2"],
        tags=["Lab"],
    )
    targets = await source.load_targets()

    assert [t.management_ip for t in targets] == ["10.0.0.1", "10.0.0.3", "10.0.0.4"]
    assert all(t.tags == ["lab"] for t in targets)


@pytest.mark.asyncio
async def test_netbox_source_skips_devices_without_primary_ip_by_default():
    source = NetBoxInventorySource("https://netbox.local", "token")

    target = source._device_to_target({"id": 1, "name": "leaf1"})
    assert target is None

    source2 = NetBoxInventorySource(
        "https://netbox.local", "token", include_devices_without_primary_ip=True
    )
    target2 = source2._device_to_target({"id": 1, "name": "leaf1"})
    assert target2 is not None
    assert target2.target_id == "netbox-1"


def test_inventory_target_uses_stable_id_and_dedupe_merges_metadata():
    a = InventoryTarget(hostname="edge1", management_ip="192.0.2.1", source="static_file")
    b = InventoryTarget(
        hostname="edge1",
        management_ip="192.0.2.1",
        source="static_file",
        platform="eos",
        tags=["wan"],
        metadata={"a": 1},
    )

    merged = dedupe_targets([a, b])
    assert len(merged) == 1
    assert merged[0].platform == "eos"
    assert merged[0].tags == ["wan"]
    assert merged[0].metadata["a"] == 1
