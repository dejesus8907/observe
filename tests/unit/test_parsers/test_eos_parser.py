"""Unit tests for the EOS parser."""

from __future__ import annotations

import pytest

from netobserv.parsers.eos import EOSParser
from netobserv.tests.conftest import raw_result_eos  # noqa: F401 (fixture)


@pytest.fixture
def parser() -> EOSParser:
    return EOSParser()


class TestEOSParser:
    def test_parses_device_facts(
        self, parser: EOSParser, raw_result_eos: Any
    ) -> None:
        records = parser.parse(raw_result_eos)
        device_records = [r for r in records if r.record_type == "device"]
        assert len(device_records) == 1
        d = device_records[0]
        assert d.data["vendor"] == "Arista"
        assert d.data["model"] == "DCS-7050CX3-32S"
        assert d.data["software_version"] == "4.28.0F"

    def test_parses_interfaces(
        self, parser: EOSParser, raw_result_eos: Any
    ) -> None:
        records = parser.parse(raw_result_eos)
        iface_records = [r for r in records if r.record_type == "interface"]
        assert len(iface_records) >= 1
        eth1 = next(r for r in iface_records if "Ethernet1" in r.data.get("name", ""))
        assert eth1.data["admin_state"] == "up"
        assert eth1.data["oper_state"] == "up"
        assert eth1.data["mtu"] == 9214

    def test_parses_lldp_neighbors(
        self, parser: EOSParser, raw_result_eos: Any
    ) -> None:
        records = parser.parse(raw_result_eos)
        neighbor_records = [r for r in records if r.record_type == "neighbor"]
        assert len(neighbor_records) == 1
        n = neighbor_records[0]
        assert n.data["remote_hostname"] == "router2"
        assert n.data["protocol"] == "lldp"

    def test_source_system_is_eos_eapi(
        self, parser: EOSParser, raw_result_eos: Any
    ) -> None:
        records = parser.parse(raw_result_eos)
        assert all(r.source_system == "eos_eapi" for r in records)


from typing import Any  # noqa: E402
