"""Unit tests for SourceTrustRegistry and SourceTrustProfile."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from netobserv.conflict_resolution.source_trust import (
    SourceTrustProfile,
    SourceTrustRegistry,
)
from netobserv.models.enums import SourceTrustTier


class TestSourceTrustProfile:
    def test_base_weight_is_within_range(self) -> None:
        profile = SourceTrustProfile(
            source_name="test", tier=SourceTrustTier.FIRST_PARTY
        )
        assert 0.0 <= profile.base_weight <= 1.0

    def test_weight_adjustment_clamps_to_one(self) -> None:
        profile = SourceTrustProfile(
            source_name="test",
            tier=SourceTrustTier.AUTHORITATIVE,
            weight_adjustment=9999.0,  # absurd override
        )
        assert profile.base_weight == 1.0

    def test_weight_adjustment_clamps_to_zero(self) -> None:
        profile = SourceTrustProfile(
            source_name="test",
            tier=SourceTrustTier.UNTRUSTED,
            weight_adjustment=-9999.0,
        )
        assert profile.base_weight == 0.0

    def test_freshness_weight_for_fresh_evidence(self) -> None:
        profile = SourceTrustProfile(
            source_name="lldp",
            tier=SourceTrustTier.FIRST_PARTY,
            freshness_ttl_seconds=300.0,
        )
        now = datetime.utcnow()
        # 10-second-old evidence should be almost fully fresh
        w = profile.freshness_weight(now - timedelta(seconds=10))
        assert w > 0.95

    def test_freshness_weight_for_stale_evidence(self) -> None:
        profile = SourceTrustProfile(
            source_name="lldp",
            tier=SourceTrustTier.FIRST_PARTY,
            freshness_ttl_seconds=300.0,
        )
        # 10× TTL → should be very stale
        w = profile.freshness_weight(datetime.utcnow() - timedelta(seconds=3000))
        assert w < 0.10

    def test_freshness_weight_none_timestamp_is_conservative(self) -> None:
        profile = SourceTrustProfile(
            source_name="test", tier=SourceTrustTier.FIRST_PARTY
        )
        w = profile.freshness_weight(None)
        assert w == 0.5

    def test_effective_weight_is_product(self) -> None:
        profile = SourceTrustProfile(
            source_name="test",
            tier=SourceTrustTier.FIRST_PARTY,
            freshness_ttl_seconds=3600.0,
        )
        now = datetime.utcnow()
        fw = profile.freshness_weight(now)
        assert abs(profile.effective_weight(now) - profile.base_weight * fw) < 1e-9


class TestSourceTrustRegistry:
    def test_known_source_returns_correct_tier(self) -> None:
        reg = SourceTrustRegistry()
        assert reg.tier_of("lldp") == SourceTrustTier.FIRST_PARTY
        assert reg.tier_of("netbox") == SourceTrustTier.AUTHORITATIVE
        assert reg.tier_of("bgp_session") == SourceTrustTier.SECOND_PARTY
        assert reg.tier_of("interface_description") == SourceTrustTier.THIRD_PARTY

    def test_unknown_source_falls_back_to_untrusted(self) -> None:
        reg = SourceTrustRegistry()
        assert reg.tier_of("totally_new_source_xyz") == SourceTrustTier.UNTRUSTED

    def test_case_insensitive_lookup(self) -> None:
        reg = SourceTrustRegistry()
        assert reg.tier_of("LLDP") == reg.tier_of("lldp")

    def test_register_custom_profile(self) -> None:
        reg = SourceTrustRegistry()
        custom = SourceTrustProfile(
            source_name="my_cmdb",
            tier=SourceTrustTier.AUTHORITATIVE,
            freshness_ttl_seconds=7200.0,
        )
        reg.register(custom)
        assert reg.tier_of("my_cmdb") == SourceTrustTier.AUTHORITATIVE

    def test_most_trusted_returns_lowest_rank_source(self) -> None:
        reg = SourceTrustRegistry()
        winner = reg.most_trusted(["interface_description", "lldp", "netbox"])
        assert winner == "netbox"

    def test_most_trusted_raises_on_empty(self) -> None:
        reg = SourceTrustRegistry()
        with pytest.raises(ValueError):
            reg.most_trusted([])

    def test_is_authoritative(self) -> None:
        reg = SourceTrustRegistry()
        assert reg.is_authoritative("netbox") is True
        assert reg.is_authoritative("lldp") is False

    def test_rank_ordering(self) -> None:
        reg = SourceTrustRegistry()
        assert reg.rank_of("netbox") < reg.rank_of("lldp")
        assert reg.rank_of("lldp") < reg.rank_of("bgp_session")
        assert reg.rank_of("bgp_session") < reg.rank_of("interface_description")
        assert reg.rank_of("interface_description") < reg.rank_of("unknown")
