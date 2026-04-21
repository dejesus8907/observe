"""Source Trust Registry.

Maps named evidence sources to a trust tier and a numeric rank weight.
The trust weight feeds directly into composite confidence scoring.

Design decisions:
- Trust tiers are immutable after registry construction.  Runtime mutations
  would make it impossible to reproduce scoring decisions from logs alone.
- Per-source overrides are additive on top of the tier baseline; they do
  NOT change the tier.  This keeps the tier taxonomy meaningful.
- Freshness is a *multiplier* on the base trust weight, not a separate
  dimension.  A very fresh untrusted source is still less trusted than a
  slightly stale authoritative source.

Tier base weights (0.0 – 1.0):
    AUTHORITATIVE   0.95
    FIRST_PARTY     0.80
    SECOND_PARTY    0.60
    THIRD_PARTY     0.35
    UNTRUSTED       0.10
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional

from netobserv.models.enums import SourceTrustTier

# Canonical base weights for each tier.
_TIER_BASE_WEIGHTS: Dict[SourceTrustTier, float] = {
    SourceTrustTier.AUTHORITATIVE: 0.95,
    SourceTrustTier.FIRST_PARTY: 0.80,
    SourceTrustTier.SECOND_PARTY: 0.60,
    SourceTrustTier.THIRD_PARTY: 0.35,
    SourceTrustTier.UNTRUSTED: 0.10,
}

# Tier ordinal — used for strict rank comparisons (lower = more trusted).
_TIER_RANK: Dict[SourceTrustTier, int] = {
    SourceTrustTier.AUTHORITATIVE: 0,
    SourceTrustTier.FIRST_PARTY: 1,
    SourceTrustTier.SECOND_PARTY: 2,
    SourceTrustTier.THIRD_PARTY: 3,
    SourceTrustTier.UNTRUSTED: 4,
}


@dataclass(frozen=True)
class SourceTrustProfile:
    """Immutable trust profile for a single named source."""

    source_name: str
    tier: SourceTrustTier
    # Optional fine-grained adjustment within the tier range [-0.15, +0.15].
    weight_adjustment: float = 0.0
    # Expected observation interval; staleness is computed relative to this.
    freshness_ttl_seconds: float = 3600.0  # default 1 h
    description: str = ""

    @property
    def base_weight(self) -> float:
        """Tier weight adjusted by the per-source fine-grain override."""
        raw = _TIER_BASE_WEIGHTS[self.tier] + self.weight_adjustment
        return max(0.0, min(1.0, raw))

    @property
    def tier_rank(self) -> int:
        """Ordinal rank (0 = most trusted).  Useful for strict comparisons."""
        return _TIER_RANK[self.tier]

    def freshness_weight(self, observed_at: Optional[datetime]) -> float:
        """Return a multiplier in [0.0, 1.0] reflecting how fresh the evidence is.

        Uses an exponential decay with half-life = freshness_ttl_seconds.
        Evidence exactly at TTL gets weight ~0.5; at 3× TTL it's ~0.125.
        Absent timestamps are conservatively treated as half-fresh (0.5).
        """
        if observed_at is None:
            return 0.5
        age_seconds = max(0.0, (datetime.utcnow() - observed_at).total_seconds())
        half_life = max(self.freshness_ttl_seconds, 1.0)
        return math.exp(-math.log(2) * age_seconds / half_life)

    def effective_weight(self, observed_at: Optional[datetime] = None) -> float:
        """Base weight multiplied by freshness — the final per-source scalar."""
        return self.base_weight * self.freshness_weight(observed_at)


# ---------------------------------------------------------------------------
# Well-known default profiles for sources found throughout netobserv
# ---------------------------------------------------------------------------

_DEFAULT_PROFILES: list[SourceTrustProfile] = [
    SourceTrustProfile(
        source_name="netbox",
        tier=SourceTrustTier.AUTHORITATIVE,
        freshness_ttl_seconds=86400.0,  # 24 h — CMDB is polled daily
        description="NetBox CMDB — authoritative source of intended state",
    ),
    SourceTrustProfile(
        source_name="netbox_cable",
        tier=SourceTrustTier.AUTHORITATIVE,
        weight_adjustment=-0.05,        # Cable records can lag reality
        freshness_ttl_seconds=86400.0,
        description="NetBox cable database",
    ),
    SourceTrustProfile(
        source_name="lldp",
        tier=SourceTrustTier.FIRST_PARTY,
        freshness_ttl_seconds=300.0,    # LLDP TTL is typically 2–5 min
        description="LLDP neighbor discovery — live device telemetry",
    ),
    SourceTrustProfile(
        source_name="cdp",
        tier=SourceTrustTier.FIRST_PARTY,
        freshness_ttl_seconds=300.0,
        description="CDP neighbor discovery — live device telemetry",
    ),
    SourceTrustProfile(
        source_name="snmp",
        tier=SourceTrustTier.FIRST_PARTY,
        weight_adjustment=-0.05,
        freshness_ttl_seconds=600.0,
        description="SNMP polling — live but lower fidelity than SSH/REST",
    ),
    SourceTrustProfile(
        source_name="rest",
        tier=SourceTrustTier.FIRST_PARTY,
        freshness_ttl_seconds=300.0,
        description="Vendor REST API — first-party device data",
    ),
    SourceTrustProfile(
        source_name="ssh",
        tier=SourceTrustTier.FIRST_PARTY,
        freshness_ttl_seconds=300.0,
        description="SSH CLI collection — first-party device data",
    ),
    SourceTrustProfile(
        source_name="bgp_session",
        tier=SourceTrustTier.SECOND_PARTY,
        freshness_ttl_seconds=600.0,
        description="BGP peer adjacency — indirect topology evidence",
    ),
    SourceTrustProfile(
        source_name="ospf",
        tier=SourceTrustTier.SECOND_PARTY,
        freshness_ttl_seconds=600.0,
        description="OSPF adjacency — indirect topology evidence",
    ),
    SourceTrustProfile(
        source_name="arp",
        tier=SourceTrustTier.SECOND_PARTY,
        weight_adjustment=-0.05,
        freshness_ttl_seconds=1800.0,
        description="ARP table — indirect L3 evidence",
    ),
    SourceTrustProfile(
        source_name="mac_table",
        tier=SourceTrustTier.SECOND_PARTY,
        weight_adjustment=-0.05,
        freshness_ttl_seconds=1800.0,
        description="MAC address table — indirect L2 evidence",
    ),
    SourceTrustProfile(
        source_name="interface_description",
        tier=SourceTrustTier.THIRD_PARTY,
        freshness_ttl_seconds=3600.0,
        description="Inferred from free-form interface descriptions",
    ),
    SourceTrustProfile(
        source_name="static_file",
        tier=SourceTrustTier.THIRD_PARTY,
        weight_adjustment=0.05,
        freshness_ttl_seconds=86400.0,
        description="Operator-maintained static inventory file",
    ),
    SourceTrustProfile(
        source_name="unknown",
        tier=SourceTrustTier.UNTRUSTED,
        freshness_ttl_seconds=3600.0,
        description="Source provenance unknown",
    ),
]


class SourceTrustRegistry:
    """Registry that maps source names to their trust profiles.

    Look-up is case-insensitive.  Any source not explicitly registered falls
    back to the UNTRUSTED profile so scoring never crashes on novel sources.
    """

    def __init__(self, profiles: Optional[list[SourceTrustProfile]] = None) -> None:
        self._profiles: Dict[str, SourceTrustProfile] = {}
        for p in _DEFAULT_PROFILES:
            self._profiles[p.source_name.lower()] = p
        if profiles:
            for p in profiles:
                self._profiles[p.source_name.lower()] = p

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(self, profile: SourceTrustProfile) -> None:
        """Add or replace a source profile.  Use sparingly — prefer defaults."""
        self._profiles[profile.source_name.lower()] = profile

    def register_many(self, profiles: list[SourceTrustProfile]) -> None:
        for p in profiles:
            self.register(p)

    # ------------------------------------------------------------------
    # Look-up
    # ------------------------------------------------------------------

    def get(self, source_name: str) -> SourceTrustProfile:
        """Return the profile for *source_name*, falling back to UNTRUSTED."""
        return self._profiles.get(
            source_name.lower(),
            SourceTrustProfile(
                source_name=source_name,
                tier=SourceTrustTier.UNTRUSTED,
                description="Auto-generated fallback for unregistered source",
            ),
        )

    def tier_of(self, source_name: str) -> SourceTrustTier:
        return self.get(source_name).tier

    def rank_of(self, source_name: str) -> int:
        """Return ordinal rank (0 = most trusted)."""
        return self.get(source_name).tier_rank

    def base_weight_of(self, source_name: str) -> float:
        return self.get(source_name).base_weight

    def effective_weight_of(
        self, source_name: str, observed_at: Optional[datetime] = None
    ) -> float:
        return self.get(source_name).effective_weight(observed_at)

    def most_trusted(self, source_names: list[str]) -> str:
        """Return the name of the highest-trust source from a list."""
        if not source_names:
            raise ValueError("source_names must not be empty")
        return min(source_names, key=lambda s: self.rank_of(s))

    def is_authoritative(self, source_name: str) -> bool:
        return self.tier_of(source_name) == SourceTrustTier.AUTHORITATIVE

    def all_profiles(self) -> list[SourceTrustProfile]:
        return list(self._profiles.values())
