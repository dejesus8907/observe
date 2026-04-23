from __future__ import annotations

from typing import Any

from .vendor_profiles.junos import PROFILE as JUNOS_PROFILE
from .vendor_profiles.eos import PROFILE as EOS_PROFILE
from .vendor_profiles.ios_xe import PROFILE as IOS_XE_PROFILE

VENDOR_PROFILES = {
    "junos": JUNOS_PROFILE,
    "eos": EOS_PROFILE,
    "ios_xe": IOS_XE_PROFILE,
}

def get_vendor_override(vendor: str | None, field_key: str, source_type: str, default: float | None = None) -> float | None:
    if not vendor:
        return default
    profile = VENDOR_PROFILES.get(vendor.lower())
    if not profile:
        return default
    overrides = profile.get("overrides", {})
    field = overrides.get(field_key, {})
    if source_type in field:
        return float(field[source_type])
    return default
