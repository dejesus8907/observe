"""Unit tests for EvidenceDecayModel."""

from __future__ import annotations

import math
from datetime import datetime, timedelta

import pytest

from netobserv.conflict_resolution.decay import (
    DecayStrategy,
    EvidenceDecayModel,
    default_decay_for_tier,
)


@pytest.fixture
def ref_time() -> datetime:
    return datetime(2026, 4, 21, 12, 0, 0)


class TestExponentialDecay:
    def test_fresh_evidence_near_one(self, ref_time: datetime) -> None:
        model = EvidenceDecayModel(
            strategy=DecayStrategy.EXPONENTIAL, half_life_seconds=300.0
        )
        dw = model.decay(1.0, ref_time - timedelta(seconds=5), reference_time=ref_time)
        assert dw.decay_factor > 0.99
        assert dw.freshness_label == "fresh"

    def test_at_half_life_is_half(self, ref_time: datetime) -> None:
        model = EvidenceDecayModel(
            strategy=DecayStrategy.EXPONENTIAL, half_life_seconds=300.0
        )
        dw = model.decay(1.0, ref_time - timedelta(seconds=300), reference_time=ref_time)
        assert abs(dw.decay_factor - 0.5) < 1e-6

    def test_effective_weight_is_raw_times_decay(self, ref_time: datetime) -> None:
        model = EvidenceDecayModel(
            strategy=DecayStrategy.EXPONENTIAL, half_life_seconds=600.0
        )
        dw = model.decay(0.8, ref_time - timedelta(seconds=600), reference_time=ref_time)
        assert abs(dw.effective_weight - 0.8 * 0.5) < 1e-6

    def test_very_old_evidence_expires(self, ref_time: datetime) -> None:
        model = EvidenceDecayModel(
            strategy=DecayStrategy.EXPONENTIAL,
            half_life_seconds=300.0,
            expiry_threshold=0.05,
        )
        dw = model.decay(1.0, ref_time - timedelta(hours=5), reference_time=ref_time)
        assert dw.is_expired
        assert dw.freshness_label == "expired"

    def test_none_observed_at_treated_as_half_life(self, ref_time: datetime) -> None:
        model = EvidenceDecayModel(
            strategy=DecayStrategy.EXPONENTIAL, half_life_seconds=300.0
        )
        dw = model.decay(1.0, None, reference_time=ref_time)
        assert abs(dw.age_seconds - 300.0) < 1e-6
        assert abs(dw.decay_factor - 0.5) < 1e-6


class TestLinearDecay:
    def test_at_zero_age_is_one(self, ref_time: datetime) -> None:
        model = EvidenceDecayModel(
            strategy=DecayStrategy.LINEAR, half_life_seconds=1000.0
        )
        dw = model.decay(1.0, ref_time, reference_time=ref_time)
        assert abs(dw.decay_factor - 1.0) < 1e-9

    def test_at_half_ttl_is_half(self, ref_time: datetime) -> None:
        model = EvidenceDecayModel(
            strategy=DecayStrategy.LINEAR, half_life_seconds=1000.0
        )
        dw = model.decay(1.0, ref_time - timedelta(seconds=500), reference_time=ref_time)
        assert abs(dw.decay_factor - 0.5) < 1e-9

    def test_past_ttl_is_zero(self, ref_time: datetime) -> None:
        model = EvidenceDecayModel(
            strategy=DecayStrategy.LINEAR, half_life_seconds=1000.0
        )
        dw = model.decay(1.0, ref_time - timedelta(seconds=1100), reference_time=ref_time)
        assert dw.decay_factor == 0.0
        assert dw.is_expired


class TestStepDecay:
    def test_within_ttl_is_full(self, ref_time: datetime) -> None:
        model = EvidenceDecayModel(
            strategy=DecayStrategy.STEP, half_life_seconds=600.0
        )
        dw = model.decay(1.0, ref_time - timedelta(seconds=599), reference_time=ref_time)
        assert dw.decay_factor == 1.0

    def test_at_ttl_boundary_is_full(self, ref_time: datetime) -> None:
        model = EvidenceDecayModel(
            strategy=DecayStrategy.STEP, half_life_seconds=600.0
        )
        dw = model.decay(1.0, ref_time - timedelta(seconds=600), reference_time=ref_time)
        assert dw.decay_factor == 1.0

    def test_past_ttl_is_zero(self, ref_time: datetime) -> None:
        model = EvidenceDecayModel(
            strategy=DecayStrategy.STEP, half_life_seconds=600.0
        )
        dw = model.decay(1.0, ref_time - timedelta(seconds=601), reference_time=ref_time)
        assert dw.decay_factor == 0.0


class TestLogarithmicDecay:
    def test_at_scale_is_half(self, ref_time: datetime) -> None:
        model = EvidenceDecayModel(
            strategy=DecayStrategy.LOGARITHMIC, half_life_seconds=3600.0
        )
        dw = model.decay(1.0, ref_time - timedelta(seconds=3600), reference_time=ref_time)
        assert abs(dw.decay_factor - 0.5) < 1e-6


class TestDefaultDecayForTier:
    def test_all_tiers_return_model(self) -> None:
        for tier in ("authoritative", "first_party", "second_party", "third_party", "untrusted"):
            model = default_decay_for_tier(tier)
            assert isinstance(model, EvidenceDecayModel)

    def test_unknown_tier_returns_default(self) -> None:
        model = default_decay_for_tier("nonexistent_tier")
        assert isinstance(model, EvidenceDecayModel)
        assert model.strategy == DecayStrategy.EXPONENTIAL

    def test_authoritative_uses_logarithmic(self) -> None:
        model = default_decay_for_tier("authoritative")
        assert model.strategy == DecayStrategy.LOGARITHMIC

    def test_first_party_uses_exponential(self) -> None:
        model = default_decay_for_tier("first_party")
        assert model.strategy == DecayStrategy.EXPONENTIAL
