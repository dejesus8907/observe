"""Unit tests for ConfidenceScorer."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from netobserv.conflict_resolution.scoring import (
    CandidateScore,
    ConfidenceScorer,
    EvidenceEntry,
    ScoringConfig,
)
from netobserv.conflict_resolution.source_trust import SourceTrustRegistry


@pytest.fixture
def scorer() -> ConfidenceScorer:
    return ConfidenceScorer()


@pytest.fixture
def ref() -> datetime:
    return datetime(2026, 4, 21, 12, 0, 0)


class TestConfidenceScorer:
    def test_empty_evidence_returns_floor_score(self, scorer: ConfidenceScorer) -> None:
        result = scorer.score("x", [])
        assert result.composite_score == scorer._config.floor
        assert result.all_expired is True

    def test_fresh_authoritative_source_scores_high(
        self, scorer: ConfidenceScorer, ref: datetime
    ) -> None:
        entries = [
            EvidenceEntry("netbox", observed_at=ref - timedelta(seconds=60))
        ]
        result = scorer.score("candidate1", entries, reference_time=ref)
        # Authoritative + fresh → should be well above 0.5
        assert result.composite_score > 0.5
        assert result.all_expired is False

    def test_expired_evidence_only_returns_expired_flag(
        self, scorer: ConfidenceScorer, ref: datetime
    ) -> None:
        entries = [
            EvidenceEntry("lldp", observed_at=ref - timedelta(days=10))
        ]
        result = scorer.score("candidate1", entries, reference_time=ref)
        assert result.all_expired is True

    def test_conflicting_sources_lower_score(
        self, scorer: ConfidenceScorer, ref: datetime
    ) -> None:
        agreeing = [EvidenceEntry("lldp", observed_at=ref - timedelta(seconds=30))]
        disagreeing = [
            EvidenceEntry("lldp", observed_at=ref - timedelta(seconds=30)),
            EvidenceEntry("bgp_session", observed_at=ref - timedelta(seconds=30), agrees=False),
            EvidenceEntry("snmp", observed_at=ref - timedelta(seconds=30), agrees=False),
        ]
        score_agree = scorer.score("a", agreeing, reference_time=ref)
        score_disagree = scorer.score("b", disagreeing, reference_time=ref)
        assert score_agree.composite_score > score_disagree.composite_score

    def test_corroboration_bonus_increases_score(
        self, scorer: ConfidenceScorer, ref: datetime
    ) -> None:
        single = [EvidenceEntry("lldp", observed_at=ref - timedelta(seconds=10))]
        multi = [
            EvidenceEntry("lldp", observed_at=ref - timedelta(seconds=10)),
            EvidenceEntry("netbox", observed_at=ref - timedelta(seconds=10)),
            EvidenceEntry("bgp_session", observed_at=ref - timedelta(seconds=10)),
        ]
        score_single = scorer.score("a", single, reference_time=ref)
        score_multi = scorer.score("b", multi, reference_time=ref)
        assert score_multi.composite_score > score_single.composite_score

    def test_score_ceiling_respected(self, scorer: ConfidenceScorer, ref: datetime) -> None:
        entries = [
            EvidenceEntry("netbox", observed_at=ref),
            EvidenceEntry("lldp", observed_at=ref),
            EvidenceEntry("cdp", observed_at=ref),
            EvidenceEntry("rest", observed_at=ref),
            EvidenceEntry("ssh", observed_at=ref),
        ]
        result = scorer.score("x", entries, reference_time=ref)
        assert result.composite_score <= scorer._config.ceiling

    def test_score_floor_respected(self, scorer: ConfidenceScorer, ref: datetime) -> None:
        entries = [
            EvidenceEntry("lldp", observed_at=ref - timedelta(seconds=10), agrees=False),
            EvidenceEntry("bgp_session", observed_at=ref - timedelta(seconds=10), agrees=False),
        ]
        result = scorer.score("x", entries, reference_time=ref)
        assert result.composite_score >= scorer._config.floor

    def test_winner_returns_correct_key(
        self, scorer: ConfidenceScorer, ref: datetime
    ) -> None:
        candidates = {
            "auth_candidate": [
                EvidenceEntry("netbox", observed_at=ref - timedelta(seconds=10))
            ],
            "weak_candidate": [
                EvidenceEntry("interface_description", observed_at=ref - timedelta(hours=2))
            ],
        }
        w = scorer.winner(candidates, reference_time=ref)
        assert w == "auth_candidate"

    def test_winner_returns_none_for_empty(self, scorer: ConfidenceScorer) -> None:
        assert scorer.winner({}) is None

    def test_supporting_and_conflicting_sources_recorded(
        self, scorer: ConfidenceScorer, ref: datetime
    ) -> None:
        entries = [
            EvidenceEntry("lldp", observed_at=ref, agrees=True),
            EvidenceEntry("snmp", observed_at=ref, agrees=False),
        ]
        result = scorer.score("x", entries, reference_time=ref)
        assert "lldp" in result.supporting_sources
        assert "snmp" in result.conflicting_sources
