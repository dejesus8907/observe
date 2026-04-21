"""Unit tests for ConflictResolutionEngine.

Covers all resolution strategies, the authoritative override, stale detection,
preserve_all gate, tie handling, and batch resolution.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from netobserv.conflict_resolution.engine import ConflictResolutionEngine, EngineConfig
from netobserv.conflict_resolution.models import ConflictCandidate, ConflictRecord
from netobserv.conflict_resolution.source_trust import SourceTrustRegistry
from netobserv.models.enums import ConflictState, ResolutionStrategy


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def ref() -> datetime:
    return datetime(2026, 4, 21, 12, 0, 0)


@pytest.fixture
def engine() -> ConflictResolutionEngine:
    return ConflictResolutionEngine()


def _make_conflict(
    *source_value_pairs: tuple[list[str], str],
    observed_at: datetime | None = None,
    field_name: str = "remote_interface",
) -> ConflictRecord:
    """Build a ConflictRecord with one candidate per (sources, value) pair."""
    record = ConflictRecord(
        object_type="topology_edge",
        object_id="edge:dev-a:dev-b",
        field_name=field_name,
    )
    for sources, value in source_value_pairs:
        record.candidates.append(
            ConflictCandidate(
                value=value,
                source_names=sources,
                observed_at=observed_at,
            )
        )
    return record


# ---------------------------------------------------------------------------
# Authoritative override
# ---------------------------------------------------------------------------


class TestAuthoritativeOverride:
    def test_single_authoritative_source_wins_immediately(
        self, engine: ConflictResolutionEngine, ref: datetime
    ) -> None:
        conflict = _make_conflict(
            (["netbox"], "Ethernet1"),
            (["lldp"], "Ethernet2"),
            observed_at=ref - timedelta(seconds=30),
        )
        engine.resolve(conflict, reference_time=ref)
        assert conflict.state == ConflictState.RESOLVED
        assert conflict.strategy == ResolutionStrategy.AUTHORITATIVE_OVERRIDE
        assert conflict.winner is not None
        assert conflict.winner.value == "Ethernet1"

    def test_two_authoritative_candidates_do_not_override(
        self, engine: ConflictResolutionEngine, ref: datetime
    ) -> None:
        conflict = _make_conflict(
            (["netbox"], "Ethernet1"),
            (["netbox_cable"], "Ethernet2"),
            observed_at=ref - timedelta(seconds=30),
        )
        engine.resolve(conflict, reference_time=ref)
        # Cannot pick — should fall through to another strategy
        assert conflict.state in (
            ConflictState.RESOLVED,
            ConflictState.PRESERVED,
        )

    def test_authoritative_override_disabled(
        self, ref: datetime
    ) -> None:
        cfg = EngineConfig(authoritative_override_enabled=False)
        eng = ConflictResolutionEngine(config=cfg)
        conflict = _make_conflict(
            (["netbox"], "Ethernet1"),
            (["lldp"], "Ethernet2"),
            observed_at=ref - timedelta(seconds=30),
        )
        eng.resolve(conflict, reference_time=ref)
        # Without authoritative override, falls through to HIGHEST_CONFIDENCE
        assert conflict.state in (ConflictState.RESOLVED, ConflictState.PRESERVED)
        if conflict.state == ConflictState.RESOLVED:
            assert conflict.strategy != ResolutionStrategy.AUTHORITATIVE_OVERRIDE


# ---------------------------------------------------------------------------
# Stale detection
# ---------------------------------------------------------------------------


class TestStaleDetection:
    def test_all_expired_evidence_marks_stale(
        self, engine: ConflictResolutionEngine, ref: datetime
    ) -> None:
        conflict = _make_conflict(
            (["lldp"], "Ethernet1"),
            (["bgp_session"], "Ethernet2"),
            # 30 days ago — both should expire
            observed_at=ref - timedelta(days=30),
        )
        engine.resolve(conflict, reference_time=ref)
        assert conflict.state == ConflictState.STALE


# ---------------------------------------------------------------------------
# PRESERVE_ALL strategy
# ---------------------------------------------------------------------------


class TestPreserveAll:
    def test_preserve_all_keeps_all_non_expired_candidates(
        self, ref: datetime
    ) -> None:
        cfg = EngineConfig(
            default_strategy=ResolutionStrategy.PRESERVE_ALL,
            authoritative_override_enabled=False,
        )
        eng = ConflictResolutionEngine(config=cfg)
        conflict = _make_conflict(
            (["lldp"], "Ethernet1"),
            (["bgp_session"], "Ethernet2"),
            observed_at=ref - timedelta(seconds=30),
        )
        eng.resolve(conflict, reference_time=ref)
        assert conflict.state == ConflictState.PRESERVED
        assert len(conflict.preserved_candidate_ids) == 2

    def test_explicit_preserve_call(
        self, engine: ConflictResolutionEngine, ref: datetime
    ) -> None:
        conflict = _make_conflict(
            (["lldp"], "Ethernet1"),
            (["bgp_session"], "Ethernet2"),
            observed_at=ref - timedelta(seconds=30),
        )
        engine.preserve(conflict, reason="operator decision")
        assert conflict.state == ConflictState.PRESERVED
        assert len(conflict.preserved_candidate_ids) == 2


# ---------------------------------------------------------------------------
# Strategy: HIGHEST_CONFIDENCE
# ---------------------------------------------------------------------------


class TestHighestConfidenceStrategy:
    def test_higher_trust_source_wins(
        self, ref: datetime
    ) -> None:
        cfg = EngineConfig(authoritative_override_enabled=False)
        eng = ConflictResolutionEngine(config=cfg)
        conflict = _make_conflict(
            (["lldp"], "Ethernet1"),             # first_party, higher trust
            (["interface_description"], "Eth2"),  # third_party, lower trust
            observed_at=ref - timedelta(seconds=30),
        )
        eng.resolve(
            conflict,
            strategy=ResolutionStrategy.HIGHEST_CONFIDENCE,
            reference_time=ref,
        )
        assert conflict.state == ConflictState.RESOLVED
        assert conflict.winner is not None
        assert conflict.winner.value == "Ethernet1"


# ---------------------------------------------------------------------------
# Strategy: HIGHEST_TRUST
# ---------------------------------------------------------------------------


class TestHighestTrustStrategy:
    def test_highest_trust_strategy_picks_lowest_tier_rank(
        self, ref: datetime
    ) -> None:
        cfg = EngineConfig(authoritative_override_enabled=False)
        eng = ConflictResolutionEngine(config=cfg)
        conflict = _make_conflict(
            (["lldp"], "Ethernet1"),
            (["interface_description"], "Eth99"),
            observed_at=ref - timedelta(seconds=30),
        )
        eng.resolve(
            conflict,
            strategy=ResolutionStrategy.HIGHEST_TRUST,
            reference_time=ref,
        )
        assert conflict.state == ConflictState.RESOLVED
        assert conflict.winner is not None
        assert conflict.winner.value == "Ethernet1"


# ---------------------------------------------------------------------------
# Strategy: MOST_RECENT
# ---------------------------------------------------------------------------


class TestMostRecentStrategy:
    def test_most_recent_timestamp_wins(
        self, ref: datetime
    ) -> None:
        cfg = EngineConfig(authoritative_override_enabled=False)
        eng = ConflictResolutionEngine(config=cfg)
        record = ConflictRecord(
            object_type="topology_edge",
            object_id="test",
            field_name="remote_interface",
        )
        old = ConflictCandidate(
            value="old_value",
            source_names=["snmp"],
            observed_at=ref - timedelta(hours=2),
        )
        new = ConflictCandidate(
            value="new_value",
            source_names=["snmp"],
            observed_at=ref - timedelta(minutes=5),
        )
        record.candidates = [old, new]
        eng.resolve(record, strategy=ResolutionStrategy.MOST_RECENT, reference_time=ref)
        assert record.state == ConflictState.RESOLVED
        assert record.winner is not None
        assert record.winner.value == "new_value"

    def test_tie_on_timestamp_preserves(self, ref: datetime) -> None:
        cfg = EngineConfig(
            authoritative_override_enabled=False,
            preserve_on_tie=True,
        )
        eng = ConflictResolutionEngine(config=cfg)
        record = ConflictRecord(
            object_type="topology_edge",
            object_id="test",
            field_name="remote_interface",
        )
        ts = ref - timedelta(minutes=10)
        record.candidates = [
            ConflictCandidate(value="v1", source_names=["snmp"], observed_at=ts),
            ConflictCandidate(value="v2", source_names=["rest"], observed_at=ts),
        ]
        eng.resolve(record, strategy=ResolutionStrategy.MOST_RECENT, reference_time=ref)
        assert record.state == ConflictState.PRESERVED


# ---------------------------------------------------------------------------
# Strategy: MAJORITY_VOTE
# ---------------------------------------------------------------------------


class TestMajorityVoteStrategy:
    def test_majority_source_count_wins(
        self, ref: datetime
    ) -> None:
        cfg = EngineConfig(authoritative_override_enabled=False)
        eng = ConflictResolutionEngine(config=cfg)
        record = ConflictRecord(
            object_type="device", object_id="dev-a", field_name="hostname"
        )
        # 3 sources agree on "router1", 1 on "rtr1"
        majority = ConflictCandidate(
            value="router1",
            source_names=["lldp", "cdp", "snmp"],
            observed_at=ref - timedelta(seconds=30),
        )
        minority = ConflictCandidate(
            value="rtr1",
            source_names=["interface_description"],
            observed_at=ref - timedelta(seconds=30),
        )
        record.candidates = [majority, minority]
        eng.resolve(
            record, strategy=ResolutionStrategy.MAJORITY_VOTE, reference_time=ref
        )
        assert record.state == ConflictState.RESOLVED
        assert record.winner is not None
        assert record.winner.value == "router1"


# ---------------------------------------------------------------------------
# Idempotency / already-terminal records
# ---------------------------------------------------------------------------


class TestIdempotency:
    def test_resolving_already_resolved_record_is_noop(
        self, engine: ConflictResolutionEngine, ref: datetime
    ) -> None:
        conflict = _make_conflict(
            (["netbox"], "Ethernet1"),
            observed_at=ref - timedelta(seconds=10),
        )
        engine.resolve(conflict, reference_time=ref)
        first_state = conflict.state
        first_history_len = len(conflict.history)

        engine.resolve(conflict, reference_time=ref)  # second call
        assert conflict.state == first_state
        assert len(conflict.history) == first_history_len


# ---------------------------------------------------------------------------
# Batch resolution
# ---------------------------------------------------------------------------


class TestBatchResolution:
    def test_batch_summary_counts_correctly(
        self, engine: ConflictResolutionEngine, ref: datetime
    ) -> None:
        resolvable = _make_conflict(
            (["netbox"], "Ethernet1"),
            (["lldp"], "Ethernet2"),
            observed_at=ref - timedelta(seconds=30),
        )
        stale_conflict = _make_conflict(
            (["lldp"], "Eth3"),
            (["bgp_session"], "Eth4"),
            observed_at=ref - timedelta(days=30),
        )
        result = engine.resolve_many(
            [resolvable, stale_conflict], reference_time=ref
        )
        assert result.total == 2
        assert result.summary()["resolved"] + result.summary()["preserved"] >= 1
        assert result.summary()["stale"] >= 1


# ---------------------------------------------------------------------------
# Audit history
# ---------------------------------------------------------------------------


class TestAuditHistory:
    def test_history_records_all_transitions(
        self, engine: ConflictResolutionEngine, ref: datetime
    ) -> None:
        conflict = _make_conflict(
            (["netbox"], "v1"),
            (["lldp"], "v2"),
            observed_at=ref - timedelta(seconds=30),
        )
        engine.resolve(conflict, reference_time=ref)
        states = [t.to_state for t in conflict.history]
        assert ConflictState.EVALUATING in states
        assert conflict.state in states

    def test_suppress_records_in_history(
        self, ref: datetime
    ) -> None:
        conflict = _make_conflict(
            (["lldp"], "v1"),
            observed_at=ref - timedelta(seconds=30),
        )
        conflict.suppress(operator="admin", reason="known false positive")
        assert conflict.state == ConflictState.SUPPRESSED
        assert any(t.actor == "admin" for t in conflict.history)
