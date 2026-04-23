from __future__ import annotations

from netobserv.streaming.evidence_mapper import (
    evidence_record_from_change_event,
    make_evidence_record,
)

from .models import EvidenceRecord, ResolvedAssertion
from .resolver import ConflictResolver


class ConflictResolutionFacade:
    """Thin integration facade for classifier/topology call sites."""

    def __init__(self, resolver: ConflictResolver | None = None) -> None:
        self.resolver = resolver or ConflictResolver()

    def resolve_group(self, evidence_records: list[EvidenceRecord], *, now=None) -> ResolvedAssertion:
        return self.resolver.resolve(evidence_records, now=now)

    def resolve_many(self, evidence_records, *, now=None):
        from collections import defaultdict
        grouped = defaultdict(list)
        for record in evidence_records:
            key = (record.subject_type.value, record.subject_id, record.field_name)
            grouped[key].append(record)
        return [self.resolver.resolve(records, now=now) for records in grouped.values()]
