"""Conflict resolution package for NetObserv streaming."""

from .models import (
    ConflictType,
    EvidenceRecord,
    ResolvedAssertion,
    ResolutionExplanation,
    ResolutionState,
    SourceHealthState,
    SubjectType,
)
from .policy import ConflictResolutionPolicy, DefaultConflictResolutionPolicy
from .resolver import ConflictResolver
from .scoring import EvidenceScorer, ScoredEvidence
from .trust import DefaultTrustModel, TrustContext
from .integration import ConflictResolutionFacade, evidence_record_from_change_event

from .stabilizer import ConflictStabilizer, StabilizationDecision
from .synthesis import synthesize_change_event_from_assertion

from .policy_profiles import *
from .policies import *
from .source_profiles import *

from .vendor_profile_loader import *
from .vendor_profiles import *
