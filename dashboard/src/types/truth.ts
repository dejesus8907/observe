export type ResolutionState = "confirmed" | "likely" | "disputed" | "stale" | "withheld" | "insufficient_evidence";

export interface EvidenceItem {
  event_id: string;
  received_at: string | null;
  detected_at: string | null;
  evidence: {
    evidence_id: string;
    subject_type: string;
    subject_id: string;
    field_name: string;
    asserted_value: unknown;
    source_type: string;
    source_instance: string;
    confidence_hint: number;
    source_health: string;
    metadata?: Record<string, unknown>;
  };
}

export interface ResolvedAssertion {
  assertion_id?: string;
  subject_type: string;
  subject_id: string;
  field_name: string;
  resolved_value: { value?: unknown } | unknown;
  resolution_state: ResolutionState | string;
  confidence: number;
  conflict_type?: string;
  observed_at?: string;
  stale_after?: string | null;
  last_authoritative_source?: string | null;
  disputed?: boolean;
  cluster_id?: string | null;
  explanation?: Record<string, unknown>;
}

export interface CorrelationClusterSummary {
  cluster_id: string;
  opened_at?: string;
  updated_at: string;
  state: string;
  root_event_id?: string | null;
  root_assertion_id?: string | null;
  root_confidence?: number;
  cluster_confidence?: number;
  summary?: Record<string, unknown>;
  device_ids?: string[];
  subject_keys?: string[];
  kind_set?: string[];
  metadata?: Record<string, unknown>;
}

export interface CorrelationClusterDetail {
  cluster: CorrelationClusterSummary;
  memberships: Array<{
    membership_id: string;
    event_id?: string | null;
    assertion_id?: string | null;
    role: string;
    joined_at: string;
    confidence: number;
    explanation?: Record<string, unknown>;
  }>;
  links: Array<{
    link_id: string;
    source_event_id?: string | null;
    target_event_id?: string | null;
    correlation_type: string;
    confidence: number;
    reason: string;
    created_at: string;
  }>;
}

export interface TopologyTruthNode {
  subject_id: string;
  subject_type: string;
  state: string;
  confidence: number;
  disputed: boolean;
  cluster_id?: string | null;
  last_authoritative_source?: string | null;
  resolved_value?: unknown;
}

export interface TopologyTruthEdge {
  subject_id: string;
  state: string;
  confidence: number;
  disputed: boolean;
  cluster_id?: string | null;
  last_authoritative_source?: string | null;
  resolved_value?: unknown;
}

export interface TopologyTruthPayload {
  nodes: TopologyTruthNode[];
  edges: TopologyTruthEdge[];
}
