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

// ---- Extended types for new pages ----

export interface Alert {
  alert_id: string;
  name: string;
  severity: "critical" | "warning" | "info";
  state: "firing" | "resolved" | "silenced";
  service?: string;
  device_id?: string;
  cluster_id?: string | null;
  started_at: string;
  resolved_at?: string | null;
  summary?: string;
  labels?: Record<string, string>;
}

export interface SLO {
  slo_id: string;
  name: string;
  service: string;
  objective: number;
  current_compliance: number;
  burn_rate_1h?: number;
  burn_rate_6h?: number;
  burn_rate_24h?: number;
  window_days: number;
  state: "healthy" | "at_risk" | "breached";
  error_budget_remaining_pct: number;
}

export interface ServiceHealth {
  service_id: string;
  name: string;
  health: "healthy" | "degraded" | "critical" | "unknown";
  error_rate?: number;
  p99_latency_ms?: number;
  request_rate?: number;
  active_alerts?: number;
  active_incidents?: number;
}

export interface TraceSpan {
  trace_id: string;
  span_id: string;
  parent_span_id?: string | null;
  service: string;
  operation: string;
  start_time: string;
  duration_ms: number;
  status: "ok" | "error" | "unset";
  attributes?: Record<string, unknown>;
}

export interface LogEntry {
  log_id: string;
  timestamp: string;
  level: "debug" | "info" | "warn" | "error" | "fatal";
  service: string;
  message: string;
  trace_id?: string;
  span_id?: string;
  labels?: Record<string, string>;
}

export interface RCANode {
  node_id: string;
  kind: "event" | "assertion" | "service" | "device" | "alert";
  label: string;
  confidence: number;
  disputed: boolean;
  cluster_id?: string | null;
  explanation?: string;
}

export interface RCAEdge {
  from: string;
  to: string;
  relation: string;
  confidence: number;
}

export interface RCAGraph {
  root_id: string;
  nodes: RCANode[];
  edges: RCAEdge[];
  summary?: string;
}

export interface OverviewSummary {
  active_alerts_critical: number;
  active_alerts_warning: number;
  active_incidents: number;
  disputed_assertions: number;
  unhealthy_services: number;
  slo_at_risk: number;
  topology_nodes: number;
  topology_disputed_nodes: number;
}
