import type {
  Alert,
  CorrelationClusterDetail,
  CorrelationClusterSummary,
  EvidenceItem,
  LogEntry,
  OverviewSummary,
  RCAGraph,
  ResolvedAssertion,
  SLO,
  ServiceHealth,
  TopologyTruthPayload,
  TraceSpan,
} from "../types/truth";

const API_BASE = (import.meta.env.VITE_NETOBSERV_API_BASE_URL ?? "http://localhost:8000/api/streaming").replace(/\/$/, "");
const SESSION_KEY = "netobserv.session.id";
const ACCESS_TOKEN_KEY = "netobserv.access.token";
const DEFAULT_TIMEOUT_MS = 10000;
const DEFAULT_RETRIES = 3;
const RETRYABLE_STATUSES = new Set([408, 409, 425, 429, 500, 502, 503, 504]);

type HttpMethod = "GET" | "POST" | "PUT" | "PATCH" | "DELETE";

export type PaginatedResult<T> = {
  items: T[];
  page: number;
  limit: number;
  hasMore: boolean;
};

export type StreamingEventHandler = {
  onMessage: (payload: unknown) => void;
  onError?: (error: Error) => void;
  onOpen?: () => void;
  onClose?: () => void;
};

type RequestOptions = {
  method?: HttpMethod;
  body?: unknown;
  timeoutMs?: number;
  retries?: number;
};

function getSessionId(): string | null {
  if (typeof window === "undefined") {
    return null;
  }
  const existing = window.sessionStorage.getItem(SESSION_KEY) ?? window.localStorage.getItem(SESSION_KEY);
  if (existing) {
    return existing;
  }
  const created = `${Date.now()}-${Math.random().toString(36).slice(2)}`;
  window.sessionStorage.setItem(SESSION_KEY, created);
  return created;
}

function getAccessToken(): string | null {
  if (typeof window === "undefined") {
    return null;
  }
  return window.sessionStorage.getItem(ACCESS_TOKEN_KEY) ?? window.localStorage.getItem(ACCESS_TOKEN_KEY);
}

function baseHeaders(body?: unknown): HeadersInit {
  const headers: Record<string, string> = {
    Accept: "application/json",
    "X-Session-Id": getSessionId() ?? "anonymous-session",
  };
  if (body !== undefined) {
    headers["Content-Type"] = "application/json";
  }
  const token = getAccessToken();
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }
  return headers;
}

function isRetryable(error: unknown, response?: Response): boolean {
  if (response && RETRYABLE_STATUSES.has(response.status)) {
    return true;
  }
  if (error instanceof TypeError) {
    return true;
  }
  return false;
}

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function toPath(path: string, query?: Record<string, string | number | boolean | undefined>): string {
  if (!query) {
    return path;
  }
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value === undefined) {
      continue;
    }
    params.set(key, String(value));
  }
  const suffix = params.toString();
  return suffix ? `${path}?${suffix}` : path;
}

async function getJson<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const retries = options.retries ?? DEFAULT_RETRIES;
  const method = options.method ?? "GET";
  const body = options.body !== undefined ? JSON.stringify(options.body) : undefined;

  let lastError: Error | null = null;

  for (let attempt = 1; attempt <= retries; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    let response: Response | undefined;
    try {
      response = await fetch(`${API_BASE}${path}`, {
        method,
        headers: baseHeaders(options.body),
        body,
        credentials: "include",
        signal: controller.signal,
      });
      if (!response.ok) {
        const bodyText = await response.text();
        const err = new Error(`Request failed (${response.status}): ${bodyText || response.statusText}`);
        if (attempt < retries && isRetryable(err, response)) {
          await wait(2 ** (attempt - 1) * 200);
          continue;
        }
        throw err;
      }
      return (await response.json()) as T;
    } catch (error) {
      const normalized = error instanceof Error ? error : new Error(String(error));
      lastError = normalized;
      if (attempt < retries && isRetryable(normalized, response)) {
        await wait(2 ** (attempt - 1) * 200);
        continue;
      }
      break;
    } finally {
      clearTimeout(timeout);
    }
  }

  throw lastError ?? new Error("Request failed");
}

async function fetchPage<T>(
  path: string,
  key: string,
  page: number,
  limit: number,
): Promise<PaginatedResult<T>> {
  const pageNumber = Math.max(1, page);
  const pageLimit = Math.max(1, limit);
  const overFetchLimit = pageNumber * pageLimit;
  const payload = await getJson<Record<string, unknown>>(toPath(path, { limit: overFetchLimit }));
  const values = (payload[key] as T[] | undefined) ?? [];
  const start = (pageNumber - 1) * pageLimit;
  const end = start + pageLimit;
  const slice = values.slice(start, end);
  return {
    items: slice,
    page: pageNumber,
    limit: pageLimit,
    hasMore: values.length > end,
  };
}

// ---- Truth / Disputes ----

export async function fetchDisputes(limit = 50): Promise<ResolvedAssertion[]> {
  const payload = await getJson<{ disputes: ResolvedAssertion[] }>(toPath("/disputes", { limit }));
  return payload.disputes;
}

export async function fetchDisputesPage(page = 1, limit = 50): Promise<PaginatedResult<ResolvedAssertion>> {
  return fetchPage<ResolvedAssertion>("/disputes", "disputes", page, limit);
}

export async function fetchEvidence(subjectId?: string, limit = 50): Promise<EvidenceItem[]> {
  const payload = await getJson<{ evidence: EvidenceItem[] }>(
    toPath("/evidence", { subject_id: subjectId, limit }),
  );
  return payload.evidence;
}

export async function fetchResolvedAssertions(subjectType?: string, subjectId?: string, limit = 100): Promise<ResolvedAssertion[]> {
  if (subjectType && subjectId) {
    const payload = await getJson<{ resolved_assertions: ResolvedAssertion[] }>(
      toPath(`/resolved-assertions/${encodeURIComponent(subjectType)}/${encodeURIComponent(subjectId)}`, { limit }),
    );
    return payload.resolved_assertions;
  }
  const payload = await getJson<{ resolved_assertions: ResolvedAssertion[] }>(toPath("/resolved-assertions", { limit }));
  return payload.resolved_assertions;
}

export async function fetchSubjectHistory(subjectId: string, subjectType?: string, limit = 100) {
  return getJson<{ subject_id: string; subject_type?: string; evidence: EvidenceItem[]; resolved_assertions: ResolvedAssertion[] }>(
    toPath(`/subjects/${encodeURIComponent(subjectId)}/history`, { subject_type: subjectType, limit }),
  );
}

// ---- Correlation Clusters / Incidents ----

export async function fetchCorrelationClusters(limit = 50): Promise<CorrelationClusterSummary[]> {
  const payload = await getJson<{ clusters: CorrelationClusterSummary[] }>(toPath("/correlation/clusters", { limit }));
  return payload.clusters;
}

export async function fetchCorrelationClustersPage(page = 1, limit = 50): Promise<PaginatedResult<CorrelationClusterSummary>> {
  return fetchPage<CorrelationClusterSummary>("/correlation/clusters", "clusters", page, limit);
}

export async function fetchCorrelationCluster(clusterId: string): Promise<CorrelationClusterDetail> {
  return getJson<CorrelationClusterDetail>(`/correlation/clusters/${encodeURIComponent(clusterId)}`);
}

// ---- Topology ----

export async function fetchTopologyTruth(limit = 200): Promise<TopologyTruthPayload> {
  return getJson<TopologyTruthPayload>(toPath("/topology/current", { limit }));
}

// ---- Alerts ----

export async function fetchAlerts(
  state?: string,
  severity?: string,
  service?: string,
  limit = 100,
): Promise<Alert[]> {
  const payload = await getJson<{ alerts: Alert[] }>(
    toPath("/alerts", { state, severity, service, limit }),
  );
  return payload.alerts;
}

export async function fetchAlertsPage(page = 1, limit = 50): Promise<PaginatedResult<Alert>> {
  return fetchPage<Alert>("/alerts", "alerts", page, limit);
}

// ---- SLOs ----

export async function fetchSLOs(service?: string): Promise<SLO[]> {
  const payload = await getJson<{ slos: SLO[] }>(toPath("/slos", { service }));
  return payload.slos;
}

// ---- Services ----

export async function fetchServices(): Promise<ServiceHealth[]> {
  const payload = await getJson<{ services: ServiceHealth[] }>("/services/health");
  return payload.services;
}

// ---- Traces ----

export async function fetchTraces(
  service?: string,
  traceId?: string,
  status?: string,
  limit = 50,
): Promise<TraceSpan[]> {
  const payload = await getJson<{ spans: TraceSpan[] }>(
    toPath("/traces", { service, trace_id: traceId, status, limit }),
  );
  return payload.spans;
}

// ---- Logs ----

export async function fetchLogs(
  service?: string,
  level?: string,
  query?: string,
  limit = 100,
): Promise<LogEntry[]> {
  const payload = await getJson<{ logs: LogEntry[] }>(
    toPath("/logs", { service, level, query, limit }),
  );
  return payload.logs;
}

// ---- RCA ----

export async function fetchRCAGraph(clusterId: string): Promise<RCAGraph> {
  return getJson<RCAGraph>(`/rca/clusters/${encodeURIComponent(clusterId)}`);
}

export async function fetchRCAGraphByAlert(alertId: string): Promise<RCAGraph> {
  return getJson<RCAGraph>(`/rca/alerts/${encodeURIComponent(alertId)}`);
}

// ---- Overview ----

export async function fetchOverviewSummary(): Promise<OverviewSummary> {
  return getJson<OverviewSummary>("/overview/summary");
}

// ---- Streaming ----

export function subscribeStreamingEvents(handler: StreamingEventHandler, kinds: string[] = []): () => void {
  const base = API_BASE.replace(/^http/i, "ws");
  const wsUrl = `${base}/ws`;
  const ws = new WebSocket(wsUrl);

  ws.onopen = () => {
    if (kinds.length > 0) {
      ws.send(JSON.stringify({ type: "subscribe", kinds }));
    }
    handler.onOpen?.();
  };

  ws.onmessage = (event) => {
    try {
      const payload = JSON.parse(event.data);
      handler.onMessage(payload);
    } catch (error) {
      const normalized = error instanceof Error ? error : new Error(String(error));
      handler.onError?.(normalized);
    }
  };

  ws.onerror = () => {
    handler.onError?.(new Error("Streaming websocket error"));
  };

  ws.onclose = () => {
    handler.onClose?.();
  };

  return () => {
    if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
      ws.close();
    }
  };
}
