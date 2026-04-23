import React, { useState } from "react";
import { useSearchParams } from "react-router-dom";
import { fetchTraces, fetchLogs } from "../api/truthApi";
import { useAutoRefresh } from "../hooks/useAutoRefresh";
import { PageHeader, PageContent } from "../components/AppShell";
import { LoadingState, ErrorState, EmptyState } from "../components/LoadingState";
import { colors, radius } from "../theme";
import { QueryBar } from "../components/QueryBar";
import { useQuery } from "../context/QueryContext";

type TelemetryTab = "traces" | "logs";

const LOG_LEVEL_COLORS: Record<string, string> = {
  debug: colors.textDim,
  info: colors.info,
  warn: colors.warning,
  error: colors.critical,
  fatal: colors.disputed,
};

function TraceBar({ duration, maxDuration }: { duration: number; maxDuration: number }) {
  const pct = maxDuration > 0 ? (duration / maxDuration) * 100 : 0;
  const color = duration > 1000 ? colors.critical : duration > 300 ? colors.warning : colors.ok;
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
      <div style={{ flex: 1, height: 6, borderRadius: radius.pill, background: colors.bg3, overflow: "hidden" }}>
        <div style={{ width: `${pct}%`, height: "100%", background: color, borderRadius: radius.pill }} />
      </div>
      <span style={{ fontSize: 11, color, minWidth: 60, textAlign: "right", fontWeight: 600 }}>
        {duration >= 1000 ? `${(duration / 1000).toFixed(2)}s` : `${duration.toFixed(0)}ms`}
      </span>
    </div>
  );
}

export default function TelemetryPage() {
  const [searchParams] = useSearchParams();
  const [tab, setTab] = useState<TelemetryTab>("traces");
  const { service, search } = useQuery();
  const serviceParam = searchParams.get("service") ?? service;
  const [serviceInput, setServiceInput] = useState(serviceParam);
  const [levelFilter, setLevelFilter] = useState("");
  const [statusFilter, setStatusFilter] = useState("");

  const tracesResult = useAutoRefresh(
    () => fetchTraces(serviceInput || undefined, undefined, statusFilter || undefined, 100),
    30_000,
  );

  const logsResult = useAutoRefresh(
    () => fetchLogs(serviceInput || undefined, levelFilter || undefined, search || undefined, 200),
    15_000,
  );

  const tabBtnStyle = (active: boolean): React.CSSProperties => ({
    padding: "6px 16px",
    borderRadius: radius.sm,
    border: `1px solid ${active ? colors.accent + "88" : colors.border}`,
    background: active ? colors.accentBg : "transparent",
    color: active ? colors.accent : colors.textMuted,
    cursor: "pointer",
    fontSize: 13,
    fontWeight: active ? 700 : 400,
  });

  const filteredTraces = (tracesResult.data ?? []).filter((t) => {
    if (search && !t.trace_id.includes(search) && !t.operation.toLowerCase().includes(search.toLowerCase())) return false;
    return true;
  });

  const filteredLogs = (logsResult.data ?? []).filter((l) => {
    if (levelFilter && l.level !== levelFilter) return false;
    if (search && !l.message.toLowerCase().includes(search.toLowerCase())) return false;
    return true;
  });

  const maxDuration = Math.max(...filteredTraces.map((t) => t.duration_ms), 1);

  const errorTraces = filteredTraces.filter((t) => t.status === "error").length;
  const totalTraces = filteredTraces.length;
  const p99 = filteredTraces.length > 0
    ? [...filteredTraces].sort((a, b) => b.duration_ms - a.duration_ms)[Math.floor(filteredTraces.length * 0.01)]?.duration_ms
    : null;

  const errorLogs = filteredLogs.filter((l) => l.level === "error" || l.level === "fatal").length;

  return (
    <div>
      <PageHeader
        title="Telemetry"
        subtitle="Traces, logs, and service-level signals"
        lastRefreshed={tab === "traces" ? tracesResult.lastRefreshed : logsResult.lastRefreshed}
        actions={
          <button
            onClick={() => { tracesResult.refresh(); logsResult.refresh(); }}
            style={{ padding: "6px 14px", borderRadius: radius.sm, border: `1px solid ${colors.border}`, background: colors.bg3, color: colors.text, cursor: "pointer", fontSize: 12 }}
          >
            Refresh
          </button>
        }
      />
      <PageContent>
        {/* Service filter */}
        <div style={{ display: "flex", gap: 8, marginBottom: 16, flexWrap: "wrap", alignItems: "center" }}>
          <input
            type="text"
            value={serviceInput}
            onChange={(e) => setServiceInput(e.target.value)}
            placeholder="Filter by service..."
            style={{ background: colors.bg3, border: `1px solid ${colors.border}`, borderRadius: radius.sm, color: colors.text, padding: "5px 10px", fontSize: 12, width: 200, outline: "none" }}
          />
          <QueryBar showSearch placeholder="Search traces or logs..." />
        </div>

        {/* Tabs */}
        <div style={{ display: "flex", gap: 6, marginBottom: 16 }}>
          <button style={tabBtnStyle(tab === "traces")} onClick={() => setTab("traces")}>
            Traces ({filteredTraces.length})
          </button>
          <button style={tabBtnStyle(tab === "logs")} onClick={() => setTab("logs")}>
            Logs ({filteredLogs.length})
          </button>
        </div>

        {/* Stats row */}
        {tab === "traces" && (
          <div style={{ display: "flex", gap: 16, marginBottom: 16, flexWrap: "wrap", fontSize: 13 }}>
            <span style={{ color: colors.textMuted }}>
              Total: <strong style={{ color: colors.text }}>{totalTraces}</strong>
            </span>
            <span style={{ color: colors.textMuted }}>
              Errors: <strong style={{ color: errorTraces > 0 ? colors.critical : colors.ok }}>{errorTraces}</strong>
            </span>
            {p99 !== undefined && p99 !== null && (
              <span style={{ color: colors.textMuted }}>
                p99: <strong style={{ color: p99 > 1000 ? colors.critical : p99 > 300 ? colors.warning : colors.ok }}>
                  {p99 >= 1000 ? `${(p99 / 1000).toFixed(2)}s` : `${p99}ms`}
                </strong>
              </span>
            )}
            <select
              value={statusFilter}
              onChange={(e) => setStatusFilter(e.target.value)}
              style={{ marginLeft: "auto", background: colors.bg3, border: `1px solid ${colors.border}`, borderRadius: radius.sm, color: colors.text, padding: "3px 8px", fontSize: 12 }}
            >
              <option value="">All statuses</option>
              <option value="ok">OK</option>
              <option value="error">Error</option>
            </select>
          </div>
        )}

        {tab === "logs" && (
          <div style={{ display: "flex", gap: 16, marginBottom: 16, flexWrap: "wrap", alignItems: "center", fontSize: 13 }}>
            <span style={{ color: colors.textMuted }}>
              Errors: <strong style={{ color: errorLogs > 0 ? colors.critical : colors.ok }}>{errorLogs}</strong>
            </span>
            <select
              value={levelFilter}
              onChange={(e) => setLevelFilter(e.target.value)}
              style={{ marginLeft: "auto", background: colors.bg3, border: `1px solid ${colors.border}`, borderRadius: radius.sm, color: colors.text, padding: "3px 8px", fontSize: 12 }}
            >
              <option value="">All levels</option>
              <option value="debug">Debug</option>
              <option value="info">Info</option>
              <option value="warn">Warn</option>
              <option value="error">Error</option>
              <option value="fatal">Fatal</option>
            </select>
          </div>
        )}

        {/* Traces view */}
        {tab === "traces" && (
          <>
            {tracesResult.status === "loading" && !tracesResult.data && <LoadingState message="Loading traces..." />}
            {tracesResult.error && <ErrorState message={tracesResult.error} onRetry={tracesResult.refresh} />}
            {filteredTraces.length === 0 && tracesResult.status !== "loading" ? (
              <EmptyState message="No traces found." />
            ) : (
              <div style={{ display: "grid", gap: 1 }}>
                {/* Header */}
                <div style={{
                  display: "grid",
                  gridTemplateColumns: "160px 1fr 80px 180px 100px",
                  gap: 12,
                  padding: "6px 12px",
                  fontSize: 11,
                  color: colors.textDim,
                  textTransform: "uppercase",
                  letterSpacing: "0.06em",
                  borderBottom: `1px solid ${colors.border}`,
                }}>
                  <span>Trace ID</span>
                  <span>Operation</span>
                  <span>Service</span>
                  <span>Duration</span>
                  <span>Status</span>
                </div>
                {filteredTraces.map((span) => (
                  <div key={span.span_id} style={{
                    display: "grid",
                    gridTemplateColumns: "160px 1fr 80px 180px 100px",
                    gap: 12,
                    padding: "8px 12px",
                    background: colors.bg2,
                    border: `1px solid ${span.status === "error" ? colors.critical + "33" : colors.border}`,
                    borderRadius: radius.sm,
                    alignItems: "center",
                    fontSize: 12,
                    marginBottom: 2,
                  }}>
                    <span style={{ fontFamily: "monospace", color: colors.accent, fontSize: 11 }}>
                      {span.trace_id.slice(0, 16)}…
                    </span>
                    <span style={{ color: colors.text, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                      {span.operation}
                    </span>
                    <span style={{ color: colors.textMuted, fontSize: 11 }}>{span.service}</span>
                    <TraceBar duration={span.duration_ms} maxDuration={maxDuration} />
                    <span style={{
                      color: span.status === "error" ? colors.critical : span.status === "ok" ? colors.ok : colors.textMuted,
                      fontWeight: 600,
                    }}>
                      {span.status}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </>
        )}

        {/* Logs view */}
        {tab === "logs" && (
          <>
            {logsResult.status === "loading" && !logsResult.data && <LoadingState message="Loading logs..." />}
            {logsResult.error && <ErrorState message={logsResult.error} onRetry={logsResult.refresh} />}
            {filteredLogs.length === 0 && logsResult.status !== "loading" ? (
              <EmptyState message="No logs found." />
            ) : (
              <div style={{
                background: colors.bg1,
                border: `1px solid ${colors.border}`,
                borderRadius: radius.md,
                padding: "12px",
                fontFamily: "monospace",
                fontSize: 12,
                display: "grid",
                gap: 2,
              }}>
                {filteredLogs.map((log) => {
                  const levelColor = LOG_LEVEL_COLORS[log.level] ?? colors.textMuted;
                  return (
                    <div key={log.log_id} style={{
                      display: "flex",
                      gap: 12,
                      padding: "4px 6px",
                      borderRadius: radius.sm,
                      background: log.level === "error" || log.level === "fatal" ? colors.criticalBg : "transparent",
                    }}>
                      <span style={{ color: colors.textDim, minWidth: 90, fontSize: 10 }}>
                        {new Date(log.timestamp).toLocaleTimeString()}
                      </span>
                      <span style={{ color: levelColor, fontWeight: 700, minWidth: 40, textAlign: "right" }}>
                        {log.level.toUpperCase()}
                      </span>
                      <span style={{ color: colors.textMuted, minWidth: 80, overflow: "hidden", textOverflow: "ellipsis" }}>
                        {log.service}
                      </span>
                      <span style={{ color: colors.text, flex: 1 }}>{log.message}</span>
                      {log.trace_id && (
                        <span style={{ color: colors.accent, fontSize: 10 }}>{log.trace_id.slice(0, 12)}…</span>
                      )}
                    </div>
                  );
                })}
              </div>
            )}
          </>
        )}
      </PageContent>
    </div>
  );
}
