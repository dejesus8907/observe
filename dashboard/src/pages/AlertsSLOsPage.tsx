import React, { useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { fetchAlerts, fetchSLOs } from "../api/truthApi";
import { useAutoRefresh } from "../hooks/useAutoRefresh";
import { PageHeader, PageContent } from "../components/AppShell";
import { LoadingState, ErrorState, EmptyState } from "../components/LoadingState";
import { SeverityBadge, StateBadge } from "../components/StatusBadge";
import { colors, radius, severityColor, stateColor } from "../theme";
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts";

type Tab = "alerts" | "slos";

function SLOBurnBar({ value, label, threshold }: { value: number; label: string; threshold: number }) {
  const color = value > threshold * 2 ? colors.critical : value > threshold ? colors.warning : colors.ok;
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
      <span style={{ fontSize: 11, color: colors.textMuted, minWidth: 40 }}>{label}</span>
      <div style={{ flex: 1, height: 6, borderRadius: radius.pill, background: colors.bg3, overflow: "hidden", position: "relative" }}>
        <div style={{
          position: "absolute",
          left: `${Math.min(threshold / 5, 100) * 20}%`,
          top: 0,
          bottom: 0,
          width: 1,
          background: colors.borderLight,
        }} />
        <div style={{
          width: `${Math.min((value / 5) * 100, 100)}%`,
          height: "100%",
          background: color,
          borderRadius: radius.pill,
        }} />
      </div>
      <span style={{ fontSize: 11, color, fontWeight: 700, minWidth: 32, textAlign: "right" }}>{value.toFixed(2)}x</span>
    </div>
  );
}

export default function AlertsSLOsPage() {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const initialTab = (searchParams.get("tab") as Tab) ?? "alerts";
  const [tab, setTab] = useState<Tab>(initialTab);
  const [severityFilter, setSeverityFilter] = useState(searchParams.get("severity") ?? "");
  const [stateFilter, setStateFilter] = useState("");
  const [search, setSearch] = useState("");

  const alertsResult = useAutoRefresh(() => fetchAlerts(undefined, undefined, undefined, 200), 15_000);
  const slosResult = useAutoRefresh(() => fetchSLOs(), 60_000);

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

  const alerts = (alertsResult.data ?? []).filter((a) => {
    if (severityFilter && a.severity !== severityFilter) return false;
    if (stateFilter && a.state !== stateFilter) return false;
    if (search && !a.name.toLowerCase().includes(search.toLowerCase()) && !(a.service ?? "").includes(search)) return false;
    return true;
  });

  const slos = (slosResult.data ?? []).filter((s) => {
    if (search && !s.name.toLowerCase().includes(search.toLowerCase()) && !s.service.toLowerCase().includes(search.toLowerCase())) return false;
    return true;
  });

  const criticalCount = (alertsResult.data ?? []).filter((a) => a.severity === "critical" && a.state === "firing").length;
  const warningCount = (alertsResult.data ?? []).filter((a) => a.severity === "warning" && a.state === "firing").length;
  const sloBreached = (slosResult.data ?? []).filter((s) => s.state === "breached").length;
  const sloAtRisk = (slosResult.data ?? []).filter((s) => s.state === "at_risk").length;

  const tooltipStyle = { background: colors.bg3, border: `1px solid ${colors.border}`, borderRadius: 6, fontSize: 12 };

  return (
    <div>
      <PageHeader
        title="Alerts & SLOs"
        subtitle="Firing conditions, silence management, and error budget tracking"
        lastRefreshed={alertsResult.lastRefreshed}
        actions={
          <button
            onClick={() => { alertsResult.refresh(); slosResult.refresh(); }}
            style={{ padding: "6px 14px", borderRadius: radius.sm, border: `1px solid ${colors.border}`, background: colors.bg3, color: colors.text, cursor: "pointer", fontSize: 12 }}
          >
            Refresh
          </button>
        }
      />
      <PageContent>
        {/* KPIs */}
        <div style={{ display: "flex", gap: 12, marginBottom: 20, flexWrap: "wrap" }}>
          {[
            { label: "Critical firing", value: criticalCount, color: criticalCount > 0 ? colors.critical : colors.ok },
            { label: "Warning firing", value: warningCount, color: warningCount > 0 ? colors.warning : colors.ok },
            { label: "SLOs breached", value: sloBreached, color: sloBreached > 0 ? colors.critical : colors.ok },
            { label: "SLOs at risk", value: sloAtRisk, color: sloAtRisk > 0 ? colors.warning : colors.ok },
          ].map((k) => (
            <div key={k.label} style={{
              background: colors.bg2,
              border: `1px solid ${k.color}44`,
              borderRadius: radius.md,
              padding: "12px 16px",
              minWidth: 120,
            }}>
              <div style={{ fontSize: 11, color: colors.textMuted, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 4 }}>{k.label}</div>
              <div style={{ fontSize: 28, fontWeight: 700, color: k.color, lineHeight: 1 }}>{k.value}</div>
            </div>
          ))}
        </div>

        {/* Tabs */}
        <div style={{ display: "flex", gap: 6, marginBottom: 16 }}>
          <button style={tabBtnStyle(tab === "alerts")} onClick={() => setTab("alerts")}>
            Alerts ({(alertsResult.data ?? []).length})
          </button>
          <button style={tabBtnStyle(tab === "slos")} onClick={() => setTab("slos")}>
            SLOs ({(slosResult.data ?? []).length})
          </button>
        </div>

        {/* Filter bar */}
        <div style={{ display: "flex", gap: 8, marginBottom: 14, flexWrap: "wrap", alignItems: "center" }}>
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search..."
            style={{ background: colors.bg3, border: `1px solid ${colors.border}`, borderRadius: radius.sm, color: colors.text, padding: "5px 10px", fontSize: 12, width: 200, outline: "none" }}
          />
          {tab === "alerts" && (
            <>
              <select
                value={severityFilter}
                onChange={(e) => setSeverityFilter(e.target.value)}
                style={{ background: colors.bg3, border: `1px solid ${colors.border}`, borderRadius: radius.sm, color: colors.text, padding: "5px 10px", fontSize: 12 }}
              >
                <option value="">All severities</option>
                <option value="critical">Critical</option>
                <option value="warning">Warning</option>
                <option value="info">Info</option>
              </select>
              <select
                value={stateFilter}
                onChange={(e) => setStateFilter(e.target.value)}
                style={{ background: colors.bg3, border: `1px solid ${colors.border}`, borderRadius: radius.sm, color: colors.text, padding: "5px 10px", fontSize: 12 }}
              >
                <option value="">All states</option>
                <option value="firing">Firing</option>
                <option value="resolved">Resolved</option>
                <option value="silenced">Silenced</option>
              </select>
            </>
          )}
        </div>

        {/* Alerts table */}
        {tab === "alerts" && (
          <>
            {alertsResult.status === "loading" && !alertsResult.data && <LoadingState message="Loading alerts..." />}
            {alertsResult.error && <ErrorState message={alertsResult.error} onRetry={alertsResult.refresh} />}
            {alerts.length === 0 && alertsResult.status !== "loading" && (
              <EmptyState message="No alerts match this filter." />
            )}
            {alerts.length > 0 && (
              <div style={{ display: "grid", gap: 6 }}>
                <div style={{
                  display: "grid",
                  gridTemplateColumns: "24px 1fr 100px 90px 80px 120px",
                  gap: 12,
                  padding: "6px 12px",
                  fontSize: 11,
                  color: colors.textDim,
                  textTransform: "uppercase",
                  letterSpacing: "0.06em",
                  borderBottom: `1px solid ${colors.border}`,
                }}>
                  <span />
                  <span>Alert</span>
                  <span>Severity</span>
                  <span>State</span>
                  <span>Service</span>
                  <span>Started</span>
                </div>
                {alerts.map((alert) => (
                  <div
                    key={alert.alert_id}
                    onClick={() => alert.cluster_id && navigate(`/rca?cluster=${alert.cluster_id}`)}
                    style={{
                      display: "grid",
                      gridTemplateColumns: "24px 1fr 100px 90px 80px 120px",
                      gap: 12,
                      padding: "9px 12px",
                      background: colors.bg2,
                      border: `1px solid ${alert.state === "firing" ? severityColor(alert.severity) + "33" : colors.border}`,
                      borderRadius: radius.sm,
                      cursor: alert.cluster_id ? "pointer" : "default",
                      alignItems: "center",
                      fontSize: 13,
                    }}
                  >
                    <span style={{ display: "inline-block", width: 8, height: 8, borderRadius: "50%", background: alert.state === "firing" ? severityColor(alert.severity) : colors.unknown }} />
                    <div>
                      <div style={{ fontWeight: 600, color: colors.text }}>{alert.name}</div>
                      {alert.summary && <div style={{ fontSize: 11, color: colors.textMuted }}>{alert.summary}</div>}
                    </div>
                    <SeverityBadge severity={alert.severity} />
                    <StateBadge state={alert.state} />
                    <span style={{ color: colors.textMuted, fontSize: 12, overflow: "hidden", textOverflow: "ellipsis" }}>{alert.service ?? "—"}</span>
                    <span style={{ fontSize: 11, color: colors.textDim }}>{new Date(alert.started_at).toLocaleString()}</span>
                  </div>
                ))}
              </div>
            )}
          </>
        )}

        {/* SLOs table */}
        {tab === "slos" && (
          <>
            {slosResult.status === "loading" && !slosResult.data && <LoadingState message="Loading SLOs..." />}
            {slosResult.error && <ErrorState message={slosResult.error} onRetry={slosResult.refresh} />}
            {slos.length === 0 && slosResult.status !== "loading" && (
              <EmptyState message="No SLOs configured." />
            )}
            {slos.length > 0 && (
              <div style={{ display: "grid", gap: 12 }}>
                {slos.map((slo) => {
                  const budgetColor = slo.state === "healthy" ? colors.ok : slo.state === "at_risk" ? colors.warning : colors.critical;
                  const compliancePct = (slo.current_compliance * 100).toFixed(3);
                  const objectivePct = (slo.objective * 100).toFixed(2);
                  return (
                    <div key={slo.slo_id} style={{
                      background: colors.bg2,
                      border: `1px solid ${budgetColor}33`,
                      borderRadius: radius.md,
                      padding: "14px 18px",
                      display: "grid",
                      gap: 10,
                    }}>
                      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", flexWrap: "wrap", gap: 8 }}>
                        <div>
                          <div style={{ fontWeight: 700, color: colors.text, fontSize: 14 }}>{slo.name}</div>
                          <div style={{ fontSize: 12, color: colors.textMuted }}>{slo.service} · {slo.window_days}d window</div>
                        </div>
                        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                          <span style={{
                            padding: "2px 8px",
                            borderRadius: radius.pill,
                            background: `${budgetColor}22`,
                            border: `1px solid ${budgetColor}44`,
                            color: budgetColor,
                            fontSize: 11,
                            fontWeight: 700,
                            textTransform: "uppercase",
                          }}>{slo.state}</span>
                        </div>
                      </div>

                      {/* Compliance bar */}
                      <div>
                        <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4, fontSize: 12 }}>
                          <span style={{ color: colors.textMuted }}>Compliance</span>
                          <span style={{ color: budgetColor, fontWeight: 700 }}>{compliancePct}% <span style={{ color: colors.textDim }}>/ {objectivePct}% target</span></span>
                        </div>
                        <div style={{ height: 8, borderRadius: radius.pill, background: colors.bg3, overflow: "hidden", position: "relative" }}>
                          {/* Objective marker */}
                          <div style={{
                            position: "absolute",
                            left: `${slo.objective * 100}%`,
                            top: 0,
                            bottom: 0,
                            width: 2,
                            background: colors.borderLight,
                            zIndex: 1,
                          }} />
                          <div style={{
                            width: `${slo.current_compliance * 100}%`,
                            height: "100%",
                            background: budgetColor,
                            borderRadius: radius.pill,
                          }} />
                        </div>
                      </div>

                      {/* Error budget */}
                      <div>
                        <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4, fontSize: 12 }}>
                          <span style={{ color: colors.textMuted }}>Error budget remaining</span>
                          <span style={{ color: budgetColor, fontWeight: 700 }}>{slo.error_budget_remaining_pct.toFixed(1)}%</span>
                        </div>
                        <div style={{ height: 6, borderRadius: radius.pill, background: colors.bg3, overflow: "hidden" }}>
                          <div style={{
                            width: `${Math.max(0, Math.min(100, slo.error_budget_remaining_pct))}%`,
                            height: "100%",
                            background: budgetColor,
                            borderRadius: radius.pill,
                          }} />
                        </div>
                      </div>

                      {/* Burn rates */}
                      {(slo.burn_rate_1h !== undefined || slo.burn_rate_6h !== undefined || slo.burn_rate_24h !== undefined) && (
                        <div style={{ display: "grid", gap: 6 }}>
                          <div style={{ fontSize: 11, color: colors.textDim, textTransform: "uppercase", letterSpacing: "0.06em" }}>Burn rate</div>
                          {slo.burn_rate_1h !== undefined && <SLOBurnBar value={slo.burn_rate_1h} label="1h" threshold={1} />}
                          {slo.burn_rate_6h !== undefined && <SLOBurnBar value={slo.burn_rate_6h} label="6h" threshold={1} />}
                          {slo.burn_rate_24h !== undefined && <SLOBurnBar value={slo.burn_rate_24h} label="24h" threshold={1} />}
                        </div>
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
