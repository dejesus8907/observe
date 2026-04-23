import React from "react";
import { useNavigate } from "react-router-dom";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell,
  LineChart, Line, CartesianGrid, Legend,
} from "recharts";
import {
  fetchDisputes,
  fetchCorrelationClusters,
  fetchTopologyTruth,
  fetchAlerts,
  fetchSLOs,
  fetchServices,
} from "../api/truthApi";
import { useAutoRefresh } from "../hooks/useAutoRefresh";
import { colors, radius, stateColor, severityColor } from "../theme";
import { StatCard } from "../components/Card";
import { PageHeader, PageContent } from "../components/AppShell";
import { LoadingState, ErrorState } from "../components/LoadingState";
import { IncidentClustersPanel } from "../components/IncidentClustersPanel";
import { DisputedPanel } from "../components/DisputedPanel";
import { HealthDot } from "../components/StatusBadge";

function RefreshButton({ onClick }: { onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      style={{
        padding: "6px 14px",
        borderRadius: radius.sm,
        border: `1px solid ${colors.border}`,
        background: colors.bg3,
        color: colors.text,
        cursor: "pointer",
        fontSize: 12,
      }}
    >
      Refresh
    </button>
  );
}

function SectionTitle({ children }: { children: React.ReactNode }) {
  return (
    <h2 style={{ margin: "0 0 12px", fontSize: 13, textTransform: "uppercase", letterSpacing: "0.06em", color: colors.textMuted, fontWeight: 700 }}>
      {children}
    </h2>
  );
}

export default function OverviewPage() {
  const navigate = useNavigate();

  const disputes = useAutoRefresh(() => fetchDisputes(50), 30_000);
  const clusters = useAutoRefresh(() => fetchCorrelationClusters(50), 30_000);
  const topology = useAutoRefresh(() => fetchTopologyTruth(200), 60_000);
  const alerts = useAutoRefresh(() => fetchAlerts(undefined, undefined, undefined, 100), 15_000);
  const slos = useAutoRefresh(() => fetchSLOs(), 60_000);
  const services = useAutoRefresh(() => fetchServices(), 30_000);

  const isLoading = [disputes, clusters, topology, alerts, slos, services].some((r) => r.status === "loading" && r.data === null);
  const errors = [disputes, clusters, topology, alerts, slos, services]
    .filter((r) => r.status === "error")
    .map((r) => r.error);

  // Derived stats
  const criticalAlerts = (alerts.data ?? []).filter((a) => a.severity === "critical" && a.state === "firing").length;
  const warningAlerts = (alerts.data ?? []).filter((a) => a.severity === "warning" && a.state === "firing").length;
  const activeIncidents = (clusters.data ?? []).filter((c) => c.state === "open" || c.state === "active").length;
  const disputedCount = (disputes.data ?? []).length;
  const unhealthyServices = (services.data ?? []).filter((s) => s.health !== "healthy").length;
  const sloAtRisk = (slos.data ?? []).filter((s) => s.state !== "healthy").length;
  const topologyNodes = topology.data?.nodes.length ?? 0;
  const topologyDisputed = (topology.data?.nodes ?? []).filter((n) => n.disputed).length;

  // Chart data for alerts by severity
  const alertSeverityData = [
    { name: "critical", count: criticalAlerts, color: colors.critical },
    { name: "warning", count: warningAlerts, color: colors.warning },
    { name: "info", count: (alerts.data ?? []).filter((a) => a.severity === "info" && a.state === "firing").length, color: colors.info },
  ];

  // Service health donut summary
  const healthCounts = {
    healthy: (services.data ?? []).filter((s) => s.health === "healthy").length,
    degraded: (services.data ?? []).filter((s) => s.health === "degraded").length,
    critical: (services.data ?? []).filter((s) => s.health === "critical").length,
    unknown: (services.data ?? []).filter((s) => s.health === "unknown").length,
  };

  const healthData = [
    { name: "Healthy", value: healthCounts.healthy, color: colors.ok },
    { name: "Degraded", value: healthCounts.degraded, color: colors.warning },
    { name: "Critical", value: healthCounts.critical, color: colors.critical },
    { name: "Unknown", value: healthCounts.unknown, color: colors.unknown },
  ].filter((d) => d.value > 0);

  // SLO compliance data
  const sloData = (slos.data ?? []).slice(0, 8).map((s) => ({
    name: s.name.length > 14 ? s.name.slice(0, 14) + "…" : s.name,
    compliance: parseFloat((s.current_compliance * 100).toFixed(2)),
    objective: parseFloat((s.objective * 100).toFixed(2)),
    color: s.state === "healthy" ? colors.ok : s.state === "at_risk" ? colors.warning : colors.critical,
  }));

  const lastRefreshed = [disputes, clusters, topology, alerts].reduce<Date | null>((latest, r) => {
    if (!r.lastRefreshed) return latest;
    if (!latest) return r.lastRefreshed;
    return r.lastRefreshed > latest ? r.lastRefreshed : latest;
  }, null);

  if (isLoading) return <LoadingState message="Loading overview..." />;

  return (
    <div>
      <PageHeader
        title="Overview"
        subtitle="Current operational state — what is broken right now?"
        lastRefreshed={lastRefreshed}
        actions={<RefreshButton onClick={() => { disputes.refresh(); clusters.refresh(); alerts.refresh(); services.refresh(); }} />}
      />

      <PageContent>
        {errors.length > 0 && (
          <div style={{
            padding: "10px 14px",
            borderRadius: radius.sm,
            background: colors.criticalBg,
            border: `1px solid ${colors.critical}44`,
            color: colors.critical,
            fontSize: 13,
            marginBottom: 20,
          }}>
            {errors.filter(Boolean).join(" · ")}
          </div>
        )}

        {/* KPI stat cards */}
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(160px, 1fr))", gap: 12, marginBottom: 28 }}>
          <StatCard
            label="Critical alerts"
            value={criticalAlerts}
            color={criticalAlerts > 0 ? colors.critical : colors.ok}
            sublabel="currently firing"
            onClick={() => navigate("/alerts?severity=critical")}
          />
          <StatCard
            label="Warning alerts"
            value={warningAlerts}
            color={warningAlerts > 0 ? colors.warning : colors.ok}
            sublabel="currently firing"
            onClick={() => navigate("/alerts?severity=warning")}
          />
          <StatCard
            label="Active incidents"
            value={activeIncidents}
            color={activeIncidents > 0 ? colors.critical : colors.ok}
            sublabel="open clusters"
            onClick={() => navigate("/incidents")}
          />
          <StatCard
            label="Disputed assertions"
            value={disputedCount}
            color={disputedCount > 0 ? colors.disputed : colors.ok}
            sublabel="truth conflicts"
            onClick={() => navigate("/truth")}
          />
          <StatCard
            label="Unhealthy services"
            value={unhealthyServices}
            color={unhealthyServices > 0 ? colors.warning : colors.ok}
            sublabel={`of ${(services.data ?? []).length} total`}
            onClick={() => navigate("/services")}
          />
          <StatCard
            label="SLOs at risk"
            value={sloAtRisk}
            color={sloAtRisk > 0 ? colors.warning : colors.ok}
            sublabel={`of ${(slos.data ?? []).length} SLOs`}
            onClick={() => navigate("/alerts?tab=slos")}
          />
          <StatCard
            label="Topology nodes"
            value={topologyNodes}
            sublabel={topologyDisputed > 0 ? `${topologyDisputed} disputed` : "all confirmed"}
            color={topologyDisputed > 0 ? colors.warning : undefined}
            onClick={() => navigate("/topology")}
          />
        </div>

        {/* Charts row */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, marginBottom: 28 }}>
          {/* Alert distribution */}
          <div style={{ background: colors.bg2, border: `1px solid ${colors.border}`, borderRadius: radius.md, padding: "16px 20px" }}>
            <SectionTitle>Alert distribution</SectionTitle>
            {alertSeverityData.every((d) => d.count === 0) ? (
              <div style={{ padding: "20px 0", textAlign: "center", color: colors.ok, fontSize: 14 }}>
                ✓ No active alerts
              </div>
            ) : (
              <ResponsiveContainer width="100%" height={140}>
                <BarChart data={alertSeverityData} barSize={36}>
                  <XAxis dataKey="name" tick={{ fill: colors.textMuted, fontSize: 12 }} axisLine={false} tickLine={false} />
                  <YAxis tick={{ fill: colors.textMuted, fontSize: 11 }} axisLine={false} tickLine={false} width={24} />
                  <Tooltip
                    contentStyle={{ background: colors.bg3, border: `1px solid ${colors.border}`, borderRadius: 6, fontSize: 12 }}
                    labelStyle={{ color: colors.text }}
                    itemStyle={{ color: colors.textMuted }}
                  />
                  <Bar dataKey="count" radius={[4, 4, 0, 0]}>
                    {alertSeverityData.map((d, i) => <Cell key={i} fill={d.color} />)}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            )}
          </div>

          {/* Service health */}
          <div style={{ background: colors.bg2, border: `1px solid ${colors.border}`, borderRadius: radius.md, padding: "16px 20px" }}>
            <SectionTitle>Service health</SectionTitle>
            {healthData.length === 0 ? (
              <div style={{ padding: "20px 0", textAlign: "center", color: colors.textMuted, fontSize: 13 }}>
                No service data
              </div>
            ) : (
              <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                {healthData.map((d) => (
                  <div key={d.name} style={{ display: "flex", alignItems: "center", gap: 10 }}>
                    <span style={{ fontSize: 12, color: colors.textMuted, minWidth: 72 }}>{d.name}</span>
                    <div style={{ flex: 1, height: 14, borderRadius: radius.pill, background: colors.bg3, overflow: "hidden" }}>
                      <div style={{
                        width: `${((d.value / (services.data?.length ?? 1)) * 100).toFixed(0)}%`,
                        height: "100%",
                        background: d.color,
                        borderRadius: radius.pill,
                      }} />
                    </div>
                    <span style={{ fontSize: 12, color: d.color, fontWeight: 700, minWidth: 24, textAlign: "right" }}>{d.value}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>

        {/* SLO compliance chart */}
        {sloData.length > 0 && (
          <div style={{ background: colors.bg2, border: `1px solid ${colors.border}`, borderRadius: radius.md, padding: "16px 20px", marginBottom: 28 }}>
            <SectionTitle>SLO compliance</SectionTitle>
            <ResponsiveContainer width="100%" height={160}>
              <BarChart data={sloData} barSize={18} barGap={2}>
                <CartesianGrid strokeDasharray="3 3" stroke={colors.border} vertical={false} />
                <XAxis dataKey="name" tick={{ fill: colors.textMuted, fontSize: 11 }} axisLine={false} tickLine={false} />
                <YAxis
                  domain={[Math.max(0, Math.min(...sloData.map((d) => d.compliance)) - 2), 100]}
                  tick={{ fill: colors.textMuted, fontSize: 11 }}
                  axisLine={false}
                  tickLine={false}
                  width={36}
                  tickFormatter={(v) => `${v}%`}
                />
                <Tooltip
                  contentStyle={{ background: colors.bg3, border: `1px solid ${colors.border}`, borderRadius: 6, fontSize: 12 }}
                  formatter={(v: number) => [`${v}%`]}
                />
                <Legend wrapperStyle={{ fontSize: 11, color: colors.textMuted }} />
                <Bar dataKey="objective" name="Objective" fill={colors.border} radius={[2, 2, 0, 0]} />
                <Bar dataKey="compliance" name="Compliance" radius={[2, 2, 0, 0]}>
                  {sloData.map((d, i) => <Cell key={i} fill={d.color} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}

        {/* Two-column lower section */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20 }}>
          <div>
            <SectionTitle>Active incidents ({activeIncidents})</SectionTitle>
            <IncidentClustersPanel
              clusters={(clusters.data ?? []).filter((c) => c.state === "open" || c.state === "active")}
              compact
            />
          </div>
          <div>
            <SectionTitle>Disputed assertions ({disputedCount})</SectionTitle>
            <DisputedPanel disputes={disputes.data ?? []} compact />
          </div>
        </div>
      </PageContent>
    </div>
  );
}
