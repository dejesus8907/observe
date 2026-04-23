import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import { fetchServices } from "../api/truthApi";
import { useAutoRefresh } from "../hooks/useAutoRefresh";
import { PageHeader, PageContent } from "../components/AppShell";
import { LoadingState, ErrorState, EmptyState } from "../components/LoadingState";
import { HealthDot } from "../components/StatusBadge";
import { colors, radius, severityColor } from "../theme";

export default function ServicesPage() {
  const navigate = useNavigate();
  const [healthFilter, setHealthFilter] = useState("");
  const [search, setSearch] = useState("");

  const { data: services, status, error, lastRefreshed, refresh } = useAutoRefresh(
    () => fetchServices(),
    30_000,
  );

  const filtered = (services ?? []).filter((s) => {
    if (healthFilter && s.health !== healthFilter) return false;
    if (search && !s.name.toLowerCase().includes(search.toLowerCase())) return false;
    return true;
  });

  const sorted = [...filtered].sort((a, b) => {
    const order = { critical: 0, degraded: 1, unknown: 2, healthy: 3 };
    return (order[a.health] ?? 4) - (order[b.health] ?? 4);
  });

  const healthCounts = (services ?? []).reduce<Record<string, number>>((acc, s) => {
    acc[s.health] = (acc[s.health] ?? 0) + 1;
    return acc;
  }, {});

  const filterBtnStyle = (active: boolean, color?: string): React.CSSProperties => ({
    padding: "4px 10px",
    borderRadius: radius.sm,
    border: `1px solid ${active ? (color ?? colors.accent) + "88" : colors.border}`,
    background: active ? (color ?? colors.accent) + "22" : "transparent",
    color: active ? (color ?? colors.accent) : colors.textMuted,
    cursor: "pointer",
    fontSize: 12,
    fontWeight: active ? 700 : 400,
  });

  return (
    <div>
      <PageHeader
        title="Services"
        subtitle="Service health, error rates, and latency at a glance"
        lastRefreshed={lastRefreshed}
        actions={
          <button onClick={refresh} style={{ padding: "6px 14px", borderRadius: radius.sm, border: `1px solid ${colors.border}`, background: colors.bg3, color: colors.text, cursor: "pointer", fontSize: 12 }}>
            Refresh
          </button>
        }
      />
      <PageContent>
        {status === "loading" && !services && <LoadingState message="Loading services..." />}
        {error && <ErrorState message={error} onRetry={refresh} />}

        {services && (
          <>
            {/* Filter bar */}
            <div style={{ display: "flex", gap: 8, marginBottom: 20, flexWrap: "wrap", alignItems: "center" }}>
              <button style={filterBtnStyle(healthFilter === "")} onClick={() => setHealthFilter("")}>
                All ({(services).length})
              </button>
              <button style={filterBtnStyle(healthFilter === "critical", colors.critical)} onClick={() => setHealthFilter("critical")}>
                Critical ({healthCounts["critical"] ?? 0})
              </button>
              <button style={filterBtnStyle(healthFilter === "degraded", colors.warning)} onClick={() => setHealthFilter("degraded")}>
                Degraded ({healthCounts["degraded"] ?? 0})
              </button>
              <button style={filterBtnStyle(healthFilter === "healthy", colors.ok)} onClick={() => setHealthFilter("healthy")}>
                Healthy ({healthCounts["healthy"] ?? 0})
              </button>
              <button style={filterBtnStyle(healthFilter === "unknown", colors.unknown)} onClick={() => setHealthFilter("unknown")}>
                Unknown ({healthCounts["unknown"] ?? 0})
              </button>
              <input
                type="text"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search services..."
                style={{ background: colors.bg3, border: `1px solid ${colors.border}`, borderRadius: radius.sm, color: colors.text, padding: "5px 10px", fontSize: 12, width: 200, outline: "none", marginLeft: "auto" }}
              />
            </div>

            {sorted.length === 0 ? (
              <EmptyState message="No services match this filter." />
            ) : (
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))", gap: 12 }}>
                {sorted.map((svc) => {
                  const hColor = svc.health === "critical" ? colors.critical
                    : svc.health === "degraded" ? colors.warning
                    : svc.health === "healthy" ? colors.ok
                    : colors.unknown;

                  return (
                    <div
                      key={svc.service_id}
                      onClick={() => navigate(`/telemetry?service=${encodeURIComponent(svc.name)}`)}
                      style={{
                        background: colors.bg2,
                        border: `1px solid ${hColor}44`,
                        borderRadius: radius.md,
                        padding: "14px 16px",
                        cursor: "pointer",
                        transition: "border-color 0.15s",
                        display: "grid",
                        gap: 10,
                      }}
                    >
                      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                        <HealthDot health={svc.health} />
                        <span style={{ fontWeight: 700, color: colors.text, fontSize: 14 }}>{svc.name}</span>
                        <span style={{ marginLeft: "auto", fontSize: 11, color: hColor, fontWeight: 700, textTransform: "uppercase" }}>{svc.health}</span>
                      </div>

                      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
                        {svc.error_rate !== undefined && (
                          <div>
                            <div style={{ fontSize: 10, color: colors.textDim, textTransform: "uppercase", letterSpacing: "0.06em" }}>Error rate</div>
                            <div style={{
                              fontSize: 18,
                              fontWeight: 700,
                              color: svc.error_rate > 0.05 ? colors.critical : svc.error_rate > 0.01 ? colors.warning : colors.ok,
                            }}>
                              {(svc.error_rate * 100).toFixed(2)}%
                            </div>
                          </div>
                        )}
                        {svc.p99_latency_ms !== undefined && (
                          <div>
                            <div style={{ fontSize: 10, color: colors.textDim, textTransform: "uppercase", letterSpacing: "0.06em" }}>p99 latency</div>
                            <div style={{
                              fontSize: 18,
                              fontWeight: 700,
                              color: svc.p99_latency_ms > 1000 ? colors.critical : svc.p99_latency_ms > 300 ? colors.warning : colors.ok,
                            }}>
                              {svc.p99_latency_ms.toFixed(0)}ms
                            </div>
                          </div>
                        )}
                        {svc.request_rate !== undefined && (
                          <div>
                            <div style={{ fontSize: 10, color: colors.textDim, textTransform: "uppercase", letterSpacing: "0.06em" }}>Req/s</div>
                            <div style={{ fontSize: 18, fontWeight: 700, color: colors.text }}>{svc.request_rate.toFixed(1)}</div>
                          </div>
                        )}
                        {svc.active_alerts !== undefined && svc.active_alerts > 0 && (
                          <div>
                            <div style={{ fontSize: 10, color: colors.textDim, textTransform: "uppercase", letterSpacing: "0.06em" }}>Active alerts</div>
                            <div style={{ fontSize: 18, fontWeight: 700, color: colors.critical }}>{svc.active_alerts}</div>
                          </div>
                        )}
                      </div>

                      {(svc.active_incidents ?? 0) > 0 && (
                        <div style={{ fontSize: 12, color: colors.warning, borderTop: `1px solid ${colors.border}`, paddingTop: 8 }}>
                          {svc.active_incidents} active incident{svc.active_incidents !== 1 ? "s" : ""}
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
