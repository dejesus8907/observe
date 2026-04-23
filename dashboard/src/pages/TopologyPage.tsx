import React from "react";
import { fetchTopologyTruth } from "../api/truthApi";
import { useAutoRefresh } from "../hooks/useAutoRefresh";
import { PageHeader, PageContent } from "../components/AppShell";
import { LoadingState, ErrorState } from "../components/LoadingState";
import { TopologyTruthOverlay } from "../components/TopologyTruthOverlay";
import { colors, radius, confidenceColor } from "../theme";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from "recharts";

export default function TopologyPage() {
  const { data: topology, status, error, lastRefreshed, refresh } = useAutoRefresh(
    () => fetchTopologyTruth(500),
    60_000,
  );

  // Build confidence distribution histogram
  const buildHistogram = (values: number[]) => {
    const buckets = [
      { label: "0–25%", min: 0, max: 0.25, count: 0 },
      { label: "25–50%", min: 0.25, max: 0.5, count: 0 },
      { label: "50–75%", min: 0.5, max: 0.75, count: 0 },
      { label: "75–90%", min: 0.75, max: 0.9, count: 0 },
      { label: "90–100%", min: 0.9, max: 1.01, count: 0 },
    ];
    values.forEach((v) => {
      const b = buckets.find((bk) => v >= bk.min && v < bk.max);
      if (b) b.count++;
    });
    return buckets;
  };

  const nodeConfs = (topology?.nodes ?? []).map((n) => n.confidence);
  const edgeConfs = (topology?.edges ?? []).map((e) => e.confidence);
  const nodeHistogram = buildHistogram(nodeConfs);
  const edgeHistogram = buildHistogram(edgeConfs);

  // Node type distribution
  const typeMap = (topology?.nodes ?? []).reduce<Record<string, number>>((acc, n) => {
    acc[n.subject_type] = (acc[n.subject_type] ?? 0) + 1;
    return acc;
  }, {});
  const typeData = Object.entries(typeMap).sort((a, b) => b[1] - a[1]).slice(0, 8).map(([name, count]) => ({ name, count }));

  // State distribution
  const stateMap = (topology?.nodes ?? []).reduce<Record<string, number>>((acc, n) => {
    acc[n.state] = (acc[n.state] ?? 0) + 1;
    return acc;
  }, {});
  const stateData = Object.entries(stateMap).sort((a, b) => b[1] - a[1]).map(([name, count]) => ({ name, count }));

  const tooltipStyle = { background: colors.bg3, border: `1px solid ${colors.border}`, borderRadius: 6, fontSize: 12 };

  return (
    <div>
      <PageHeader
        title="Topology"
        subtitle="Network entity truth — confidence, disputes, and causal state"
        lastRefreshed={lastRefreshed}
        actions={
          <button
            onClick={refresh}
            style={{ padding: "6px 14px", borderRadius: radius.sm, border: `1px solid ${colors.border}`, background: colors.bg3, color: colors.text, cursor: "pointer", fontSize: 12 }}
          >
            Refresh
          </button>
        }
      />
      <PageContent>
        {status === "loading" && !topology && <LoadingState message="Loading topology..." />}
        {error && <ErrorState message={error} onRetry={refresh} />}

        {topology && (
          <>
            {/* Visual summary */}
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))", gap: 20, marginBottom: 28 }}>
              {/* Confidence distribution – nodes */}
              <div style={{ background: colors.bg2, border: `1px solid ${colors.border}`, borderRadius: radius.md, padding: "14px 16px" }}>
                <h3 style={{ margin: "0 0 12px", fontSize: 12, textTransform: "uppercase", letterSpacing: "0.06em", color: colors.textMuted }}>
                  Node confidence
                </h3>
                <ResponsiveContainer width="100%" height={100}>
                  <BarChart data={nodeHistogram} barSize={20}>
                    <XAxis dataKey="label" tick={{ fill: colors.textMuted, fontSize: 9 }} axisLine={false} tickLine={false} />
                    <YAxis hide />
                    <Tooltip contentStyle={tooltipStyle} />
                    <Bar dataKey="count" radius={[3, 3, 0, 0]}>
                      {nodeHistogram.map((b, i) => (
                        <Cell key={i} fill={confidenceColor((b.min + b.max) / 2)} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>

              {/* Confidence distribution – edges */}
              <div style={{ background: colors.bg2, border: `1px solid ${colors.border}`, borderRadius: radius.md, padding: "14px 16px" }}>
                <h3 style={{ margin: "0 0 12px", fontSize: 12, textTransform: "uppercase", letterSpacing: "0.06em", color: colors.textMuted }}>
                  Edge confidence
                </h3>
                <ResponsiveContainer width="100%" height={100}>
                  <BarChart data={edgeHistogram} barSize={20}>
                    <XAxis dataKey="label" tick={{ fill: colors.textMuted, fontSize: 9 }} axisLine={false} tickLine={false} />
                    <YAxis hide />
                    <Tooltip contentStyle={tooltipStyle} />
                    <Bar dataKey="count" radius={[3, 3, 0, 0]}>
                      {edgeHistogram.map((b, i) => (
                        <Cell key={i} fill={confidenceColor((b.min + b.max) / 2)} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>

              {/* Node type breakdown */}
              {typeData.length > 0 && (
                <div style={{ background: colors.bg2, border: `1px solid ${colors.border}`, borderRadius: radius.md, padding: "14px 16px" }}>
                  <h3 style={{ margin: "0 0 12px", fontSize: 12, textTransform: "uppercase", letterSpacing: "0.06em", color: colors.textMuted }}>
                    Node types
                  </h3>
                  <div style={{ display: "grid", gap: 6 }}>
                    {typeData.map((t) => (
                      <div key={t.name} style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 12 }}>
                        <span style={{ color: colors.textMuted, minWidth: 80, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{t.name}</span>
                        <div style={{ flex: 1, height: 6, borderRadius: radius.pill, background: colors.bg3, overflow: "hidden" }}>
                          <div style={{ width: `${(t.count / Math.max(...typeData.map((x) => x.count))) * 100}%`, height: "100%", background: colors.accent, borderRadius: radius.pill }} />
                        </div>
                        <span style={{ color: colors.text, fontWeight: 600, minWidth: 24, textAlign: "right" }}>{t.count}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* State breakdown */}
              {stateData.length > 0 && (
                <div style={{ background: colors.bg2, border: `1px solid ${colors.border}`, borderRadius: radius.md, padding: "14px 16px" }}>
                  <h3 style={{ margin: "0 0 12px", fontSize: 12, textTransform: "uppercase", letterSpacing: "0.06em", color: colors.textMuted }}>
                    Node states
                  </h3>
                  <div style={{ display: "grid", gap: 6 }}>
                    {stateData.map((t) => (
                      <div key={t.name} style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 12 }}>
                        <span style={{ color: colors.textMuted, minWidth: 80 }}>{t.name}</span>
                        <div style={{ flex: 1, height: 6, borderRadius: radius.pill, background: colors.bg3, overflow: "hidden" }}>
                          <div style={{ width: `${(t.count / Math.max(...stateData.map((x) => x.count))) * 100}%`, height: "100%", background: colors.accent, borderRadius: radius.pill }} />
                        </div>
                        <span style={{ color: colors.text, fontWeight: 600, minWidth: 24, textAlign: "right" }}>{t.count}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>

            {/* Interactive topology explorer */}
            <TopologyTruthOverlay topology={topology} />
          </>
        )}
      </PageContent>
    </div>
  );
}
