import React, { useState } from "react";
import { useSearchParams } from "react-router-dom";
import { fetchCorrelationClusters, fetchCorrelationCluster } from "../api/truthApi";
import { useAutoRefresh } from "../hooks/useAutoRefresh";
import { PageHeader, PageContent } from "../components/AppShell";
import { LoadingState, ErrorState, EmptyState } from "../components/LoadingState";
import { IncidentClustersPanel } from "../components/IncidentClustersPanel";
import { ClusterTimeline } from "../components/ClusterTimeline";
import { QueryBar } from "../components/QueryBar";
import { colors, radius } from "../theme";
import type { CorrelationClusterDetail } from "../types/truth";

export default function IncidentsPage() {
  const [searchParams] = useSearchParams();
  const initialCluster = searchParams.get("cluster");
  const [selectedClusterId, setSelectedClusterId] = useState<string | null>(initialCluster);
  const [clusterDetail, setClusterDetail] = useState<CorrelationClusterDetail | null>(null);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [stateFilter, setStateFilter] = useState<string>("all");

  const { data: clusters, status, error, lastRefreshed, refresh } = useAutoRefresh(
    () => fetchCorrelationClusters(200),
    30_000,
  );

  async function handleSelectCluster(clusterId: string) {
    setSelectedClusterId(clusterId);
    setDetailError(null);
    setDetailLoading(true);
    try {
      const detail = await fetchCorrelationCluster(clusterId);
      setClusterDetail(detail);
    } catch (err) {
      setDetailError(err instanceof Error ? err.message : String(err));
    } finally {
      setDetailLoading(false);
    }
  }

  const allStates = Array.from(new Set((clusters ?? []).map((c) => c.state))).sort();

  const filtered = (clusters ?? []).filter((c) => {
    if (stateFilter !== "all" && c.state !== stateFilter) return false;
    return true;
  });

  const stateCounts = (clusters ?? []).reduce<Record<string, number>>((acc, c) => {
    acc[c.state] = (acc[c.state] ?? 0) + 1;
    return acc;
  }, {});

  const tabBtnStyle = (active: boolean, color?: string): React.CSSProperties => ({
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
        title="Incidents"
        subtitle="Correlation clusters — grouped events sharing a causal root"
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
        {error && <ErrorState message={error} onRetry={refresh} />}

        {/* State filter tabs */}
        <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginBottom: 16 }}>
          <button style={tabBtnStyle(stateFilter === "all")} onClick={() => setStateFilter("all")}>
            All ({(clusters ?? []).length})
          </button>
          {allStates.map((s) => (
            <button
              key={s}
              style={tabBtnStyle(stateFilter === s)}
              onClick={() => setStateFilter(s)}
            >
              {s} ({stateCounts[s] ?? 0})
            </button>
          ))}
        </div>

        {status === "loading" && !clusters ? (
          <LoadingState message="Loading incidents..." />
        ) : (
          <div style={{ display: "grid", gridTemplateColumns: selectedClusterId ? "1fr 1fr" : "1fr", gap: 24 }}>
            {/* Left: cluster list */}
            <div>
              {filtered.length === 0 ? (
                <EmptyState message="No incidents match this filter." />
              ) : (
                <IncidentClustersPanel
                  clusters={filtered}
                  onSelectCluster={handleSelectCluster}
                />
              )}
            </div>

            {/* Right: timeline drawer */}
            {selectedClusterId && (
              <div style={{ position: "sticky", top: 0, alignSelf: "start" }}>
                <div style={{
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "center",
                  marginBottom: 12,
                }}>
                  <h3 style={{ margin: 0, fontSize: 14, color: colors.text }}>
                    Cluster timeline
                  </h3>
                  <button
                    onClick={() => { setSelectedClusterId(null); setClusterDetail(null); }}
                    style={{ background: "none", border: "none", color: colors.textMuted, cursor: "pointer", fontSize: 18, lineHeight: 1, padding: 0 }}
                  >
                    ×
                  </button>
                </div>
                {detailLoading && <LoadingState message="Loading cluster detail..." />}
                {detailError && <ErrorState message={detailError} />}
                {!detailLoading && !detailError && <ClusterTimeline cluster={clusterDetail} />}
              </div>
            )}
          </div>
        )}
      </PageContent>
    </div>
  );
}
