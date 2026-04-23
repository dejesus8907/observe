import React, { useState } from "react";
import { fetchCorrelationCluster, fetchCorrelationClusters, fetchDisputes, fetchTopologyTruth } from "../api/truthApi";
import { useAutoRefresh } from "../hooks/useAutoRefresh";
import { PageHeader, PageContent } from "../components/AppShell";
import { LoadingState, ErrorState } from "../components/LoadingState";
import { ClusterTimeline } from "../components/ClusterTimeline";
import { DisputedPanel } from "../components/DisputedPanel";
import { IncidentClustersPanel } from "../components/IncidentClustersPanel";
import { TopologyTruthOverlay } from "../components/TopologyTruthOverlay";
import { colors, radius } from "../theme";
import type { CorrelationClusterDetail } from "../types/truth";

export default function TruthOperationsPage() {
  const [selectedCluster, setSelectedCluster] = useState<CorrelationClusterDetail | null>(null);
  const [clusterError, setClusterError] = useState<string | null>(null);
  const [clusterLoading, setClusterLoading] = useState(false);

  const disputes = useAutoRefresh(() => fetchDisputes(100), 30_000);
  const clusters = useAutoRefresh(() => fetchCorrelationClusters(100), 30_000);
  const topology = useAutoRefresh(() => fetchTopologyTruth(500), 60_000);

  async function handleSelectCluster(clusterId: string) {
    setClusterError(null);
    setClusterLoading(true);
    try {
      const detail = await fetchCorrelationCluster(clusterId);
      setSelectedCluster(detail);
    } catch (err) {
      setClusterError(err instanceof Error ? err.message : String(err));
    } finally {
      setClusterLoading(false);
    }
  }

  const isLoading = [disputes, clusters, topology].some((r) => r.status === "loading" && r.data === null);
  const combinedError = [disputes, clusters, topology].find((r) => r.status === "error");

  const sectionStyle: React.CSSProperties = {
    background: colors.bg2,
    border: `1px solid ${colors.border}`,
    borderRadius: radius.md,
    padding: "16px 20px",
  };

  const sectionTitleStyle: React.CSSProperties = {
    margin: "0 0 14px",
    fontSize: 12,
    textTransform: "uppercase" as const,
    letterSpacing: "0.06em",
    color: colors.textMuted,
    fontWeight: 700,
    display: "flex",
    alignItems: "center",
    gap: 8,
  };

  return (
    <div>
      <PageHeader
        title="Truth Operations"
        subtitle="Conflict-aware view of disputed assertions, correlation clusters, and topology truth."
        lastRefreshed={disputes.lastRefreshed}
        actions={
          <button
            onClick={() => { disputes.refresh(); clusters.refresh(); topology.refresh(); }}
            style={{ padding: "6px 14px", borderRadius: radius.sm, border: `1px solid ${colors.border}`, background: colors.bg3, color: colors.text, cursor: "pointer", fontSize: 12 }}
          >
            Refresh
          </button>
        }
      />

      <PageContent>
        {isLoading && <LoadingState message="Loading truth state..." />}
        {combinedError?.error && <ErrorState message={combinedError.error} />}

        <div style={{ display: "grid", gap: 20 }}>
          {/* Disputes */}
          <div style={sectionStyle}>
            <h2 style={sectionTitleStyle}>
              Disputed assertions
              <span style={{ fontWeight: 400, color: colors.disputed, fontSize: 12 }}>
                ({(disputes.data ?? []).length})
              </span>
            </h2>
            <DisputedPanel disputes={disputes.data ?? []} />
          </div>

          {/* Incident clusters + timeline */}
          <div style={{ display: "grid", gridTemplateColumns: selectedCluster ? "1fr 1fr" : "1fr", gap: 20 }}>
            <div style={sectionStyle}>
              <h2 style={sectionTitleStyle}>
                Incident clusters
                <span style={{ fontWeight: 400, color: colors.textMuted, fontSize: 12 }}>
                  ({(clusters.data ?? []).length})
                </span>
              </h2>
              <IncidentClustersPanel
                clusters={clusters.data ?? []}
                onSelectCluster={handleSelectCluster}
              />
            </div>

            {(selectedCluster || clusterLoading || clusterError) && (
              <div style={sectionStyle}>
                <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 14 }}>
                  <h2 style={{ ...sectionTitleStyle, margin: 0 }}>Cluster timeline</h2>
                  <button
                    onClick={() => setSelectedCluster(null)}
                    style={{ background: "none", border: "none", color: colors.textMuted, cursor: "pointer", fontSize: 18, lineHeight: 1, padding: 0 }}
                  >
                    ×
                  </button>
                </div>
                {clusterLoading && <LoadingState message="Loading cluster..." />}
                {clusterError && <ErrorState message={clusterError} />}
                {!clusterLoading && !clusterError && <ClusterTimeline cluster={selectedCluster} />}
              </div>
            )}
          </div>

          {/* Topology */}
          <div style={sectionStyle}>
            <h2 style={sectionTitleStyle}>
              Topology truth overlay
              <span style={{ fontWeight: 400, color: colors.textMuted, fontSize: 12 }}>
                dispute / confidence / lineage
              </span>
            </h2>
            <TopologyTruthOverlay topology={topology.data} />
          </div>
        </div>
      </PageContent>
    </div>
  );
}
