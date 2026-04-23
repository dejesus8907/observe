import React from "react";
import { useNavigate } from "react-router-dom";
import type { CorrelationClusterSummary } from "../types/truth";
import { colors, radius, stateColor } from "../theme";
import { ConfidenceBadge } from "./ConfidenceBadge";
import { StateBadge } from "./StatusBadge";
import { EmptyState } from "./LoadingState";

export function IncidentClustersPanel({
  clusters,
  onSelectCluster,
  compact = false,
}: {
  clusters: CorrelationClusterSummary[];
  onSelectCluster?: (clusterId: string) => void;
  compact?: boolean;
}) {
  const navigate = useNavigate();
  const items = compact ? clusters.slice(0, 5) : clusters;

  if (clusters.length === 0) {
    return <EmptyState message="No incident clusters." />;
  }

  return (
    <div style={{ display: "grid", gap: 8 }}>
      {items.map((cluster) => {
        const borderColor = stateColor(cluster.state);
        return (
          <div
            key={cluster.cluster_id}
            style={{
              background: colors.bg2,
              border: `1px solid ${borderColor}33`,
              borderRadius: radius.md,
              padding: "12px 14px",
              display: "grid",
              gap: 8,
            }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "flex-start", flexWrap: "wrap" }}>
              <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                <span style={{ fontFamily: "monospace", fontSize: 13, color: colors.text, fontWeight: 600 }}>
                  {cluster.cluster_id}
                </span>
                <div style={{ display: "flex", gap: 6, flexWrap: "wrap", alignItems: "center" }}>
                  <StateBadge state={cluster.state} />
                  <ConfidenceBadge confidence={cluster.cluster_confidence ?? 0} />
                </div>
              </div>
              <div style={{ display: "flex", gap: 6 }}>
                {onSelectCluster && (
                  <button
                    onClick={() => onSelectCluster(cluster.cluster_id)}
                    style={{
                      fontSize: 12,
                      padding: "4px 10px",
                      borderRadius: radius.sm,
                      border: `1px solid ${colors.border}`,
                      background: colors.bg3,
                      color: colors.text,
                      cursor: "pointer",
                    }}
                  >
                    Timeline
                  </button>
                )}
                <button
                  onClick={() => navigate(`/rca?cluster=${cluster.cluster_id}`)}
                  style={{
                    fontSize: 12,
                    padding: "4px 10px",
                    borderRadius: radius.sm,
                    border: `1px solid ${colors.accent}44`,
                    background: colors.accentBg,
                    color: colors.accent,
                    cursor: "pointer",
                  }}
                >
                  RCA
                </button>
              </div>
            </div>

            <div style={{ display: "flex", gap: 16, flexWrap: "wrap", fontSize: 12, color: colors.textMuted }}>
              {cluster.root_event_id && (
                <span>root: <span style={{ color: colors.text, fontFamily: "monospace" }}>{cluster.root_event_id}</span></span>
              )}
              {(cluster.device_ids ?? []).length > 0 && (
                <span>devices: <span style={{ color: colors.text }}>{(cluster.device_ids ?? []).join(", ")}</span></span>
              )}
              {(cluster.kind_set ?? []).length > 0 && (
                <span>kinds: <span style={{ color: colors.text }}>{(cluster.kind_set ?? []).join(", ")}</span></span>
              )}
              {cluster.updated_at && (
                <span>updated: <span style={{ color: colors.text }}>{new Date(cluster.updated_at).toLocaleString()}</span></span>
              )}
            </div>
          </div>
        );
      })}
      {compact && clusters.length > 5 && (
        <div style={{ fontSize: 12, color: colors.textMuted, textAlign: "center", paddingTop: 4 }}>
          +{clusters.length - 5} more clusters
        </div>
      )}
    </div>
  );
}
