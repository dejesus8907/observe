import React from "react";
import type { CorrelationClusterDetail } from "../types/truth";
import { colors, radius, stateColor } from "../theme";
import { ConfidenceBadge, ConfidenceBar } from "./ConfidenceBadge";
import { StateBadge } from "./StatusBadge";
import { EmptyState } from "./LoadingState";

export function ClusterTimeline({ cluster }: { cluster: CorrelationClusterDetail | null }) {
  if (!cluster) {
    return <EmptyState message="Select a cluster to view its timeline." />;
  }

  const sorted = [...cluster.memberships].sort(
    (a, b) => new Date(a.joined_at).getTime() - new Date(b.joined_at).getTime(),
  );

  return (
    <div style={{ display: "grid", gap: 16 }}>
      {/* Cluster header */}
      <div style={{
        background: colors.bg2,
        border: `1px solid ${stateColor(cluster.cluster.state)}44`,
        borderRadius: radius.md,
        padding: "14px 18px",
        display: "grid",
        gap: 8,
      }}>
        <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
          <span style={{ fontWeight: 700, color: colors.text, fontFamily: "monospace" }}>
            {cluster.cluster.cluster_id}
          </span>
          <StateBadge state={cluster.cluster.state} />
          {cluster.cluster.root_confidence !== undefined && (
            <ConfidenceBadge confidence={cluster.cluster.root_confidence} />
          )}
        </div>
        <div style={{ fontSize: 12, color: colors.textMuted, display: "flex", gap: 16, flexWrap: "wrap" }}>
          {cluster.cluster.root_event_id && (
            <span>root event: <span style={{ color: colors.text, fontFamily: "monospace" }}>{cluster.cluster.root_event_id}</span></span>
          )}
          {cluster.cluster.opened_at && (
            <span>opened: <span style={{ color: colors.text }}>{new Date(cluster.cluster.opened_at).toLocaleString()}</span></span>
          )}
          <span>updated: <span style={{ color: colors.text }}>{new Date(cluster.cluster.updated_at).toLocaleString()}</span></span>
          <span>{cluster.memberships.length} members · {cluster.links.length} links</span>
        </div>
      </div>

      {/* Timeline */}
      <div style={{ display: "grid", gap: 0, position: "relative" }}>
        <div style={{
          position: "absolute",
          left: 15,
          top: 20,
          bottom: 20,
          width: 2,
          background: colors.border,
        }} />
        {sorted.map((member, idx) => (
          <div key={member.membership_id} style={{ display: "flex", gap: 16, paddingBottom: idx < sorted.length - 1 ? 16 : 0 }}>
            {/* Timeline dot */}
            <div style={{
              width: 32,
              height: 32,
              borderRadius: "50%",
              background: colors.bg3,
              border: `2px solid ${colors.accent}`,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              fontSize: 11,
              color: colors.accent,
              fontWeight: 700,
              flexShrink: 0,
              zIndex: 1,
            }}>
              {idx + 1}
            </div>

            <div style={{
              flex: 1,
              background: colors.bg2,
              border: `1px solid ${colors.border}`,
              borderRadius: radius.md,
              padding: "10px 14px",
              display: "grid",
              gap: 6,
            }}>
              <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
                <span style={{ fontFamily: "monospace", fontSize: 12, color: colors.text }}>
                  {member.event_id ?? member.assertion_id ?? member.membership_id}
                </span>
                <span style={{
                  fontSize: 11,
                  padding: "1px 6px",
                  borderRadius: radius.pill,
                  background: colors.bg3,
                  color: colors.textMuted,
                }}>
                  {member.role}
                </span>
              </div>
              <ConfidenceBar confidence={member.confidence} />
              <div style={{ fontSize: 11, color: colors.textDim }}>
                joined {new Date(member.joined_at).toLocaleString()}
              </div>
            </div>
          </div>
        ))}
      </div>

      {/* Correlation links */}
      {cluster.links.length > 0 && (
        <div style={{ display: "grid", gap: 8 }}>
          <h4 style={{ color: colors.textMuted, fontSize: 12, textTransform: "uppercase", letterSpacing: "0.06em", margin: 0 }}>
            Correlation links ({cluster.links.length})
          </h4>
          {cluster.links.map((link) => (
            <div key={link.link_id} style={{
              background: colors.bg2,
              border: `1px solid ${colors.border}`,
              borderRadius: radius.sm,
              padding: "8px 12px",
              display: "flex",
              gap: 8,
              flexWrap: "wrap",
              alignItems: "center",
              fontSize: 12,
            }}>
              <span style={{ fontFamily: "monospace", color: colors.text }}>
                {link.source_event_id ?? "?"} → {link.target_event_id ?? "?"}
              </span>
              <span style={{ color: colors.textMuted }}>{link.correlation_type}</span>
              <ConfidenceBadge confidence={link.confidence} />
              <span style={{ color: colors.textMuted, marginLeft: "auto" }}>{link.reason}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
