import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import type { ResolvedAssertion } from "../types/truth";
import { colors, radius } from "../theme";
import { ConfidenceBadge } from "./ConfidenceBadge";
import { FreshnessBadge } from "./FreshnessBadge";
import { DisputedTag } from "./StatusBadge";
import { EmptyState } from "./LoadingState";

export function DisputedPanel({ disputes, compact = false }: { disputes: ResolvedAssertion[]; compact?: boolean }) {
  const navigate = useNavigate();
  const [expanded, setExpanded] = useState<string | null>(null);
  const items = compact ? disputes.slice(0, 5) : disputes;

  if (disputes.length === 0) {
    return <EmptyState message="No disputed assertions." />;
  }

  return (
    <div style={{ display: "grid", gap: 8 }}>
      {items.map((item, idx) => {
        const key = `${item.subject_id}-${item.field_name}-${idx}`;
        const isOpen = expanded === key;
        return (
          <div
            key={key}
            style={{
              background: colors.bg2,
              border: `1px solid ${colors.disputed}33`,
              borderRadius: radius.md,
              overflow: "hidden",
              transition: "border-color 0.15s",
            }}
          >
            <div
              onClick={() => setExpanded(isOpen ? null : key)}
              style={{
                padding: "10px 14px",
                cursor: "pointer",
                display: "flex",
                gap: 8,
                flexWrap: "wrap",
                alignItems: "center",
              }}
            >
              <DisputedTag />
              <span style={{ fontWeight: 600, color: colors.text, fontFamily: "monospace", fontSize: 13 }}>
                {item.subject_type}:{item.subject_id}
              </span>
              <span style={{ color: colors.textMuted, fontSize: 12 }}>{item.field_name}</span>
              <ConfidenceBadge confidence={item.confidence} />
              <FreshnessBadge observedAt={item.observed_at} staleAfter={item.stale_after} />
              {item.cluster_id && (
                <button
                  onClick={(e) => { e.stopPropagation(); navigate(`/incidents?cluster=${item.cluster_id}`); }}
                  style={{
                    marginLeft: "auto",
                    fontSize: 11,
                    padding: "2px 8px",
                    borderRadius: radius.pill,
                    border: `1px solid ${colors.accent}44`,
                    background: colors.accentBg,
                    color: colors.accent,
                    cursor: "pointer",
                  }}
                >
                  Cluster
                </button>
              )}
            </div>
            {isOpen && (
              <div style={{ padding: "8px 14px 12px", borderTop: `1px solid ${colors.border}`, display: "grid", gap: 4 }}>
                <div style={{ fontSize: 12, color: colors.textMuted }}>
                  <span style={{ marginRight: 16 }}>state: <strong style={{ color: colors.text }}>{item.resolution_state}</strong></span>
                  <span style={{ marginRight: 16 }}>source: <strong style={{ color: colors.text }}>{item.last_authoritative_source ?? "unknown"}</strong></span>
                  {item.conflict_type && <span>conflict: <strong style={{ color: colors.warning }}>{item.conflict_type}</strong></span>}
                </div>
                {item.cluster_id && (
                  <div style={{ fontSize: 12, color: colors.textMuted }}>
                    cluster: <span style={{ color: colors.accent, fontFamily: "monospace" }}>{item.cluster_id}</span>
                  </div>
                )}
              </div>
            )}
          </div>
        );
      })}
      {compact && disputes.length > 5 && (
        <div style={{ fontSize: 12, color: colors.textMuted, textAlign: "center", paddingTop: 4 }}>
          +{disputes.length - 5} more disputed assertions
        </div>
      )}
    </div>
  );
}
