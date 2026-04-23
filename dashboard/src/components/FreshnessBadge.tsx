import React from "react";
import { colors, radius } from "../theme";

function formatAge(ms: number): string {
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.floor(h / 24)}d ago`;
}

export function FreshnessBadge({ observedAt, staleAfter }: { observedAt?: string; staleAfter?: string | null }) {
  const now = Date.now();
  const observed = observedAt ? new Date(observedAt).getTime() : null;
  const stale = staleAfter ? new Date(staleAfter).getTime() : null;

  const isStale = stale !== null && stale < now;

  let label = "unknown freshness";
  if (observed) {
    label = formatAge(now - observed);
  }
  if (isStale) {
    label += " · stale";
  }

  const color = isStale ? colors.warning : colors.textMuted;

  return (
    <span style={{
      display: "inline-flex",
      alignItems: "center",
      gap: 4,
      padding: "2px 8px",
      borderRadius: radius.pill,
      border: `1px solid ${color}44`,
      background: isStale ? colors.warningBg : "transparent",
      fontSize: 11,
      color,
      whiteSpace: "nowrap",
    }}>
      {label}
    </span>
  );
}
