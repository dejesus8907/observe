import React from "react";

export function FreshnessBadge({ observedAt, staleAfter }: { observedAt?: string; staleAfter?: string | null }) {
  const now = Date.now();
  const observed = observedAt ? new Date(observedAt).getTime() : null;
  const stale = staleAfter ? new Date(staleAfter).getTime() : null;

  let label = "unknown freshness";
  if (observed) {
    const ageSeconds = Math.max(0, Math.floor((now - observed) / 1000));
    label = `seen ${ageSeconds}s ago`;
  }
  if (stale && stale < now) {
    label += " · stale";
  }

  return (
    <span style={{
      display: "inline-block",
      padding: "4px 8px",
      borderRadius: 999,
      border: "1px solid #888",
      fontSize: 12
    }}>
      {label}
    </span>
  );
}
