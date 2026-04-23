import React from "react";

export function ConfidenceBadge({ confidence }: { confidence: number }) {
  const label =
    confidence >= 0.9 ? "very strong" :
    confidence >= 0.75 ? "strong" :
    confidence >= 0.55 ? "tentative" : "weak";

  return (
    <span style={{
      display: "inline-block",
      padding: "4px 8px",
      borderRadius: 999,
      border: "1px solid #888",
      fontSize: 12
    }}>
      confidence {confidence.toFixed(2)} · {label}
    </span>
  );
}
