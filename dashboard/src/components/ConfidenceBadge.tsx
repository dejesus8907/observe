import React from "react";
import { colors, confidenceColor, radius } from "../theme";

export function ConfidenceBadge({ confidence }: { confidence: number }) {
  const label =
    confidence >= 0.9 ? "very strong" :
    confidence >= 0.75 ? "strong" :
    confidence >= 0.55 ? "tentative" : "weak";

  const color = confidenceColor(confidence);

  return (
    <span style={{
      display: "inline-flex",
      alignItems: "center",
      gap: 4,
      padding: "2px 8px",
      borderRadius: radius.pill,
      border: `1px solid ${color}44`,
      background: `${color}18`,
      fontSize: 11,
      fontWeight: 600,
      color,
      letterSpacing: "0.02em",
      whiteSpace: "nowrap",
    }}>
      <span style={{
        display: "inline-block",
        width: 6,
        height: 6,
        borderRadius: "50%",
        background: color,
        flexShrink: 0,
      }} />
      {(confidence * 100).toFixed(0)}% · {label}
    </span>
  );
}

export function ConfidenceBar({ confidence }: { confidence: number }) {
  const color = confidenceColor(confidence);
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8, minWidth: 120 }}>
      <div style={{
        flex: 1,
        height: 4,
        borderRadius: radius.pill,
        background: colors.bg3,
        overflow: "hidden",
      }}>
        <div style={{
          width: `${Math.round(confidence * 100)}%`,
          height: "100%",
          background: color,
          borderRadius: radius.pill,
          transition: "width 0.3s ease",
        }} />
      </div>
      <span style={{ fontSize: 11, color, fontWeight: 600, minWidth: 32, textAlign: "right" }}>
        {(confidence * 100).toFixed(0)}%
      </span>
    </div>
  );
}
