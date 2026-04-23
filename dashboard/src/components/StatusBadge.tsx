import React from "react";
import { colors, radius, severityColor, stateColor } from "../theme";

export function SeverityBadge({ severity }: { severity: string }) {
  const color = severityColor(severity);
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
      fontWeight: 700,
      color,
      textTransform: "uppercase",
      letterSpacing: "0.06em",
      whiteSpace: "nowrap",
    }}>
      <span style={{ width: 6, height: 6, borderRadius: "50%", background: color, flexShrink: 0 }} />
      {severity}
    </span>
  );
}

export function StateBadge({ state }: { state: string }) {
  const color = stateColor(state);
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
      whiteSpace: "nowrap",
    }}>
      {state}
    </span>
  );
}

export function DisputedTag() {
  return (
    <span style={{
      display: "inline-flex",
      alignItems: "center",
      gap: 4,
      padding: "2px 8px",
      borderRadius: radius.pill,
      background: colors.disputedBg,
      border: `1px solid ${colors.disputed}44`,
      fontSize: 11,
      fontWeight: 700,
      color: colors.disputed,
      textTransform: "uppercase",
      letterSpacing: "0.06em",
    }}>
      DISPUTED
    </span>
  );
}

export function HealthDot({ health }: { health: string }) {
  const color = severityColor(health === "degraded" ? "warning" : health);
  return (
    <span style={{
      display: "inline-block",
      width: 10,
      height: 10,
      borderRadius: "50%",
      background: color,
      flexShrink: 0,
      boxShadow: `0 0 6px ${color}88`,
    }} />
  );
}
