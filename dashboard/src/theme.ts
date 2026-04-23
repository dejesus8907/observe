/** NetObserv design tokens – dark-first, operator-grade. */

export const colors = {
  // Backgrounds
  bg0: "#0d0f14",
  bg1: "#151820",
  bg2: "#1c2030",
  bg3: "#232840",
  border: "#2a3050",
  borderLight: "#3a4060",

  // Text
  text: "#e2e8f0",
  textMuted: "#8892aa",
  textDim: "#4a5568",

  // Severity / status
  critical: "#ff4757",
  criticalBg: "rgba(255,71,87,0.12)",
  warning: "#ffa502",
  warningBg: "rgba(255,165,2,0.12)",
  ok: "#2ed573",
  okBg: "rgba(46,213,115,0.12)",
  info: "#1e90ff",
  infoBg: "rgba(30,144,255,0.12)",
  unknown: "#747d8c",
  unknownBg: "rgba(116,125,140,0.12)",

  // Accent
  accent: "#5e72e4",
  accentHover: "#7c8ef0",
  accentBg: "rgba(94,114,228,0.15)",

  // Confidence tiers
  confVeryStrong: "#2ed573",
  confStrong: "#7bed9f",
  confTentative: "#ffa502",
  confWeak: "#ff6b81",

  // Dispute
  disputed: "#ff4757",
  disputedBg: "rgba(255,71,87,0.10)",
} as const;

export const radius = {
  sm: "6px",
  md: "10px",
  lg: "14px",
  pill: "999px",
} as const;

export const font = {
  mono: "'JetBrains Mono', 'Fira Code', 'Consolas', monospace",
  sans: "'Inter', 'Segoe UI', Arial, sans-serif",
} as const;

export function severityColor(sev: string): string {
  switch (sev.toLowerCase()) {
    case "critical": return colors.critical;
    case "warning":
    case "warn": return colors.warning;
    case "ok":
    case "healthy":
    case "confirmed": return colors.ok;
    case "info": return colors.info;
    default: return colors.unknown;
  }
}

export function confidenceColor(c: number): string {
  if (c >= 0.9) return colors.confVeryStrong;
  if (c >= 0.75) return colors.confStrong;
  if (c >= 0.55) return colors.confTentative;
  return colors.confWeak;
}

export function stateColor(state: string): string {
  switch (state.toLowerCase()) {
    case "open":
    case "active":
    case "firing": return colors.critical;
    case "investigating": return colors.warning;
    case "resolved":
    case "closed": return colors.ok;
    default: return colors.unknown;
  }
}
