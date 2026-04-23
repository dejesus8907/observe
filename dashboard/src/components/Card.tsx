import React from "react";
import { colors, radius } from "../theme";

interface CardProps {
  children: React.ReactNode;
  style?: React.CSSProperties;
  onClick?: () => void;
  highlighted?: boolean;
}

export function Card({ children, style, onClick, highlighted }: CardProps) {
  return (
    <div
      onClick={onClick}
      style={{
        background: colors.bg2,
        border: `1px solid ${highlighted ? colors.accent : colors.border}`,
        borderRadius: radius.md,
        padding: "16px 20px",
        cursor: onClick ? "pointer" : undefined,
        transition: "border-color 0.15s, background 0.15s",
        ...(onClick ? { ":hover": { borderColor: colors.accentHover } } : {}),
        ...style,
      }}
    >
      {children}
    </div>
  );
}

interface StatCardProps {
  label: string;
  value: number | string;
  color?: string;
  sublabel?: string;
  onClick?: () => void;
}

export function StatCard({ label, value, color, sublabel, onClick }: StatCardProps) {
  return (
    <div
      onClick={onClick}
      style={{
        background: colors.bg2,
        border: `1px solid ${color ? `${color}44` : colors.border}`,
        borderRadius: radius.md,
        padding: "16px 20px",
        cursor: onClick ? "pointer" : undefined,
        display: "flex",
        flexDirection: "column",
        gap: 4,
      }}
    >
      <span style={{ fontSize: 12, color: colors.textMuted, textTransform: "uppercase", letterSpacing: "0.06em", fontWeight: 600 }}>
        {label}
      </span>
      <span style={{ fontSize: 32, fontWeight: 700, color: color ?? colors.text, lineHeight: 1 }}>
        {value}
      </span>
      {sublabel ? (
        <span style={{ fontSize: 11, color: colors.textMuted }}>{sublabel}</span>
      ) : null}
    </div>
  );
}
