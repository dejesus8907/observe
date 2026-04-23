import React from "react";
import { colors } from "../theme";

export function LoadingSpinner({ size = 24 }: { size?: number }) {
  return (
    <div style={{
      width: size,
      height: size,
      borderRadius: "50%",
      border: `2px solid ${colors.border}`,
      borderTopColor: colors.accent,
      animation: "spin 0.7s linear infinite",
      display: "inline-block",
    }} />
  );
}

export function LoadingState({ message = "Loading..." }: { message?: string }) {
  return (
    <div style={{
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      gap: 12,
      padding: "48px 24px",
      color: colors.textMuted,
      fontSize: 14,
    }}>
      <LoadingSpinner />
      <span>{message}</span>
    </div>
  );
}

export function ErrorState({ message, onRetry }: { message: string; onRetry?: () => void }) {
  return (
    <div style={{
      display: "flex",
      flexDirection: "column",
      alignItems: "center",
      gap: 12,
      padding: "48px 24px",
      color: colors.critical,
      fontSize: 14,
      textAlign: "center",
    }}>
      <span style={{ fontSize: 24 }}>⚠</span>
      <span>{message}</span>
      {onRetry ? (
        <button
          onClick={onRetry}
          style={{
            padding: "6px 16px",
            borderRadius: 6,
            border: `1px solid ${colors.border}`,
            background: colors.bg3,
            color: colors.text,
            cursor: "pointer",
            fontSize: 13,
          }}
        >
          Retry
        </button>
      ) : null}
    </div>
  );
}

export function EmptyState({ message }: { message: string }) {
  return (
    <div style={{
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      padding: "48px 24px",
      color: colors.textDim,
      fontSize: 14,
      fontStyle: "italic",
    }}>
      {message}
    </div>
  );
}

// Add keyframe CSS into document head once
if (typeof document !== "undefined") {
  const id = "netobserv-keyframes";
  if (!document.getElementById(id)) {
    const style = document.createElement("style");
    style.id = id;
    style.textContent = `@keyframes spin { to { transform: rotate(360deg); } }`;
    document.head.appendChild(style);
  }
}
