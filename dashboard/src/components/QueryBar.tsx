import React from "react";
import { colors, radius } from "../theme";
import { useQuery, type TimeRange } from "../context/QueryContext";

const TIME_RANGES: { label: string; value: TimeRange }[] = [
  { label: "15m", value: "15m" },
  { label: "1h", value: "1h" },
  { label: "6h", value: "6h" },
  { label: "24h", value: "24h" },
  { label: "7d", value: "7d" },
  { label: "30d", value: "30d" },
];

const SEVERITIES = ["", "critical", "warning", "info"];

interface QueryBarProps {
  showService?: boolean;
  showSeverity?: boolean;
  showSearch?: boolean;
  placeholder?: string;
}

export function QueryBar({
  showService = false,
  showSeverity = false,
  showSearch = true,
  placeholder = "Filter by keyword...",
}: QueryBarProps) {
  const { timeRange, service, severity, search, setTimeRange, setService, setSeverity, setSearch, reset } = useQuery();

  const inputStyle: React.CSSProperties = {
    background: colors.bg3,
    border: `1px solid ${colors.border}`,
    borderRadius: radius.sm,
    color: colors.text,
    padding: "6px 10px",
    fontSize: 13,
    outline: "none",
    height: 32,
  };

  const selectStyle: React.CSSProperties = {
    ...inputStyle,
    cursor: "pointer",
    appearance: "none" as const,
    paddingRight: 24,
  };

  return (
    <div style={{
      display: "flex",
      flexWrap: "wrap",
      gap: 8,
      alignItems: "center",
      padding: "10px 0",
    }}>
      {/* Time range buttons */}
      <div style={{ display: "flex", gap: 2, background: colors.bg3, borderRadius: radius.sm, padding: 2 }}>
        {TIME_RANGES.map((tr) => (
          <button
            key={tr.value}
            onClick={() => setTimeRange(tr.value)}
            style={{
              padding: "3px 10px",
              borderRadius: radius.sm,
              border: "none",
              background: timeRange === tr.value ? colors.accent : "transparent",
              color: timeRange === tr.value ? "#fff" : colors.textMuted,
              cursor: "pointer",
              fontSize: 12,
              fontWeight: timeRange === tr.value ? 700 : 400,
              transition: "background 0.15s",
            }}
          >
            {tr.label}
          </button>
        ))}
      </div>

      {showSeverity && (
        <select value={severity} onChange={(e) => setSeverity(e.target.value)} style={selectStyle}>
          <option value="">All severities</option>
          {SEVERITIES.slice(1).map((s) => (
            <option key={s} value={s}>{s}</option>
          ))}
        </select>
      )}

      {showService && (
        <input
          type="text"
          value={service}
          onChange={(e) => setService(e.target.value)}
          placeholder="Service..."
          style={{ ...inputStyle, width: 140 }}
        />
      )}

      {showSearch && (
        <input
          type="text"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder={placeholder}
          style={{ ...inputStyle, width: 220 }}
        />
      )}

      {(timeRange !== "1h" || service || severity || search) && (
        <button
          onClick={reset}
          style={{
            padding: "4px 10px",
            borderRadius: radius.sm,
            border: `1px solid ${colors.border}`,
            background: "transparent",
            color: colors.textMuted,
            cursor: "pointer",
            fontSize: 12,
          }}
        >
          Reset
        </button>
      )}
    </div>
  );
}
