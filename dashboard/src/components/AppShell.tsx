import React, { useState } from "react";
import { NavLink, Outlet, useLocation } from "react-router-dom";
import { colors, radius } from "../theme";

interface NavItem {
  path: string;
  label: string;
  icon: string;
  group?: string;
}

const NAV: NavItem[] = [
  { path: "/", label: "Overview", icon: "◈", group: "main" },
  { path: "/incidents", label: "Incidents", icon: "⚡", group: "main" },
  { path: "/alerts", label: "Alerts & SLOs", icon: "🔔", group: "main" },
  { path: "/topology", label: "Topology", icon: "⬡", group: "explore" },
  { path: "/services", label: "Services", icon: "⬗", group: "explore" },
  { path: "/telemetry", label: "Telemetry", icon: "〜", group: "explore" },
  { path: "/truth", label: "Truth Ops", icon: "⊛", group: "truth" },
  { path: "/rca", label: "RCA", icon: "⊞", group: "truth" },
];

const GROUPS: { id: string; label: string }[] = [
  { id: "main", label: "Operations" },
  { id: "explore", label: "Explore" },
  { id: "truth", label: "Truth & Causality" },
];

export function AppShell() {
  const [collapsed, setCollapsed] = useState(false);
  const location = useLocation();

  const linkStyle = (active: boolean): React.CSSProperties => ({
    display: "flex",
    alignItems: "center",
    gap: collapsed ? 0 : 10,
    padding: "7px 12px",
    borderRadius: radius.sm,
    textDecoration: "none",
    color: active ? colors.text : colors.textMuted,
    background: active ? colors.bg3 : "transparent",
    borderLeft: `2px solid ${active ? colors.accent : "transparent"}`,
    fontSize: 13,
    fontWeight: active ? 600 : 400,
    transition: "background 0.12s, color 0.12s",
    overflow: "hidden",
    whiteSpace: "nowrap",
  });

  const iconStyle: React.CSSProperties = {
    fontSize: 16,
    width: 20,
    textAlign: "center",
    flexShrink: 0,
    color: "inherit",
  };

  return (
    <div style={{ display: "flex", minHeight: "100vh", background: colors.bg0, color: colors.text }}>
      {/* Sidebar */}
      <aside style={{
        width: collapsed ? 52 : 200,
        minWidth: collapsed ? 52 : 200,
        background: colors.bg1,
        borderRight: `1px solid ${colors.border}`,
        display: "flex",
        flexDirection: "column",
        transition: "width 0.2s ease, min-width 0.2s ease",
        overflow: "hidden",
        position: "sticky",
        top: 0,
        height: "100vh",
      }}>
        {/* Logo */}
        <div style={{
          padding: collapsed ? "16px 14px" : "16px 16px",
          borderBottom: `1px solid ${colors.border}`,
          display: "flex",
          alignItems: "center",
          gap: 8,
          justifyContent: collapsed ? "center" : "space-between",
        }}>
          {!collapsed && (
            <span style={{ fontWeight: 800, fontSize: 14, color: colors.accent, letterSpacing: "0.04em" }}>
              NetObserv
            </span>
          )}
          <button
            onClick={() => setCollapsed(!collapsed)}
            style={{
              background: "none",
              border: "none",
              color: colors.textMuted,
              cursor: "pointer",
              fontSize: 16,
              padding: 0,
              lineHeight: 1,
            }}
            title={collapsed ? "Expand sidebar" : "Collapse sidebar"}
          >
            {collapsed ? "»" : "«"}
          </button>
        </div>

        {/* Nav */}
        <nav style={{ flex: 1, padding: "8px 6px", overflowY: "auto", display: "flex", flexDirection: "column", gap: 4 }}>
          {GROUPS.map((group) => {
            const items = NAV.filter((n) => n.group === group.id);
            return (
              <div key={group.id} style={{ display: "flex", flexDirection: "column", gap: 2, marginBottom: 8 }}>
                {!collapsed && (
                  <div style={{
                    fontSize: 10,
                    color: colors.textDim,
                    textTransform: "uppercase",
                    letterSpacing: "0.08em",
                    padding: "4px 12px 2px",
                    fontWeight: 700,
                  }}>
                    {group.label}
                  </div>
                )}
                {items.map((item) => {
                  const active = item.path === "/"
                    ? location.pathname === "/"
                    : location.pathname.startsWith(item.path);
                  return (
                    <NavLink key={item.path} to={item.path} style={linkStyle(active)} title={collapsed ? item.label : undefined}>
                      <span style={iconStyle}>{item.icon}</span>
                      {!collapsed && <span>{item.label}</span>}
                    </NavLink>
                  );
                })}
              </div>
            );
          })}
        </nav>

        {/* Footer */}
        <div style={{
          padding: "10px 12px",
          borderTop: `1px solid ${colors.border}`,
          fontSize: 10,
          color: colors.textDim,
          textAlign: collapsed ? "center" : "left",
        }}>
          {!collapsed ? "v0.2.0" : "v"}
        </div>
      </aside>

      {/* Main content */}
      <main style={{ flex: 1, overflow: "auto", display: "flex", flexDirection: "column" }}>
        <Outlet />
      </main>
    </div>
  );
}

export function PageHeader({
  title,
  subtitle,
  actions,
  lastRefreshed,
}: {
  title: string;
  subtitle?: string;
  actions?: React.ReactNode;
  lastRefreshed?: Date | null;
}) {
  return (
    <div style={{
      padding: "20px 28px 16px",
      borderBottom: `1px solid ${colors.border}`,
      display: "flex",
      alignItems: "flex-start",
      gap: 16,
      justifyContent: "space-between",
      flexWrap: "wrap",
      background: colors.bg1,
    }}>
      <div>
        <h1 style={{ margin: 0, fontSize: 20, fontWeight: 700, color: colors.text }}>{title}</h1>
        {subtitle && <p style={{ margin: "4px 0 0", fontSize: 13, color: colors.textMuted }}>{subtitle}</p>}
      </div>
      <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
        {lastRefreshed && (
          <span style={{ fontSize: 11, color: colors.textDim }}>
            updated {lastRefreshed.toLocaleTimeString()}
          </span>
        )}
        {actions}
      </div>
    </div>
  );
}

export function PageContent({ children }: { children: React.ReactNode }) {
  return (
    <div style={{ padding: "24px 28px", flex: 1 }}>
      {children}
    </div>
  );
}
