import React, { useState } from "react";
import type { TopologyTruthNode, TopologyTruthPayload } from "../types/truth";
import { colors, radius, stateColor } from "../theme";
import { ConfidenceBadge, ConfidenceBar } from "./ConfidenceBadge";
import { DisputedTag } from "./StatusBadge";
import { EmptyState } from "./LoadingState";

type SortKey = "confidence" | "state" | "id";

function sortNodes(nodes: TopologyTruthNode[], key: SortKey): TopologyTruthNode[] {
  return [...nodes].sort((a, b) => {
    if (key === "confidence") return b.confidence - a.confidence;
    if (key === "state") return a.state.localeCompare(b.state);
    return a.subject_id.localeCompare(b.subject_id);
  });
}

export function TopologyTruthOverlay({ topology }: { topology: TopologyTruthPayload | null }) {
  const [tab, setTab] = useState<"nodes" | "edges">("nodes");
  const [sortKey, setSortKey] = useState<SortKey>("confidence");
  const [filter, setFilter] = useState("");
  const [showDisputedOnly, setShowDisputedOnly] = useState(false);

  if (!topology) {
    return <EmptyState message="No topology data loaded." />;
  }

  const disputedNodes = topology.nodes.filter((n) => n.disputed).length;
  const disputedEdges = topology.edges.filter((e) => e.disputed).length;

  const tabStyle = (active: boolean): React.CSSProperties => ({
    padding: "6px 14px",
    borderRadius: radius.sm,
    border: "none",
    background: active ? colors.accent : "transparent",
    color: active ? "#fff" : colors.textMuted,
    cursor: "pointer",
    fontSize: 13,
    fontWeight: active ? 700 : 400,
  });

  const sortBtnStyle = (active: boolean): React.CSSProperties => ({
    padding: "3px 8px",
    borderRadius: radius.sm,
    border: `1px solid ${active ? colors.accent : colors.border}`,
    background: active ? colors.accentBg : "transparent",
    color: active ? colors.accent : colors.textMuted,
    cursor: "pointer",
    fontSize: 11,
  });

  const nodes = sortNodes(
    topology.nodes.filter((n) => {
      if (showDisputedOnly && !n.disputed) return false;
      if (filter && !n.subject_id.includes(filter) && !n.subject_type.includes(filter)) return false;
      return true;
    }),
    sortKey,
  );

  const edges = topology.edges.filter((e) => {
    if (showDisputedOnly && !e.disputed) return false;
    if (filter && !e.subject_id.includes(filter)) return false;
    return true;
  }).sort((a, b) => b.confidence - a.confidence);

  return (
    <div style={{ display: "grid", gap: 12 }}>
      {/* Stats row */}
      <div style={{ display: "flex", gap: 16, flexWrap: "wrap", fontSize: 13, color: colors.textMuted }}>
        <span>{topology.nodes.length} nodes · <span style={{ color: disputedNodes > 0 ? colors.critical : colors.ok }}>{disputedNodes} disputed</span></span>
        <span>{topology.edges.length} edges · <span style={{ color: disputedEdges > 0 ? colors.critical : colors.ok }}>{disputedEdges} disputed</span></span>
      </div>

      {/* Controls */}
      <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
        <div style={{ display: "flex", gap: 2, background: colors.bg3, borderRadius: radius.sm, padding: 2 }}>
          <button style={tabStyle(tab === "nodes")} onClick={() => setTab("nodes")}>Nodes ({topology.nodes.length})</button>
          <button style={tabStyle(tab === "edges")} onClick={() => setTab("edges")}>Edges ({topology.edges.length})</button>
        </div>

        <input
          type="text"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder="Filter by ID or type..."
          style={{
            background: colors.bg3,
            border: `1px solid ${colors.border}`,
            borderRadius: radius.sm,
            color: colors.text,
            padding: "5px 10px",
            fontSize: 12,
            outline: "none",
            width: 180,
          }}
        />

        <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12, color: colors.textMuted, cursor: "pointer" }}>
          <input
            type="checkbox"
            checked={showDisputedOnly}
            onChange={(e) => setShowDisputedOnly(e.target.checked)}
            style={{ accentColor: colors.disputed }}
          />
          Disputed only
        </label>

        <div style={{ display: "flex", gap: 4, marginLeft: "auto", alignItems: "center" }}>
          <span style={{ fontSize: 11, color: colors.textDim }}>Sort:</span>
          {(["confidence", "state", "id"] as SortKey[]).map((k) => (
            <button key={k} style={sortBtnStyle(sortKey === k)} onClick={() => setSortKey(k)}>{k}</button>
          ))}
        </div>
      </div>

      {/* Node/Edge table */}
      {tab === "nodes" && (
        <div style={{ display: "grid", gap: 4 }}>
          {nodes.length === 0 ? (
            <EmptyState message="No nodes match the current filter." />
          ) : nodes.map((node) => (
            <div key={node.subject_id} style={{
              display: "flex",
              gap: 8,
              flexWrap: "wrap",
              alignItems: "center",
              padding: "8px 12px",
              background: node.disputed ? colors.disputedBg : colors.bg2,
              border: `1px solid ${node.disputed ? colors.disputed + "44" : colors.border}`,
              borderRadius: radius.sm,
              fontSize: 12,
            }}>
              <span style={{
                display: "inline-block",
                width: 8,
                height: 8,
                borderRadius: "50%",
                background: stateColor(node.state),
                flexShrink: 0,
              }} />
              <span style={{ fontFamily: "monospace", color: colors.text, minWidth: 120, fontWeight: 600 }}>{node.subject_id}</span>
              <span style={{ color: colors.textMuted }}>{node.subject_type}</span>
              <span style={{ color: stateColor(node.state) }}>{node.state}</span>
              {node.disputed && <DisputedTag />}
              <div style={{ marginLeft: "auto", minWidth: 140 }}>
                <ConfidenceBar confidence={node.confidence} />
              </div>
              {node.cluster_id && (
                <span style={{ fontSize: 11, color: colors.accent, fontFamily: "monospace" }}>{node.cluster_id}</span>
              )}
            </div>
          ))}
        </div>
      )}

      {tab === "edges" && (
        <div style={{ display: "grid", gap: 4 }}>
          {edges.length === 0 ? (
            <EmptyState message="No edges match the current filter." />
          ) : edges.map((edge) => (
            <div key={edge.subject_id} style={{
              display: "flex",
              gap: 8,
              flexWrap: "wrap",
              alignItems: "center",
              padding: "8px 12px",
              background: edge.disputed ? colors.disputedBg : colors.bg2,
              border: `1px solid ${edge.disputed ? colors.disputed + "44" : colors.border}`,
              borderRadius: radius.sm,
              fontSize: 12,
            }}>
              <span style={{
                display: "inline-block",
                width: 8,
                height: 8,
                borderRadius: "50%",
                background: stateColor(edge.state),
                flexShrink: 0,
              }} />
              <span style={{ fontFamily: "monospace", color: colors.text, fontWeight: 600 }}>{edge.subject_id}</span>
              <span style={{ color: stateColor(edge.state) }}>{edge.state}</span>
              {edge.disputed && <DisputedTag />}
              <div style={{ marginLeft: "auto", minWidth: 140 }}>
                <ConfidenceBar confidence={edge.confidence} />
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
