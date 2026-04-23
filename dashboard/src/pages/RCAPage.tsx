import React, { useEffect, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { fetchRCAGraph, fetchCorrelationClusters } from "../api/truthApi";
import { useAutoRefresh } from "../hooks/useAutoRefresh";
import { PageHeader, PageContent } from "../components/AppShell";
import { LoadingState, ErrorState, EmptyState } from "../components/LoadingState";
import { ConfidenceBadge, ConfidenceBar } from "../components/ConfidenceBadge";
import { DisputedTag } from "../components/StatusBadge";
import { colors, radius, confidenceColor } from "../theme";
import type { RCAGraph, RCANode } from "../types/truth";

const NODE_COLORS: Record<RCANode["kind"], string> = {
  event: colors.info,
  assertion: colors.accent,
  service: colors.ok,
  device: colors.warning,
  alert: colors.critical,
};

function RCANodeCard({ node, isRoot, onClick, selected }: {
  node: RCANode;
  isRoot: boolean;
  onClick: () => void;
  selected: boolean;
}) {
  const kindColor = NODE_COLORS[node.kind] ?? colors.textMuted;
  return (
    <div
      onClick={onClick}
      style={{
        background: selected ? colors.accentBg : colors.bg2,
        border: `1px solid ${selected ? colors.accent : kindColor + "44"}`,
        borderRadius: radius.md,
        padding: "10px 14px",
        cursor: "pointer",
        display: "grid",
        gap: 6,
        position: "relative",
        transition: "border-color 0.15s, background 0.15s",
      }}
    >
      {isRoot && (
        <div style={{
          position: "absolute",
          top: -1,
          right: 10,
          background: colors.critical,
          color: "#fff",
          fontSize: 9,
          fontWeight: 700,
          padding: "1px 6px",
          borderRadius: "0 0 4px 4px",
          textTransform: "uppercase",
          letterSpacing: "0.06em",
        }}>
          Root cause
        </div>
      )}
      <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
        <span style={{
          fontSize: 10,
          fontWeight: 700,
          padding: "1px 6px",
          borderRadius: radius.pill,
          background: `${kindColor}22`,
          color: kindColor,
          textTransform: "uppercase",
          letterSpacing: "0.06em",
        }}>
          {node.kind}
        </span>
        {node.disputed && <DisputedTag />}
        <span style={{ fontSize: 12, fontWeight: 600, color: colors.text }}>{node.label}</span>
      </div>
      <ConfidenceBar confidence={node.confidence} />
      {node.explanation && (
        <p style={{ fontSize: 11, color: colors.textMuted, margin: 0, lineHeight: 1.5 }}>{node.explanation}</p>
      )}
    </div>
  );
}

function RCAGraphView({ graph }: { graph: RCAGraph }) {
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const navigate = useNavigate();

  // Build adjacency for traversal
  const adjacency = graph.edges.reduce<Record<string, string[]>>((acc, e) => {
    if (!acc[e.from]) acc[e.from] = [];
    acc[e.from].push(e.to);
    return acc;
  }, {});

  // BFS from root to get levels
  const levels: string[][] = [[graph.root_id]];
  const visited = new Set([graph.root_id]);
  let frontier = [graph.root_id];
  while (frontier.length > 0) {
    const next: string[] = [];
    for (const id of frontier) {
      for (const child of adjacency[id] ?? []) {
        if (!visited.has(child)) {
          visited.add(child);
          next.push(child);
        }
      }
    }
    if (next.length > 0) levels.push(next);
    frontier = next;
  }

  // Nodes not reachable from root
  const orphans = graph.nodes.filter((n) => !visited.has(n.node_id));

  const nodeMap = Object.fromEntries(graph.nodes.map((n) => [n.node_id, n]));
  const selectedNode = selectedNodeId ? nodeMap[selectedNodeId] : null;
  const selectedEdges = graph.edges.filter((e) => e.from === selectedNodeId || e.to === selectedNodeId);

  return (
    <div style={{ display: "grid", gridTemplateColumns: "1fr 280px", gap: 20 }}>
      {/* Graph levels */}
      <div style={{ display: "grid", gap: 20 }}>
        {graph.summary && (
          <div style={{
            background: colors.bg2,
            border: `1px solid ${colors.border}`,
            borderRadius: radius.md,
            padding: "14px 18px",
            fontSize: 13,
            color: colors.textMuted,
            lineHeight: 1.6,
          }}>
            <strong style={{ color: colors.text }}>Summary:</strong> {graph.summary}
          </div>
        )}

        {levels.map((level, li) => (
          <div key={li}>
            <div style={{ fontSize: 10, color: colors.textDim, textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 8 }}>
              {li === 0 ? "Root cause" : `Downstream impact — level ${li}`}
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(240px, 1fr))", gap: 10 }}>
              {level.map((nodeId) => {
                const node = nodeMap[nodeId];
                if (!node) return null;
                return (
                  <RCANodeCard
                    key={nodeId}
                    node={node}
                    isRoot={nodeId === graph.root_id}
                    selected={selectedNodeId === nodeId}
                    onClick={() => setSelectedNodeId(selectedNodeId === nodeId ? null : nodeId)}
                  />
                );
              })}
            </div>
          </div>
        ))}

        {orphans.length > 0 && (
          <div>
            <div style={{ fontSize: 10, color: colors.textDim, textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 8 }}>
              Related nodes (unlinked)
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(240px, 1fr))", gap: 10 }}>
              {orphans.map((node) => (
                <RCANodeCard
                  key={node.node_id}
                  node={node}
                  isRoot={false}
                  selected={selectedNodeId === node.node_id}
                  onClick={() => setSelectedNodeId(selectedNodeId === node.node_id ? null : node.node_id)}
                />
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Detail panel */}
      <div style={{ position: "sticky", top: 0, alignSelf: "start", display: "grid", gap: 12 }}>
        {selectedNode ? (
          <div style={{
            background: colors.bg2,
            border: `1px solid ${colors.border}`,
            borderRadius: radius.md,
            padding: "14px 16px",
            display: "grid",
            gap: 10,
          }}>
            <div style={{ fontSize: 11, color: colors.textDim, textTransform: "uppercase", letterSpacing: "0.06em" }}>Node detail</div>
            <div style={{ fontSize: 14, fontWeight: 700, color: colors.text }}>{selectedNode.label}</div>
            <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
              <span style={{
                fontSize: 10,
                fontWeight: 700,
                padding: "1px 6px",
                borderRadius: radius.pill,
                background: `${NODE_COLORS[selectedNode.kind] ?? colors.accent}22`,
                color: NODE_COLORS[selectedNode.kind] ?? colors.accent,
                textTransform: "uppercase",
              }}>{selectedNode.kind}</span>
              {selectedNode.disputed && <DisputedTag />}
            </div>
            <ConfidenceBadge confidence={selectedNode.confidence} />
            {selectedNode.explanation && (
              <p style={{ fontSize: 12, color: colors.textMuted, margin: 0, lineHeight: 1.6 }}>{selectedNode.explanation}</p>
            )}
            {selectedNode.cluster_id && (
              <button
                onClick={() => navigate(`/incidents?cluster=${selectedNode.cluster_id}`)}
                style={{ padding: "5px 10px", borderRadius: radius.sm, border: `1px solid ${colors.accent}44`, background: colors.accentBg, color: colors.accent, cursor: "pointer", fontSize: 12, textAlign: "left" }}
              >
                View incident cluster →
              </button>
            )}
          </div>
        ) : (
          <div style={{
            background: colors.bg2,
            border: `1px solid ${colors.border}`,
            borderRadius: radius.md,
            padding: "14px 16px",
            fontSize: 12,
            color: colors.textMuted,
            fontStyle: "italic",
          }}>
            Click a node to inspect details and related edges.
          </div>
        )}

        {selectedEdges.length > 0 && (
          <div style={{
            background: colors.bg2,
            border: `1px solid ${colors.border}`,
            borderRadius: radius.md,
            padding: "14px 16px",
            display: "grid",
            gap: 8,
          }}>
            <div style={{ fontSize: 11, color: colors.textDim, textTransform: "uppercase", letterSpacing: "0.06em" }}>
              Causal edges ({selectedEdges.length})
            </div>
            {selectedEdges.map((edge, i) => (
              <div key={i} style={{ display: "grid", gap: 4, borderTop: i > 0 ? `1px solid ${colors.border}` : "none", paddingTop: i > 0 ? 8 : 0 }}>
                <div style={{ fontSize: 12, color: colors.text }}>
                  <span style={{ color: colors.textMuted }}>{nodeMap[edge.from]?.label ?? edge.from}</span>
                  <span style={{ color: colors.accent, margin: "0 6px" }}>→</span>
                  <span style={{ color: colors.textMuted }}>{nodeMap[edge.to]?.label ?? edge.to}</span>
                </div>
                <div style={{ fontSize: 11, color: colors.textMuted }}>{edge.relation}</div>
                <ConfidenceBadge confidence={edge.confidence} />
              </div>
            ))}
          </div>
        )}

        {/* Cluster summary stats */}
        <div style={{
          background: colors.bg2,
          border: `1px solid ${colors.border}`,
          borderRadius: radius.md,
          padding: "14px 16px",
          display: "grid",
          gap: 8,
          fontSize: 12,
          color: colors.textMuted,
        }}>
          <div style={{ fontSize: 11, color: colors.textDim, textTransform: "uppercase", letterSpacing: "0.06em" }}>Graph stats</div>
          <div>{graph.nodes.length} nodes · {graph.edges.length} edges</div>
          <div>Root: <span style={{ color: colors.text, fontFamily: "monospace" }}>{graph.root_id.slice(0, 20)}…</span></div>
          <div>Disputed: <span style={{ color: graph.nodes.filter((n) => n.disputed).length > 0 ? colors.disputed : colors.ok }}>
            {graph.nodes.filter((n) => n.disputed).length}
          </span></div>
          <div>Avg confidence: <span style={{ color: confidenceColor(graph.nodes.reduce((s, n) => s + n.confidence, 0) / Math.max(graph.nodes.length, 1)) }}>
            {((graph.nodes.reduce((s, n) => s + n.confidence, 0) / Math.max(graph.nodes.length, 1)) * 100).toFixed(0)}%
          </span></div>
        </div>
      </div>
    </div>
  );
}

export default function RCAPage() {
  const [searchParams] = useSearchParams();
  const clusterId = searchParams.get("cluster");
  const [inputClusterId, setInputClusterId] = useState(clusterId ?? "");
  const [activeClusterId, setActiveClusterId] = useState(clusterId ?? "");
  const [graph, setGraph] = useState<RCAGraph | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const clusters = useAutoRefresh(() => fetchCorrelationClusters(50), 60_000);

  async function loadRCA(id: string) {
    if (!id) return;
    setLoading(true);
    setError(null);
    setGraph(null);
    try {
      const g = await fetchRCAGraph(id);
      setGraph(g);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (activeClusterId) {
      loadRCA(activeClusterId);
    }
  }, [activeClusterId]);

  return (
    <div>
      <PageHeader
        title="Root Cause Analysis"
        subtitle="Causal chain visualization — trace from alert to root infrastructure cause"
      />
      <PageContent>
        {/* Cluster selector */}
        <div style={{ display: "flex", gap: 8, marginBottom: 24, flexWrap: "wrap", alignItems: "center" }}>
          <input
            type="text"
            value={inputClusterId}
            onChange={(e) => setInputClusterId(e.target.value)}
            placeholder="Enter cluster ID..."
            style={{
              background: colors.bg3,
              border: `1px solid ${colors.border}`,
              borderRadius: radius.sm,
              color: colors.text,
              padding: "6px 12px",
              fontSize: 13,
              outline: "none",
              width: 280,
              fontFamily: "monospace",
            }}
            onKeyDown={(e) => e.key === "Enter" && setActiveClusterId(inputClusterId)}
          />
          <button
            onClick={() => setActiveClusterId(inputClusterId)}
            disabled={!inputClusterId || loading}
            style={{
              padding: "6px 16px",
              borderRadius: radius.sm,
              border: `1px solid ${colors.accent}88`,
              background: colors.accentBg,
              color: colors.accent,
              cursor: inputClusterId && !loading ? "pointer" : "default",
              fontSize: 13,
              fontWeight: 600,
              opacity: !inputClusterId || loading ? 0.5 : 1,
            }}
          >
            Load RCA
          </button>

          {/* Quick-select from active clusters */}
          {(clusters.data ?? []).length > 0 && (
            <select
              value={activeClusterId}
              onChange={(e) => { setInputClusterId(e.target.value); setActiveClusterId(e.target.value); }}
              style={{
                background: colors.bg3,
                border: `1px solid ${colors.border}`,
                borderRadius: radius.sm,
                color: colors.text,
                padding: "6px 10px",
                fontSize: 12,
                marginLeft: 8,
              }}
            >
              <option value="">Select cluster…</option>
              {(clusters.data ?? []).map((c) => (
                <option key={c.cluster_id} value={c.cluster_id}>
                  {c.cluster_id} ({c.state})
                </option>
              ))}
            </select>
          )}
        </div>

        {loading && <LoadingState message="Building RCA graph..." />}
        {error && <ErrorState message={error} />}
        {!loading && !error && !graph && !activeClusterId && (
          <EmptyState message="Enter a cluster ID or select one from the list to view the RCA graph." />
        )}
        {!loading && !error && !graph && activeClusterId && (
          <EmptyState message={`No RCA data available for cluster ${activeClusterId}.`} />
        )}
        {graph && <RCAGraphView graph={graph} />}
      </PageContent>
    </div>
  );
}
