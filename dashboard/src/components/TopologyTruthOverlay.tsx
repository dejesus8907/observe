import React from "react";
import type { TopologyTruthPayload } from "../types/truth";
import { ConfidenceBadge } from "./ConfidenceBadge";

export function TopologyTruthOverlay({ topology }: { topology: TopologyTruthPayload | null }) {
  if (!topology) {
    return (
      <section>
        <h2>Topology overlays</h2>
        <p>No topology truth payload loaded yet.</p>
      </section>
    );
  }

  return (
    <section>
      <h2>Topology overlays for dispute / confidence / lineage</h2>
      <div style={{ display: "grid", gap: 12 }}>
        <article style={{ border: "1px solid #444", borderRadius: 12, padding: 12 }}>
          <h3>Nodes</h3>
          <div style={{ display: "grid", gap: 8 }}>
            {topology.nodes.map((node) => (
              <div key={node.subject_id} style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
                <strong>{node.subject_id}</strong>
                <span>state={node.state}</span>
                {node.disputed ? <span>DISPUTED</span> : null}
                <ConfidenceBadge confidence={node.confidence} />
                {node.cluster_id ? <span>cluster={node.cluster_id}</span> : null}
              </div>
            ))}
          </div>
        </article>
        <article style={{ border: "1px solid #444", borderRadius: 12, padding: 12 }}>
          <h3>Edges</h3>
          <div style={{ display: "grid", gap: 8 }}>
            {topology.edges.map((edge) => (
              <div key={edge.subject_id} style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
                <strong>{edge.subject_id}</strong>
                <span>state={edge.state}</span>
                {edge.disputed ? <span>DISPUTED</span> : null}
                <ConfidenceBadge confidence={edge.confidence} />
                {edge.cluster_id ? <span>cluster={edge.cluster_id}</span> : null}
              </div>
            ))}
          </div>
        </article>
      </div>
    </section>
  );
}
