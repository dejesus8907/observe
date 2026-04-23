import React from "react";
import type { CorrelationClusterSummary } from "../types/truth";
import { ConfidenceBadge } from "./ConfidenceBadge";

export function IncidentClustersPanel({
  clusters,
  onSelectCluster,
}: {
  clusters: CorrelationClusterSummary[];
  onSelectCluster: (clusterId: string) => void;
}) {
  return (
    <section>
      <h2>Incident clusters</h2>
      {clusters.length === 0 ? (
        <p>No clusters returned by the API.</p>
      ) : (
        <div style={{ display: "grid", gap: 12 }}>
          {clusters.map((cluster) => (
            <article key={cluster.cluster_id} style={{ border: "1px solid #444", borderRadius: 12, padding: 12 }}>
              <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "center" }}>
                <div>
                  <strong>{cluster.cluster_id}</strong>
                  <div>state={cluster.state}</div>
                </div>
                <ConfidenceBadge confidence={cluster.cluster_confidence ?? 0} />
              </div>
              <p>
                root={cluster.root_event_id ?? "unknown"} · devices={(cluster.device_ids ?? []).join(", ") || "n/a"}
              </p>
              <button onClick={() => onSelectCluster(cluster.cluster_id)}>View timeline</button>
            </article>
          ))}
        </div>
      )}
    </section>
  );
}
