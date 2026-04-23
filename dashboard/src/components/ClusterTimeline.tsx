import React from "react";
import type { CorrelationClusterDetail } from "../types/truth";
import { ConfidenceBadge } from "./ConfidenceBadge";

export function ClusterTimeline({ cluster }: { cluster: CorrelationClusterDetail | null }) {
  if (!cluster) {
    return (
      <section>
        <h2>Cluster detail timeline</h2>
        <p>Select a cluster to view its timeline.</p>
      </section>
    );
  }

  return (
    <section>
      <h2>Cluster detail timeline</h2>
      <article style={{ border: "1px solid #444", borderRadius: 12, padding: 12 }}>
        <p>
          cluster=<strong>{cluster.cluster.cluster_id}</strong> · state=<strong>{cluster.cluster.state}</strong> ·
          root=<strong>{cluster.cluster.root_event_id ?? "unknown"}</strong>
        </p>
        <div style={{ display: "grid", gap: 10 }}>
          {cluster.memberships.map((member) => (
            <div key={member.membership_id} style={{ borderTop: "1px solid #333", paddingTop: 10 }}>
              <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
                <strong>{member.event_id ?? member.assertion_id ?? "unknown member"}</strong>
                <span>role={member.role}</span>
                <ConfidenceBadge confidence={member.confidence} />
              </div>
              <div>joined={member.joined_at}</div>
            </div>
          ))}
        </div>
      </article>
    </section>
  );
}
