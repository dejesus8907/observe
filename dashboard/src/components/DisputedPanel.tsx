import React from "react";
import type { ResolvedAssertion } from "../types/truth";
import { ConfidenceBadge } from "./ConfidenceBadge";
import { FreshnessBadge } from "./FreshnessBadge";

export function DisputedPanel({ disputes }: { disputes: ResolvedAssertion[] }) {
  return (
    <section>
      <h2>Disputed links / devices</h2>
      {disputes.length === 0 ? (
        <p>No disputed assertions returned by the API.</p>
      ) : (
        <div style={{ display: "grid", gap: 12 }}>
          {disputes.map((item, idx) => (
            <article key={`${item.subject_id}-${item.field_name}-${idx}`} style={{ border: "1px solid #444", borderRadius: 12, padding: 12 }}>
              <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
                <strong>{item.subject_type}:{item.subject_id}</strong>
                <span>{item.field_name}</span>
                <ConfidenceBadge confidence={item.confidence} />
                <FreshnessBadge observedAt={item.observed_at} staleAfter={item.stale_after} />
              </div>
              <p style={{ marginTop: 8 }}>
                state=<strong>{item.resolution_state}</strong> · source=<strong>{item.last_authoritative_source ?? "unknown"}</strong>
                {item.cluster_id ? <> · cluster=<strong>{item.cluster_id}</strong></> : null}
              </p>
            </article>
          ))}
        </div>
      )}
    </section>
  );
}
