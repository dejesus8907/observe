import React, { useEffect, useState } from "react";
import { fetchCorrelationCluster, fetchCorrelationClusters, fetchDisputes, fetchTopologyTruth } from "../api/truthApi";
import { ClusterTimeline } from "../components/ClusterTimeline";
import { DisputedPanel } from "../components/DisputedPanel";
import { IncidentClustersPanel } from "../components/IncidentClustersPanel";
import { TopologyTruthOverlay } from "../components/TopologyTruthOverlay";
import type { CorrelationClusterDetail, CorrelationClusterSummary, ResolvedAssertion, TopologyTruthPayload } from "../types/truth";

export default function TruthOperationsPage() {
  const [disputes, setDisputes] = useState<ResolvedAssertion[]>([]);
  const [clusters, setClusters] = useState<CorrelationClusterSummary[]>([]);
  const [selectedCluster, setSelectedCluster] = useState<CorrelationClusterDetail | null>(null);
  const [topology, setTopology] = useState<TopologyTruthPayload | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    Promise.all([
      fetchDisputes(),
      fetchCorrelationClusters(),
      fetchTopologyTruth(),
    ]).then(([d, c, t]) => {
      setDisputes(d);
      setClusters(c);
      setTopology(t);
    }).catch((err) => {
      setError(err instanceof Error ? err.message : String(err));
    });
  }, []);

  async function handleSelectCluster(clusterId: string) {
    try {
      const detail = await fetchCorrelationCluster(clusterId);
      setSelectedCluster(detail);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  return (
    <main style={{ display: "grid", gap: 24, padding: 24 }}>
      <header>
        <h1>NetObserv truth operations</h1>
        <p>
          This page is intentionally conflict-aware. It does not flatten disputed or low-confidence state into fake certainty.
        </p>
        {error ? <p style={{ color: "crimson" }}>{error}</p> : null}
      </header>

      <DisputedPanel disputes={disputes} />
      <IncidentClustersPanel clusters={clusters} onSelectCluster={handleSelectCluster} />
      <ClusterTimeline cluster={selectedCluster} />
      <TopologyTruthOverlay topology={topology} />
    </main>
  );
}
