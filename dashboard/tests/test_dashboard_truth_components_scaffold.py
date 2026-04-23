from pathlib import Path


def test_truth_operations_page_wires_operational_components():
    page_path = Path(__file__).resolve().parents[3] / "dashboard" / "src" / "pages" / "TruthOperationsPage.tsx"
    text = page_path.read_text(encoding="utf-8")
    assert "fetchDisputes" in text
    assert "fetchCorrelationClusters" in text
    assert "fetchTopologyTruth" in text
    assert "<DisputedPanel" in text
    assert "<IncidentClustersPanel" in text
    assert "<ClusterTimeline" in text
    assert "<TopologyTruthOverlay" in text
