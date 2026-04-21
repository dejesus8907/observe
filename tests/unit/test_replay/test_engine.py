from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace

import pytest

from netobserv.replay.engine import ReplayEngine


@dataclass
class Step:
    stage_name: str
    status: str = "completed"
    elapsed_seconds: float | None = 1.0
    artifacts: dict = field(default_factory=dict)
    errors: list = field(default_factory=list)
    warnings: list = field(default_factory=list)
    started_at: object | None = None
    completed_at: object | None = None


@dataclass
class Workflow:
    id: str
    workflow_type: str = "discovery"
    status: str = "completed"
    metadata_: dict = field(default_factory=lambda: {"scope": "global"})
    steps: list[Step] = field(default_factory=list)


@dataclass
class Snapshot:
    id: str
    workflow_id: str
    scope: str = "global"
    status: str = "completed"
    device_count: int = 0
    interface_count: int = 0
    edge_count: int = 0
    drift_count: int = 0
    collection_errors: int = 0


class WorkflowRepo:
    def __init__(self, workflow: Workflow | None):
        self.workflow = workflow

    async def get(self, workflow_id: str):
        if self.workflow and self.workflow.id == workflow_id:
            return self.workflow
        return None


class SnapshotRepo:
    def __init__(self, snapshots: list[Snapshot]):
        self.snapshots = snapshots

    async def list_recent(self, limit: int = 25, scope: str = "global"):
        return [s for s in self.snapshots if s.scope == scope][:limit]


class DeviceRepo:
    def __init__(self, devices_by_snapshot: dict[str, list]):
        self.devices_by_snapshot = devices_by_snapshot

    async def get_by_snapshot(self, snapshot_id: str):
        return self.devices_by_snapshot.get(snapshot_id, [])


class TopologyRepo:
    def __init__(self, edges_by_snapshot: dict[str, list]):
        self.edges_by_snapshot = edges_by_snapshot

    async def get_edges_for_snapshot(self, snapshot_id: str):
        return self.edges_by_snapshot.get(snapshot_id, [])


class ReplayRepo:
    def __init__(self):
        self.records = {}

    async def upsert(self, **kwargs):
        self.records[kwargs["replay_id"]] = kwargs


@pytest.mark.asyncio
async def test_replay_passes_when_snapshot_recomputed_counts_match():
    workflow = Workflow(
        id="wf-1",
        steps=[Step("COLLECTION"), Step("NORMALIZATION", artifacts={"count": 2})],
    )
    snapshot = Snapshot(id="snap-1", workflow_id="wf-1", device_count=2, edge_count=1)
    devices = [
        SimpleNamespace(hostname="leaf1", vendor="arista", interfaces=[1, 2]),
        SimpleNamespace(hostname="leaf2", vendor="cisco", interfaces=[1]),
    ]
    edges = [SimpleNamespace(source_node_id="leaf1", target_node_id="leaf2")]
    replay_repo = ReplayRepo()

    engine = ReplayEngine(
        workflow_repo=WorkflowRepo(workflow),
        snapshot_repo=SnapshotRepo([snapshot]),
        replay_repo=replay_repo,
    )

    result = await engine.replay(
        source_workflow_id="wf-1",
        device_repo=DeviceRepo({"snap-1": devices}),
        topology_repo=TopologyRepo({"snap-1": edges}),
    )

    assert result.status == "completed"
    assert result.passed is True
    assert result.diff_summary["snapshot"]["mismatches"] == {}
    assert replay_repo.records[result.replay_id]["status"] == "completed"


@pytest.mark.asyncio
async def test_replay_fails_when_snapshot_counts_do_not_match():
    workflow = Workflow(id="wf-1", steps=[Step("COLLECTION")])
    snapshot = Snapshot(id="snap-1", workflow_id="wf-1", device_count=99, edge_count=1)
    devices = [SimpleNamespace(hostname="leaf1", vendor="arista", interfaces=[])]
    edges = [SimpleNamespace(source_node_id="leaf1", target_node_id="leaf2")]

    engine = ReplayEngine(
        workflow_repo=WorkflowRepo(workflow),
        snapshot_repo=SnapshotRepo([snapshot]),
        replay_repo=ReplayRepo(),
    )

    result = await engine.replay(
        source_workflow_id="wf-1",
        device_repo=DeviceRepo({"snap-1": devices}),
        topology_repo=TopologyRepo({"snap-1": edges}),
    )

    assert result.passed is False
    assert "device_count" in result.diff_summary["snapshot"]["mismatches"]


@pytest.mark.asyncio
async def test_replay_fails_cleanly_when_source_workflow_missing():
    engine = ReplayEngine(
        workflow_repo=WorkflowRepo(None),
        snapshot_repo=SnapshotRepo([]),
        replay_repo=ReplayRepo(),
    )

    result = await engine.replay("missing")

    assert result.status == "failed"
    assert result.passed is False
    assert any("not found" in note for note in result.notes)


@pytest.mark.asyncio
async def test_diff_snapshots_reports_added_removed_and_changed_devices():
    device_repo = DeviceRepo(
        {
            "a": [
                SimpleNamespace(
                    hostname="leaf1",
                    management_ip="10.0.0.1",
                    vendor="arista",
                    model="7050",
                    software_version="1",
                    site="dc1",
                    status="active",
                )
            ],
            "b": [
                SimpleNamespace(
                    hostname="leaf1",
                    management_ip="10.0.0.1",
                    vendor="arista",
                    model="7050",
                    software_version="2",
                    site="dc1",
                    status="active",
                ),
                SimpleNamespace(
                    hostname="leaf2",
                    management_ip="10.0.0.2",
                    vendor="cisco",
                    model="9k",
                    software_version="1",
                    site="dc1",
                    status="active",
                ),
            ],
        }
    )
    topology_repo = TopologyRepo(
        {
            "a": [SimpleNamespace(source_node_id="leaf1", target_node_id="spine1")],
            "b": [SimpleNamespace(source_node_id="leaf1", target_node_id="spine2")],
        }
    )

    diff = await ReplayEngine().diff_snapshots("a", "b", device_repo=device_repo, topology_repo=topology_repo)

    assert diff["devices"]["added"] == ["leaf2"]
    assert diff["devices"]["changed"] == ["leaf1"]
    assert diff["edges"]["counts"]["added"] == 1
    assert diff["edges"]["counts"]["removed"] == 1
