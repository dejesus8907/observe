"""Integration test: end-to-end discovery pipeline using mock connector."""

from __future__ import annotations

import asyncio
import pytest

from netobserv.connectors.base import (
    BaseConnector,
    CollectionContext,
    DiscoveryTarget,
    RawCollectionResult,
)
from netobserv.connectors.registry import ConnectorRegistry
from netobserv.inventory_sources.base import InventoryTarget
from netobserv.models.canonical import CanonicalDevice, CanonicalNeighborRelationship
from netobserv.models.enums import WorkflowStatus
from netobserv.normalizers.canonical_normalizer import CanonicalNormalizer
from netobserv.topology.builder import TopologyBuilder
from netobserv.workflows.discovery import DiscoveryWorkflow
from netobserv.workflows.manager import WorkflowManager


class MockSSHConnector(BaseConnector):
    """Connector that returns deterministic fixture data for testing."""

    connector_type = "ssh"

    _FIXTURES = {
        "router1": RawCollectionResult(
            target=DiscoveryTarget(
                target_id="t1", hostname="router1", management_ip="10.0.0.1", transport="ssh"
            ),
            workflow_id="",
            snapshot_id="",
            connector_type="ssh",
            success=True,
            raw_device_facts={"hostname": "router1", "vendor": "Arista", "model": "DCS-7050"},
            raw_interfaces=[{"name": "Ethernet1", "admin_state": "up", "oper_state": "up"}],
            raw_neighbors=[
                {
                    "local_hostname": "router1",
                    "local_interface": "Ethernet1",
                    "remote_hostname": "router2",
                    "remote_interface": "Ethernet1",
                    "protocol": "lldp",
                }
            ],
        ),
        "router2": RawCollectionResult(
            target=DiscoveryTarget(
                target_id="t2", hostname="router2", management_ip="10.0.0.2", transport="ssh"
            ),
            workflow_id="",
            snapshot_id="",
            connector_type="ssh",
            success=True,
            raw_device_facts={"hostname": "router2", "vendor": "Arista", "model": "DCS-7050"},
            raw_interfaces=[{"name": "Ethernet1", "admin_state": "up", "oper_state": "up"}],
            raw_neighbors=[
                {
                    "local_hostname": "router2",
                    "local_interface": "Ethernet1",
                    "remote_hostname": "router1",
                    "remote_interface": "Ethernet1",
                    "protocol": "lldp",
                }
            ],
        ),
    }

    async def _collect_impl(
        self, target: DiscoveryTarget, context: CollectionContext
    ) -> RawCollectionResult:
        fixture = self._FIXTURES.get(target.hostname)
        if fixture:
            return fixture.model_copy(
                update={
                    "workflow_id": context.workflow_id,
                    "snapshot_id": context.snapshot_id,
                    "target": target,
                }
            )
        return RawCollectionResult(
            target=target,
            workflow_id=context.workflow_id,
            snapshot_id=context.snapshot_id,
            connector_type=self.connector_type,
            success=False,
            error_message="No fixture for target",
        )


@pytest.fixture
def mock_registry() -> ConnectorRegistry:
    registry = ConnectorRegistry()
    registry.register("ssh", MockSSHConnector())
    return registry


@pytest.fixture
def discovery_workflow(mock_registry: ConnectorRegistry) -> DiscoveryWorkflow:
    manager = WorkflowManager()
    return DiscoveryWorkflow(
        manager=manager,
        connector_registry=mock_registry,
        concurrency_limit=5,
    )


class TestEndToEndDiscovery:
    @pytest.mark.asyncio
    async def test_discovers_two_devices(
        self, discovery_workflow: DiscoveryWorkflow
    ) -> None:
        targets = [
            InventoryTarget(
                hostname="router1",
                management_ip="10.0.0.1",
                transport="ssh",
                platform="generic",
            ),
            InventoryTarget(
                hostname="router2",
                management_ip="10.0.0.2",
                transport="ssh",
                platform="generic",
            ),
        ]
        workflow_id = await discovery_workflow.run(targets)
        assert workflow_id is not None

        status = await discovery_workflow.manager.get_status(workflow_id)
        assert status["status"] == "completed"

    @pytest.mark.asyncio
    async def test_topology_built_from_lldp(
        self, discovery_workflow: DiscoveryWorkflow
    ) -> None:
        targets = [
            InventoryTarget(hostname="router1", management_ip="10.0.0.1", transport="ssh"),
            InventoryTarget(hostname="router2", management_ip="10.0.0.2", transport="ssh"),
        ]
        workflow_id = await discovery_workflow.run(targets)
        ctx = discovery_workflow.manager._active.get(workflow_id)
        assert ctx is not None

        # Verify topology build stage ran
        topology_step = next(
            (s for s in ctx.steps if s.stage == "TOPOLOGY_BUILD"), None
        )
        assert topology_step is not None
        assert topology_step.status == "completed"
        assert topology_step.artifacts.get("edges", 0) >= 1

    @pytest.mark.asyncio
    async def test_failed_target_does_not_abort_workflow(
        self, discovery_workflow: DiscoveryWorkflow
    ) -> None:
        targets = [
            InventoryTarget(hostname="router1", management_ip="10.0.0.1", transport="ssh"),
            InventoryTarget(
                hostname="unreachable-device",
                management_ip="10.99.99.99",
                transport="ssh",
            ),
        ]
        workflow_id = await discovery_workflow.run(targets)
        status = await discovery_workflow.manager.get_status(workflow_id)
        # Should still complete even though one target had no fixture
        assert status["status"] == "completed"
