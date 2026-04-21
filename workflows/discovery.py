"""Discovery workflow — orchestrates the full collection pipeline."""

from __future__ import annotations

import asyncio
from typing import Any

from netobserv.connectors.base import CollectionContext, DiscoveryTarget
from netobserv.connectors.registry import ConnectorRegistry
from netobserv.inventory_sources.base import InventoryTarget
from netobserv.models.canonical import CanonicalObject
from netobserv.models.enums import WorkflowStage, WorkflowStatus
from netobserv.normalizers.canonical_normalizer import CanonicalNormalizer
from netobserv.observability.logging import get_logger
from netobserv.observability.metrics import get_metrics
from netobserv.parsers.eos import EOSParser
from netobserv.parsers.generic import GenericParser
from netobserv.parsers.ios import IOSParser
from netobserv.topology.builder import IntendedStateContext, TopologyBuilder
from netobserv.workflows.manager import WorkflowManager

logger = get_logger("workflows.discovery")


class DiscoveryWorkflow:
    """
    Orchestrates the discovery-to-topology pipeline.

    Stages:
      1. TARGET_RESOLUTION
      2. CREDENTIAL_BINDING
      3. COLLECTION
      4. NORMALIZATION
      5. SNAPSHOT_PERSIST
      6. TOPOLOGY_BUILD
      7. COMPLETE
    """

    def __init__(
        self,
        manager: WorkflowManager,
        connector_registry: ConnectorRegistry | None = None,
        topology_builder: TopologyBuilder | None = None,
        normalizer: CanonicalNormalizer | None = None,
        concurrency_limit: int = 20,
        read_only: bool = True,
        snapshot_repo: Any | None = None,
        device_repo: Any | None = None,
        topology_repo: Any | None = None,
    ) -> None:
        self.manager = manager
        self.registry = connector_registry or ConnectorRegistry()
        self.topology_builder = topology_builder or TopologyBuilder()
        self.normalizer = normalizer or CanonicalNormalizer()
        self.concurrency_limit = concurrency_limit
        self.read_only = read_only
        self.snapshot_repo = snapshot_repo
        self.device_repo = device_repo
        self.topology_repo = topology_repo

        # Parser registry
        self._parsers: dict[str, Any] = {
            "eos": EOSParser(),
            "ios": IOSParser(),
            "iosxe": IOSParser(),
            "generic": GenericParser(),
        }
        self._generic_parser = GenericParser()

    async def run(
        self,
        targets: list[InventoryTarget],
        credentials: dict[str, Any] | None = None,
        scope: str = "global",
        intended_context: IntendedStateContext | None = None,
    ) -> str:
        """Start and run the full discovery workflow. Returns workflow_id."""
        workflow_id = await self.manager.start_workflow(
            "discovery",
            {"scope": scope, "target_count": len(targets)},
        )

        # Create snapshot
        snapshot_id = workflow_id  # Use workflow_id as snapshot_id for simplicity
        if self.snapshot_repo:
            snap = await self.snapshot_repo.create(workflow_id=workflow_id, scope=scope)
            snapshot_id = snap.id

        try:
            # Stage 1: Target resolution
            await self.manager.execute_stage(
                workflow_id,
                WorkflowStage.TARGET_RESOLUTION.value,
                self._resolve_targets,
                targets,
            )

            # Stage 2: Credential binding (pass-through for now)
            await self.manager.execute_stage(
                workflow_id,
                WorkflowStage.CREDENTIAL_BINDING.value,
                self._bind_credentials,
                credentials or {},
            )

            # Stage 3: Collection
            collection_result = await self.manager.execute_stage(
                workflow_id,
                WorkflowStage.COLLECTION.value,
                self._collect,
                targets,
                credentials or {},
                workflow_id,
                snapshot_id,
            )

            raw_results = collection_result.get("raw_results", [])

            # Stage 4: Normalization
            norm_result = await self.manager.execute_stage(
                workflow_id,
                WorkflowStage.NORMALIZATION.value,
                self._normalize,
                raw_results,
                snapshot_id,
            )
            canonical_objects: list[CanonicalObject] = norm_result.get("canonical_objects", [])

            # Stage 5: Snapshot persist
            await self.manager.execute_stage(
                workflow_id,
                WorkflowStage.SNAPSHOT_PERSIST.value,
                self._persist_snapshot,
                canonical_objects,
                snapshot_id,
            )

            # Stage 6: Topology build
            topo_result = await self.manager.execute_stage(
                workflow_id,
                WorkflowStage.TOPOLOGY_BUILD.value,
                self._build_topology,
                canonical_objects,
                snapshot_id,
                intended_context,
            )

            await self.manager.complete_workflow(workflow_id, WorkflowStatus.COMPLETED)

        except Exception as exc:
            logger.error(
                "Discovery workflow failed",
                workflow_id=workflow_id,
                error=str(exc),
                exc_info=True,
            )
            await self.manager.complete_workflow(
                workflow_id, WorkflowStatus.FAILED, error=str(exc)
            )

        return workflow_id

    async def _resolve_targets(
        self, targets: list[InventoryTarget]
    ) -> dict[str, Any]:
        logger.info("Resolving targets", count=len(targets))
        return {"target_count": len(targets)}

    async def _bind_credentials(
        self, credentials: dict[str, Any]
    ) -> dict[str, Any]:
        return {"credential_count": len(credentials)}

    async def _collect(
        self,
        targets: list[InventoryTarget],
        credentials: dict[str, Any],
        workflow_id: str,
        snapshot_id: str,
    ) -> dict[str, Any]:
        semaphore = asyncio.Semaphore(self.concurrency_limit)
        metrics = get_metrics()

        async def collect_one(target: InventoryTarget) -> Any:
            async with semaphore:
                dt = DiscoveryTarget(
                    target_id=target.target_id,
                    hostname=target.hostname,
                    management_ip=target.management_ip,
                    transport=target.transport,
                    port=target.port,
                    platform=target.platform,
                    credential_id=target.credential_id,
                    site=target.site,
                    tags=target.tags,
                )
                connector = self.registry.resolve(dt)

                # Resolve credential
                from netobserv.connectors.base import CollectionContext as CC

                cred = None
                if target.credential_id and target.credential_id in credentials:
                    cred = credentials[target.credential_id]

                ctx = CC(
                    workflow_id=workflow_id,
                    snapshot_id=snapshot_id,
                    read_only=self.read_only,
                    credential=cred,
                )

                metrics.record_discovery_target()
                result = await connector.collect(dt, ctx)

                if result.success:
                    metrics.record_discovery_success(connector.connector_type)
                else:
                    metrics.record_discovery_failure(
                        connector.connector_type,
                        result.error_class or "unknown",
                    )
                return result

        raw_results = await asyncio.gather(
            *[collect_one(t) for t in targets],
            return_exceptions=True,
        )

        successful = [r for r in raw_results if not isinstance(r, Exception) and r.success]
        failed = len(raw_results) - len(successful)

        logger.info(
            "Collection complete",
            total=len(targets),
            successful=len(successful),
            failed=failed,
        )

        return {
            "raw_results": [r for r in raw_results if not isinstance(r, Exception)],
            "successful": len(successful),
            "failed": failed,
        }

    async def _normalize(
        self, raw_results: list[Any], snapshot_id: str
    ) -> dict[str, Any]:
        all_canonical: list[CanonicalObject] = []
        for result in raw_results:
            if not result.success:
                continue
            platform = result.target.platform or "generic"
            parser = self._parsers.get(platform, self._generic_parser)
            parsed = parser.parse(result)
            canonical = self.normalizer.normalize(parsed, snapshot_id=snapshot_id)
            all_canonical.extend(canonical)

        logger.info(
            "Normalization complete",
            canonical_objects=len(all_canonical),
        )
        return {"canonical_objects": all_canonical, "count": len(all_canonical)}

    async def _persist_snapshot(
        self, canonical_objects: list[CanonicalObject], snapshot_id: str
    ) -> dict[str, Any]:
        from netobserv.models.canonical import CanonicalDevice, CanonicalInterface
        from netobserv.storage.models import DeviceRecord, InterfaceRecord

        device_count = 0
        iface_count = 0

        if self.device_repo:
            device_records = []
            for obj in canonical_objects:
                if isinstance(obj, CanonicalDevice):
                    device_records.append(
                        DeviceRecord(
                            id=obj.device_id,
                            snapshot_id=snapshot_id,
                            hostname=obj.hostname,
                            management_ip=obj.management_ip,
                            vendor=obj.vendor,
                            model=obj.model,
                            serial_number=obj.serial_number,
                            software_version=obj.software_version,
                            device_type=obj.device_type,
                            device_role=obj.device_role,
                            site=obj.site,
                            status=obj.status.value,
                            quality=obj.quality.model_dump(),
                        )
                    )
                    device_count += 1

            if device_records:
                await self.device_repo.upsert_bulk(device_records)

        logger.info(
            "Snapshot persisted",
            snapshot_id=snapshot_id,
            devices=device_count,
        )
        return {"snapshot_id": snapshot_id, "devices": device_count}

    async def _build_topology(
        self,
        canonical_objects: list[CanonicalObject],
        snapshot_id: str,
        intended_context: IntendedStateContext | None,
    ) -> dict[str, Any]:
        result = self.topology_builder.build(
            snapshot_id=snapshot_id,
            canonical_objects=canonical_objects,
            intended_context=intended_context,
        )

        if self.topology_repo:
            from netobserv.storage.models import TopologyEdgeRecord, TopologyConflictRecord

            edge_records = []
            for edge in result.edges:
                edge_records.append(
                    TopologyEdgeRecord(
                        id=edge.edge_id,
                        snapshot_id=snapshot_id,
                        source_node_id=edge.source_node_id,
                        target_node_id=edge.target_node_id,
                        source_interface=edge.source_interface,
                        target_interface=edge.target_interface,
                        edge_type=edge.edge_type.value,
                        evidence_source=edge.evidence_source,
                        evidence_count=edge.evidence_count,
                        confidence=edge.confidence.value,
                        confidence_score=edge.confidence_score,
                        is_direct=edge.is_direct,
                        conflict_flags=edge.conflict_flags,
                        inference_method=edge.inference_method,
                    )
                )
            if edge_records:
                await self.topology_repo.save_edges(edge_records)

        get_metrics().record_snapshot("completed")

        return {
            "snapshot_id": snapshot_id,
            "edges": result.edge_count,
            "conflicts": result.conflict_count,
        }
