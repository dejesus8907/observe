"""Topology builder — constructs topology from canonical objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from netobserv.models.canonical import (
    CanonicalDevice,
    CanonicalInterface,
    CanonicalNeighborRelationship,
    CanonicalTopologyEdge,
    CanonicalBGPSession,
    CanonicalObject,
)
from netobserv.models.enums import EdgeConfidence, EdgeType
from netobserv.observability.logging import get_logger
from netobserv.topology.conflict import ConflictDetector, TopologyConflict

logger = get_logger("topology.builder")


@dataclass
class IntendedStateContext:
    """Optional intended-state data used to supplement topology building."""

    cable_records: list[dict[str, Any]] = field(default_factory=list)
    intended_devices: list[CanonicalDevice] = field(default_factory=list)


@dataclass
class TopologyBuildResult:
    """Output of a single topology build pass."""

    snapshot_id: str
    edges: list[CanonicalTopologyEdge]
    conflicts: list[TopologyConflict]
    node_count: int
    edge_count: int
    conflict_count: int
    inference_summary: dict[str, int] = field(default_factory=dict)


class TopologyBuilder:
    """
    Builds topology from canonical objects using multiple evidence sources.

    Evidence priority (highest → lowest):
      1. LLDP/CDP direct neighbor records (bidirectional preferred)
      2. NetBox cable records (intended state)
      3. Interface description patterns
      4. BGP/OSPF adjacency inference
      5. ARP/MAC correlation

    Conflict resolution is delegated to ``ConflictResolutionEngine`` when
    ``use_conflict_engine=True`` (default).  The engine applies trust ranking,
    freshness weighting, evidence decay, and composite confidence scoring to
    each group of competing edges before a winner is selected.
    """

    def __init__(
        self,
        min_confidence_threshold: float = 0.3,
        use_conflict_engine: bool = True,
        conflict_engine: Optional[Any] = None,  # ConflictResolutionEngine
    ) -> None:
        self.min_confidence_threshold = min_confidence_threshold
        self._conflict_detector = ConflictDetector()
        self._use_conflict_engine = use_conflict_engine
        # Lazy import to avoid circular dependency; injected in tests
        self._conflict_engine = conflict_engine

    def build(
        self,
        snapshot_id: str,
        canonical_objects: list[CanonicalObject],
        intended_context: IntendedStateContext | None = None,
    ) -> TopologyBuildResult:
        # Separate objects by type
        devices: dict[str, CanonicalDevice] = {}
        interfaces: dict[str, CanonicalInterface] = {}
        neighbors: list[CanonicalNeighborRelationship] = []
        bgp_sessions: list[CanonicalBGPSession] = []

        for obj in canonical_objects:
            if isinstance(obj, CanonicalDevice):
                devices[obj.device_id] = obj
            elif isinstance(obj, CanonicalInterface):
                interfaces[obj.interface_id] = obj
            elif isinstance(obj, CanonicalNeighborRelationship):
                neighbors.append(obj)
            elif isinstance(obj, CanonicalBGPSession):
                bgp_sessions.append(obj)

        # Build hostname → device_id map for neighbor resolution
        hostname_to_id: dict[str, str] = {d.hostname: d.device_id for d in devices.values()}

        edges: list[CanonicalTopologyEdge] = []
        inference_counts: dict[str, int] = {}

        # 1. LLDP/CDP edges
        lldp_edges = self._build_lldp_edges(
            snapshot_id, neighbors, hostname_to_id
        )
        edges.extend(lldp_edges)
        inference_counts["lldp_cdp"] = len(lldp_edges)

        # 2. NetBox cable edges
        if intended_context:
            cable_edges = self._build_cable_edges(
                snapshot_id, intended_context.cable_records, hostname_to_id
            )
            edges.extend(cable_edges)
            inference_counts["netbox_cable"] = len(cable_edges)

        # 3. Interface description inference
        desc_edges = self._build_description_edges(
            snapshot_id, interfaces, hostname_to_id
        )
        edges.extend(desc_edges)
        inference_counts["interface_description"] = len(desc_edges)

        # 4. BGP adjacency edges
        bgp_edges = self._build_bgp_edges(snapshot_id, bgp_sessions, devices)
        edges.extend(bgp_edges)
        inference_counts["bgp"] = len(bgp_edges)

        # Reconcile duplicate edges (same pair, multiple evidence sources)
        edges = self._reconcile_edges(edges, snapshot_id=snapshot_id)

        # Filter by confidence threshold
        edges = [
            e for e in edges if e.confidence_score >= self.min_confidence_threshold
        ]

        # Detect conflicts
        conflicts = self._conflict_detector.detect(edges, devices, neighbors)

        logger.info(
            "Topology build complete",
            snapshot_id=snapshot_id,
            nodes=len(devices),
            edges=len(edges),
            conflicts=len(conflicts),
        )

        return TopologyBuildResult(
            snapshot_id=snapshot_id,
            edges=edges,
            conflicts=conflicts,
            node_count=len(devices),
            edge_count=len(edges),
            conflict_count=len(conflicts),
            inference_summary=inference_counts,
        )

    # ------------------------------------------------------------------
    # LLDP / CDP edge builder
    # ------------------------------------------------------------------

    def _build_lldp_edges(
        self,
        snapshot_id: str,
        neighbors: list[CanonicalNeighborRelationship],
        hostname_to_id: dict[str, str],
    ) -> list[CanonicalTopologyEdge]:
        """
        Build edges from LLDP/CDP neighbor records.

        Bidirectional records get HIGH confidence; single-side gets MEDIUM.
        """
        # Group by (local_hostname, remote_hostname) for bidirectionality check
        seen: dict[tuple[str, str], CanonicalNeighborRelationship] = {}
        for n in neighbors:
            key = (n.local_device_id, n.remote_device_id or n.remote_hostname or "")
            seen[key] = n

        edges: list[CanonicalTopologyEdge] = []
        processed_pairs: set[frozenset[str]] = set()

        for n in neighbors:
            local_id = hostname_to_id.get(n.local_device_id, n.local_device_id)
            remote_key = n.remote_device_id or n.remote_hostname or ""
            remote_id = hostname_to_id.get(remote_key, remote_key)

            if not local_id or not remote_id:
                continue

            pair = frozenset([local_id, remote_id])
            if pair in processed_pairs:
                continue
            processed_pairs.add(pair)

            # Check for reverse record
            reverse_key = (remote_id, local_id)
            is_bidirectional = reverse_key in seen

            confidence = EdgeConfidence.HIGH if is_bidirectional else EdgeConfidence.MEDIUM
            score = 0.9 if is_bidirectional else 0.6

            edges.append(
                CanonicalTopologyEdge(
                    snapshot_id=snapshot_id,
                    source_node_id=local_id,
                    target_node_id=remote_id,
                    source_interface=n.local_interface,
                    target_interface=n.remote_interface,
                    edge_type=EdgeType.L2_ADJACENCY,
                    evidence_source=[n.protocol],
                    evidence_count=2 if is_bidirectional else 1,
                    confidence=confidence,
                    confidence_score=score,
                    is_direct=True,
                    inference_method="lldp_cdp",
                )
            )

        return edges

    # ------------------------------------------------------------------
    # NetBox cable edge builder
    # ------------------------------------------------------------------

    def _build_cable_edges(
        self,
        snapshot_id: str,
        cable_records: list[dict[str, Any]],
        hostname_to_id: dict[str, str],
    ) -> list[CanonicalTopologyEdge]:
        edges: list[CanonicalTopologyEdge] = []
        for cable in cable_records:
            local_hostname = cable.get("local_hostname", "")
            remote_hostname = cable.get("remote_hostname", "")
            if not local_hostname or not remote_hostname:
                continue

            local_id = hostname_to_id.get(local_hostname, local_hostname)
            remote_id = hostname_to_id.get(remote_hostname, remote_hostname)

            edges.append(
                CanonicalTopologyEdge(
                    snapshot_id=snapshot_id,
                    source_node_id=local_id,
                    target_node_id=remote_id,
                    source_interface=cable.get("local_interface"),
                    target_interface=cable.get("remote_interface"),
                    edge_type=EdgeType.PHYSICAL,
                    evidence_source=["netbox_cable"],
                    evidence_count=1,
                    confidence=EdgeConfidence.HIGH,
                    confidence_score=0.85,
                    is_direct=True,
                    inference_method="netbox_cable",
                )
            )
        return edges

    # ------------------------------------------------------------------
    # Interface description inference
    # ------------------------------------------------------------------

    def _build_description_edges(
        self,
        snapshot_id: str,
        interfaces: dict[str, CanonicalInterface],
        hostname_to_id: dict[str, str],
    ) -> list[CanonicalTopologyEdge]:
        """
        Infer edges from interface descriptions like "link to router1 Eth1".
        Low-confidence — used only when no LLDP data exists.
        """
        import re

        edges: list[CanonicalTopologyEdge] = []
        desc_patterns = [
            re.compile(r"(?:to|link to|conn to|uplink to)\s+([a-zA-Z0-9._-]+)", re.IGNORECASE),
        ]

        for iface in interfaces.values():
            desc = iface.description or ""
            if not desc:
                continue

            for pattern in desc_patterns:
                match = pattern.search(desc)
                if match:
                    remote_hint = match.group(1).strip()
                    remote_id = hostname_to_id.get(remote_hint)
                    if not remote_id:
                        continue

                    local_device = iface.device_id
                    edges.append(
                        CanonicalTopologyEdge(
                            snapshot_id=snapshot_id,
                            source_node_id=local_device,
                            target_node_id=remote_id,
                            source_interface=iface.name,
                            edge_type=EdgeType.INFERRED,
                            evidence_source=["interface_description"],
                            evidence_count=1,
                            confidence=EdgeConfidence.LOW,
                            confidence_score=0.35,
                            is_direct=False,
                            inference_method="interface_description",
                        )
                    )
                    break

        return edges

    # ------------------------------------------------------------------
    # BGP adjacency edges
    # ------------------------------------------------------------------

    def _build_bgp_edges(
        self,
        snapshot_id: str,
        bgp_sessions: list[CanonicalBGPSession],
        devices: dict[str, CanonicalDevice],
    ) -> list[CanonicalTopologyEdge]:
        """Build edges from BGP sessions (L3 adjacency)."""
        # Build IP → device_id map from devices
        ip_to_device: dict[str, str] = {}
        for dev in devices.values():
            if dev.management_ip:
                ip_to_device[dev.management_ip] = dev.device_id

        edges: list[CanonicalTopologyEdge] = []
        processed: set[frozenset[str]] = set()

        for session in bgp_sessions:
            remote_device_id = ip_to_device.get(session.remote_address)
            if not remote_device_id:
                continue

            local_id = session.device_id
            pair = frozenset([local_id, remote_device_id])
            if pair in processed:
                continue
            processed.add(pair)

            edges.append(
                CanonicalTopologyEdge(
                    snapshot_id=snapshot_id,
                    source_node_id=local_id,
                    target_node_id=remote_device_id,
                    edge_type=EdgeType.BGP_PEER,
                    evidence_source=["bgp_session"],
                    evidence_count=1,
                    confidence=EdgeConfidence.MEDIUM,
                    confidence_score=0.7,
                    is_direct=False,
                    inference_method="bgp",
                    metadata={
                        "local_asn": session.local_asn,
                        "remote_asn": session.remote_asn,
                        "state": session.state.value if hasattr(session.state, "value") else str(session.state),
                    },
                )
            )

        return edges

    # ------------------------------------------------------------------
    # Edge reconciliation
    # ------------------------------------------------------------------

    def _reconcile_edges(
        self,
        edges: list[CanonicalTopologyEdge],
        snapshot_id: Optional[str] = None,
    ) -> list[CanonicalTopologyEdge]:
        """Merge duplicate edges (same device pair) from multiple evidence sources.

        When ``use_conflict_engine=True`` the ``ConflictResolutionEngine`` is
        used for groups with more than one edge, applying trust ranking,
        freshness weighting, and composite confidence scoring before picking a
        winner.  This replaces the previous simple "take highest score + boost"
        heuristic.

        Falls back to the original merge heuristic when the engine is
        disabled (``use_conflict_engine=False``) for backwards compatibility.
        """
        from collections import defaultdict

        def pair_key(e: CanonicalTopologyEdge) -> tuple[str, ...]:
            nodes = tuple(sorted([e.source_node_id, e.target_node_id]))
            return (*nodes, e.edge_type.value)

        groups: dict[tuple[str, ...], list[CanonicalTopologyEdge]] = defaultdict(list)
        for edge in edges:
            groups[pair_key(edge)].append(edge)

        # Delegate to engine when available
        if self._use_conflict_engine:
            engine = self._get_conflict_engine()
            str_groups: dict[str, list[CanonicalTopologyEdge]] = {
                ":".join(k): v for k, v in groups.items()
            }
            resolved = engine.resolve_topology_edges(
                str_groups, snapshot_id=snapshot_id
            )
            return list(resolved.values())

        # Legacy heuristic path (engine disabled)
        result: list[CanonicalTopologyEdge] = []
        for group_edges in groups.values():
            if len(group_edges) == 1:
                result.append(group_edges[0])
                continue

            best = max(group_edges, key=lambda e: e.confidence_score)
            all_evidence: list[str] = []
            for e in group_edges:
                all_evidence.extend(e.evidence_source)

            merged = best.model_copy(
                update={
                    "evidence_source": list(dict.fromkeys(all_evidence)),
                    "evidence_count": len(group_edges),
                    "confidence_score": min(
                        best.confidence_score + 0.05 * (len(group_edges) - 1), 1.0
                    ),
                }
            )
            result.append(merged)

        return result

    def _get_conflict_engine(self) -> Any:
        """Lazily create the ConflictResolutionEngine on first use."""
        if self._conflict_engine is None:
            from netobserv.conflict_resolution.engine import ConflictResolutionEngine
            self._conflict_engine = ConflictResolutionEngine()
        return self._conflict_engine
