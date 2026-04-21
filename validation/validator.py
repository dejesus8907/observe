"""State validator — orchestrates comparison of intended vs actual state."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from netobserv.models.canonical import (
    CanonicalDevice,
    CanonicalDriftRecord,
    CanonicalInterface,
    CanonicalTopologyEdge,
    CanonicalVLAN,
)
from netobserv.models.enums import MismatchType, DriftSeverity
from netobserv.observability.logging import get_logger
from netobserv.validation.comparators import (
    DeviceComparator,
    InterfaceComparator,
    VLANComparator,
    TopologyComparator,
)

logger = get_logger("validation.validator")


@dataclass
class IntendedStateBundle:
    """Intended network state from a source-of-truth system."""

    devices: list[CanonicalDevice] = field(default_factory=list)
    interfaces: list[CanonicalInterface] = field(default_factory=list)
    vlans: list[CanonicalVLAN] = field(default_factory=list)
    topology_edges: list[CanonicalTopologyEdge] = field(default_factory=list)
    snapshot_id: str | None = None
    source_system: str = "intended"


@dataclass
class ActualStateBundle:
    """Actual network state from live discovery."""

    devices: list[CanonicalDevice] = field(default_factory=list)
    interfaces: list[CanonicalInterface] = field(default_factory=list)
    vlans: list[CanonicalVLAN] = field(default_factory=list)
    topology_edges: list[CanonicalTopologyEdge] = field(default_factory=list)
    snapshot_id: str | None = None


@runtime_checkable
class StateValidator(Protocol):
    def compare(
        self,
        intended_state: IntendedStateBundle,
        actual_state: ActualStateBundle,
    ) -> list[CanonicalDriftRecord]:
        ...


class DefaultStateValidator:
    """
    Compares intended and actual state across devices, interfaces, VLANs,
    and topology relationships. Produces a list of DriftRecord objects.
    """

    def __init__(
        self,
        snapshot_id: str | None = None,
        workflow_id: str | None = None,
    ) -> None:
        self.snapshot_id = snapshot_id
        self.workflow_id = workflow_id
        self._device_cmp = DeviceComparator()
        self._iface_cmp = InterfaceComparator()
        self._vlan_cmp = VLANComparator()
        self._topo_cmp = TopologyComparator()

    def compare(
        self,
        intended_state: IntendedStateBundle,
        actual_state: ActualStateBundle,
    ) -> list[CanonicalDriftRecord]:
        records: list[CanonicalDriftRecord] = []

        records.extend(
            self._device_cmp.compare(
                intended_state.devices,
                actual_state.devices,
                self.snapshot_id,
                self.workflow_id,
            )
        )
        records.extend(
            self._iface_cmp.compare(
                intended_state.interfaces,
                actual_state.interfaces,
                self.snapshot_id,
                self.workflow_id,
            )
        )
        records.extend(
            self._vlan_cmp.compare(
                intended_state.vlans,
                actual_state.vlans,
                self.snapshot_id,
                self.workflow_id,
            )
        )
        records.extend(
            self._topo_cmp.compare(
                intended_state.topology_edges,
                actual_state.topology_edges,
                self.snapshot_id,
                self.workflow_id,
            )
        )

        logger.info(
            "Validation complete",
            drift_count=len(records),
            snapshot_id=self.snapshot_id,
        )
        return records
