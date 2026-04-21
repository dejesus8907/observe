"""Field-level comparators for each canonical object type."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from netobserv.models.canonical import (
    CanonicalDevice,
    CanonicalDriftRecord,
    CanonicalInterface,
    CanonicalTopologyEdge,
    CanonicalVLAN,
)
from netobserv.models.enums import DriftSeverity, MismatchType


def _new_drift(
    object_type: str,
    mismatch_type: MismatchType,
    severity: DriftSeverity,
    explanation: str,
    remediation_hint: str,
    snapshot_id: str | None = None,
    workflow_id: str | None = None,
    intended_ref: str | None = None,
    actual_ref: str | None = None,
    field_name: str | None = None,
    intended_value: Any = None,
    actual_value: Any = None,
    confidence: float = 1.0,
) -> CanonicalDriftRecord:
    return CanonicalDriftRecord(
        drift_id=str(uuid.uuid4()),
        snapshot_id=snapshot_id,
        workflow_id=workflow_id,
        intended_ref=intended_ref,
        actual_ref=actual_ref,
        object_type=object_type,
        mismatch_type=mismatch_type,
        field_name=field_name,
        intended_value=intended_value,
        actual_value=actual_value,
        severity=severity,
        explanation=explanation,
        remediation_hint=remediation_hint,
        confidence=confidence,
        created_at=datetime.utcnow(),
    )


# ---------------------------------------------------------------------------
# Device Comparator
# ---------------------------------------------------------------------------


class DeviceComparator:
    """Compare intended and actual device lists."""

    # Fields to compare and their severity mappings
    _COMPARE_FIELDS: list[tuple[str, DriftSeverity]] = [
        ("status", DriftSeverity.HIGH),
        ("vendor", DriftSeverity.MEDIUM),
        ("model", DriftSeverity.MEDIUM),
        ("device_role", DriftSeverity.HIGH),
        ("site", DriftSeverity.HIGH),
        ("software_version", DriftSeverity.LOW),
    ]

    def compare(
        self,
        intended: list[CanonicalDevice],
        actual: list[CanonicalDevice],
        snapshot_id: str | None,
        workflow_id: str | None,
    ) -> list[CanonicalDriftRecord]:
        drifts: list[CanonicalDriftRecord] = []

        intended_map: dict[str, CanonicalDevice] = {
            self._device_key(d): d for d in intended
        }
        actual_map: dict[str, CanonicalDevice] = {
            self._device_key(d): d for d in actual
        }

        # Missing in actual
        for key, idev in intended_map.items():
            if key not in actual_map:
                drifts.append(
                    _new_drift(
                        object_type="device",
                        mismatch_type=MismatchType.MISSING_IN_ACTUAL,
                        severity=DriftSeverity.CRITICAL,
                        explanation=f"Device '{idev.hostname}' is defined in the source-of-truth but was not discovered.",
                        remediation_hint="Verify device is powered on and reachable. Check discovery credentials.",
                        snapshot_id=snapshot_id,
                        workflow_id=workflow_id,
                        intended_ref=idev.device_id,
                        actual_ref=None,
                    )
                )

        # Missing in intended
        for key, adev in actual_map.items():
            if key not in intended_map:
                drifts.append(
                    _new_drift(
                        object_type="device",
                        mismatch_type=MismatchType.MISSING_IN_INTENDED,
                        severity=DriftSeverity.MEDIUM,
                        explanation=f"Device '{adev.hostname}' was discovered but is not in the source-of-truth.",
                        remediation_hint="Add device to source-of-truth or investigate unexpected device.",
                        snapshot_id=snapshot_id,
                        workflow_id=workflow_id,
                        intended_ref=None,
                        actual_ref=adev.device_id,
                    )
                )

        # Field mismatches
        for key, idev in intended_map.items():
            adev = actual_map.get(key)
            if not adev:
                continue
            for field_name, severity in self._COMPARE_FIELDS:
                iv = getattr(idev, field_name, None)
                av = getattr(adev, field_name, None)
                if iv and av and str(iv).lower() != str(av).lower():
                    drifts.append(
                        _new_drift(
                            object_type="device",
                            mismatch_type=MismatchType.STATE_MISMATCH,
                            severity=severity,
                            explanation=(
                                f"Device '{idev.hostname}' field '{field_name}' mismatch: "
                                f"intended='{iv}', actual='{av}'."
                            ),
                            remediation_hint=f"Update {field_name} in source-of-truth or investigate actual state.",
                            snapshot_id=snapshot_id,
                            workflow_id=workflow_id,
                            intended_ref=idev.device_id,
                            actual_ref=adev.device_id,
                            field_name=field_name,
                            intended_value=iv,
                            actual_value=av,
                        )
                    )

        return drifts

    @staticmethod
    def _device_key(d: CanonicalDevice) -> str:
        return (d.hostname or "").lower()


# ---------------------------------------------------------------------------
# Interface Comparator
# ---------------------------------------------------------------------------


class InterfaceComparator:
    """Compare intended and actual interface lists."""

    _COMPARE_FIELDS: list[tuple[str, DriftSeverity]] = [
        ("admin_state", DriftSeverity.HIGH),
        ("oper_state", DriftSeverity.HIGH),
        ("mtu", DriftSeverity.MEDIUM),
        ("description", DriftSeverity.LOW),
        ("vlan_mode", DriftSeverity.MEDIUM),
        ("access_vlan", DriftSeverity.HIGH),
    ]

    def compare(
        self,
        intended: list[CanonicalInterface],
        actual: list[CanonicalInterface],
        snapshot_id: str | None,
        workflow_id: str | None,
    ) -> list[CanonicalDriftRecord]:
        drifts: list[CanonicalDriftRecord] = []

        intended_map = {self._iface_key(i): i for i in intended}
        actual_map = {self._iface_key(i): i for i in actual}

        for key, iiface in intended_map.items():
            if key not in actual_map:
                drifts.append(
                    _new_drift(
                        object_type="interface",
                        mismatch_type=MismatchType.MISSING_IN_ACTUAL,
                        severity=DriftSeverity.HIGH,
                        explanation=f"Interface '{iiface.name}' on device '{iiface.device_id}' not discovered.",
                        remediation_hint="Verify interface exists. Check connector scope.",
                        snapshot_id=snapshot_id,
                        workflow_id=workflow_id,
                        intended_ref=iiface.interface_id,
                    )
                )

        for key, aiface in actual_map.items():
            if key not in intended_map:
                drifts.append(
                    _new_drift(
                        object_type="interface",
                        mismatch_type=MismatchType.MISSING_IN_INTENDED,
                        severity=DriftSeverity.LOW,
                        explanation=f"Interface '{aiface.name}' on device '{aiface.device_id}' not in source-of-truth.",
                        remediation_hint="Add interface to source-of-truth if it should be tracked.",
                        snapshot_id=snapshot_id,
                        workflow_id=workflow_id,
                        actual_ref=aiface.interface_id,
                    )
                )

        for key, iiface in intended_map.items():
            aiface = actual_map.get(key)
            if not aiface:
                continue
            for field_name, severity in self._COMPARE_FIELDS:
                iv = getattr(iiface, field_name, None)
                av = getattr(aiface, field_name, None)
                if iv is not None and av is not None and str(iv) != str(av):
                    drifts.append(
                        _new_drift(
                            object_type="interface",
                            mismatch_type=MismatchType.STATE_MISMATCH,
                            severity=severity,
                            explanation=(
                                f"Interface '{iiface.name}' field '{field_name}': "
                                f"intended='{iv}', actual='{av}'."
                            ),
                            remediation_hint=f"Correct {field_name} on device.",
                            snapshot_id=snapshot_id,
                            workflow_id=workflow_id,
                            intended_ref=iiface.interface_id,
                            actual_ref=aiface.interface_id,
                            field_name=field_name,
                            intended_value=iv,
                            actual_value=av,
                        )
                    )

        return drifts

    @staticmethod
    def _iface_key(i: CanonicalInterface) -> str:
        device = (i.device_id or "").lower()
        name = (i.normalized_name or i.name or "").lower()
        return f"{device}:{name}"


# ---------------------------------------------------------------------------
# VLAN Comparator
# ---------------------------------------------------------------------------


class VLANComparator:
    def compare(
        self,
        intended: list[CanonicalVLAN],
        actual: list[CanonicalVLAN],
        snapshot_id: str | None,
        workflow_id: str | None,
    ) -> list[CanonicalDriftRecord]:
        drifts: list[CanonicalDriftRecord] = []
        intended_map = {(v.vid, v.site or ""): v for v in intended}
        actual_map = {(v.vid, v.site or ""): v for v in actual}

        for key, iv in intended_map.items():
            if key not in actual_map:
                drifts.append(
                    _new_drift(
                        object_type="vlan",
                        mismatch_type=MismatchType.MISSING_IN_ACTUAL,
                        severity=DriftSeverity.HIGH,
                        explanation=f"VLAN {iv.vid} ('{iv.name}') at site '{iv.site}' not found in actual state.",
                        remediation_hint="Verify VLAN is provisioned on devices.",
                        snapshot_id=snapshot_id,
                        workflow_id=workflow_id,
                        intended_ref=iv.vlan_id,
                    )
                )

        for key, av in actual_map.items():
            if key not in intended_map:
                drifts.append(
                    _new_drift(
                        object_type="vlan",
                        mismatch_type=MismatchType.MISSING_IN_INTENDED,
                        severity=DriftSeverity.MEDIUM,
                        explanation=f"VLAN {av.vid} ('{av.name}') found in actual state but not in source-of-truth.",
                        remediation_hint="Add VLAN to source-of-truth or remove from network.",
                        snapshot_id=snapshot_id,
                        workflow_id=workflow_id,
                        actual_ref=av.vlan_id,
                    )
                )

        return drifts


# ---------------------------------------------------------------------------
# Topology Comparator
# ---------------------------------------------------------------------------


class TopologyComparator:
    def compare(
        self,
        intended: list[CanonicalTopologyEdge],
        actual: list[CanonicalTopologyEdge],
        snapshot_id: str | None,
        workflow_id: str | None,
    ) -> list[CanonicalDriftRecord]:
        drifts: list[CanonicalDriftRecord] = []

        def edge_key(e: CanonicalTopologyEdge) -> frozenset[str]:
            return frozenset([e.source_node_id, e.target_node_id])

        intended_keys = {edge_key(e) for e in intended}
        actual_keys = {edge_key(e) for e in actual}

        for e in intended:
            k = edge_key(e)
            if k not in actual_keys:
                drifts.append(
                    _new_drift(
                        object_type="topology_edge",
                        mismatch_type=MismatchType.MISSING_IN_ACTUAL,
                        severity=DriftSeverity.HIGH,
                        explanation=(
                            f"Intended topology link between '{e.source_node_id}' and "
                            f"'{e.target_node_id}' not found in actual topology."
                        ),
                        remediation_hint="Verify cabling and check LLDP/CDP status.",
                        snapshot_id=snapshot_id,
                        workflow_id=workflow_id,
                        intended_ref=e.edge_id,
                    )
                )

        for e in actual:
            k = edge_key(e)
            if k not in intended_keys:
                drifts.append(
                    _new_drift(
                        object_type="topology_edge",
                        mismatch_type=MismatchType.MISSING_IN_INTENDED,
                        severity=DriftSeverity.MEDIUM,
                        explanation=(
                            f"Actual topology link between '{e.source_node_id}' and "
                            f"'{e.target_node_id}' not in intended topology."
                        ),
                        remediation_hint="Add cable to source-of-truth or investigate unexpected connection.",
                        snapshot_id=snapshot_id,
                        workflow_id=workflow_id,
                        actual_ref=e.edge_id,
                    )
                )

        return drifts
