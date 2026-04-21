"""Unit tests for the state validator and comparators."""

from __future__ import annotations

import pytest

from netobserv.models.canonical import (
    CanonicalDevice,
    CanonicalInterface,
    CanonicalVLAN,
)
from netobserv.models.enums import (
    DeviceStatus,
    DriftSeverity,
    InterfaceAdminState,
    InterfaceOperState,
    MismatchType,
    VLANStatus,
)
from netobserv.validation.comparators import (
    DeviceComparator,
    InterfaceComparator,
    VLANComparator,
)
from netobserv.validation.validator import (
    ActualStateBundle,
    DefaultStateValidator,
    IntendedStateBundle,
)


class TestDeviceComparator:
    def test_missing_in_actual(self, device_a: CanonicalDevice) -> None:
        cmp = DeviceComparator()
        drift = cmp.compare([device_a], [], "snap1", "wf1")
        assert len(drift) == 1
        assert drift[0].mismatch_type == MismatchType.MISSING_IN_ACTUAL
        assert drift[0].severity == DriftSeverity.CRITICAL

    def test_missing_in_intended(self, device_a: CanonicalDevice) -> None:
        cmp = DeviceComparator()
        drift = cmp.compare([], [device_a], "snap1", "wf1")
        assert len(drift) == 1
        assert drift[0].mismatch_type == MismatchType.MISSING_IN_INTENDED
        assert drift[0].severity == DriftSeverity.MEDIUM

    def test_no_drift_when_identical(self, device_a: CanonicalDevice) -> None:
        cmp = DeviceComparator()
        drift = cmp.compare([device_a], [device_a], "snap1", "wf1")
        # Only field mismatches should fire, not presence mismatches
        field_drifts = [d for d in drift if d.mismatch_type == MismatchType.STATE_MISMATCH]
        assert len(field_drifts) == 0

    def test_status_mismatch(
        self, device_a: CanonicalDevice, device_b: CanonicalDevice
    ) -> None:
        # Same hostname, different status
        intended = device_a.model_copy(update={"status": DeviceStatus.PLANNED})
        actual = device_a.model_copy(update={"status": DeviceStatus.ACTIVE})
        cmp = DeviceComparator()
        drift = cmp.compare([intended], [actual], "snap1", "wf1")
        status_drift = [d for d in drift if d.field_name == "status"]
        assert len(status_drift) == 1
        assert status_drift[0].mismatch_type == MismatchType.STATE_MISMATCH
        assert status_drift[0].severity == DriftSeverity.HIGH

    def test_vendor_mismatch(self, device_a: CanonicalDevice) -> None:
        intended = device_a.model_copy(update={"vendor": "Arista"})
        actual = device_a.model_copy(update={"vendor": "Cisco"})
        cmp = DeviceComparator()
        drift = cmp.compare([intended], [actual], "snap1", "wf1")
        vendor_drift = [d for d in drift if d.field_name == "vendor"]
        assert len(vendor_drift) == 1


class TestInterfaceComparator:
    def test_missing_interface_in_actual(self, interface_a: CanonicalInterface) -> None:
        cmp = InterfaceComparator()
        drift = cmp.compare([interface_a], [], "snap1", "wf1")
        assert len(drift) == 1
        assert drift[0].mismatch_type == MismatchType.MISSING_IN_ACTUAL

    def test_oper_state_mismatch(self, interface_a: CanonicalInterface) -> None:
        intended = interface_a.model_copy(update={"oper_state": InterfaceOperState.UP})
        actual = interface_a.model_copy(update={"oper_state": InterfaceOperState.DOWN})
        cmp = InterfaceComparator()
        drift = cmp.compare([intended], [actual], "snap1", "wf1")
        oper_drift = [d for d in drift if d.field_name == "oper_state"]
        assert len(oper_drift) == 1
        assert oper_drift[0].severity == DriftSeverity.HIGH


class TestVLANComparator:
    def test_vlan_missing_in_actual(self, vlan_100: CanonicalVLAN) -> None:
        cmp = VLANComparator()
        drift = cmp.compare([vlan_100], [], "snap1", "wf1")
        assert len(drift) == 1
        assert drift[0].mismatch_type == MismatchType.MISSING_IN_ACTUAL

    def test_vlan_missing_in_intended(self, vlan_100: CanonicalVLAN) -> None:
        cmp = VLANComparator()
        drift = cmp.compare([], [vlan_100], "snap1", "wf1")
        assert len(drift) == 1
        assert drift[0].mismatch_type == MismatchType.MISSING_IN_INTENDED

    def test_matching_vlan_no_drift(self, vlan_100: CanonicalVLAN) -> None:
        cmp = VLANComparator()
        drift = cmp.compare([vlan_100], [vlan_100], "snap1", "wf1")
        assert len(drift) == 0


class TestDefaultStateValidator:
    def test_full_validation_with_missing_devices(
        self,
        device_a: CanonicalDevice,
        device_b: CanonicalDevice,
    ) -> None:
        intended = IntendedStateBundle(devices=[device_a, device_b])
        actual = ActualStateBundle(devices=[device_a])  # device_b missing

        validator = DefaultStateValidator(snapshot_id="snap1", workflow_id="wf1")
        drifts = validator.compare(intended, actual)

        missing = [d for d in drifts if d.mismatch_type == MismatchType.MISSING_IN_ACTUAL]
        assert len(missing) == 1
        assert "router2" in (missing[0].explanation or "")

    def test_full_validation_with_extra_device(
        self,
        device_a: CanonicalDevice,
        device_b: CanonicalDevice,
    ) -> None:
        intended = IntendedStateBundle(devices=[device_a])
        actual = ActualStateBundle(devices=[device_a, device_b])  # device_b is extra

        validator = DefaultStateValidator(snapshot_id="snap1", workflow_id="wf1")
        drifts = validator.compare(intended, actual)

        extra = [d for d in drifts if d.mismatch_type == MismatchType.MISSING_IN_INTENDED]
        assert len(extra) >= 1

    def test_empty_bundles_produce_no_drift(self) -> None:
        intended = IntendedStateBundle()
        actual = ActualStateBundle()
        validator = DefaultStateValidator()
        drifts = validator.compare(intended, actual)
        assert len(drifts) == 0
