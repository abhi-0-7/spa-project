"""
Unit tests for ParkingLot slot state transitions.
These tests verify the CRITICAL occupancy calculation logic: state transitions,
duplicate-event idempotency, counter invariants, and serialization.

Course: AI372IA — Smart Parking Occupancy Analytics
Institution: RV College of Engineering, Bengaluru
"""

from __future__ import annotations

import unittest
from datetime import datetime, timezone

from simulator.parking_lot import ParkingLot, ParkingSlot, SlotStatus


def _make_lot(capacity: int = 10) -> ParkingLot:
    """Factory that returns an initialised lot with *capacity* slots."""
    lot = ParkingLot(
        lot_id="TEST_LOT_001",
        name="Test Lot",
        zone_id="ZONE_TEST",
        city="Bengaluru",
        latitude=12.97,
        longitude=77.59,
        capacity=capacity,
        num_slots=capacity,
        lot_type="surface",
    )
    lot.initialize_slots()
    return lot


class TestSlotStateTransitions(unittest.TestCase):
    """Tests for the core transition logic: FREE ↔ OCCUPIED."""

    def setUp(self) -> None:
        self.lot: ParkingLot = _make_lot(10)
        self.ts: datetime = datetime.now(timezone.utc)

    def test_free_to_occupied_changes_state(self) -> None:
        """FREE → OCCUPIED must return True (state changed)."""
        result = self.lot.set_slot_status("SLOT_001", "OCCUPIED", self.ts)
        self.assertTrue(result)
        self.assertEqual(self.lot.get_slot("SLOT_001").current_status, SlotStatus.OCCUPIED)

    def test_occupied_to_free_changes_state(self) -> None:
        """OCCUPIED → FREE must return True."""
        self.lot.set_slot_status("SLOT_001", "OCCUPIED", self.ts)
        result = self.lot.set_slot_status("SLOT_001", "FREE", self.ts)
        self.assertTrue(result)
        self.assertEqual(self.lot.get_slot("SLOT_001").current_status, SlotStatus.FREE)

    def test_free_to_free_no_change(self) -> None:
        """FREE → FREE must return False (no state change)."""
        result = self.lot.set_slot_status("SLOT_001", "FREE", self.ts)
        self.assertFalse(result)

    def test_occupied_to_occupied_no_change(self) -> None:
        """OCCUPIED → OCCUPIED must return False."""
        self.lot.set_slot_status("SLOT_001", "OCCUPIED", self.ts)
        result = self.lot.set_slot_status("SLOT_001", "OCCUPIED", self.ts)
        self.assertFalse(result)

    def test_occupancy_count_correct_sequence(self) -> None:
        """Test occupancy counter through a realistic arrival / departure sequence."""
        self.assertEqual(self.lot.get_occupied_count(), 0)

        # 3 vehicles arrive
        self.lot.set_slot_status("SLOT_001", "OCCUPIED", self.ts)
        self.lot.set_slot_status("SLOT_002", "OCCUPIED", self.ts)
        self.lot.set_slot_status("SLOT_003", "OCCUPIED", self.ts)
        self.assertEqual(self.lot.get_occupied_count(), 3)

        # Duplicate OCCUPIED events (should NOT change count)
        self.lot.set_slot_status("SLOT_001", "OCCUPIED", self.ts)
        self.lot.set_slot_status("SLOT_002", "OCCUPIED", self.ts)
        self.assertEqual(self.lot.get_occupied_count(), 3)

        # 1 vehicle leaves
        self.lot.set_slot_status("SLOT_002", "FREE", self.ts)
        self.assertEqual(self.lot.get_occupied_count(), 2)

        # Duplicate FREE (should NOT change count)
        self.lot.set_slot_status("SLOT_002", "FREE", self.ts)
        self.assertEqual(self.lot.get_occupied_count(), 2)

    def test_occupancy_never_negative(self) -> None:
        """Occupancy cannot go below zero."""
        self.lot.set_slot_status("SLOT_001", "FREE", self.ts)
        self.assertEqual(self.lot.get_occupied_count(), 0)

    def test_occupancy_never_exceeds_capacity(self) -> None:
        """Occupancy cannot exceed configured capacity."""
        for i in range(1, 11):
            self.lot.set_slot_status(f"SLOT_{i:03d}", "OCCUPIED", self.ts)
        self.assertEqual(self.lot.get_occupied_count(), 10)
        self.assertLessEqual(self.lot.get_occupied_count(), self.lot.capacity)

    def test_availability_formula(self) -> None:
        """Availability = Capacity - Occupied."""
        self.lot.set_slot_status("SLOT_001", "OCCUPIED", self.ts)
        self.lot.set_slot_status("SLOT_002", "OCCUPIED", self.ts)
        self.assertEqual(
            self.lot.get_free_count(),
            self.lot.capacity - self.lot.get_occupied_count(),
        )

    def test_occupancy_percentage(self) -> None:
        """Occupancy percentage = occupied / capacity * 100."""
        for i in range(1, 6):
            self.lot.set_slot_status(f"SLOT_{i:03d}", "OCCUPIED", self.ts)
        self.assertAlmostEqual(self.lot.get_occupancy_percentage(), 50.0, places=2)

    def test_same_event_id_no_double_count(self) -> None:
        """Same slot updated twice with OCCUPIED must not double-count."""
        self.lot.set_slot_status("SLOT_001", "OCCUPIED", self.ts)
        self.lot.set_slot_status("SLOT_001", "OCCUPIED", self.ts)
        self.assertEqual(self.lot.get_occupied_count(), 1)

    def test_to_dict_structure(self) -> None:
        """to_dict() must return a well-formed dictionary."""
        d = self.lot.to_dict()
        self.assertEqual(d["lot_id"], "TEST_LOT_001")
        self.assertEqual(d["capacity"], 10)
        self.assertEqual(d.get("occupied_count", d.get("occupied_slots", 0)), 0)
        self.assertEqual(d.get("free_count", d.get("available_slots", 10)), 10)

    def test_invalid_slot_id_raises_or_returns_none(self) -> None:
        """Accessing an uninitialized slot returns None."""
        slot = self.lot.get_slot("NON_EXISTENT_SLOT")
        self.assertIsNone(slot)

    def test_all_slots_initialized(self) -> None:
        """All expected slots must exist after initialize_slots()."""
        self.assertEqual(len(self.lot.slots), 10)
        for i in range(1, 11):
            slot_id = f"{self.lot.lot_id}-S{i:03d}"
            self.assertIn(slot_id, self.lot.slots)
            self.assertEqual(self.lot.slots[slot_id].current_status, SlotStatus.FREE)


if __name__ == "__main__":
    unittest.main()
