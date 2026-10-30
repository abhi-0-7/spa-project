"""
simulator.parking_lot
=====================
Domain model for a parking lot and its individual slots.

Classes
-------
SlotStatus      Enum describing a slot's occupancy state.
ParkingSlot     Dataclass representing a single sensor-equipped slot.
ParkingLot      Manages a collection of ParkingSlot objects and exposes
                aggregate metrics plus an FSM-guarded state-transition method.

Notes
-----
``set_slot_status`` implements a strict two-state FSM:
    FREE → OCCUPIED  → returns True  (state changed)
    OCCUPIED → FREE  → returns True  (state changed)
    FREE → FREE      → returns False (no-op)
    OCCUPIED → OCCUPIED → returns False (no-op)

Returning ``True`` only on genuine transitions prevents double-counting in
the occupancy counter.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class SlotStatus(str, Enum):
    """Occupancy state of a single parking slot."""

    OCCUPIED = "OCCUPIED"
    FREE = "FREE"


# ---------------------------------------------------------------------------
# Dataclass: ParkingSlot
# ---------------------------------------------------------------------------

@dataclass
class ParkingSlot:
    """Represents a single sensor-equipped parking slot.

    Attributes
    ----------
    slot_id:
        Unique identifier for this slot within a lot, e.g. ``"LOT_MG_01-S042"``.
    lot_id:
        Identifier of the parent parking lot.
    current_status:
        Current occupancy state (FREE or OCCUPIED).
    last_event_time:
        UTC timestamp of the most-recent status update.
    occupancy_start_time:
        UTC timestamp when the slot most recently transitioned to OCCUPIED;
        ``None`` when the slot is FREE.
    """

    slot_id: str
    lot_id: str
    current_status: SlotStatus = SlotStatus.FREE
    last_event_time: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    occupancy_start_time: Optional[datetime] = None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def is_occupied(self) -> bool:
        """Return ``True`` if the slot is currently occupied."""
        return self.current_status == SlotStatus.OCCUPIED

    def occupancy_duration_seconds(self, reference: Optional[datetime] = None) -> Optional[float]:
        """Return how long (in seconds) this slot has been occupied.

        Parameters
        ----------
        reference:
            The point-in-time to measure from.  Defaults to UTC now.

        Returns
        -------
        float or None
            Duration in seconds if the slot is OCCUPIED, else ``None``.
        """
        if not self.is_occupied() or self.occupancy_start_time is None:
            return None
        ref = reference or datetime.now(timezone.utc)
        return (ref - self.occupancy_start_time).total_seconds()


# ---------------------------------------------------------------------------
# Class: ParkingLot
# ---------------------------------------------------------------------------

class ParkingLot:
    """Manages a collection of parking slots and exposes aggregate metrics.

    Parameters
    ----------
    lot_id:
        Unique identifier, e.g. ``"LOT_MG_ROAD_01"``.
    name:
        Human-readable name.
    zone_id:
        Geographic zone, e.g. ``"ZONE_CENTRAL"``.
    city:
        City name, e.g. ``"Bengaluru"``.
    latitude:
        Decimal-degree latitude of the lot centroid.
    longitude:
        Decimal-degree longitude of the lot centroid.
    capacity:
        Total design capacity (may equal ``num_slots``).
    num_slots:
        Actual number of sensor-equipped slots to initialise.
    lot_type:
        Physical type: ``"surface"``, ``"multi-level"``, or ``"underground"``.
    """

    def __init__(
        self,
        lot_id: str,
        name: str,
        zone_id: str,
        city: str,
        latitude: float,
        longitude: float,
        capacity: int,
        num_slots: int,
        lot_type: str,
    ) -> None:
        self.lot_id: str = lot_id
        self.name: str = name
        self.zone_id: str = zone_id
        self.city: str = city
        self.latitude: float = latitude
        self.longitude: float = longitude
        self.capacity: int = capacity
        self.num_slots: int = num_slots
        self.lot_type: str = lot_type

        self.slots: Dict[str, ParkingSlot] = {}
        self.initialize_slots()

        logger.debug(
            "Initialized ParkingLot '%s' with %d slots.", self.lot_id, self.num_slots
        )

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    def initialize_slots(self) -> None:
        """Create all ``ParkingSlot`` objects, all starting as FREE."""
        self.slots = {}
        for i in range(1, self.num_slots + 1):
            slot_id = f"{self.lot_id}-S{i:03d}"
            self.slots[slot_id] = ParkingSlot(
                slot_id=slot_id,
                lot_id=self.lot_id,
                current_status=SlotStatus.FREE,
                last_event_time=datetime.now(timezone.utc),
                occupancy_start_time=None,
            )

    # ------------------------------------------------------------------
    # Slot access
    # ------------------------------------------------------------------

    def get_slot(self, slot_id: str) -> Optional[ParkingSlot]:
        """Return a ``ParkingSlot`` by its ID, or ``None`` if not found."""
        slot = self.slots.get(slot_id)
        if slot is None:
            # Fallback check with lot prefix or S-prefix
            alt_id = f"{self.lot_id}-{slot_id.replace('SLOT_', 'S')}"
            slot = self.slots.get(alt_id)
        if slot is None:
            logger.warning(
                "Slot '%s' not found in lot '%s'.", slot_id, self.lot_id
            )
        return slot

    # ------------------------------------------------------------------
    # Aggregates
    # ------------------------------------------------------------------

    def get_occupied_count(self) -> int:
        """Return the number of currently-occupied slots."""
        return sum(1 for s in self.slots.values() if s.is_occupied())

    def get_free_count(self) -> int:
        """Return the number of currently-free slots."""
        return self.num_slots - self.get_occupied_count()

    def get_occupancy_percentage(self) -> float:
        """Return occupancy as a percentage (0.0 – 100.0).

        Returns 0.0 if the lot has no slots.
        """
        if self.num_slots == 0:
            return 0.0
        return round((self.get_occupied_count() / self.num_slots) * 100.0, 2)

    # ------------------------------------------------------------------
    # State transition (FSM)
    # ------------------------------------------------------------------

    def set_slot_status(
        self,
        slot_id: str,
        status: SlotStatus | str,
        timestamp: Optional[datetime] = None,
    ) -> bool:
        """Attempt to transition *slot_id* to *status*."""
        slot = self.get_slot(slot_id)
        if slot is None:
            logger.error(
                "Cannot set status for unknown slot '%s' in lot '%s'.",
                slot_id,
                self.lot_id,
            )
            return False

        # Convert string to SlotStatus enum if needed
        if isinstance(status, str):
            try:
                status = SlotStatus(status.upper())
            except ValueError:
                logger.error("Invalid status value '%s'.", status)
                return False

        ts = timestamp or datetime.now(timezone.utc)

        # FSM guard: identical state → no-op
        if slot.current_status == status:
            logger.debug(
                "Slot '%s' already in state %s — no-op.", slot_id, status.value
            )
            return False

        # Genuine transition
        previous_status = slot.current_status
        slot.current_status = status
        slot.last_event_time = ts

        if status == SlotStatus.OCCUPIED:
            slot.occupancy_start_time = ts
        else:
            slot.occupancy_start_time = None

        logger.debug(
            "Slot '%s': %s → %s at %s.",
            slot_id,
            previous_status.value,
            status.value,
            ts.isoformat(),
        )
        return True

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self) -> dict:
        """Return a JSON-serialisable snapshot of this lot.

        Returns
        -------
        dict
            Includes lot metadata and current aggregate occupancy metrics.
            Individual slot details are omitted for brevity; use
            ``self.slots`` directly if fine-grained data is needed.
        """
        return {
            "lot_id": self.lot_id,
            "name": self.name,
            "zone_id": self.zone_id,
            "city": self.city,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "capacity": self.capacity,
            "num_slots": self.num_slots,
            "lot_type": self.lot_type,
            "occupied_count": self.get_occupied_count(),
            "free_count": self.get_free_count(),
            "occupancy_percentage": self.get_occupancy_percentage(),
        }

    # ------------------------------------------------------------------
    # Dunder
    # ------------------------------------------------------------------

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"ParkingLot(lot_id={self.lot_id!r}, "
            f"num_slots={self.num_slots}, "
            f"occupancy={self.get_occupancy_percentage():.1f}%)"
        )
