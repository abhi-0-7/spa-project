"""
simulator.sensor
================
IoT sensor network abstraction that enriches parking events with realistic
sensor-hardware metadata.

``SensorNetwork`` wraps an ``EventGenerator`` and adds the following fields
to every event it produces:

sensor_id
    Globally unique sensor identifier derived from the slot ID.
battery_level
    Simulated battery percentage (0 – 100).  Sensors near discharge
    (< 20 %) are flagged with ``battery_low: True``.
signal_strength_dbm
    Simulated RSSI in dBm.  Typical BLE/LoRa range is −120 to −30 dBm.
sensor_type
    Fixed string ``"ULTRASONIC_IR_COMBINED"`` — a common parking-sensor
    technology.
firmware_version
    Simulated semver firmware string.
sensor_health
    ``"OK"`` when battery and signal are within normal ranges, otherwise
    ``"DEGRADED"``.
sensor_metadata
    Sub-dict with ``{battery_level, battery_low, signal_strength_dbm,
    sensor_type, firmware_version, sensor_health}``.

The sensor readings are stateful per sensor — battery drains slowly over
simulated time and each sensor has a persistent RSSI that drifts slightly
between readings.

Notes
-----
Battery and signal values are synthesised from the NumPy RNG seeded at
construction time, so results are reproducible when the parent
``EventGenerator`` is seeded.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

import numpy as np

from simulator.generator import EventGenerator
from simulator.parking_lot import SlotStatus

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SENSOR_TYPE = "ULTRASONIC_IR_COMBINED"
_FIRMWARE_VERSIONS = ["1.4.2", "1.5.0", "1.5.1", "2.0.0", "2.1.3"]
_BATTERY_DRAIN_PER_EVENT = 0.001   # percent per event (very slow drain)
_RSSI_DRIFT_STD = 1.5              # dBm standard deviation per reading


# ---------------------------------------------------------------------------
# SensorNetwork
# ---------------------------------------------------------------------------

class SensorNetwork:
    """Wraps an ``EventGenerator`` and enriches events with IoT sensor metadata.

    Parameters
    ----------
    event_generator:
        A fully-initialised ``EventGenerator`` instance.
    rng:
        Optional NumPy Generator for reproducible hardware simulation.
        Defaults to the generator seeded inside ``event_generator`` plus 1000.
    """

    def __init__(
        self,
        event_generator: EventGenerator,
        rng: Optional[np.random.Generator] = None,
    ) -> None:
        self._generator = event_generator
        self._rng: np.random.Generator = (
            rng if rng is not None else np.random.default_rng(None)
        )

        # Per-sensor persistent state: {sensor_id: (battery_pct, base_rssi_dbm)}
        self._sensor_state: Dict[str, Tuple[float, float]] = {}
        # Mapping from slot_id → sensor_id
        self._slot_to_sensor: Dict[str, str] = {}
        # Firmware version per sensor (randomised once at init)
        self._sensor_firmware: Dict[str, str] = {}

        self._initialise_sensors()
        logger.info(
            "SensorNetwork ready: %d sensors registered.",
            len(self._sensor_state),
        )

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    def _initialise_sensors(self) -> None:
        """Pre-register every slot as a sensor with randomised initial state."""
        for lot in self._generator.lots.values():
            for slot_id in lot.slots:
                sensor_id = self._make_sensor_id(slot_id)
                self._slot_to_sensor[slot_id] = sensor_id

                # Battery starts between 60 % and 100 %
                initial_battery = float(self._rng.uniform(60.0, 100.0))
                # RSSI between −100 and −40 dBm
                base_rssi = float(self._rng.uniform(-100.0, -40.0))

                self._sensor_state[sensor_id] = (initial_battery, base_rssi)

                fw_idx = int(self._rng.integers(0, len(_FIRMWARE_VERSIONS)))
                self._sensor_firmware[sensor_id] = _FIRMWARE_VERSIONS[fw_idx]

    @staticmethod
    def _make_sensor_id(slot_id: str) -> str:
        """Derive a stable sensor ID from a slot ID.

        Uses the last 8 hex characters of the slot string's UUID-v5 (DNS
        namespace) to ensure the same sensor_id is produced every time for
        the same slot.
        """
        slot_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, slot_id)
        return f"SEN_{slot_uuid.hex[:12].upper()}"

    # ------------------------------------------------------------------
    # Sensor state update
    # ------------------------------------------------------------------

    def _get_sensor_reading(self, sensor_id: str) -> dict:
        """Sample and update the mutable sensor state for *sensor_id*.

        Returns
        -------
        dict
            ``{battery_level, battery_low, signal_strength_dbm,
               sensor_type, firmware_version, sensor_health}``
        """
        battery, base_rssi = self._sensor_state.get(
            sensor_id, (100.0, -60.0)
        )

        # Drain battery slightly
        battery = max(0.0, battery - _BATTERY_DRAIN_PER_EVENT)

        # Drift RSSI
        rssi = float(
            np.clip(
                base_rssi + self._rng.normal(0.0, _RSSI_DRIFT_STD),
                -120.0,
                -20.0,
            )
        )

        # Persist updated state
        self._sensor_state[sensor_id] = (battery, base_rssi)

        battery_low = battery < 20.0
        signal_ok = rssi >= -105.0
        sensor_health = "OK" if (not battery_low and signal_ok) else "DEGRADED"

        firmware = self._sensor_firmware.get(sensor_id, "1.4.2")

        return {
            "battery_level": round(battery, 2),
            "battery_low": battery_low,
            "signal_strength_dbm": round(rssi, 1),
            "sensor_type": _SENSOR_TYPE,
            "firmware_version": firmware,
            "sensor_health": sensor_health,
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def simulate_sensor_readings(
        self,
        lot_id: str,
        slot_id: str,
        current_status: SlotStatus,
    ) -> dict:
        """Simulate what a real IoT sensor would report for *slot_id*.

        This method does **not** change slot state.  It simply returns a
        simulated hardware reading for the given slot, enriched with sensor
        metadata.

        Parameters
        ----------
        lot_id:
            Parent lot identifier.
        slot_id:
            Slot identifier.
        current_status:
            Reported occupancy status.

        Returns
        -------
        dict
            A partial event dict with sensor metadata fields.
        """
        sensor_id = self._slot_to_sensor.get(
            slot_id, self._make_sensor_id(slot_id)
        )
        reading = self._get_sensor_reading(sensor_id)

        lot = self._generator.lots.get(lot_id)
        if lot is None:
            logger.warning(
                "simulate_sensor_readings: lot_id '%s' not found.", lot_id
            )
            return {}

        return {
            "lot_id": lot_id,
            "slot_id": slot_id,
            "zone_id": lot.zone_id,
            "city": lot.city,
            "status": current_status.value,
            "sensor_id": sensor_id,
            "sensor_metadata": reading,
        }

    def generate_sensor_event(
        self,
        lot_id: str,
        slot_id: str,
        timestamp: Optional[datetime] = None,
    ) -> dict:
        """Generate a complete enriched event for *slot_id* at *timestamp*.

        This method reads the **current** slot state from the underlying
        ``ParkingLot``, builds a base event via ``EventGenerator.generate_event``,
        then merges in sensor metadata.

        Parameters
        ----------
        lot_id:
            Parent lot identifier.
        slot_id:
            Slot identifier.
        timestamp:
            Event time in UTC.  Defaults to now.

        Returns
        -------
        dict
            Fully-formed event with sensor metadata, or an empty dict if
            lot/slot lookup fails.
        """
        ts = timestamp or datetime.now(timezone.utc)

        lot = self._generator.lots.get(lot_id)
        if lot is None:
            logger.error(
                "generate_sensor_event: lot_id '%s' not found.", lot_id
            )
            return {}

        slot = lot.get_slot(slot_id)
        if slot is None:
            logger.error(
                "generate_sensor_event: slot_id '%s' not found in lot '%s'.",
                slot_id,
                lot_id,
            )
            return {}

        # Build base event
        base_event = self._generator.generate_event(
            lot=lot,
            slot_id=slot_id,
            status=slot.current_status,
            timestamp=ts,
        )

        # Merge sensor metadata
        sensor_id = self._slot_to_sensor.get(
            slot_id, self._make_sensor_id(slot_id)
        )
        reading = self._get_sensor_reading(sensor_id)
        base_event["sensor_id"] = sensor_id
        base_event["sensor_metadata"] = reading

        # Convenience top-level fields (mirrors sensor_metadata for easier Spark access)
        base_event["battery_level"] = reading["battery_level"]
        base_event["signal_strength_dbm"] = reading["signal_strength_dbm"]
        base_event["sensor_health"] = reading["sensor_health"]

        return base_event

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    @property
    def generator(self) -> EventGenerator:
        """Return the underlying ``EventGenerator``."""
        return self._generator

    def sensor_count(self) -> int:
        """Return the number of registered sensors."""
        return len(self._sensor_state)

    def get_sensor_id(self, slot_id: str) -> Optional[str]:
        """Return the sensor ID for *slot_id*, or ``None`` if unknown."""
        return self._slot_to_sensor.get(slot_id)

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"SensorNetwork(sensors={self.sensor_count()}, "
            f"generator={self._generator!r})"
        )
