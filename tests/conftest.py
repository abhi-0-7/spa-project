"""
tests/conftest.py
=================
Shared Fixtures for Smart Parking Analytics Test Suite.
Course: AI372IA — Stream Processing and Analytics, RV College of Engineering.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pytest
import yaml

from simulator.parking_lot import ParkingLot, SlotStatus


@pytest.fixture
def sample_valid_event() -> Dict[str, Any]:
    """Provides a valid parking sensor event dictionary."""
    now_utc = datetime.now(timezone.utc).isoformat()
    return {
        "event_id": "EVT_TEST_0001",
        "lot_id": "LOT_KORAMANGALA_01",
        "slot_id": "LOT_KORAMANGALA_01-S001",
        "zone_id": "ZONE_SOUTH",
        "city": "Bengaluru",
        "status": "OCCUPIED",
        "event_timestamp": now_utc,
        "ingestion_timestamp": now_utc,
        "source": "SIMULATED_SENSOR",
        "event_sequence": 1,
        "schema_version": "1.0",
        "latitude": 12.9352,
        "longitude": 77.6245,
        "lot_name": "Koramangala 5th Block Parking",
    }


@pytest.fixture
def sample_invalid_event() -> Dict[str, Any]:
    """Provides an intentionally malformed event missing mandatory fields."""
    return {
        "event_id": "",
        "lot_id": "LOT_TEST",
        "status": "INVALID_STATE",  # Illegal status
    }


@pytest.fixture
def sample_parking_lot() -> ParkingLot:
    """Provides a clean ParkingLot instance initialized with 10 slots."""
    lot = ParkingLot(
        lot_id="LOT_TEST_01",
        name="Test Surface Lot",
        zone_id="ZONE_TEST",
        city="Bengaluru",
        latitude=12.9716,
        longitude=77.6412,
        capacity=10,
        num_slots=10,
        lot_type="surface",
    )
    lot.initialize_slots()
    return lot


@pytest.fixture
def test_sim_config() -> Dict[str, Any]:
    """Loads simulator.yaml test configuration."""
    base_dir = Path(__file__).resolve().parent.parent
    config_file = base_dir / "config" / "simulator.yaml"
    with open(config_file, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
