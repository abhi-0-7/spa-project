"""
tests/unit/test_validator.py
============================
Unit tests for ingestion/validator.py.
Covers: required-field validation, status enum, timestamp bounds, coordinate
bounds, DLQ record structure, and batch validation.

Course: AI372IA — Smart Parking Occupancy Analytics
Institution: RV College of Engineering, Bengaluru
"""

from __future__ import annotations

import unittest
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from ingestion.validator import EventValidator, ValidationResult


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_valid_event(**overrides: Any) -> dict[str, Any]:
    """Return a fully valid raw event dict conforming to schema v1.0."""
    now_utc = _utc_now_iso()
    base: dict[str, Any] = {
        "event_id": f"EVT_{uuid.uuid4().hex[:12].upper()}",
        "lot_id": "LOT_MG_ROAD_01",
        "slot_id": "LOT_MG_ROAD_01-S001",
        "zone_id": "ZONE_CENTRAL",
        "city": "Bengaluru",
        "status": "OCCUPIED",
        "event_timestamp": now_utc,
        "ingestion_timestamp": now_utc,
        "source": "SIMULATED_SENSOR",
        "event_sequence": 1,
        "schema_version": "1.0",
        "latitude": 12.9756,
        "longitude": 77.6097,
        "lot_name": "MG Road Metro Parking",
    }
    base.update(overrides)
    return base


class TestEventValidator(unittest.TestCase):
    """Validator Unit Test Suite."""

    def setUp(self) -> None:
        self.validator = EventValidator()

    def test_valid_event_passes(self) -> None:
        """A well-formed event passes validation without errors."""
        event = _make_valid_event()
        result = self.validator.validate(event)
        self.assertTrue(result.is_valid)
        self.assertEqual(len(result.errors), 0)

    def test_missing_event_id_fails(self) -> None:
        """Missing or empty event_id causes validation to fail."""
        event = _make_valid_event(event_id="")
        result = self.validator.validate(event)
        self.assertFalse(result.is_valid)
        self.assertTrue(any("event_id" in err for err in result.errors))

    def test_missing_status_fails(self) -> None:
        """Missing status field fails validation."""
        event = _make_valid_event()
        del event["status"]
        result = self.validator.validate(event)
        self.assertFalse(result.is_valid)

    def test_invalid_status_value_fails(self) -> None:
        """Status other than OCCUPIED or FREE fails validation."""
        event = _make_valid_event(status="RESERVED")
        result = self.validator.validate(event)
        self.assertFalse(result.is_valid)
        self.assertTrue(any("status" in err for err in result.errors))

    def test_invalid_timestamp_format_fails(self) -> None:
        """Unparseable timestamp string fails validation."""
        event = _make_valid_event(event_timestamp="not-a-timestamp")
        result = self.validator.validate(event)
        self.assertFalse(result.is_valid)

    def test_far_future_timestamp_fails(self) -> None:
        """Timestamp more than 24 hours in the future fails validation."""
        future_ts = (datetime.now(timezone.utc) + timedelta(days=2)).isoformat()
        event = _make_valid_event(event_timestamp=future_ts)
        result = self.validator.validate(event)
        self.assertFalse(result.is_valid)

    def test_far_past_timestamp_fails(self) -> None:
        """Timestamp more than 7 days in the past fails validation."""
        old_ts = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
        event = _make_valid_event(event_timestamp=old_ts)
        result = self.validator.validate(event)
        self.assertFalse(result.is_valid)

    def test_valid_coordinates_pass(self) -> None:
        """Valid latitude (-90 to 90) and longitude (-180 to 180) pass."""
        event = _make_valid_event(latitude=12.9716, longitude=77.5946)
        result = self.validator.validate(event)
        self.assertTrue(result.is_valid)

    def test_invalid_latitude_fails(self) -> None:
        """Latitude outside [-90, 90] fails validation."""
        event = _make_valid_event(latitude=999.0)
        result = self.validator.validate(event)
        self.assertFalse(result.is_valid)

    def test_invalid_longitude_fails(self) -> None:
        """Longitude outside [-180, 180] fails validation."""
        event = _make_valid_event(longitude=-999.0)
        result = self.validator.validate(event)
        self.assertFalse(result.is_valid)

    def test_dlq_record_structure(self) -> None:
        """create_dlq_record produces expected schema for quarantined records."""
        raw_event = {"bad": "event"}
        errors = ["Missing event_id", "Missing lot_id"]
        dlq = self.validator.create_dlq_record(raw_event, errors, stage="INGESTION")

        self.assertIn("original_payload", dlq)
        self.assertIn("errors", dlq)
        self.assertIn("failure_timestamp", dlq)
        self.assertEqual(dlq["validation_stage"], "INGESTION")
        self.assertEqual(dlq["errors"], errors)

    def test_batch_validation(self) -> None:
        """validate_batch correctly evaluates list of mixed events."""
        events = [
            _make_valid_event(),
            _make_valid_event(status="UNKNOWN"),
            _make_valid_event(),
        ]
        results = self.validator.validate_batch(events)
        self.assertEqual(len(results), 3)
        self.assertTrue(results[0].is_valid)
        self.assertFalse(results[1].is_valid)
        self.assertTrue(results[2].is_valid)


if __name__ == "__main__":
    unittest.main()
