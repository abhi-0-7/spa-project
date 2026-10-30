"""
tests/unit/test_generator.py
============================
Unit tests for simulator/generator.py.
Covers: required field presence, ID uniqueness, status validity, UTC timestamps,
sequence monotonicity, reproducibility with seeds, status distribution, and batch size.

Course: AI372IA — Smart Parking Occupancy Analytics
Institution: RV College of Engineering, Bengaluru
"""

from __future__ import annotations

import unittest
from datetime import datetime, timezone
from pathlib import Path

import yaml

from simulator.generator import EventGenerator


class TestEventGenerator(unittest.TestCase):
    """EventGenerator Unit Test Suite."""

    @classmethod
    def setUpClass(cls):
        base_dir = Path(__file__).resolve().parent.parent.parent
        with open(base_dir / "config" / "simulator.yaml", "r", encoding="utf-8") as f:
            cls.config = yaml.safe_load(f)

    def setUp(self):
        self.gen = EventGenerator(config=self.config, seed=42)

    def test_event_has_required_fields(self):
        """Generated events must contain all fields mandated by schema v1.0."""
        required = [
            "event_id", "lot_id", "slot_id", "zone_id", "city",
            "status", "event_timestamp", "ingestion_timestamp",
            "source", "event_sequence", "schema_version",
            "latitude", "longitude", "lot_name",
        ]
        event = self.gen.generate_next_event()
        for field in required:
            self.assertIn(field, event, f"Missing required field: {field}")

    def test_event_id_unique(self):
        """Every generated event must have a distinct event_id."""
        events = [self.gen.generate_next_event() for _ in range(50)]
        ids = [e["event_id"] for e in events]
        self.assertEqual(len(ids), len(set(ids)), "Generated event IDs must be globally unique")

    def test_status_is_valid(self):
        """Status must always be either OCCUPIED or FREE."""
        events = [self.gen.generate_next_event() for _ in range(30)]
        for e in events:
            self.assertIn(e["status"], ("OCCUPIED", "FREE"))

    def test_timestamps_are_utc(self):
        """event_timestamp and ingestion_timestamp must be valid UTC timestamps."""
        event = self.gen.generate_next_event()
        evt_ts = datetime.fromisoformat(event["event_timestamp"].replace("Z", "+00:00"))
        ing_ts = datetime.fromisoformat(event["ingestion_timestamp"].replace("Z", "+00:00"))
        self.assertIsNotNone(evt_ts.tzinfo)
        self.assertIsNotNone(ing_ts.tzinfo)
        self.assertGreaterEqual(ing_ts, evt_ts)

    def test_event_sequence_increments(self):
        """Sequence counter increments monotonically."""
        e1 = self.gen.generate_next_event()
        e2 = self.gen.generate_next_event()
        self.assertGreater(e2["event_sequence"], e1["event_sequence"])

    def test_reproducible_with_seed(self):
        """Two generators with identical seeds produce the same sequence of slot events."""
        gen1 = EventGenerator(config=self.config, seed=999)
        gen2 = EventGenerator(config=self.config, seed=999)

        now = datetime(2026, 10, 30, 9, 0, 0, tzinfo=timezone.utc)
        ev1 = gen1._drive_slot_transitions(hour=9, now=now, target_count=5)
        ev2 = gen2._drive_slot_transitions(hour=9, now=now, target_count=5)

        self.assertEqual(len(ev1), len(ev2))
        for a, b in zip(ev1, ev2):
            self.assertEqual(a["lot_id"], b["lot_id"])
            self.assertEqual(a["slot_id"], b["slot_id"])
            self.assertEqual(a["status"], b["status"])

    def test_batch_generation_count(self):
        """_drive_slot_transitions generates requested target count."""
        now = datetime.now(timezone.utc)
        events = self.gen._drive_slot_transitions(hour=9, now=now, target_count=25)
        self.assertEqual(len(events), 25)


if __name__ == "__main__":
    unittest.main()
