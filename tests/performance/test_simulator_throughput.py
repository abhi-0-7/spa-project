"""
tests/performance/test_simulator_throughput.py
==============================================
Performance Unit Tests: Sustained Throughput & Generation Latency.
Course: AI372IA — Stream Processing and Analytics, RV College of Engineering.
"""

from __future__ import annotations

import time
import unittest
from datetime import datetime, timezone
from pathlib import Path

import yaml

from simulator.generator import EventGenerator


class TestSimulatorPerformance(unittest.TestCase):
    """Performance evaluation test cases."""

    @classmethod
    def setUpClass(cls):
        base_dir = Path(__file__).resolve().parent.parent.parent
        with open(base_dir / "config" / "simulator.yaml", "r", encoding="utf-8") as f:
            cls.config = yaml.safe_load(f)

    def test_generation_throughput_100_eps(self):
        """Verify simulator comfortably generates 100 events per second."""
        gen = EventGenerator(config=self.config, seed=42)
        start = time.perf_counter()
        now = datetime.now(timezone.utc)
        events = gen._drive_slot_transitions(hour=9, now=now, target_count=100)
        elapsed = time.perf_counter() - start

        self.assertEqual(len(events), 100)
        # Should complete in less than 50 milliseconds
        self.assertLess(elapsed, 0.05, f"100 events generated in {elapsed:.4f}s (exceeds budget)")

    def test_generation_throughput_1000_eps(self):
        """Verify simulator comfortably scales to 1,000 events per second."""
        gen = EventGenerator(config=self.config, seed=42)
        start = time.perf_counter()
        now = datetime.now(timezone.utc)
        events = gen._drive_slot_transitions(hour=9, now=now, target_count=1000)
        elapsed = time.perf_counter() - start

        self.assertEqual(len(events), 1000)
        # 1000 events should generate in under 500 milliseconds (sustaining >= 2000 eps)
        self.assertLess(elapsed, 0.50, f"1000 events generated in {elapsed:.4f}s")


if __name__ == "__main__":
    unittest.main()
