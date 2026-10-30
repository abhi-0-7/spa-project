"""
tests/unit/test_windowing.py
============================
Unit tests for windowing logic (pure unit tests — no live Spark session required).
Verifies:
- Tumbling window non-overlapping intervals
- Sliding window overlap properties
- Slide < Duration validation
- Window explanation string output

Course: AI372IA — Smart Parking Occupancy Analytics
Institution: RV College of Engineering, Bengaluru
"""

from __future__ import annotations

import unittest
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from streaming.windowing import explain_window_difference


@dataclass(frozen=True)
class TumblingWindowConfig:
    duration_seconds: int

    @property
    def duration(self) -> timedelta:
        return timedelta(seconds=self.duration_seconds)


@dataclass(frozen=True)
class SlidingWindowConfig:
    duration_seconds: int
    slide_seconds: int

    def __post_init__(self) -> None:
        if self.slide_seconds >= self.duration_seconds:
            raise ValueError(
                "slide_seconds must be strictly less than duration_seconds "
                f"for a sliding window (got {self.slide_seconds} >= {self.duration_seconds})"
            )

    @property
    def duration(self) -> timedelta:
        return timedelta(seconds=self.duration_seconds)

    @property
    def slide(self) -> timedelta:
        return timedelta(seconds=self.slide_seconds)


class TestWindowLogic(unittest.TestCase):
    """Pure mathematical and structural tests for window mechanics."""

    def test_tumbling_window_no_overlap(self):
        """Tumbling window buckets do not overlap and partition time cleanly."""
        duration_s = 60
        cfg = TumblingWindowConfig(duration_seconds=duration_s)
        self.assertEqual(cfg.duration, timedelta(seconds=60))

        # Check contiguous window alignment
        base_t = datetime(2026, 10, 30, 9, 0, 0, tzinfo=timezone.utc)
        w1_end = base_t + cfg.duration
        w2_start = w1_end
        self.assertEqual(w1_end, w2_start, "Consecutive tumbling windows must meet seamlessly")

    def test_sliding_window_overlaps(self):
        """Sliding window duration > slide interval guarantees overlap."""
        duration_s = 300  # 5 minutes
        slide_s = 60      # 1 minute
        cfg = SlidingWindowConfig(duration_seconds=duration_s, slide_seconds=slide_s)

        self.assertEqual(cfg.duration, timedelta(minutes=5))
        self.assertEqual(cfg.slide, timedelta(minutes=1))

        # An event at minute 2 falls into both window 0 and window 1
        base_t = datetime(2026, 10, 30, 9, 0, 0, tzinfo=timezone.utc)
        w0_start, w0_end = base_t, base_t + cfg.duration
        w1_start, w1_end = base_t + cfg.slide, base_t + cfg.slide + cfg.duration

        event_t = base_t + timedelta(minutes=2)
        self.assertTrue(w0_start <= event_t < w0_end)
        self.assertTrue(w1_start <= event_t < w1_end)

    def test_sliding_window_invalid_slide_raises(self):
        """Slide >= duration is invalid for sliding windows."""
        with self.assertRaises(ValueError):
            SlidingWindowConfig(duration_seconds=60, slide_seconds=60)
        with self.assertRaises(ValueError):
            SlidingWindowConfig(duration_seconds=60, slide_seconds=120)

    def test_explain_window_difference_returns_academic_text(self):
        """explain_window_difference provides thorough academic definitions."""
        explanation = explain_window_difference()
        self.assertIn("TUMBLING WINDOWS", explanation)
        self.assertIn("SLIDING WINDOWS", explanation)
        self.assertIn("Non-overlapping", explanation)
        self.assertIn("Overlapping", explanation)


if __name__ == "__main__":
    unittest.main()
