"""
monitoring/metrics.py
=====================
Performance Metrics and Telemetry Collector for Smart Parking Analytics.
Course: AI372IA — Stream Processing and Analytics, RV College of Engineering.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger("monitoring.metrics")


@dataclass
class StreamMetrics:
    """Snapshot of real-time stream processing metrics."""
    events_processed: int = 0
    events_invalid: int = 0
    events_duplicate: int = 0
    events_late: int = 0
    bytes_processed: int = 0
    processing_latencies_ms: List[float] = field(default_factory=list)
    batch_durations_ms: List[float] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)
    last_batch_time: Optional[float] = None


class MetricsCollector:
    """
    Thread-safe metrics collector for recording throughput,
    batch durations, latency percentiles, and fault counters.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._metrics = StreamMetrics()

    def record_event(
        self,
        latency_ms: float = 0.0,
        is_valid: bool = True,
        is_duplicate: bool = False,
        is_late: bool = False,
        payload_bytes: int = 0,
    ) -> None:
        """Records telemetry for an individual stream event."""
        with self._lock:
            self._metrics.events_processed += 1
            if not is_valid:
                self._metrics.events_invalid += 1
            if is_duplicate:
                self._metrics.events_duplicate += 1
            if is_late:
                self._metrics.events_late += 1
            self._metrics.bytes_processed += payload_bytes

            if latency_ms > 0:
                self._metrics.processing_latencies_ms.append(latency_ms)
                # Keep rolling window of last 10,000 samples to prevent unbounded memory
                if len(self._metrics.processing_latencies_ms) > 10_000:
                    self._metrics.processing_latencies_ms = self._metrics.processing_latencies_ms[-10_000:]

    def record_batch(self, duration_ms: float, event_count: int) -> None:
        """Records metrics for a completed micro-batch."""
        with self._lock:
            self._metrics.batch_durations_ms.append(duration_ms)
            if len(self._metrics.batch_durations_ms) > 1000:
                self._metrics.batch_durations_ms = self._metrics.batch_durations_ms[-1000:]
            self._metrics.last_batch_time = time.time()

    def get_throughput(self) -> float:
        """Returns overall throughput in events per second."""
        with self._lock:
            elapsed = time.time() - self._metrics.start_time
            if elapsed <= 0:
                return 0.0
            return round(self._metrics.events_processed / elapsed, 2)

    def get_latency_percentiles(self) -> Dict[str, float]:
        """Calculates p50, p95, and p99 latency in milliseconds."""
        with self._lock:
            if not self._metrics.processing_latencies_ms:
                return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "mean": 0.0}
            arr = np.array(self._metrics.processing_latencies_ms)
            return {
                "p50": round(float(np.percentile(arr, 50)), 2),
                "p95": round(float(np.percentile(arr, 95)), 2),
                "p99": round(float(np.percentile(arr, 99)), 2),
                "mean": round(float(np.mean(arr)), 2),
            }

    def get_summary(self) -> Dict[str, Any]:
        """Returns complete performance summary snapshot."""
        lat = self.get_latency_percentiles()
        with self._lock:
            elapsed = time.time() - self._metrics.start_time
            avg_batch_ms = (
                round(float(np.mean(self._metrics.batch_durations_ms)), 2)
                if self._metrics.batch_durations_ms
                else 0.0
            )
            return {
                "uptime_seconds": round(elapsed, 2),
                "events_processed": self._metrics.events_processed,
                "throughput_events_per_sec": self.get_throughput(),
                "events_invalid": self._metrics.events_invalid,
                "events_duplicate": self._metrics.events_duplicate,
                "events_late": self._metrics.events_late,
                "latency_p50_ms": lat["p50"],
                "latency_p95_ms": lat["p95"],
                "latency_p99_ms": lat["p99"],
                "avg_batch_duration_ms": avg_batch_ms,
            }

    def reset(self) -> None:
        """Resets all metrics to clean state."""
        with self._lock:
            self._metrics = StreamMetrics()
