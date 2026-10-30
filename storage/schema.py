"""
InfluxDB Schema Definition for Smart Parking Analytics.

Measurement: parking_occupancy
  Tags:   lot_id, zone_id, city, source
  Fields: occupied_slots (int), available_slots (int), capacity (int),
          occupancy_percentage (float)
  Time:   event timestamp (UTC, nanosecond precision)

Measurement: parking_transitions
  Tags:   lot_id, zone_id, city
  Fields: entries (int), exits (int), net_change (int)
  Time:   event timestamp (UTC)

Measurement: parking_window_analytics
  Tags:   lot_id, zone_id, city,
          window_type (TUMBLING_1MIN | TUMBLING_5MIN |
                       SLIDING_5MIN_1MIN | SLIDING_10MIN_2MIN)
  Fields: occupied_slots (int), available_slots (int), capacity (int),
          occupancy_percentage (float), entries (int), exits (int),
          event_count (int), events_per_second (float),
          avg_occupancy_pct (float), max_occupancy_pct (float),
          min_occupancy_pct (float),
          window_start_ts (int — ns epoch), window_end_ts (int — ns epoch)
  Time:   window start timestamp

Measurement: stream_performance
  Tags:   source (always "spark-streaming")
  Fields: events_per_second (float), processing_delay_ms (float),
          batch_duration_ms (float), invalid_event_count (int),
          duplicate_event_count (int), late_event_count (int),
          total_events_processed (int), kafka_consumer_lag (int)
  Time:   processing timestamp (UTC)

Course: AI372IA — Smart Parking Occupancy Analytics
Institution: RV College of Engineering, Bengaluru
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Measurement schema catalogue
# ---------------------------------------------------------------------------

MEASUREMENTS: dict[str, dict[str, list[str]]] = {
    "parking_occupancy": {
        "tags": ["lot_id", "zone_id", "city", "source"],
        "fields": [
            "occupied_slots",
            "available_slots",
            "capacity",
            "occupancy_percentage",
        ],
    },
    "parking_transitions": {
        "tags": ["lot_id", "zone_id", "city"],
        "fields": ["entries", "exits", "net_change"],
    },
    "parking_window_analytics": {
        "tags": ["lot_id", "zone_id", "city", "window_type"],
        "fields": [
            "occupied_slots",
            "available_slots",
            "capacity",
            "occupancy_percentage",
            "entries",
            "exits",
            "event_count",
            "events_per_second",
            "avg_occupancy_pct",
            "max_occupancy_pct",
            "min_occupancy_pct",
            "window_start_ts",
            "window_end_ts",
        ],
    },
    "stream_performance": {
        "tags": ["source"],
        "fields": [
            "events_per_second",
            "processing_delay_ms",
            "batch_duration_ms",
            "invalid_event_count",
            "duplicate_event_count",
            "late_event_count",
            "total_events_processed",
            "kafka_consumer_lag",
        ],
    },
}

# ---------------------------------------------------------------------------
# Valid window type tokens
# ---------------------------------------------------------------------------

WINDOW_TYPES: frozenset[str] = frozenset(
    {
        "TUMBLING_1MIN",
        "TUMBLING_5MIN",
        "SLIDING_5MIN_1MIN",
        "SLIDING_10MIN_2MIN",
    }
)

# ---------------------------------------------------------------------------
# Flux query library (reference for Grafana dashboard provisioning)
# ---------------------------------------------------------------------------

FLUX_QUERIES: dict[str, str] = {
    # ------------------------------------------------------------------
    # Dashboard panel: current occupancy per lot (last known value)
    # ------------------------------------------------------------------
    "current_occupancy_by_lot": """
from(bucket: "parking_analytics")
  |> range(start: -1m)
  |> filter(fn: (r) => r._measurement == "parking_occupancy")
  |> filter(fn: (r) => r._field == "occupancy_percentage")
  |> last()
  |> group(columns: ["lot_id"])
""",

    # ------------------------------------------------------------------
    # Dashboard panel: occupancy trend over the last hour
    # ------------------------------------------------------------------
    "occupancy_over_time": """
from(bucket: "parking_analytics")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "parking_occupancy")
  |> filter(fn: (r) => r._field == "occupancy_percentage")
  |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
""",

    # ------------------------------------------------------------------
    # Dashboard panel: entries vs exits from 1-minute tumbling windows
    # ------------------------------------------------------------------
    "entries_exits_1min": """
from(bucket: "parking_analytics")
  |> range(start: -30m)
  |> filter(fn: (r) => r._measurement == "parking_window_analytics")
  |> filter(fn: (r) => r.window_type == "TUMBLING_1MIN")
  |> filter(fn: (r) => r._field == "entries" or r._field == "exits")
  |> pivot(rowKey: ["_time", "lot_id"], columnKey: ["_field"], valueColumn: "_value")
""",

    # ------------------------------------------------------------------
    # Dashboard panel: pipeline throughput
    # ------------------------------------------------------------------
    "throughput": """
from(bucket: "parking_analytics")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "stream_performance")
  |> filter(fn: (r) => r._field == "events_per_second")
  |> aggregateWindow(every: 10s, fn: mean, createEmpty: false)
""",

    # ------------------------------------------------------------------
    # Dashboard panel: latency (processing_delay_ms)
    # ------------------------------------------------------------------
    "processing_latency": """
from(bucket: "parking_analytics")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "stream_performance")
  |> filter(fn: (r) => r._field == "processing_delay_ms")
  |> aggregateWindow(every: 10s, fn: mean, createEmpty: false)
""",

    # ------------------------------------------------------------------
    # Dashboard panel: error event breakdown (invalid / duplicate / late)
    # ------------------------------------------------------------------
    "error_event_counts": """
from(bucket: "parking_analytics")
  |> range(start: -10m)
  |> filter(fn: (r) => r._measurement == "stream_performance")
  |> filter(fn: (r) =>
       r._field == "invalid_event_count"
    or r._field == "duplicate_event_count"
    or r._field == "late_event_count"
  )
  |> aggregateWindow(every: 30s, fn: sum, createEmpty: false)
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
""",

    # ------------------------------------------------------------------
    # Dashboard panel: Kafka consumer lag
    # ------------------------------------------------------------------
    "kafka_consumer_lag": """
from(bucket: "parking_analytics")
  |> range(start: -10m)
  |> filter(fn: (r) => r._measurement == "stream_performance")
  |> filter(fn: (r) => r._field == "kafka_consumer_lag")
  |> aggregateWindow(every: 10s, fn: last, createEmpty: false)
""",

    # ------------------------------------------------------------------
    # Dashboard panel: average occupancy by window type (5-min)
    # ------------------------------------------------------------------
    "avg_occupancy_by_window_type": """
from(bucket: "parking_analytics")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "parking_window_analytics")
  |> filter(fn: (r) => r._field == "avg_occupancy_pct")
  |> group(columns: ["lot_id", "window_type"])
  |> aggregateWindow(every: 5m, fn: mean, createEmpty: false)
""",

    # ------------------------------------------------------------------
    # Alert rule: lot exceeding 90 % occupancy
    # ------------------------------------------------------------------
    "alert_high_occupancy": """
from(bucket: "parking_analytics")
  |> range(start: -2m)
  |> filter(fn: (r) => r._measurement == "parking_occupancy")
  |> filter(fn: (r) => r._field == "occupancy_percentage")
  |> last()
  |> filter(fn: (r) => r._value >= 90.0)
""",
}

# ---------------------------------------------------------------------------
# Field type map (used by write helpers for safe casting)
# ---------------------------------------------------------------------------

FIELD_TYPES: dict[str, dict[str, str]] = {
    "parking_occupancy": {
        "occupied_slots": "int",
        "available_slots": "int",
        "capacity": "int",
        "occupancy_percentage": "float",
    },
    "parking_transitions": {
        "entries": "int",
        "exits": "int",
        "net_change": "int",
    },
    "parking_window_analytics": {
        "occupied_slots": "int",
        "available_slots": "int",
        "capacity": "int",
        "occupancy_percentage": "float",
        "entries": "int",
        "exits": "int",
        "event_count": "int",
        "events_per_second": "float",
        "avg_occupancy_pct": "float",
        "max_occupancy_pct": "float",
        "min_occupancy_pct": "float",
        "window_start_ts": "int",
        "window_end_ts": "int",
    },
    "stream_performance": {
        "events_per_second": "float",
        "processing_delay_ms": "float",
        "batch_duration_ms": "float",
        "invalid_event_count": "int",
        "duplicate_event_count": "int",
        "late_event_count": "int",
        "total_events_processed": "int",
        "kafka_consumer_lag": "int",
    },
}
