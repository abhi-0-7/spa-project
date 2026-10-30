"""
ingestion/schema.py — Canonical Event Schema v1.0
==================================================
Smart Parking Occupancy Analytics · AI372IA
RV College of Engineering, Bengaluru

Defines:
  - REQUIRED_FIELDS / OPTIONAL_FIELDS      : field name lists
  - VALID_STATUSES / VALID_SOURCES / ...   : allowed enumeration values
  - EVENT_SCHEMA                           : jsonschema-compatible dict
  - SPARK_SCHEMA                           : PySpark StructType for Kafka JSON ingestion
  - INFLUXDB_SCHEMA                        : InfluxDB measurement/tag/field mapping
"""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Field lists
# ---------------------------------------------------------------------------

REQUIRED_FIELDS: list[str] = [
    "event_id",
    "lot_id",
    "slot_id",
    "zone_id",
    "city",
    "status",
    "event_timestamp",
    "ingestion_timestamp",
    "source",
    "event_sequence",
    "schema_version",
]

OPTIONAL_FIELDS: list[str] = [
    "latitude",
    "longitude",
    "lot_name",
    "sensor_id",
    "battery_level",
    "signal_strength",
]

# ---------------------------------------------------------------------------
# Allowed enumeration values
# ---------------------------------------------------------------------------

VALID_STATUSES: set[str] = {"OCCUPIED", "FREE"}
VALID_SOURCES: set[str] = {"SIMULATED_SENSOR", "HISTORICAL_REPLAY", "LIVE_SENSOR"}
VALID_SCHEMA_VERSIONS: set[str] = {"1.0"}

# ---------------------------------------------------------------------------
# JSON-Schema-compatible event schema
# ---------------------------------------------------------------------------

EVENT_SCHEMA: dict = {
    "type": "object",
    "required": REQUIRED_FIELDS,
    "additionalProperties": True,   # allow optional fields without rejection
    "properties": {
        # --- required ---
        "event_id": {
            "type": "string",
            "minLength": 1,
            "description": "Unique identifier for this parking event (UUID recommended).",
        },
        "lot_id": {
            "type": "string",
            "minLength": 1,
            "description": "Unique identifier for the parking lot.",
        },
        "slot_id": {
            "type": "string",
            "minLength": 1,
            "description": "Unique identifier for a single parking slot within the lot.",
        },
        "zone_id": {
            "type": "string",
            "description": "Zone identifier within the lot (e.g. 'A', 'B', 'North').",
        },
        "city": {
            "type": "string",
            "description": "City in which the parking lot is located.",
        },
        "status": {
            "type": "string",
            "enum": ["OCCUPIED", "FREE"],
            "description": "Occupancy state of the slot at the time of the event.",
        },
        "event_timestamp": {
            "type": "string",
            "description": "ISO 8601 UTC datetime when the occupancy change was detected.",
        },
        "ingestion_timestamp": {
            "type": "string",
            "description": "ISO 8601 UTC datetime when the event entered the ingestion pipeline.",
        },
        "source": {
            "type": "string",
            "enum": list(VALID_SOURCES),
            "description": "Origin system that produced this event.",
        },
        "event_sequence": {
            "type": "integer",
            "minimum": 0,
            "description": "Monotonically increasing sequence number per producer session.",
        },
        "schema_version": {
            "type": "string",
            "enum": list(VALID_SCHEMA_VERSIONS),
            "description": "Schema version string, e.g. '1.0'.",
        },
        # --- optional ---
        "latitude": {
            "type": "number",
            "minimum": -90.0,
            "maximum": 90.0,
            "description": "WGS-84 latitude of the parking lot.",
        },
        "longitude": {
            "type": "number",
            "minimum": -180.0,
            "maximum": 180.0,
            "description": "WGS-84 longitude of the parking lot.",
        },
        "lot_name": {
            "type": "string",
            "description": "Human-readable name of the parking lot.",
        },
        "sensor_id": {
            "type": "string",
            "description": "Hardware sensor identifier that raised this event.",
        },
        "battery_level": {
            "type": "number",
            "minimum": 0,
            "maximum": 100,
            "description": "Sensor battery charge percentage (0–100).",
        },
        "signal_strength": {
            "type": "number",
            "description": "Wireless signal strength in dBm (typically negative).",
        },
    },
}

# ---------------------------------------------------------------------------
# PySpark StructType — used by the Structured Streaming consumer to parse
# the JSON value bytes arriving from Kafka.
# ---------------------------------------------------------------------------

SPARK_SCHEMA: StructType = StructType(
    [
        # Required fields
        StructField("event_id", StringType(), nullable=False),
        StructField("lot_id", StringType(), nullable=False),
        StructField("slot_id", StringType(), nullable=False),
        StructField("zone_id", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("status", StringType(), nullable=False),
        StructField("event_timestamp", StringType(), nullable=False),
        StructField("ingestion_timestamp", StringType(), nullable=False),
        StructField("source", StringType(), nullable=False),
        StructField("event_sequence", LongType(), nullable=False),
        StructField("schema_version", StringType(), nullable=False),
        # Optional fields
        StructField("latitude", DoubleType(), nullable=True),
        StructField("longitude", DoubleType(), nullable=True),
        StructField("lot_name", StringType(), nullable=True),
        StructField("sensor_id", StringType(), nullable=True),
        StructField("battery_level", DoubleType(), nullable=True),
        StructField("signal_strength", DoubleType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# InfluxDB schema descriptor
# ---------------------------------------------------------------------------
# Describes how a parking event maps to InfluxDB line-protocol concepts:
#   measurement  → the "table" name in InfluxDB
#   tags         → indexed string metadata, kept in the series key
#   fields       → numeric / string values written per point
#   timestamp_field → which event field holds the nanosecond-precision timestamp
#
# Usage example (pseudo-code):
#   point = Point(INFLUXDB_SCHEMA["measurement"])
#   for tag in INFLUXDB_SCHEMA["tags"]:
#       point.tag(tag, event[tag])
#   for field_name, field_type in INFLUXDB_SCHEMA["fields"].items():
#       point.field(field_name, field_type(event[field_name]))
# ---------------------------------------------------------------------------

INFLUXDB_SCHEMA: dict = {
    "measurement": "parking_events",
    "tags": [
        "lot_id",
        "slot_id",
        "zone_id",
        "city",
        "status",
        "source",
        "schema_version",
        "sensor_id",     # optional — may be None; writers must handle gracefully
        "lot_name",      # optional
    ],
    "fields": {
        # numeric / typed fields stored as InfluxDB field values
        "event_sequence": int,
        "battery_level": float,    # optional; None → skip
        "signal_strength": float,  # optional; None → skip
        "latitude": float,         # optional; None → skip
        "longitude": float,        # optional; None → skip
        # string fields (non-tag) kept as InfluxDB string fields
        "event_id": str,
    },
    "timestamp_field": "event_timestamp",   # ISO 8601 string; convert to ns epoch
    "retention_policy": "autogen",
    "database": "smart_parking",            # InfluxDB 1.x database name
    #  InfluxDB 2.x equivalents:
    "bucket": "smart_parking",
    "org": "rvce",
}
