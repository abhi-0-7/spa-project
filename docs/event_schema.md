# Event Schema Specification
## Smart Parking Occupancy Analytics (v1.0)
**Course:** AI372IA — Stream Processing and Analytics, RV College of Engineering  

---

## 1. Canonical Event Schema Definition

Every sensor reading emitted by the virtual sensor network conforms to the following standardized JSON schema:

```json
{
  "event_id": "EVT_A1B2C3D4E5F6",
  "lot_id": "LOT_KORAMANGALA_01",
  "slot_id": "LOT_KORAMANGALA_01-S015",
  "zone_id": "ZONE_SOUTH",
  "city": "Bengaluru",
  "status": "OCCUPIED",
  "event_timestamp": "2026-10-30T09:15:00.125Z",
  "ingestion_timestamp": "2026-10-30T09:15:00.280Z",
  "source": "SIMULATED_SENSOR",
  "event_sequence": 1042,
  "schema_version": "1.0",
  "latitude": 12.9352,
  "longitude": 77.6245,
  "lot_name": "Koramangala 5th Block Parking"
}
```

---

## 2. Field Specifications

| Field Name | Type | Mandatory? | Constraints & Format | Academic Purpose |
|---|---|---|---|---|
| `event_id` | String | Yes | Unique string (e.g. `EVT_<12-hex>`) | Used by Spark for watermark deduplication (`dropDuplicates`) |
| `lot_id` | String | Yes | Non-empty string (e.g. `LOT_KORAMANGALA_01`) | Used as Kafka partition key to guarantee per-lot order |
| `slot_id` | String | Yes | Unique per slot (e.g. `LOT_01-S001`) | Entity key for slot-level state tracking |
| `zone_id` | String | Yes | Spatial zone identifier (e.g. `ZONE_SOUTH`) | Aggregation key for geographical rollups |
| `city` | String | Yes | Context city name (`Bengaluru` / `Melbourne`) | Enforces provenance and prevents data cross-contamination |
| `status` | String | Yes | Enum: `["OCCUPIED", "FREE"]` | Sensor status driving the slot transition state machine |
| `event_timestamp`| String | Yes | ISO-8601 UTC with milliseconds (`...Z`) | Primary event-time clock driving windows & watermarks |
| `ingestion_timestamp`| String | Yes | ISO-8601 UTC; `>= event_timestamp` | Used to compute network ingestion delay and simulate jitter |
| `source` | String | Yes | Enum: `["SIMULATED_SENSOR", "HISTORICAL_REPLAY", "LIVE_SENSOR"]` | Explicit data provenance labeling |
| `event_sequence` | Integer | Yes | Monotonically increasing integer `>= 0` | Detects out-of-order deliveries and packet drops |
| `schema_version` | String | Yes | Enum: `["1.0"]` | Backward compatibility and schema evolution |
| `latitude` | Float | No | Real range: `[-90.0, 90.0]` | Geographic visualization |
| `longitude` | Float | No | Real range: `[-180.0, 180.0]` | Geographic visualization |
| `lot_name` | String | No | Human-readable lot title | Dashboard labeling |

---

## 3. Dead-Letter Queue (DLQ) Schema

Events failing schema validation or Spark JSON parsing are routed to `parking-events-dlq` with the following wrapper:

```json
{
  "dlq_schema_version": "1.0",
  "failure_timestamp": "2026-10-30T09:15:05.100Z",
  "validation_stage": "SPARK_SCHEMA_VALIDATOR",
  "errors": [
    "Field 'status' must be either 'OCCUPIED' or 'FREE', got 'RESERVED'",
    "Field 'latitude' must be between -90 and 90, got 999.0"
  ],
  "original_payload": {
    "event_id": "EVT_MALFORMED_01",
    "lot_id": "LOT_GHOST_99",
    "status": "RESERVED",
    "latitude": 999.0
  }
}
```

---

## 4. PySpark StructType Schema

Used in `streaming/preprocessing.py` for structured deserialization from Kafka byte payloads:

```python
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType
)

PARKING_EVENT_SPARK_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("lot_id", StringType(), False),
    StructField("slot_id", StringType(), False),
    StructField("zone_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("status", StringType(), False),
    StructField("event_timestamp", StringType(), False),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("source", StringType(), True),
    StructField("event_sequence", LongType(), True),
    StructField("schema_version", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("lot_name", StringType(), True),
])
```
