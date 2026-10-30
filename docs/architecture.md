# System Architecture
## Smart Parking Occupancy Analytics
**Course:** AI372IA — Stream Processing and Analytics  
**Institution:** RV College of Engineering, Bengaluru  
**Domain:** Artificial Intelligence and Machine Learning  

---

## 1. High-Level Architectural Pipeline

The system is organized as an asynchronous, event-driven streaming pipeline designed for continuous real-time occupancy processing, windowed temporal aggregations, and resilient time-series persistence.

```
+-------------------------------------------------------------------------------+
|                       VIRTUAL IoT SENSOR NETWORK                              |
|   (Python Simulator: 8 Lots, 1,000 Slots, Diurnal Traffic, Fault Injection)   |
+-------------------------------------------------------------------------------+
                                      |
                                      | [JSON Events via KafkaProducer]
                                      v
+-------------------------------------------------------------------------------+
|                         APACHE KAFKA BROKER (KRaft)                           |
|        Topic: raw-parking-events (4 partitions, Key = lot_id)                 |
+-------------------------------------------------------------------------------+
                                      |
                                      | [Continuous Stream Ingestion]
                                      v
+-------------------------------------------------------------------------------+
|                    APACHE SPARK STRUCTURED STREAMING                          |
|                                                                               |
|  [Stage 1: Deserialization & Metadata Enrichment]                             |
|       - from_json schema enforcement                                          |
|       - ingestion jitter calculation & timestamp verification                 |
|                                                                               |
|  [Stage 2: Validation & DLQ Branching]                                        |
|       - Valid events  ──> Main Processing Stream                              |
|       - Malformed / Nulls ──> parking-events-dlq (Dead Letter Queue)          |
|                                                                               |
|  [Stage 3: Deduplication & Watermarking]                                      |
|       - dropDuplicates(["event_id"]) bounded by 10-min state store            |
|       - withWatermark("event_timestamp", "2 minutes")                         |
|                                                                               |
|  [Stage 4: Stateful Slot Occupancy Tracking]                                  |
|       - Strict 2-state Finite State Machine (FSM):                            |
|           * FREE     -> OCCUPIED : entry (+1 occupancy)                       |
|           * OCCUPIED -> FREE     : exit  (-1 occupancy)                       |
|           * Redundant Status     : no-op (0 count drift)                      |
|       - Deterministic bounds: 0 <= occupied_count <= lot_capacity             |
|                                                                               |
|  [Stage 5: Windowed Temporal Aggregations]                                    |
|       - 1-minute Tumbling Windows   (entries, exits, net change)              |
|       - 5-minute Tumbling Windows                                             |
|       - 5-minute Sliding Windows (1-minute slide interval)                    |
|       - 10-minute Sliding Windows (2-minute slide interval)                   |
|                                                                               |
|  [Stage 6: Multi-Sink Micro-Batch Dispatch]                                   |
|       - InfluxDB Line Protocol Writer (Idempotent HTTP / SDK)                 |
|       - Kafka Aggregated Stats Topic                                          |
|       - Fault-Tolerant Local File Checkpoint Stores                           |
+-------------------------------------------------------------------------------+
           |                                                   |
           v                                                   v
+---------------------------------------+   +-----------------------------------+
|             INFLUXDB 2.7              |   |       APACHE KAFKA (Downstream)   |
| Bucket: parking_analytics             |   | Topic: aggregated-occupancy-stats |
| Measurements:                         |   +-----------------------------------+
|   - parking_occupancy                 |
|   - parking_window_analytics          |
|   - stream_performance                |
+---------------------------------------+
                   |
                   | [Flux Queries / 10s Auto-Refresh]
                   v
+-------------------------------------------------------------------------------+
|                             GRAFANA 10.4 DASHBOARD                            |
|             UID: smart-parking-overview (24 Real-Time Analytical Panels)       |
+-------------------------------------------------------------------------------+
```

---

## 2. Component Breakdown and Responsibilities

### 2.1 Virtual IoT Sensor Network (`simulator/`)
- **Responsibility:** Replaces missing physical hardware on the laptop with an accurate stochastic simulation.
- **Components:**
  - `parking_lot.py`: Models individual `ParkingSlot` objects and `ParkingLot` containers with explicit capacity limits.
  - `traffic_model.py`: Models diurnal traffic cycles (morning peak 07:00–10:00, evening peak 17:00–20:00, night lull 22:00–06:00) with independent arrival and departure Poisson multipliers.
  - `fault_injector.py`: Injects controlled hardware failures including duplicate transmissions (2%), delayed/late arrivals up to 180 seconds (3%), and malformed coordinate payloads (1%).
  - `generator.py`: Generates schema v1.0 JSON payloads pacing output from 10 to 5,000 events/second using a thread-safe global sequence counter.

### 2.2 Event Ingestion Layer (`ingestion/`)
- **Responsibility:** Validates schema integrity and publishes events to Apache Kafka.
- **Components:**
  - `schema.py`: Defines canonical JSON schema, PySpark StructType, and InfluxDB measurement schemas.
  - `validator.py`: Executes multi-rule schema validation; generates structured DLQ records for failed events.
  - `producer.py`: Implements `ParkingEventProducer` with gzip compression, retries (`retries=3`), all-replica acknowledgement (`acks='all'`), and per-lot partition keying.
  - `historical_replay.py`: Normalizes external historical datasets (e.g. City of Melbourne 2019) for replay benchmarks.

### 2.3 Stream Processing Engine (`streaming/`)
- **Responsibility:** Apache Spark Structured Streaming application driving event-time transformations, deduplication, stateful slot management, and windowing.
- **Key Files:**
  - `consumer.py`: Establishes the streaming Kafka source with configurable starting offsets and max offsets per trigger.
  - `preprocessing.py`: JSON parsing, schema casting, and ingestion latency measurement.
  - `deduplication.py`: Watermark-bounded deduplication on `event_id`.
  - `event_time.py`: Watermarking and latency classification (`ON_TIME`, `SLIGHTLY_LATE`, `VERY_LATE`).
  - `state_manager.py`: Implements `StatefulOccupancyTracker` maintaining slot-level state across micro-batches with strict 0 to capacity clipping.
  - `windowing.py`: Generates 1-min & 5-min tumbling windows and 5-min & 10-min sliding windows.
  - `sink.py`: Implements atomic micro-batch dispatch to InfluxDB, Kafka, and diagnostic console.

### 2.4 Time-Series Analytics Storage (`storage/`)
- **Responsibility:** InfluxDB 2.7 persistence with nanosecond-precision timestamps for real-time aggregation.
- **Features:**
  - Idempotent writes: Re-processing a micro-batch overwrites points at identical `(measurement, tags, timestamp)` coordinates without duplicate row inflation.
  - Dual-mode client (`storage/influx_client.py`): Supports native `influxdb-client` SDK when present, with a built-in HTTP REST line-protocol fallback engine.

### 2.5 Visualization Layer (`dashboards/grafana/`)
- **Responsibility:** 24 live panels provisioned via YAML and JSON dashboards.
- **Refresh Interval:** 10 seconds.
- **Query Language:** InfluxDB Flux.

---

## 3. Resource Allocation and Footprint (16 GB Laptop)

| Service | Memory Budget | CPU Allocation | Notes |
|---|---|---|---|
| Apache Kafka (KRaft) | 512 MB Heap | 1 core | Zookeeper eliminated |
| InfluxDB 2.7 | 256 MB | 1 core | In-memory cache + TSM engine |
| Grafana 10.4 | 128 MB | 0.5 core | Lightweight Go daemon |
| Spark Driver (local[*]) | 2.0 GB Heap | 4-6 cores | PySpark 3.5.1 Structured Streaming |
| Python Simulator | ~120 MB | 1 core | Single-process vectorized numpy |
| **Total Pipeline Overhead** | **~3.0 GB** | **~8 cores** | **Well within 16 GB DDR4 envelope** |
