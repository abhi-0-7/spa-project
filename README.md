# Smart Parking Occupancy Analytics
## End-to-End Real-Time Stream Processing and Analytics System

[![Python: 3.10+](https://img.shields.io/badge/Python-3.10%20%7C%203.11%20%7C%203.13-brightgreen.svg)](https://python.org)
[![Spark: 3.5.1](https://img.shields.io/badge/Apache%20Spark-3.5.1%20Structured%20Streaming-orange.svg)](https://spark.apache.org)
[![Kafka: 3.7/3.9](https://img.shields.io/badge/Apache%20Kafka-KRaft%20Mode-black.svg)](https://kafka.apache.org)
[![InfluxDB: 2.7](https://img.shields.io/badge/InfluxDB-2.7%20Time--Series-purple.svg)](https://influxdata.com)
[![Grafana: 10/11](https://img.shields.io/badge/Grafana-Live%20Telemetry%20Dashboard-yellow.svg)](https://grafana.com)
[![Tests: 43/43 Passed](https://img.shields.io/badge/Tests-43%2F43%20Passed-success.svg)](https://github.com/abhi-0-7/spa-project)

---

## 1. Academic Identity & Course Context

- **Course Title:** Stream Processing and Analytics
- **Repository:** [https://github.com/abhi-0-7/spa-project.git](https://github.com/abhi-0-7/spa-project.git)

### Academic Focus Disclaimer
> **IMPORTANT:** The core academic objective of this project is **Stream Processing and Analytics (SPA)**. Smart parking serves as the real-world operational domain used to implement, evaluate, and defend distributed stream-processing concepts: continuous event ingestion, event-time semantics, watermarking, stateful micro-batch transformations, tumbling and sliding window aggregations, late-arriving event mitigation, deduplication under network failures, checkpoint recovery, and time-series telemetry visualization.

---

## 2. End-to-End System Architecture

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│               SMART PARKING OCCUPANCY ANALYTICS — PIPELINE ARCHITECTURE                 │
└────────────────────────────────────────────────────────────────────────────────────────┘

 [ SOURCE A: Live API Feed ]           [ SOURCE B: Virtual IoT Simulator ]
 (City Open Data REST API)              (8 Lots, 1,000 Slots, Diurnal Waves)
  • Live API Key authentication          • Normal (50 ev/s), High Load (500 ev/s)
  • Real-time bay occupancy              • Fault injection (duplicates, late arrivals)
           │                                      │
           └──────────────────┬───────────────────┘
                              ▼
            ┌──────────────────────────────────────────┐
            │        APACHE KAFKA (KRaft Mode)         │
            │   Broker: localhost:9092, 4 Partitions   │
            ├──────────────────────────────────────────┤
            │  • raw-parking-events (Partition by lot) │
            │  • parking-events-dlq (Schema anomalies) │
            │  • aggregated-occupancy-stats            │
            └─────────────────┬────────────────────────┘
                              │ readStream (Structured Streaming)
                              ▼
            ┌──────────────────────────────────────────┐
            │       APACHE SPARK STREAMING ENGINE      │
            │               (Master: local[*])         │
            ├──────────────────────────────────────────┤
            │ 1. Schema Parser & Timestamp Ingestion   │
            │ 2. Data Cleansing & DLQ Routing (Filter) │
            │ 3. Authoritative Event-Time Watermark    │
            │    └─ withWatermark("event_timestamp",   │
            │         "2 minutes")                     │
            │ 4. Stateful Deduplication                │
            │    └─ dropDuplicates(["event_id"])       │
            │ 5. Latency Classification:               │
            │    └─ ON_TIME, SLIGHTLY_LATE, VERY_LATE  │
            │ 6. Window Aggregations:                  │
            │    ├─ Tumbling Window (1 min duration)   │
            │    └─ Sliding Window (5 min, 1 min slide)│
            │ 7. Stateful Micro-Batch Occupancy Tracker│
            │    └─ In-memory Slot State FSM & Counters│
            └─────────────────┬────────────────────────┘
                              │ foreachBatch & HTTP Line Protocol
                              ▼
            ┌──────────────────────────────────────────┐
            │         INFLUXDB 2.7 TIME-SERIES         │
            │       Bucket: parking | Org: rvce        │
            ├──────────────────────────────────────────┤
            │ Measurements:                            │
            │  • parking_occupancy                     │
            │  • parking_occupancy_window              │
            │  • parking_window_analytics              │
            │  • parking_processing                    │
            │  • stream_performance                    │
            └─────────────────┬────────────────────────┘
                              │ Flux Query Proxy
                              ▼
            ┌──────────────────────────────────────────┐
            │         GRAFANA 11.5 DASHBOARD           │
            │            (Port 3001 / 3000)            │
            ├──────────────────────────────────────────┤
            │  • Live City Capacity & Available Slots  │
            │  • Dynamic Occupancy % Gauges            │
            │  • Bar Charts: Occupancy by Lot & Zone   │
            │  • Time Series: Entries / Exits per min  │
            │  • Stream Telemetry: Throughput & Latency│
            └──────────────────────────────────────────┘
```

---

## 3. Core Stream Processing Concepts Implemented

| Stream Processing Concept | Implementation in Pipeline | Academic Significance |
| :--- | :--- | :--- |
| **Event-Time Processing** | `event_timestamp` parsed directly from IoT payload | Ensures temporal correctness regardless of network latency or Kafka queuing delay. |
| **Watermark Semantics** | `apply_watermark("event_timestamp", "2 minutes")` | Bounds Spark's state store memory; straggler events arriving past $T_{watermark}$ are cleanly evicted. |
| **Single-Authoritative Watermark** | Enforced once at ingestion before stateful stages | Completely resolves Spark 3.5's `AnalysisException: Redefining watermark is disallowed`. |
| **Stateful Deduplication** | `dropDuplicates(["event_id"])` bounded by watermark | Guarantees exactly-once processing semantics over Kafka's at-least-once transport. |
| **Tumbling Windows** | Fixed 1-minute non-overlapping windows | Computes discrete batch entry/exit velocities and net flow rates without double-counting. |
| **Sliding Windows** | 5-minute duration with 1-minute slide (overlapping) | Smooths out transient traffic spikes to compute rolling trend analysis. |
| **Dead Letter Queue (DLQ)** | Malformed records partitioned to `parking-events-dlq` | Fault isolation: corrupt sensor packets do not crash the pipeline or corrupt aggregations. |
| **Dual-Schema Persistence** | Synchronous line-protocol writes to `parking_occupancy` & `parking_occupancy_window` | Seamless Grafana visualization with automatic fallback across multi-version dashboard templates. |
| **Stateful Slot Tracking** | `StatefulOccupancyTracker` with slot-level FSM | Computes deterministic lot occupancy percentages and detects duplicate or ghost transmissions. |
| **Checkpoint Recovery** | Offsets & state committed to `./checkpoints/` | Guarantees zero data loss across ungraceful process termination or system restarts. |

---

## 4. Hardware and System Constraints

The entire architecture is engineered to run seamlessly on a **single laptop** without requiring physical external hardware:

- **Host Processor:** Intel Core i7-12700H (14 cores / 20 threads)
- **RAM Budget:** 16 GB DDR4 (Pipeline consumes ~3.0 GB total container + JVM heap)
- **Execution Mode:** Multi-threaded local CPU (`master("local[*]")` with 4 shuffle partitions)
- **OS Support:** Windows 10/11 (PowerShell), Linux, macOS

---

## 5. Step-by-Step Execution Guide

### Prerequisites
- Python 3.10+ installed and added to `PATH`.
- Docker Desktop running.
- Java JDK 8 installed (`JAVA_HOME` configured).

---

### Step 1: Start Infrastructure Containers

From the project root:
```powershell
docker compose up -d
```
Verify that all containers are active:
- **Apache Kafka:** `localhost:9092`
- **InfluxDB 2.7:** `http://localhost:8086`
- **Grafana:** `http://localhost:3001` (or `http://localhost:3000`)

---

### Step 2: Reset Checkpoints & Stale State

To ensure a clean starting state without offset conflicts:
```powershell
cd C:\Users\Abhishek\Downloads\spa\smart-parking-analytics
python scripts/reset.py --force
```

---

### Step 3: Launch Spark Structured Streaming Engine (Terminal 1)

In your first PowerShell terminal, run:
```powershell
cd C:\Users\Abhishek\Downloads\spa\smart-parking-analytics
python -m streaming.main --watermark "2 minutes" --trigger "5 seconds"
```
> **Output:** Spark connects to Kafka topic `raw-parking-events`, applies watermarking, starts tumbling/sliding window aggregations, and begins printing micro-batch execution summaries:
> ```text
> [INFO] streaming.main - Stateful occupancy & window queries active and writing to InfluxDB.
> [INFO] streaming.main - Batch 0: Processed X events in 0.42s (115.2 ev/s)
> ```

---

### Step 4: Launch Real-Time Event Ingestion (Terminal 2)

Choose either the **Live External API** or the **Virtual IoT Simulator**:

#### Option A: Live External REST API (Demonstrates Live API Key Integration)
```powershell
cd C:\Users\Abhishek\Downloads\spa\smart-parking-analytics
python -m ingestion.live_api_producer --interval 3 --api-key "DEMO_KEY_RVCE_AI372IA"
```
*Features authenticated polling against live smart parking municipal open data APIs with automatic fallback to live sensor streams.*

#### Option B: Virtual IoT Sensor Network Simulator
```powershell
cd C:\Users\Abhishek\Downloads\spa\smart-parking-analytics
# Normal steady traffic (50 events/sec)
python -m simulator.main --mode normal --rate 50

# High-load stress test (500 events/sec)
python -m simulator.main --mode high_load --rate 500

# Peak-hour traffic pattern (morning & evening rush hour waves)
python -m simulator.main --mode peak_hour

# Fault-injection mode (injects duplicate events, late events, and corrupt coordinates)
python -m simulator.main --mode failure --rate 30
```

---

### Step 5: View Real-Time Telemetry in Grafana

Open your web browser and navigate to:
**[http://localhost:3001](http://localhost:3001)** *(or `http://localhost:3000`)*

- **Username:** `admin`
- **Password:** `admin` *(or `parking123`)*
- **Dashboard:** **Dashboards** $\rightarrow$ **Smart Parking Occupancy Analytics — Bengaluru**
- **Refresh Interval:** Select **5s** in the top-right corner.
- **Time Range:** Select **Last 15 minutes** or **Last 1 hour**.

#### Dashboard Highlights:
1. **Capacity & Availability KPIs:** Real-time gauges for Total Capacity (1,000), Occupied Slots, Available Slots, and Overall Occupancy %.
2. **Geographical Dynamics:** Bar charts showing occupancy by lot (MG Road, Koramangala, Indiranagar, Whitefield, Hebbal, Jayanagar, Electronic City, Yelahanka).
3. **Windowed Velocities:** Continuous timeseries plotting Entries vs. Exits per minute computed via 1-minute tumbling windows.
4. **Stream Telemetry:** Live monitoring of Spark micro-batch processing duration, throughput (events/sec), and pipeline latency.

---

## 6. Verification and Automated Test Suite

The system includes a 43-test suite covering unit components, schema validators, state transitions, window algorithms, and pipeline integrations:

```powershell
cd C:\Users\Abhishek\Downloads\spa\smart-parking-analytics
python tests/run_all_tests.py
```

### Test Suite Output:
```text
=================================================================
      SMART PARKING OCCUPANCY ANALYTICS -- TEST SUITE
      Course: AI372IA -- RV College of Engineering
=================================================================
Tests Run:       43
Errors:          0
Failures:        0
Skipped:         2 (offline broker mocks)
Execution Time:  0.28 seconds
=================================================================
[SUCCESS] ALL TESTS PASSED!
```

---

## 7. Viva & Academic Defense Q&A

### Q1: Why use Event Time instead of Processing Time?
**Answer:** Network congestion, wireless IoT packet retries, and Kafka buffering create variable transit delays. If processing time were used, late-arriving events would be assigned to the wrong temporal window, skewing entry/exit velocities and peak-hour statistics. Using `event_timestamp` guarantees deterministic, reproducible window aggregates.

### Q2: What is the purpose of the 2-Minute Watermark?
**Answer:** In stateful stream processing, Spark maintains in-memory state for windows and deduplication. Without a watermark, Spark would retain state indefinitely in anticipation of infinitely late events, eventually triggering an Out-Of-Memory (OOM) crash. The 2-minute watermark instructs the execution engine to wait up to 120 seconds beyond the maximum observed event time before closing the window and evicting state.

### Q3: How was Spark 3.5's Watermark Redefinition Bug resolved?
**Answer:** In Apache Spark 3.5+, calling `.withWatermark()` more than once on the same column in an execution graph throws `AnalysisException: Redefining watermark is disallowed`. The architecture was refactored to apply a **single authoritative watermark** on `valid_stream` immediately after ingestion. Downstream stages (`dropDuplicates` and window operators) reuse this existing watermark without re-declaring it.

### Q4: How does the system handle corrupt sensor payloads?
**Answer:** Schema enforcement validates records against required fields (`event_id`, `lot_id`, `slot_id`, status `OCCUPIED`/`FREE`, valid lat/lon, parseable ISO timestamps). Valid records enter the analytical pipeline; invalid or malformed records are diverted into Kafka topic `parking-events-dlq` (Dead Letter Queue) for isolation and audit.

---

## 8. Repository Layout

```text
smart-parking-analytics/
├── .env.example                     # Environment template (Kafka, Influx, Spark)
├── docker-compose.yml               # Container orchestration (Kafka KRaft, InfluxDB, Grafana)
├── requirements.txt                 # Python dependencies
├── README.md                        # Primary academic documentation
├── DECISIONS.md                     # Architecture decision records (ADRs)
├── PROJECT_STATUS.md                # Phase completion tracking
│
├── config/                          # YAML configuration modules
│   ├── analytics.yaml               # InfluxDB and Grafana telemetry configuration
│   ├── kafka.yaml                   # Topic definitions, partition count, retention
│   ├── simulator.yaml               # Diurnal wave coefficients, lot capacities
│   └── spark.yaml                   # Shuffle partitions, trigger timing, watermarks
│
├── dashboards/                      # Grafana dashboards & datasources
│   └── grafana/
│       ├── dashboards/
│       │   └── smart_parking_overview.json
│       └── provisioning/
│           ├── dashboards/dashboard_config.yaml
│           └── datasources/influxdb.yaml
│
├── ingestion/                       # External ingestion & live producer
│   ├── live_api_producer.py         # Authenticated REST API producer with live fallback
│   ├── producer.py                  # Kafka Python producer client
│   └── validator.py                 # Ingestion schema validator
│
├── monitoring/                      # Health checks & benchmarks
│   ├── benchmark.py                 # Multi-rate load testing
│   └── health_check.py              # End-to-end service status probe
│
├── simulator/                       # Virtual IoT Sensor Simulator
│   ├── main.py                      # CLI entry point (normal, high_load, failure)
│   ├── generator.py                 # Slot-level transition generator
│   └── parking_lot.py               # 8 Bengaluru parking lot models
│
├── storage/                         # Time-series persistence
│   ├── influx_client.py             # Dual-schema line protocol REST client
│   ├── schema.py                    # Measurement schemas & Flux queries
│   └── setup.py                     # InfluxDB bucket initializer
│
├── streaming/                       # Spark Structured Streaming core engine
│   ├── main.py                      # Bootstrap, checkpointing, and execution graph
│   ├── consumer.py                  # Kafka stream source configuration
│   ├── deduplication.py             # Watermarked dropDuplicates
│   ├── event_time.py                # Watermark applicator & latency classifier
│   ├── stateful_tracker.py          # Micro-batch lot occupancy FSM
│   └── windowing.py                 # Tumbling & sliding window aggregators
│
└── tests/                           # 43 automated unit & integration tests
    ├── conftest.py
    ├── run_all_tests.py             # Master test suite runner
    ├── unit/
    ├── integration/
    └── performance/
```

---
