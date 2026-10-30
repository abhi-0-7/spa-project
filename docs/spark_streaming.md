# Apache Spark Structured Streaming Engine
## Smart Parking Occupancy Analytics
**Course:** AI372IA — Stream Processing and Analytics, RV College of Engineering  

---

## 1. Structured Streaming Computation Model

Apache Spark Structured Streaming treats continuous real-time data streams not as discrete batch files, but as an **unbounded, continuously appended table**.

```
Event Stream (Kafka) ──> [ Micro-Batch Ingestion ] ──> Unbounded Input Table
                                                              │
                                                   Continuous Query (Triggers)
                                                              │
                                                              v
Result Table (Windows / State) ──> [ Sinks ] ──> InfluxDB / Kafka
```

---

## 2. Event-Time vs. Processing-Time Distinctions

In distributed stream processing, the clock used to evaluate time-series windows fundamentally determines correctness:

1. **Event Time (`event_timestamp`):** The exact UTC instant when the vehicle physically occupied or vacated the parking bay, as recorded by the IoT sensor clock.
2. **Ingestion Time (`ingestion_timestamp`):** The instant when the message was received and committed to Kafka log storage.
3. **Processing Time (`processing_timestamp`):** The wall-clock instant when the Spark executor core executed the transformation on the event.

### Why Event Time is Mandatory in Smart Parking:
Consider a vehicle exiting a parking slot at 09:14:55 UTC (`event_timestamp`). Due to network latency or sensor retry backoff, the message arrives at Kafka at 09:15:05 UTC (`ingestion_timestamp`) and Spark processes it at 09:15:10 UTC (`processing_timestamp`).
If processing-time windows were used, this exit would be incorrectly assigned to the [09:15 - 09:16) window. With event-time processing, Spark correctly assigns the event to the [09:14 - 09:15) window, preserving physical reality.

---

## 3. Watermarking and Late-Arriving Event Policy

To prevent unbounded state memory accumulation, Spark requires a **watermark** that defines how late an event can arrive before being discarded.

### Mathematical Definition:
$$\text{Watermark}(t) = \max(\text{event\_timestamp}) - \text{Allowed Lateness}$$

In this project, the watermark is configured via `config/spark.yaml` with a default of **2 minutes**:

```python
watermarked_df = df.withWatermark("event_timestamp", "2 minutes")
```

### Lateness Policy:
1. **On-Time Events:** Arrive before the watermark passes their event time. Fully incorporated into state and window aggregations.
2. **Late Events within Watermark (0 to 2 minutes late):** Accepted and used to update the active aggregation windows.
3. **Late Events Beyond Watermark (> 2 minutes late):** Dropped from the window state store to ensure state memory remains bounded.
4. **Monitoring:** All events are classified into `ON_TIME`, `SLIGHTLY_LATE`, or `VERY_LATE` tags to maintain visibility into sensor delays.

---

## 4. Correct Stateful Slot Occupancy Tracking

### The Critical Flaw of Naive Counting:
A naive approach calculates occupancy as:
$$\text{Occupancy} \neq \sum \text{OCCUPIED events} - \sum \text{FREE events}$$
This naive formula fails catastrophically under real IoT conditions because:
- Periodic heartbeat messages repeatedly send `OCCUPIED` for an already parked vehicle.
- Network retries re-transmit identical status events.
- Duplicate packets inflate the counters, driving occupancy above lot capacity or below zero.

### The Strict 2-State Finite State Machine (FSM):
Our `StatefulOccupancyTracker` (`streaming/state_manager.py`) tracks the **exact current state** of each physical slot:

| Prior Slot State | New Status Event | State Changed? | Occupancy Delta | Transition Type |
|---|---|---|---|---|
| `FREE` | `OCCUPIED` | **YES** | **+1** | `ENTRY` |
| `OCCUPIED` | `FREE` | **YES** | **-1** | `EXIT` |
| `FREE` | `FREE` | **NO** | **0** | No-op (heartbeat) |
| `OCCUPIED` | `OCCUPIED` | **NO** | **0** | No-op (heartbeat) |

### Invariant Equations Enforced:
$$\text{Occupancy}(t) = \min\Big(\text{Capacity},\, \max\big(0,\, \text{Occupancy}(t-1) + \text{Entries} - \text{Exits}\big)\Big)$$
$$\text{Availability}(t) = \text{Capacity} - \text{Occupancy}(t)$$

---

## 5. Windowing Implementations

### 5.1 Tumbling Windows (Fixed, Non-Overlapping)
- **1-Minute Tumbling:** High-resolution monitoring of arrival/departure velocity.
- **5-Minute Tumbling:** Medium-term occupancy trend baseline.
- **Spark Expression:**
  ```python
  df.groupBy(F.window("event_timestamp", "1 minute"), "lot_id", "zone_id")
  ```

### 5.2 Sliding Windows (Fixed Duration, Smooth Step)
- **5-Minute Window, 1-Minute Slide:** Produces an updated 5-minute rolling average every 60 seconds, eliminating artificial step boundaries.
- **10-Minute Window, 2-Minute Slide:** Long-range moving trend detector.
- **Spark Expression:**
  ```python
  df.groupBy(F.window("event_timestamp", "5 minutes", "1 minute"), "lot_id", "zone_id")
  ```

---

## 6. Deduplication Strategy

Duplicate events injected by sensor retries or network reflection are intercepted using Spark's event-time deduplication:

```python
deduped_df = df.withWatermark("event_timestamp", "10 minutes") \
               .dropDuplicates(["event_id"])
```
- Deduplication state is maintained in Spark's RocksDB / HDFS state store.
- The 10-minute watermark ensures that state for previously seen `event_id` keys is automatically evicted once it is impossible for a duplicate to arrive within the allowable lateness threshold.
