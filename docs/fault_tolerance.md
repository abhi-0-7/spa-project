# Fault Tolerance and Interruption Recovery
## Smart Parking Occupancy Analytics
**Course:** AI372IA — Stream Processing and Analytics, RV College of Engineering  

---

## 1. Fault Tolerance Guarantees by Pipeline Stage

Achieving end-to-end reliability requires understanding the specific delivery and recovery guarantees implemented at each layer of the pipeline:

| Pipeline Stage | Implementation Mechanism | Delivery Guarantee | Failure Semantics |
|---|---|---|---|
| **Simulator ──> Kafka** | `ingestion/producer.py` | At-Least-Once | `acks='all'`, retries=3, backoff=500ms. If broker restarts, producer retries until buffer timeout. |
| **Kafka Log Storage** | `bitnami/kafka:3.7.0` | At-Least-Once Durability | KRaft commit log with fsync. Committed offsets and records persist across container restarts. |
| **Kafka ──> Spark** | `streaming/consumer.py` | At-Least-Once | Spark tracks committed partition offsets in its checkpoint metadata directory (`./checkpoints/`). |
| **Spark Deduplication** | `streaming/deduplication.py` | Effectively Exactly-Once | `dropDuplicates(["event_id"])` with a 10-minute watermark drops identical replayed packets. |
| **Stateful Tracking** | `streaming/state_manager.py` | Deterministic State | Strict two-state FSM prevents double-counting redundant or duplicate transitions. |
| **Spark ──> InfluxDB** | `storage/influx_client.py` | At-Least-Once with Idempotency | Each point is written with its original event UTC timestamp. Repeated micro-batches result in upserts rather than duplicate rows. |

---

## 2. Checkpointing Mechanics in Structured Streaming

Spark Structured Streaming relies on write-ahead offset logs and state store snapshots written to `./checkpoints/`.

### Checkpoint Directory Layout:
```
checkpoints/
├── offsets/             # Monotonically increasing micro-batch offset commits (e.g., 0, 1, 2)
├── commits/             # Acknowledgement marks indicating micro-batch completion
├── metadata/            # Query ID and engine initialization metadata
└── sources/0/           # Kafka consumer group offset high-water marks per partition
```

### The Exact Recovery Sequence:
When the Spark streaming process is killed (e.g., via `Ctrl+C`, `SIGTERM`, or crash) and subsequently restarted:
1. **Engine Initialization:** Spark reads the latest completed batch integer `N` from `commits/`.
2. **Offset Rewind:** Spark queries `offsets/N` to discover the exact Kafka partition offsets consumed during the last successful batch.
3. **Log Re-alignment:** Spark re-establishes connections to `raw-parking-events` at the exact recorded offsets, ensuring that no messages are skipped.
4. **State Restoration:** In-memory slot state and window accumulators are re-hydrated from the state store before processing resumption.

---

## 3. Empirical Failure Experiments

### Experiment 1: Abrupt Spark Process Termination and Restart
- **Procedure:** Start the simulator emitting at 50 events/sec. Start Spark streaming. Allow 5 micro-batches to commit to InfluxDB. Terminate the Spark process abruptly using `SIGINT`. Keep simulator running for 30 seconds. Restart Spark using `python -m streaming.main`.
- **Expected Result:** Spark reads offsets from `./checkpoints/`, resumes consumption of all buffered Kafka events that accumulated during the outage, and catches up to real-time.
- **Observed Result:** Processing resumes immediately. Consumer lag drains within 1–2 micro-batches. No duplicate occupancy points appear in Grafana due to timestamped upserts.

### Experiment 2: Duplicate Event Injection
- **Procedure:** Configure simulator with `--mode fault_injection` (`duplicate_probability: 0.10`).
- **Expected Result:** Duplicate events carrying the same `event_id` are intercepted by `dropDuplicates`. Redundant status events that bypass dedup are filtered by the slot FSM.
- **Observed Result:** Occupancy graphs remain flat and bounded. Counter drift is zero.

### Experiment 3: Out-of-Order and Late Event Injection
- **Procedure:** Inject events with timestamps delayed by 60s, 120s, and 240s.
- **Expected Result:** Events delayed by 60s (< 2-minute watermark) are integrated into the [t-1m, t) window. Events delayed by 240s (> 2-minute watermark) are dropped by the watermarking engine.
- **Observed Result:** Bounded state memory is maintained.

---

## 4. Reset and Clean-Slate Instructions

To cleanly reset checkpoints and start from a fresh state:
```powershell
python scripts/reset.py --force
```
*Note: This deletes `./checkpoints/` and runtime logs without altering InfluxDB data unless `--clear-data` is specified.*
