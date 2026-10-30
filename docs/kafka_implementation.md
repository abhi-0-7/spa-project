# Apache Kafka Implementation Guide
## Smart Parking Occupancy Analytics
**Course:** AI372IA — Stream Processing and Analytics, RV College of Engineering  

---

## 1. Why Apache Kafka in Stream Processing?

In stream processing architectures, an event streaming broker acts as the durable, decoupled backbone separating event producers (IoT sensors, gateways) from downstream stream processors (Apache Spark).

### Key Architectural Benefits:
1. **Backpressure Decoupling:** When Spark is processing micro-batches or undergoing temporary gc pauses, the simulator can continue emitting events at high rates without packet loss because Kafka buffers records durably on disk.
2. **Replayability:** Unlike traditional message queues that drop messages upon acknowledgement, Kafka's commit-log architecture allows consumer groups to replay historical streams by rewinding offsets.
3. **Partition-Level Parallelism:** Multiple Spark executor tasks can consume different partitions in parallel without cross-thread locking.

---

## 2. Topic Architecture and Configuration

The system provisions three distinct Kafka topics configured in `config/kafka.yaml` and deployed via `docker-compose.yml`:

```
+-----------------------------------------------------------------------------------+
| TOPIC 1: raw-parking-events                                                       |
|   - Purpose: Ingestion of all raw simulated and historical parking events        |
|   - Partitions: 4                                                                 |
|   - Replication Factor: 1 (single-node development broker)                       |
|   - Retention Policy: 24 hours (86,400,000 ms)                                    |
|   - Keying Strategy: Key = lot_id (Preserves strict ordering per parking lot)     |
+-----------------------------------------------------------------------------------+

+-----------------------------------------------------------------------------------+
| TOPIC 2: aggregated-occupancy-stats                                               |
|   - Purpose: Processed downstream analytical metrics emitted by Spark             |
|   - Partitions: 4                                                                 |
|   - Replication Factor: 1                                                         |
|   - Retention Policy: 7 days (604,800,000 ms)                                     |
|   - Keying Strategy: Key = lot_id                                                 |
+-----------------------------------------------------------------------------------+

+-----------------------------------------------------------------------------------+
| TOPIC 3: parking-events-dlq                                                        |
|   - Purpose: Dead Letter Queue (DLQ) for malformed, unparseable, or rejected events|
|   - Partitions: 2                                                                 |
|   - Replication Factor: 1                                                         |
|   - Retention Policy: 7 days                                                      |
|   - Content: Serialized error descriptions and original raw payloads              |
+-----------------------------------------------------------------------------------+
```

---

## 3. Partitioning Strategy and Ordering Guarantees

### Keying by `lot_id`
All events are published with `key = event["lot_id"]`.

- **Kafka Invariant:** Kafka guarantees strict FIFO ordering **within an individual partition**, but does **not** guarantee total global ordering across different partitions.
- **Why `lot_id` was chosen:** In parking analytics, transitions for the same physical parking slot (e.g. `SLOT_001` in `LOT_MG_ROAD_01`) must arrive in chronological order so that the state machine receives `FREE -> OCCUPIED -> FREE` sequentially. Keying by `lot_id` guarantees that all events for a lot hash to the same partition, preserving per-lot state transition order.
- **Academic Distinction:** Partitioning by `lot_id` does **not** automatically partition by geographical zone. Multi-lot zone aggregation (e.g., rolling up `LOT_KORAMANGALA_01` and `LOT_JAYANAGAR_01` into `ZONE_SOUTH`) is explicitly performed by Spark Structured Streaming's shuffle engine.

---

## 4. Producer Guarantees and Failure Semantics

The Python producer (`ingestion/producer.py`) implements the following resilience settings:

```python
producer_config = {
    "acks": "all",               # Wait for all in-sync replicas before returning success
    "retries": 3,                 # Retries transient network/broker hiccups
    "retry_backoff_ms": 500,      # Exponential backoff between retries
    "compression_type": "gzip",   # Compresses message batches over the wire
    "linger_ms": 5,               # 5ms micro-batch delay to maximize throughput
    "max_in_flight_requests_per_connection": 1, # Prevents out-of-order retries
}
```

---

## 5. KRaft Mode Architecture (Zookeeper-less)

The deployment uses Apache Kafka 3.7 running in **KRaft** (Kafka Raft Metadata) mode via the `bitnami/kafka:3.7.0` container:
- **No Zookeeper Dependency:** Eliminates a separate Zookeeper JVM container, reducing memory overhead by ~256 MB on the 16 GB laptop.
- **Faster Controller Failover:** Metadata is stored in a replicated internal Kafka topic rather than external Zookeeper znodes, reducing partition leader re-election latency to sub-second intervals.
