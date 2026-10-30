# DECISIONS — Smart Parking Occupancy Analytics
**Last Updated:** 2026-10-30T10:00:00+05:30

---

## D-001: Python Version — Python 3.13.1 (not 3.10)
**Decision:** Use the system Python 3.13.1 instead of the requested 3.10.  
**Reason:** Python 3.10 is not installed. Python 3.13.1 is the installed interpreter. PySpark 3.5.1 supports Python 3.8–3.12 officially; Python 3.13 has been tested and works with PySpark 3.5.x in practice (kafka-python and pyspark are already installed and importable under 3.13).  
**Risk:** Minor API deprecation differences. No functional risk identified.  
**Mitigation:** All code tested under 3.13. Type hints use `from __future__ import annotations` for compatibility.

## D-002: Kafka Client — kafka-python 2.2.11 (not confluent-kafka)
**Decision:** Use the already-installed `kafka-python 2.2.11` as the primary Kafka client for the Python producer/consumer.  
**Reason:** kafka-python is already installed. confluent-kafka requires librdkafka C library compilation on Windows, which is complex.  
**Risk:** Lower throughput ceiling vs. confluent-kafka. Acceptable for a local academic project.  
**Mitigation:** The simulator's throughput is bounded by Spark processing anyway. kafka-python is thread-safe and sufficient for all target event rates.

## D-003: Spark Kafka Integration — spark-sql-kafka JAR
**Decision:** Use the `spark-sql-kafka-0-10` connector (Maven) with `--packages` flag.  
**Reason:** PySpark 3.5.1 reads Kafka via the structured streaming Kafka connector. This is the standard approach.  
**Version selected:** `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1`  
**Risk:** First run downloads ~40 MB. Requires internet for first execution. Cached in `~/.ivy2` after that.

## D-004: Java Version — OpenJDK 8u492
**Decision:** Use the installed OpenJDK 8 (Temurin build).  
**Reason:** Already installed. PySpark 3.5.1 supports Java 8.  
**Risk:** Java 8 is the minimum supported version. No functional risk.  
**Action:** JAVA_HOME must be set before running Spark.

## D-005: Docker — kafka-kraft Mode (No Zookeeper)
**Decision:** Use Kafka in KRaft mode (Zookeeper-less) via `bitnami/kafka:3.7` image.  
**Reason:** KRaft mode simplifies deployment to a single container, reduces RAM footprint (no Zookeeper JVM), and is the current production direction.  
**RAM savings:** Eliminates ~256 MB Zookeeper JVM.

## D-006: InfluxDB — Version 2.7 OSS
**Decision:** Use InfluxDB 2.7 OSS via Docker.  
**Reason:** InfluxDB 2.x provides the Flux query language, better Grafana integration, and the modern Python client (`influxdb-client`). Free OSS tier has no data limits.  
**Bucket:** `parking_analytics` with infinite retention for development, 30-day for production-like mode.

## D-007: Grafana — Version 10.x via Docker
**Decision:** Use `grafana/grafana:10.4.3` (LTS channel).  
**Reason:** Version 10 provides improved dashboard provisioning, native InfluxDB Flux datasource, and the stat panel improvements needed for the occupancy overview.

## D-008: Spark Deployment Mode — Local[*]
**Decision:** Run Spark in `local[*]` mode (all local CPU cores).  
**Reason:** A full Spark cluster (driver + workers via Docker) would consume 4-6 GB of RAM for overhead. Local mode uses the same Spark APIs and demonstrates all stream-processing concepts while being practical on 16 GB RAM.  
**Academic note:** This decision is documented and does not diminish the academic validity — Spark Structured Streaming APIs are identical in local and cluster mode.

## D-009: Occupancy State Management — updateStateByKey / flatMapGroupsWithState
**Decision:** Use `flatMapGroupsWithState` with `GroupStateTimeout.ProcessingTimeTimeout`.  
**Reason:** PySpark 3.5.1 supports `flatMapGroupsWithState`. This provides slot-level stateful processing with proper timeout handling. `transformWithState` (experimental in 3.5) is not used.  
**Alternative considered:** `applyInPandasWithState` — also supported but less idiomatic for streaming state.

## D-010: Watermark Policy — Configurable, Default 2 Minutes
**Decision:** Default watermark is 2 minutes allowed lateness. Configurable via `config/spark.yaml`.  
**Reason:** In a parking lot scenario, sensors transmit within seconds. A 2-minute allowance handles realistic network delays and brief outages while keeping state bounded.  
**Experimental values tested:** 30s, 1m, 2m, 5m.

## D-011: Kafka Partitioning — 4 Partitions, Key = lot_id
**Decision:** `raw-parking-events` topic uses 4 partitions. Producer key = `lot_id`.  
**Reason:** 4 partitions × 1 broker for development. lot_id key ensures all events for the same parking lot land on the same partition, preserving per-lot event order.  
**Limitation:** With only 1 broker, replication factor = 1. This is documented as a development limitation.

## D-012: Checkpoint Directory — Local Filesystem
**Decision:** Checkpoints stored in `./checkpoints/` relative to project root, mapped as a Docker volume.  
**Reason:** Simplest reliable approach on a single laptop. HDFS would add unnecessary complexity.

## D-013: Dataset Strategy
**Decision:** Synthetic simulator is the guaranteed primary data source.  
**Bengaluru data:** Use OpenCity.in parking dataset for geographical context (lot names, coordinates, zones). To be downloaded manually if browser authentication required.  
**Melbourne 2019:** Use as optional historical replay dataset. Does not replace Bengaluru context.  
**No data mixing:** Bengaluru geography + synthetic events. Historical Melbourne used only for replay experiments, clearly labeled.
