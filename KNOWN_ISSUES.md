# KNOWN ISSUES — Smart Parking Occupancy Analytics
**Last Updated:** 2026-10-30T10:00:00+05:30

---

## KI-001: Docker Not in Sandbox PATH
**Severity:** High (blocks Phase 4)  
**Description:** Docker Desktop 4.56.0 is installed but the docker binary is not accessible in the sandbox environment PATH. Docker Compose commands cannot run from the automated agent.  
**Impact:** Kafka, InfluxDB, Grafana containers cannot be started autonomously.  
**Resolution:** User must start Docker Desktop manually and run `docker compose up -d`. Instructions provided in deployment docs.  
**Status:** OPEN — workaround documented.

## KI-002: Java 8 Compatibility With PySpark 3.5.1
**Severity:** Low  
**Description:** PySpark 3.5.1 supports Java 8 but Java 11+ is recommended for production use. Some advanced features (like specific GC algorithms) are Java 11+.  
**Impact:** None for this project's feature set.  
**Status:** KNOWN, acceptable.

## KI-003: Python 3.13 Not in PySpark's Official Support Matrix
**Severity:** Medium  
**Description:** PySpark 3.5.1 officially lists Python 3.8–3.12. Python 3.13.1 is installed. Pre-installed kafka-python and pyspark work, but edge cases may exist.  
**Impact:** Potential issues with some PySpark internal APIs. No issues encountered during initial testing.  
**Status:** MONITORING. Will document any failures.

## KI-004: spark-sql-kafka JAR Download Requires Internet
**Severity:** Medium  
**Description:** First execution of the Spark streaming job requires internet to download the Kafka connector JAR (~40 MB) from Maven Central.  
**Resolution:** After first download, JAR is cached in `~/.ivy2`. Offline operation possible thereafter.  
**Status:** KNOWN, documented in README.

## KI-005: Single Kafka Broker — No Replication
**Severity:** Low (academic context)  
**Description:** With one Kafka broker, replication factor must be 1. True HA/fault-tolerance for the broker itself is not achievable.  
**Impact:** Broker failure = data loss. Acceptable for laptop demonstration.  
**Status:** KNOWN, documented in limitations.md.

## KI-006: Spark Local Mode — Not True Distributed
**Severity:** Low (academic context)  
**Description:** Running Spark in local[*] mode demonstrates all stream-processing APIs but is not a distributed cluster.  
**Impact:** Cannot demonstrate multi-node failover. All other streaming concepts remain demonstrable.  
**Status:** KNOWN, documented in architecture.md.

## KI-007: influxdb-client and confluent-kafka Not Yet Installed
**Severity:** Medium  
**Description:** These packages need to be pip-installed before the storage and high-performance ingestion modules work.  
**Resolution:** Included in requirements.txt. `pip install -r requirements.txt` resolves this.  
**Status:** OPEN — will be resolved in Phase 1 setup.

## KI-008: OpenCity.in Dataset May Require Browser Download
**Severity:** Low  
**Description:** OpenCity.in may require account login or CAPTCHA to download Bengaluru parking datasets.  
**Resolution:** Manual download instructions provided. Project uses built-in Bengaluru lot definitions as fallback.  
**Status:** OPEN — to be investigated in Phase 2.
