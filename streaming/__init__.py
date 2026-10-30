"""
streaming — Spark Structured Streaming module for Smart Parking Occupancy Analytics.

This package provides a complete real-time stream-processing pipeline built on
Apache Spark Structured Streaming and Apache Kafka.  It covers every stage of
the data journey from raw sensor events to InfluxDB-ready occupancy metrics:

Sub-modules
-----------
consumer        : Kafka → Spark streaming DataFrame bridge.
preprocessing   : JSON parsing, schema validation, metadata enrichment.
deduplication   : Event-level deduplication using Spark watermarks.
event_time      : Watermark application and latency classification.
state_manager   : Stateful per-slot occupancy tracking (flatMapGroupsWithState).
windowing       : Tumbling and sliding window aggregations.
analytics       : Zone / system / throughput aggregation helpers.
sink            : Output sinks — InfluxDB, Kafka, DLQ, console.
main            : Application entry point with CLI argument parsing.

Course  : AI372IA — Stream Processing & Analytics
College : RV College of Engineering, Bengaluru
"""
