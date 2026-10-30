"""
streaming/main.py
=================
Apache Spark Structured Streaming Pipeline for Smart Parking Analytics.
Course: AI372IA — Stream Processing and Analytics, RV College of Engineering.

Academic Concepts Implemented:
1. Continuous Kafka Stream Ingestion
2. JSON Parsing and Schema Enforcement
3. Dead-Letter Queue (DLQ) Routing for Invalid Records
4. Watermarking and Late-Arriving Event Policy
5. Deduplication using Event IDs within Watermark Window
6. Stateful Slot-Level Occupancy Tracking (No Naive Counting)
7. Tumbling and Sliding Window Aggregations
8. Idempotent InfluxDB Time-Series Output Sink
9. Checkpointing and Interruption Recovery

Usage:
    python -m streaming.main
    python -m streaming.main --watermark "2 minutes" --trigger "10 seconds"
    python -m streaming.main --console-only
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from storage.influx_client import ParkingInfluxClient, create_client_from_env
from streaming.analytics import calculate_system_totals, calculate_zone_aggregates
from streaming.consumer import create_kafka_stream
from streaming.deduplication import deduplicate_events
from streaming.event_time import apply_watermark, classify_event_latency
from streaming.preprocessing import add_processing_metadata, flatten_event, parse_event_json
from streaming.sink import write_to_console, write_to_dlq, write_to_influxdb, write_to_kafka
from streaming.state_manager import StatefulOccupancyTracker
from streaming.windowing import create_all_window_aggregations

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("streaming.main")


def load_yaml(path: str | Path) -> dict:
    """Load configuration from a YAML file."""
    p = Path(path)
    if not p.is_file():
        return {}
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def parse_arguments() -> argparse.Namespace:
    """Parse CLI options for the Spark Structured Streaming application."""
    parser = argparse.ArgumentParser(
        description="Smart Parking Spark Structured Streaming Pipeline (AI372IA)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--watermark",
        type=str,
        default="2 minutes",
        help="Allowed event-time lateness watermark (e.g. '30 seconds', '2 minutes')",
    )
    parser.add_argument(
        "--trigger",
        type=str,
        default="10 seconds",
        help="Micro-batch trigger interval (e.g. '5 seconds', '10 seconds')",
    )
    parser.add_argument(
        "--checkpoint-dir",
        type=str,
        default="./checkpoints",
        help="Base directory for query checkpoints",
    )
    parser.add_argument(
        "--console-only",
        action="store_true",
        help="Output streaming results to console instead of InfluxDB (useful for testing)",
    )
    parser.add_argument(
        "--starting-offsets",
        type=str,
        default="earliest",
        help="Kafka starting offsets: 'earliest' or 'latest'",
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config/spark.yaml",
        help="Path to spark.yaml configuration",
    )
    return parser.parse_args()


def create_spark_session(app_name: str = "SmartParkingAnalytics") -> SparkSession:
    """
    Constructs and optimizes the SparkSession with Kafka package support,
    configured for single-laptop execution (conservative heap, 4 shuffle partitions).
    """
    # Ensure HADOOP_HOME and winutils path on Windows
    if os.path.isdir(r"C:\Hadoop\hadoop-3.3.6"):
        os.environ["HADOOP_HOME"] = r"C:\Hadoop\hadoop-3.3.6"
        os.environ["PATH"] = r"C:\Hadoop\hadoop-3.3.6\bin;" + os.environ.get("PATH", "")
    elif "HADOOP_HOME" in os.environ and not os.path.exists(os.environ["HADOOP_HOME"]):
        del os.environ["HADOOP_HOME"]

    # Ensure JAVA_HOME is configured
    if "JAVA_HOME" not in os.environ:
        cand_java = r"C:\Program Files\Eclipse Adoptium\jdk-8.0.492.9-hotspot"
        if os.path.isdir(cand_java):
            os.environ["JAVA_HOME"] = cand_java
            logger.info("Auto-configured JAVA_HOME=%s", cand_java)

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4040")
    )
    return builder.getOrCreate()


def main() -> None:
    args = parse_arguments()
    logger.info("Starting Smart Parking Streaming Engine [Watermark: %s | Trigger: %s]",
                args.watermark, args.trigger)

    base_dir = Path(__file__).resolve().parent.parent
    spark_cfg = load_yaml(base_dir / args.config)
    kafka_cfg = load_yaml(base_dir / "config" / "kafka.yaml")
    sim_cfg = load_yaml(base_dir / "config" / "simulator.yaml")

    # Extract parking lot capacities for bounds enforcement
    lot_capacities: Dict[str, int] = {}
    for lot in sim_cfg.get("parking_lots", []):
        lot_capacities[lot["lot_id"]] = int(lot.get("capacity", 100))

    # Initialize Spark
    spark = create_spark_session(app_name=spark_cfg.get("application", {}).get("name", "SmartParkingAnalytics"))
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession ready (Version: %s | Master: local[*])", spark.version)

    # 1. Ingest Raw Stream from Kafka
    bootstrap_servers = kafka_cfg.get("bootstrap_servers", "localhost:9092")
    raw_topic = kafka_cfg.get("topics", {}).get("raw_events", {}).get("name", "raw-parking-events")
    dlq_topic = kafka_cfg.get("topics", {}).get("dlq", {}).get("name", "parking-events-dlq")
    agg_topic = kafka_cfg.get("topics", {}).get("aggregated_stats", {}).get("name", "aggregated-occupancy-stats")

    consumer_config = {
        "kafka": {
            "bootstrap_servers": bootstrap_servers,
            "topic": raw_topic,
            "starting_offsets": args.starting_offsets,
            "max_offsets_per_trigger": 10000,
            "fail_on_data_loss": "false",
        },
        "bootstrap_servers": bootstrap_servers,
        "topic": raw_topic,
        "starting_offsets": args.starting_offsets,
        "max_offsets_per_trigger": 10000,
        "fail_on_data_loss": "false",
    }

    try:
        raw_stream = create_kafka_stream(spark, consumer_config)
    except Exception as exc:
        logger.error("Failed to initialize Kafka stream (%s). Check if Kafka broker is active.", exc)
        sys.exit(1)

    # 2. Deserialization & Preprocessing
    parsed_stream = parse_event_json(raw_stream)
    flattened_stream = flatten_event(parsed_stream)
    enriched_stream = add_processing_metadata(flattened_stream)

    # 3. Schema Validation & DLQ Splitting
    valid_condition = (
        F.col("event_id").isNotNull()
        & F.col("lot_id").isNotNull()
        & F.col("slot_id").isNotNull()
        & F.col("status").isin("OCCUPIED", "FREE")
        & F.col("event_timestamp").isNotNull()
    )

    valid_stream = enriched_stream.filter(valid_condition)
    invalid_stream = enriched_stream.filter(~valid_condition)

    # 4. Deduplication
    deduped_stream = deduplicate_events(valid_stream, watermark_duration="10 minutes")

    # 5. Event-Time Watermarking
    watermarked_stream = apply_watermark(deduped_stream, lateness=args.watermark)
    classified_stream = classify_event_latency(watermarked_stream)

    # 6. Window Aggregations
    windows = create_all_window_aggregations(classified_stream)
    tumbling_1m = windows["tumbling_1min"]
    sliding_5m_1m = windows["sliding_5min_1min"]

    # 7. Stateful Occupancy Tracker
    state_tracker = StatefulOccupancyTracker(capacity_lookup=lot_capacities)
    influx_client = create_client_from_env()

    # Define queries list
    active_queries = []
    checkpoint_base = Path(args.checkpoint_dir)
    checkpoint_base.mkdir(parents=True, exist_ok=True)

    # DLQ Sink: route malformed events to parking-events-dlq
    try:
        dlq_q = write_to_dlq(
            df=invalid_stream,
            bootstrap_servers=bootstrap_servers,
            dlq_topic=dlq_topic,
            checkpoint_location=str(checkpoint_base / "dlq"),
            trigger_interval=args.trigger,
        )
        active_queries.append(dlq_q)
        logger.info("DLQ query active (Topic: %s)", dlq_topic)
    except Exception as e:
        logger.warning("Could not start DLQ Kafka sink: %s", e)

    # Console Output or InfluxDB Output
    if args.console_only:
        logger.info("Running in console-only mode.")
        q1 = write_to_console(
            df=tumbling_1m,
            query_name="console_tumbling_1m",
            trigger_interval=args.trigger,
            num_rows=10,
        )
        active_queries.append(q1)
    else:
        # InfluxDB Sinks:
        # A. Window Analytics Sink (Tumbling 1-minute)
        q_window = write_to_influxdb(
            df=tumbling_1m,
            influx_client=influx_client,
            measurement="parking_window_analytics",
            tag_columns=["lot_id", "zone_id", "city", "window_type"],
            field_columns=["event_count", "entries", "exits", "net_change", "events_per_second"],
            time_column="window_start",
            checkpoint_location=str(checkpoint_base / "influx_window_1m"),
            trigger_interval=args.trigger,
            query_name="influx_window_1m",
        )
        active_queries.append(q_window)

        # B. Micro-batch Stateful Occupancy & Performance Sink
        def foreach_batch_occupancy_handler(batch_df: DataFrame, batch_id: int) -> None:
            if batch_df.isEmpty():
                return

            t0 = time.perf_counter()
            pdf = batch_df.toPandas()

            # Process state transitions deterministically
            state_pdf = state_tracker.process_micro_batch(pdf)

            # Write updated lot occupancy snapshots to InfluxDB
            if not state_pdf.empty:
                influx_client.write_dataframe_batch(
                    df=state_pdf,
                    measurement="parking_occupancy",
                    tag_columns=["lot_id", "zone_id", "city"],
                    field_columns=["occupied_slots", "available_slots", "capacity",
                                   "occupancy_percentage", "batch_entries", "batch_exits",
                                   "total_entries", "total_exits"],
                    time_column="timestamp",
                )

            # Write stream performance telemetry
            duration_s = max(0.001, time.perf_counter() - t0)
            throughput = len(pdf) / duration_s
            influx_client.write_performance_metric(
                events_per_second=throughput,
                processing_delay_ms=duration_s * 1000.0,
                batch_duration_ms=duration_s * 1000.0,
                invalid_count=0,
                duplicate_count=0,
                late_count=0,
                total_processed=len(pdf),
                kafka_lag=0,
                timestamp=datetime.now(),
            )
            logger.info("Batch %d: Processed %d events in %.2fs (%.1f ev/s)",
                        batch_id, len(pdf), duration_s, throughput)

        q_state = (
            classified_stream.writeStream
            .queryName("stateful_occupancy_sink")
            .trigger(processingTime=args.trigger)
            .option("checkpointLocation", str(checkpoint_base / "stateful_occupancy"))
            .foreachBatch(foreach_batch_occupancy_handler)
            .start()
        )
        active_queries.append(q_state)
        logger.info("Stateful occupancy & window queries active and writing to InfluxDB.")

    # Graceful shutdown handling
    def stop_handler(sig, frame):
        logger.info("Stopping all Spark streaming queries...")
        for q in active_queries:
            try:
                q.stop()
            except Exception:
                pass
        spark.stop()
        logger.info("Spark streaming cleanly shut down.")
        sys.exit(0)

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)

    logger.info("Streaming pipeline active. Awaiting new events from Kafka (Ctrl+C to stop)...")
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        stop_handler(None, None)


if __name__ == "__main__":
    main()
