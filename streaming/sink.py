"""
streaming/sink.py
=================
Streaming Output Sinks for Apache Spark Structured Streaming.
Course: AI372IA — Stream Processing and Analytics, RV College of Engineering.

Sinks provided:
1. write_to_influxdb:
   Uses `foreachBatch` to atomically write micro-batches to InfluxDB with
   retries, idempotent timestamps, and error recovery.

2. write_to_kafka:
   Publishes aggregated statistics to `aggregated-occupancy-stats` topic.

3. write_to_dlq:
   Routes schema-invalid or quarantined records to `parking-events-dlq`.

4. write_to_console:
   Diagnostic console sink for live debugging and testing.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from storage.influx_client import ParkingInfluxClient

logger = logging.getLogger("streaming.sink")


def write_to_influxdb(
    df: DataFrame,
    influx_client: ParkingInfluxClient,
    measurement: str,
    tag_columns: List[str],
    field_columns: List[str],
    time_column: str,
    checkpoint_location: str,
    trigger_interval: str = "10 seconds",
    query_name: Optional[str] = None,
) -> StreamingQuery:
    """
    Writes a streaming DataFrame to InfluxDB using foreachBatch.

    Each micro-batch is converted to a Pandas DataFrame in the driver,
    mapped to InfluxDB Line Protocol points, and written with retries.
    Idempotency is preserved by preserving event-time timestamps.
    """
    q_name = query_name or f"influx_sink_{measurement}"

    def write_batch_handler(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        row_count = batch_df.count()
        logger.debug("Writing micro-batch %d (%d rows) to InfluxDB measurement: %s",
                     batch_id, row_count, measurement)

        try:
            pdf = batch_df.toPandas()
            success = influx_client.write_dataframe_batch(
                df=pdf,
                measurement=measurement,
                tag_columns=tag_columns,
                field_columns=field_columns,
                time_column=time_column,
            )
            if success:
                logger.debug("Batch %d committed successfully to InfluxDB", batch_id)
            else:
                logger.warning("Batch %d failed to write to InfluxDB", batch_id)
        except Exception as exc:
            logger.error("Exception in batch %d write to InfluxDB: %s", batch_id, exc)

    return (
        df.writeStream
        .queryName(q_name)
        .trigger(processingTime=trigger_interval)
        .option("checkpointLocation", checkpoint_location)
        .foreachBatch(write_batch_handler)
        .start()
    )


def write_to_kafka(
    df: DataFrame,
    bootstrap_servers: str,
    topic: str,
    checkpoint_location: str,
    key_column: Optional[str] = "lot_id",
    trigger_interval: str = "10 seconds",
    query_name: str = "kafka_sink",
) -> StreamingQuery:
    """
    Writes a streaming DataFrame to an Apache Kafka topic.
    Serializes records to JSON in the 'value' column.
    """
    # Prepare Kafka key and JSON payload
    out_df = df.select(
        F.col(key_column).cast("string").alias("key") if key_column else F.lit(None).cast("string").alias("key"),
        F.to_json(F.struct("*")).alias("value"),
    )

    return (
        out_df.writeStream
        .queryName(query_name)
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", topic)
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime=trigger_interval)
        .start()
    )


def write_to_dlq(
    df: DataFrame,
    bootstrap_servers: str,
    dlq_topic: str = "parking-events-dlq",
    checkpoint_location: str = "./checkpoints/dlq_sink",
    trigger_interval: str = "10 seconds",
) -> StreamingQuery:
    """
    Writes malformed or unparseable events to the Dead Letter Queue Kafka topic.
    """
    return write_to_kafka(
        df=df,
        bootstrap_servers=bootstrap_servers,
        topic=dlq_topic,
        checkpoint_location=checkpoint_location,
        key_column="lot_id" if "lot_id" in df.columns else None,
        trigger_interval=trigger_interval,
        query_name="dlq_sink",
    )


def write_to_console(
    df: DataFrame,
    query_name: str = "console_sink",
    trigger_interval: str = "10 seconds",
    num_rows: int = 20,
    truncate: bool = False,
    output_mode: str = "update",
) -> StreamingQuery:
    """
    Diagnostic console sink displaying live streaming rows in stdout.
    """
    return (
        df.writeStream
        .queryName(query_name)
        .format("console")
        .outputMode(output_mode)
        .option("numRows", num_rows)
        .option("truncate", truncate)
        .trigger(processingTime=trigger_interval)
        .start()
    )
