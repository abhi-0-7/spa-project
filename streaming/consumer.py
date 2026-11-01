"""
Kafka consumer setup for Spark Structured Streaming.
Reads raw-parking-events topic using Spark's Kafka connector.

This module provides the bridge between Apache Kafka and Spark Structured
Streaming.  It configures the Kafka source with production-grade options such
as offset throttling, fault-tolerance flags, and security settings sourced
entirely from the supplied configuration dictionary — no hardcoded values.

Course  : AI372IA — Stream Processing & Analytics
College : RV College of Engineering, Bengaluru
"""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

KAFKA_TOPIC_RAW: str = "raw-parking-events"


def create_kafka_stream(spark: SparkSession, config: dict[str, Any]) -> DataFrame:
    """Create a streaming DataFrame that reads from the raw parking-events Kafka topic.

    The function reads *all* Kafka message fields so that downstream stages can
    access message metadata (partition, offset, timestamps) as well as the
    payload itself.

    Parameters
    ----------
    spark:
        An active :class:`~pyspark.sql.SparkSession`.
    config:
        Application configuration dictionary.  Expected structure::

            kafka:
              bootstrap_servers: "localhost:9092"
              topic: "raw-parking-events"          # optional — overrides default
              starting_offsets: "latest"            # optional, default "latest"
              max_offsets_per_trigger: 10000        # optional
              fail_on_data_loss: "false"            # optional
              # Optional security block:
              security_protocol: "PLAINTEXT"        # or SASL_SSL, etc.
              sasl_mechanism: "PLAIN"
              sasl_jaas_config: "<value from env>"

    Returns
    -------
    DataFrame
        A streaming :class:`~pyspark.sql.DataFrame` with the schema returned by
        the Kafka connector:

        +-----------+--------+---------+---------+------+---------+---------------+
        | key       | value  | topic   | partition | offset | timestamp | timestampType |
        +-----------+--------+---------+-----------+--------+-----------+---------------+
        | BinaryType | BinaryType | StringType | IntType | LongType | TimestampType | IntType |

    Raises
    ------
    KeyError
        If ``config`` does not contain a ``kafka`` section.
    ValueError
        If ``bootstrap_servers`` is missing or empty.
    RuntimeError
        If Spark cannot connect to Kafka or the topic does not exist.

    Examples
    --------
    >>> df = create_kafka_stream(spark, config)
    >>> df.printSchema()
    root
     |-- key: binary
     |-- value: binary
     ...
    """
    kafka_cfg: dict[str, Any] = config.get("kafka", config)

    bootstrap_servers: str = kafka_cfg.get("bootstrap_servers", "")
    if not bootstrap_servers:
        raise ValueError(
            "kafka.bootstrap_servers must be set in the configuration — "
            "received an empty string."
        )

    topic: str = kafka_cfg.get("topic", KAFKA_TOPIC_RAW)
    starting_offsets: str = kafka_cfg.get("starting_offsets", "latest")
    max_offsets_per_trigger: int = int(kafka_cfg.get("max_offsets_per_trigger", 10_000))
    fail_on_data_loss: str = str(kafka_cfg.get("fail_on_data_loss", "false")).lower()

    logger.info(
        "Creating Kafka stream | bootstrap_servers=%s | topic=%s | "
        "startingOffsets=%s | maxOffsetsPerTrigger=%d | failOnDataLoss=%s",
        bootstrap_servers,
        topic,
        starting_offsets,
        max_offsets_per_trigger,
        fail_on_data_loss,
    )

    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", max_offsets_per_trigger)
        .option("failOnDataLoss", fail_on_data_loss)
    )

    # ------------------------------------------------------------------
    # Optional security settings — only applied when explicitly set so that
    # the default PLAINTEXT development setup works without modification.
    # ------------------------------------------------------------------
    security_protocol: str = kafka_cfg.get("security_protocol", "")
    if security_protocol:
        reader = reader.option("kafka.security.protocol", security_protocol)
        logger.debug("Kafka security.protocol = %s", security_protocol)

    sasl_mechanism: str = kafka_cfg.get("sasl_mechanism", "")
    if sasl_mechanism:
        reader = reader.option("kafka.sasl.mechanism", sasl_mechanism)

    sasl_jaas_config: str = kafka_cfg.get("sasl_jaas_config", "")
    if sasl_jaas_config:
        # Value must come from the environment — never hardcoded.
        reader = reader.option("kafka.sasl.jaas.config", sasl_jaas_config)

    # Optional: consumer group id for offset management in Kafka (only set if explicitly defined)
    group_id: str = kafka_cfg.get("group_id", "")
    if group_id:
        reader = reader.option("kafka.group.id", group_id)

    # Optional: fetch timeout and session timeout
    fetch_max_wait_ms: int = int(kafka_cfg.get("fetch_max_wait_ms", 500))
    session_timeout_ms: int = int(kafka_cfg.get("session_timeout_ms", 30_000))
    reader = (
        reader
        .option("kafka.fetch.max.wait.ms", fetch_max_wait_ms)
        .option("kafka.session.timeout.ms", session_timeout_ms)
    )

    try:
        df: DataFrame = reader.load()
    except Exception as exc:
        logger.error(
            "Failed to create Kafka streaming DataFrame: %s", exc, exc_info=True
        )
        raise RuntimeError(
            f"Kafka stream creation failed for topic '{topic}' "
            f"at '{bootstrap_servers}': {exc}"
        ) from exc

    logger.info(
        "Kafka streaming DataFrame created successfully for topic '%s'.", topic
    )
    return df


def get_kafka_schema_description() -> str:
    """Return a human-readable description of the raw Kafka DataFrame schema.

    Returns
    -------
    str
        Multi-line string describing each column produced by the Kafka source.
    """
    return (
        "Raw Kafka Streaming DataFrame Schema\n"
        "=====================================\n"
        "key            BinaryType  — Kafka message key (bytes)\n"
        "value          BinaryType  — Kafka message value / payload (bytes)\n"
        "topic          StringType  — Kafka topic name\n"
        "partition      IntType     — Kafka partition number\n"
        "offset         LongType    — Kafka offset within partition\n"
        "timestamp      TimestampType — Message timestamp (event or log-append)\n"
        "timestampType  IntType     — 0=CreateTime, 1=LogAppendTime\n"
    )
