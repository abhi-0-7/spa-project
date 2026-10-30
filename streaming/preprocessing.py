"""
Preprocessing — JSON parsing, schema validation, and metadata enrichment.

Transforms raw Kafka binary messages into structured, typed DataFrames ready
for downstream stateful processing and analytics.

Pipeline stages
---------------
1. ``parse_event_json``   — Decode bytes → JSON struct using the parking-event schema.
2. ``flatten_event``      — Promote nested struct fields to top-level columns.
3. ``add_processing_metadata`` — Append processing timestamps and delay metrics.

Course  : AI372IA — Stream Processing & Analytics
College : RV College of Engineering, Bengaluru
"""

from __future__ import annotations

import logging
from typing import Final

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema definition for a single raw parking event
# ---------------------------------------------------------------------------

PARKING_EVENT_SCHEMA: Final[StructType] = StructType(
    [
        StructField("event_id", StringType(), nullable=True),
        StructField("lot_id", StringType(), nullable=True),
        StructField("slot_id", StringType(), nullable=True),
        StructField("zone_id", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),           # "OCCUPIED" | "FREE"
        StructField("event_timestamp", StringType(), nullable=True),  # ISO-8601 string
        StructField("ingestion_timestamp", StringType(), nullable=True),
        StructField("source", StringType(), nullable=True),
        StructField("event_sequence", LongType(), nullable=True),
        StructField("schema_version", StringType(), nullable=True),
        StructField("latitude", DoubleType(), nullable=True),
        StructField("longitude", DoubleType(), nullable=True),
        StructField("lot_name", StringType(), nullable=True),
    ]
)

# Latency threshold constants (seconds)
_ON_TIME_THRESHOLD_S: Final[int] = 30
_SLIGHTLY_LATE_THRESHOLD_S: Final[int] = 120  # 2 minutes


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse_event_json(
    df: DataFrame,
    schema: StructType = PARKING_EVENT_SCHEMA,
) -> DataFrame:
    """Parse the Kafka value (bytes) as a JSON-encoded parking event.

    The raw Kafka value column is a ``BinaryType``.  This function:

    1. Casts the binary ``value`` column to ``StringType`` (UTF-8).
    2. Applies :func:`~pyspark.sql.functions.from_json` with the supplied
       ``schema`` to materialise a struct column ``parsed_event``.
    3. Retains ``raw_value`` (string) and ``kafka_timestamp`` for auditing.

    Events that do not match the schema produce ``null`` fields inside
    ``parsed_event`` rather than raising an exception, enabling downstream
    DLQ routing.

    Parameters
    ----------
    df:
        Raw streaming DataFrame from the Kafka source (contains ``value``
        and ``timestamp`` binary/timestamp columns).
    schema:
        :class:`~pyspark.sql.types.StructType` describing the JSON payload.
        Defaults to :data:`PARKING_EVENT_SCHEMA`.

    Returns
    -------
    DataFrame
        Streaming DataFrame with additional columns:

        * ``parsed_event`` — ``StructType`` matching ``schema``
        * ``raw_value``    — ``StringType`` raw JSON string
        * ``kafka_timestamp`` — ``TimestampType`` Kafka message timestamp

    Raises
    ------
    ValueError
        If ``df`` does not contain a ``value`` column.
    """
    if "value" not in df.columns:
        raise ValueError(
            "Input DataFrame must contain a 'value' column (raw Kafka payload)."
        )

    logger.debug("Parsing Kafka binary payload as JSON with %d schema fields.", len(schema))

    result = (
        df
        .withColumn("raw_value", F.col("value").cast(StringType()))
        .withColumn("parsed_event", F.from_json(F.col("raw_value"), schema))
        .withColumn("kafka_timestamp", F.col("timestamp").cast(TimestampType()))
    )

    logger.debug("JSON parsing stage applied — columns: %s", result.columns)
    return result


def flatten_event(df: DataFrame) -> DataFrame:
    """Flatten the nested ``parsed_event`` struct into top-level typed columns.

    Takes the ``parsed_event`` struct produced by :func:`parse_event_json` and
    promotes each sub-field to a first-class column with explicit casts:

    * ``event_timestamp`` and ``ingestion_timestamp`` are converted from ISO-8601
      strings to ``TimestampType`` using Spark's ``to_timestamp`` with the
      standard ISO format ``yyyy-MM-dd'T'HH:mm:ss``  (and a fallback to
      ``yyyy-MM-dd'T'HH:mm:ssZ`` for timezone-aware strings).
    * Numeric fields retain their schema-defined types.

    Parameters
    ----------
    df:
        DataFrame containing a ``parsed_event`` struct column (output of
        :func:`parse_event_json`).

    Returns
    -------
    DataFrame
        Streaming DataFrame with the following top-level columns added:

        ``event_id``, ``lot_id``, ``slot_id``, ``zone_id``, ``city``,
        ``status``, ``event_timestamp``, ``ingestion_timestamp``, ``source``,
        ``event_sequence``, ``schema_version``, ``latitude``, ``longitude``,
        ``lot_name``.

    Raises
    ------
    ValueError
        If ``df`` does not contain a ``parsed_event`` column.
    """
    if "parsed_event" not in df.columns:
        raise ValueError(
            "Input DataFrame must contain a 'parsed_event' struct column. "
            "Call parse_event_json() first."
        )

    logger.debug("Flattening parsed_event struct to top-level columns.")

    pe = F.col("parsed_event")

    # ISO-8601 timestamp parsing supporting variable fractions and timezones
    def _ts(col_expr: "Column") -> "Column":  # noqa: F821
        """Coerce an ISO-8601 string column to TimestampType."""
        return F.coalesce(
            col_expr.cast(TimestampType()),
            F.to_timestamp(col_expr, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"),
            F.to_timestamp(col_expr, "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
            F.to_timestamp(col_expr, "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(col_expr, "yyyy-MM-dd'T'HH:mm:ss"),
        )

    result = (
        df
        .withColumn("event_id",            pe.getField("event_id"))
        .withColumn("lot_id",              pe.getField("lot_id"))
        .withColumn("slot_id",             pe.getField("slot_id"))
        .withColumn("zone_id",             pe.getField("zone_id"))
        .withColumn("city",                pe.getField("city"))
        .withColumn("status",              F.upper(pe.getField("status")))
        .withColumn("event_timestamp",     _ts(pe.getField("event_timestamp")))
        .withColumn("ingestion_timestamp", _ts(pe.getField("ingestion_timestamp")))
        .withColumn("source",              pe.getField("source"))
        .withColumn("event_sequence",      pe.getField("event_sequence").cast(LongType()))
        .withColumn("schema_version",      pe.getField("schema_version"))
        .withColumn("latitude",            pe.getField("latitude").cast(DoubleType()))
        .withColumn("longitude",           pe.getField("longitude").cast(DoubleType()))
        .withColumn("lot_name",            pe.getField("lot_name"))
    )

    logger.debug("Flatten stage complete — columns: %s", result.columns)
    return result


def add_processing_metadata(df: DataFrame) -> DataFrame:
    """Enrich the DataFrame with processing-side timestamps and delay metrics.

    Appends two columns that are useful for latency monitoring dashboards:

    * ``processing_timestamp`` — wall-clock time when Spark processed the row,
      obtained via :func:`~pyspark.sql.functions.current_timestamp`.
    * ``processing_delay_ms``  — difference (milliseconds) between
      ``processing_timestamp`` and ``event_timestamp``.  A large positive value
      indicates late events or a slow consumer.

    Parameters
    ----------
    df:
        DataFrame containing ``event_timestamp`` (``TimestampType``).

    Returns
    -------
    DataFrame
        Input DataFrame with ``processing_timestamp`` and
        ``processing_delay_ms`` columns appended.

    Raises
    ------
    ValueError
        If ``event_timestamp`` column is absent.
    """
    if "event_timestamp" not in df.columns:
        raise ValueError(
            "Input DataFrame must contain an 'event_timestamp' column. "
            "Call flatten_event() first."
        )

    logger.debug("Adding processing metadata columns.")

    result = (
        df
        .withColumn("processing_timestamp", F.current_timestamp())
        .withColumn(
            "processing_delay_ms",
            (
                F.col("processing_timestamp").cast(LongType())
                - F.col("event_timestamp").cast(LongType())
            ) * F.lit(1000),
        )
    )

    return result


def build_validation_filter() -> "Column":  # noqa: F821
    """Return a Column expression that is ``True`` for valid parking events.

    A valid event satisfies ALL of:

    * ``event_id``        is not null
    * ``lot_id``          is not null
    * ``slot_id``         is not null
    * ``status``          is one of ``{"OCCUPIED", "FREE"}``
    * ``event_timestamp`` is not null

    Use this in :meth:`~pyspark.sql.DataFrame.filter` to split a stream into
    valid and invalid (DLQ-bound) branches.

    Returns
    -------
    Column
        Boolean Spark column expression.
    """
    valid_statuses = ["OCCUPIED", "FREE"]
    return (
        F.col("event_id").isNotNull()
        & F.col("lot_id").isNotNull()
        & F.col("slot_id").isNotNull()
        & F.col("status").isin(valid_statuses)
        & F.col("event_timestamp").isNotNull()
    )
