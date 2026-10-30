"""
Event-time utilities — watermark application, latency classification,
and time-partitioning columns.

Apache Spark Structured Streaming uses *event time* (the timestamp embedded
in the event itself) rather than *processing time* for window semantics.
This module centralises every transformation that relates to time so that
the rest of the pipeline can treat timestamps as an implementation detail.

Key concepts
------------
Watermark
    Tells Spark how late it should wait for straggler events before finalising
    a time window.  Setting it too small loses late data; setting it too large
    increases memory pressure.

Latency category
    Classifies each event by how far behind it arrived relative to its
    embedded ``event_timestamp``.  Used in monitoring dashboards.

Time partitioning
    Derived integer / date columns that Parquet / InfluxDB sinks can use for
    efficient querying without re-parsing timestamps.

IST offset
    Indian Standard Time is UTC + 5 hours 30 minutes (19 800 seconds).

Course  : AI372IA — Stream Processing & Analytics
College : RV College of Engineering, Bengaluru
"""

from __future__ import annotations

import logging
from typing import Final

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, LongType

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_IST_OFFSET_SECONDS: Final[int] = 19_800  # UTC + 5h 30m

# Latency thresholds (seconds)
_ON_TIME_MAX_S: Final[int] = 30
_SLIGHTLY_LATE_MAX_S: Final[int] = 120  # 2 minutes

# Labels
_LABEL_ON_TIME: Final[str] = "ON_TIME"
_LABEL_SLIGHTLY_LATE: Final[str] = "SLIGHTLY_LATE"
_LABEL_VERY_LATE: Final[str] = "VERY_LATE"

# Default watermark if caller passes an empty string
DEFAULT_WATERMARK: Final[str] = "2 minutes"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def apply_watermark(
    df: DataFrame,
    event_timestamp_col: str = "event_timestamp",
    lateness: str = DEFAULT_WATERMARK,
) -> DataFrame:
    """Apply a Spark event-time watermark to the streaming DataFrame.

    The watermark instructs the Spark planner to wait up to *lateness* beyond
    the current maximum event time before closing a window.  After this point,
    any events for that window are dropped.

    This is the **single authoritative place** where ``withWatermark`` is
    called in the main pipeline.  All downstream window / stateful operators
    should receive the watermarked DataFrame returned by this function.

    Parameters
    ----------
    df:
        Streaming DataFrame with an event-time column.
    event_timestamp_col:
        Name of the event-time column (must be ``TimestampType``).
        Defaults to ``"event_timestamp"``.
    lateness:
        Spark duration string describing how late events may be, e.g.
        ``"2 minutes"``, ``"10 minutes"``, ``"1 hour"``.

    Returns
    -------
    DataFrame
        The input DataFrame with a watermark applied.

    Raises
    ------
    ValueError
        If *event_timestamp_col* is absent from *df*.

    Examples
    --------
    >>> wm_df = apply_watermark(flat_df, lateness="5 minutes")
    """
    if event_timestamp_col not in df.columns:
        raise ValueError(
            f"Column '{event_timestamp_col}' not found in DataFrame. "
            f"Available columns: {df.columns}"
        )

    logger.info(
        "Applying watermark on '%s' with allowedLateness='%s'.",
        event_timestamp_col,
        lateness,
    )

    return df.withWatermark(event_timestamp_col, lateness)


def classify_event_latency(df: DataFrame) -> DataFrame:
    """Add a ``latency_category`` column based on event-vs-ingestion delay.

    Classifies events into three tiers:

    ============== ========================================
    Category       Condition
    ============== ========================================
    ``ON_TIME``    ``processing_delay_ms < 30 000`` ms
    ``SLIGHTLY_LATE`` ``30 000 ≤ delay < 120 000`` ms
    ``VERY_LATE``  ``delay ≥ 120 000`` ms
    ============== ========================================

    The delay is computed between ``ingestion_timestamp`` (when the event
    arrived at the Kafka broker / gateway) and ``event_timestamp`` (when the
    sensor actually observed the parking state change).

    .. note::
        If ``processing_delay_ms`` is already present (added by
        :func:`~streaming.preprocessing.add_processing_metadata`), this
        function reuses it.  Otherwise it computes the delay from
        ``ingestion_timestamp`` and ``event_timestamp``.

    Parameters
    ----------
    df:
        DataFrame with ``event_timestamp`` and either
        ``processing_delay_ms`` or ``ingestion_timestamp``.

    Returns
    -------
    DataFrame
        Input DataFrame with ``latency_category`` (``StringType``) appended.

    Raises
    ------
    ValueError
        If neither delay source column is available.
    """
    required_fallback = ["event_timestamp", "ingestion_timestamp"]

    if "processing_delay_ms" in df.columns:
        delay_col = F.col("processing_delay_ms")
        logger.debug("Using existing processing_delay_ms for latency classification.")
    elif all(c in df.columns for c in required_fallback):
        logger.debug("Computing latency from ingestion_timestamp − event_timestamp.")
        delay_col = (
            F.col("ingestion_timestamp").cast(LongType())
            - F.col("event_timestamp").cast(LongType())
        ) * F.lit(1000)
    else:
        raise ValueError(
            "classify_event_latency requires either 'processing_delay_ms' "
            "or both 'event_timestamp' and 'ingestion_timestamp' columns. "
            f"Available: {df.columns}"
        )

    # Convert ms to seconds for threshold comparison
    delay_s = delay_col / F.lit(1000.0)

    latency_category = (
        F.when(delay_s < F.lit(_ON_TIME_MAX_S), F.lit(_LABEL_ON_TIME))
        .when(delay_s < F.lit(_SLIGHTLY_LATE_MAX_S), F.lit(_LABEL_SLIGHTLY_LATE))
        .otherwise(F.lit(_LABEL_VERY_LATE))
    )

    return df.withColumn("latency_category", latency_category)


def add_time_partitioning_columns(df: DataFrame) -> DataFrame:
    """Derive temporal partition columns from ``event_timestamp``.

    Adds integer and date columns useful for:

    * InfluxDB tag-based querying (``event_hour``, ``event_day_of_week``).
    * Parquet partition pruning (``event_date``).
    * IST-aligned dashboards (``ist_hour``).

    Column descriptions
    -------------------
    ``event_hour``       — UTC hour of the event (0–23), ``IntegerType``.
    ``event_day_of_week``— Day of week, Sunday=1 … Saturday=7, ``IntegerType``.
    ``event_date``       — Calendar date in UTC, ``DateType``.
    ``ist_hour``         — Hour in Indian Standard Time (UTC+5:30), ``IntegerType``.

    Parameters
    ----------
    df:
        DataFrame with ``event_timestamp`` (``TimestampType``).

    Returns
    -------
    DataFrame
        Input DataFrame with four additional columns appended.

    Raises
    ------
    ValueError
        If ``event_timestamp`` is absent.
    """
    if "event_timestamp" not in df.columns:
        raise ValueError(
            "add_time_partitioning_columns requires 'event_timestamp'. "
            f"Available: {df.columns}"
        )

    logger.debug("Adding time-partitioning columns (UTC + IST).")

    # IST timestamp = event_timestamp + 19 800 seconds
    ist_ts = F.col("event_timestamp") + F.expr(f"INTERVAL {_IST_OFFSET_SECONDS} SECONDS")

    result = (
        df
        .withColumn(
            "event_hour",
            F.hour(F.col("event_timestamp")).cast(IntegerType()),
        )
        .withColumn(
            "event_day_of_week",
            F.dayofweek(F.col("event_timestamp")).cast(IntegerType()),
        )
        .withColumn(
            "event_date",
            F.to_date(F.col("event_timestamp")).cast(DateType()),
        )
        .withColumn(
            "ist_hour",
            F.hour(ist_ts).cast(IntegerType()),
        )
    )

    return result
