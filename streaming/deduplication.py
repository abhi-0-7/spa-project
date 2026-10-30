"""
Deduplication — event-level deduplication and repeated-status detection.

Apache Kafka's at-least-once delivery guarantee means the same parking-sensor
event can arrive multiple times.  This module uses Spark Structured Streaming's
built-in stateful ``dropDuplicates`` combined with an event-time *watermark* to
bound the in-memory deduplication state.

Key design decisions
--------------------
* Deduplication key : ``event_id`` (a UUID assigned by the sensor gateway).
* Watermark         : Controls how long Spark retains state for late arrivals.
  Events older than ``event_timestamp - watermark`` are assumed to be safe to
  drop from the deduplication hash table.
* ``detect_repeated_status`` adds a monitoring flag but does NOT perform any
  filtering — that decision belongs to the stateful occupancy tracker.

Course  : AI372IA — Stream Processing & Analytics
College : RV College of Engineering, Bengaluru
"""

from __future__ import annotations

import logging
from typing import Final

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)

# Default watermark used when the caller does not specify one.
DEFAULT_WATERMARK: Final[str] = "10 minutes"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def deduplicate_events(
    df: DataFrame,
    watermark_duration: str = DEFAULT_WATERMARK,
) -> DataFrame:
    """Remove duplicate parking events using event-time watermarking.

    Spark Structured Streaming's ``dropDuplicates`` is a stateful operator that
    keeps a hash set of seen ``event_id`` values.  The watermark bounds the
    size of that state: once the event-time watermark advances past a given
    window, Spark can safely evict old ``event_id`` entries from the hash set.

    .. important::
        ``withWatermark`` **must** be called *before* ``dropDuplicates`` for
        the state to be bounded.  Calling them in the wrong order causes Spark
        to maintain unbounded state, eventually leading to OOM errors in
        long-running jobs.

    Parameters
    ----------
    df:
        Streaming DataFrame containing at minimum ``event_id`` (StringType)
        and ``event_timestamp`` (TimestampType) columns.
    watermark_duration:
        How long past the current watermark Spark keeps deduplication state.
        Examples: ``"10 minutes"``, ``"1 hour"``.  Defaults to
        ``"10 minutes"``.

    Returns
    -------
    DataFrame
        Deduplicated streaming DataFrame.  Late duplicate events (arriving
        after the watermark) are silently dropped — this is the correct
        behaviour for parking sensor data.

    Raises
    ------
    ValueError
        If ``event_id`` or ``event_timestamp`` columns are missing.

    Examples
    --------
    >>> deduped = deduplicate_events(flat_df, watermark_duration="5 minutes")
    """
    _require_columns(df, ["event_id", "event_timestamp"], "deduplicate_events")

    logger.info(
        "Applying deduplication on event_id with watermark='%s'.",
        watermark_duration,
    )

    deduped: DataFrame = (
        df
        .withWatermark("event_timestamp", watermark_duration)
        .dropDuplicates(["event_id"])
    )

    logger.debug("Deduplication stage configured successfully.")
    return deduped


def detect_repeated_status(df: DataFrame) -> DataFrame:
    """Add a boolean flag indicating whether a slot's status is unchanged.

    A *repeated status* event occurs when the same ``slot_id`` emits the same
    ``status`` value in two consecutive events.  For example, receiving two
    back-to-back ``OCCUPIED`` events for the same slot suggests either:

    * A sensor glitch / duplicate transmission (should already be caught by
      :func:`deduplicate_events`), OR
    * A legitimate double-park or sensor re-initialisation.

    This function adds an ``is_repeated_status`` boolean column for monitoring
    dashboards.  It does **not** filter or drop any rows — the decision about
    how to handle repeated-status events is left to the stateful occupancy
    tracker in :mod:`streaming.state_manager`.

    .. note::
        This operation uses a streaming-compatible window ordered by
        ``event_timestamp`` within each ``slot_id`` partition.  In Structured
        Streaming, ``Window`` functions over unbounded ranges are only allowed
        in complete-output-mode aggregations.  For the append/update modes used
        in the main pipeline, repeated-status detection is approximated using
        the ``lag`` window function with a bounded window.

    Parameters
    ----------
    df:
        Streaming DataFrame with ``slot_id``, ``status``, and
        ``event_timestamp`` columns.

    Returns
    -------
    DataFrame
        Input DataFrame with an additional ``is_repeated_status`` column
        (``BooleanType``).

    Raises
    ------
    ValueError
        If required columns are missing.
    """
    _require_columns(df, ["slot_id", "status", "event_timestamp"], "detect_repeated_status")

    logger.debug("Adding is_repeated_status monitoring column.")

    # In Structured Streaming, unbounded window functions are not directly
    # supported in append/update modes.  We use a bounded row-frame window
    # ordered by event_timestamp to retrieve the immediately preceding event
    # for the same slot.
    slot_window = (
        Window.partitionBy("slot_id")
        .orderBy("event_timestamp")
        .rowsBetween(-1, -1)   # only the single row immediately before current
    )

    result: DataFrame = df.withColumn(
        "is_repeated_status",
        F.col("status") == F.lag("status", 1).over(slot_window),
    )

    # Null arises for the first event of each slot (no previous row) — treat
    # it as non-repeated (False).
    result = result.withColumn(
        "is_repeated_status",
        F.coalesce(F.col("is_repeated_status"), F.lit(False)),
    )

    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _require_columns(df: DataFrame, columns: list[str], stage: str) -> None:
    """Raise ValueError if any of *columns* are absent from *df*.

    Parameters
    ----------
    df:
        DataFrame to validate.
    columns:
        List of expected column names.
    stage:
        Human-readable name of the calling stage (for error messages).

    Raises
    ------
    ValueError
        If one or more columns are missing.
    """
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(
            f"[{stage}] Missing required columns: {missing}. "
            f"Available: {df.columns}"
        )
