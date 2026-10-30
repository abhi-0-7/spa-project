"""
streaming/windowing.py
======================
Window-Based Aggregations for Apache Spark Structured Streaming.
Course: AI372IA — Stream Processing and Analytics, RV College of Engineering.

Academic Concepts Demonstrated:
--------------------------------
1. Tumbling Windows:
   - Fixed-size, non-overlapping, contiguous time intervals.
   - Each event belongs to exactly ONE window.
   - Example: [09:00 - 09:01), [09:01 - 09:02).

2. Sliding Windows:
   - Fixed-size, overlapping time intervals defined by (duration, slide).
   - An event can belong to MULTIPLE windows.
   - Smooths out short-term fluctuations and captures moving trends.
   - Example: 5-minute window with 1-minute slide produces windows
     [09:00 - 09:05), [09:01 - 09:06), [09:02 - 09:07).

3. Event-Time vs Processing-Time Windows:
   - Windows are constructed over `event_timestamp`, representing when the
     physical sensor detected the vehicle, not when Spark processed it.
"""

from __future__ import annotations

import logging
from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger("streaming.windowing")


def explain_window_difference() -> str:
    """
    Returns an academic explanation contrasting Tumbling and Sliding windows.
    Useful for system documentation, logging, and coursework report generation.
    """
    return (
        "=== WINDOWING COMPARISON IN STREAM PROCESSING ===\n"
        "1. TUMBLING WINDOWS:\n"
        "   - Non-overlapping, fixed-duration windows that partition event time.\n"
        "   - Each event is assigned to exactly one window.\n"
        "   - Good for disjoint batch-like summaries (e.g., hourly revenue, entries per minute).\n"
        "   - Low memory overhead since state is cleared immediately when watermark passes.\n\n"
        "2. SLIDING WINDOWS:\n"
        "   - Overlapping windows where duration > slide interval.\n"
        "   - An event is assigned to (duration / slide) concurrent windows.\n"
        "   - Good for moving averages, smoothing bursty traffic, and detecting sudden trends.\n"
        "   - Higher state memory overhead since multiple active windows overlap concurrently.\n"
    )


def apply_tumbling_window(
    df: DataFrame,
    window_duration: str = "1 minute",
    window_type_label: str = "TUMBLING_1MIN",
) -> DataFrame:
    """
    Apply a tumbling window aggregation on watermarked parking events.

    Parameters
    ----------
    df:
        Streaming DataFrame with watermarked `event_timestamp` column.
    window_duration:
        Duration of the tumbling window (e.g. '1 minute', '5 minutes').
    window_type_label:
        Descriptive label stored as a tag for time-series indexing.

    Returns
    -------
    Aggregated streaming DataFrame.
    """
    return (
        df.groupBy(
            F.window(F.col("event_timestamp"), window_duration),
            F.col("lot_id"),
            F.col("zone_id"),
            F.col("city"),
        )
        .agg(
            F.count(F.lit(1)).alias("event_count"),
            F.sum(F.when(F.col("status") == "OCCUPIED", 1).otherwise(0)).alias("entries"),
            F.sum(F.when(F.col("status") == "FREE", 1).otherwise(0)).alias("exits"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.lit(window_type_label).alias("window_type"),
            F.col("lot_id"),
            F.col("zone_id"),
            F.col("city"),
            F.col("event_count"),
            F.col("entries"),
            F.col("exits"),
            (F.col("entries") - F.col("exits")).alias("net_change"),
            F.round(F.col("event_count") / 60.0, 2).alias("events_per_second"),
        )
    )


def apply_sliding_window(
    df: DataFrame,
    window_duration: str = "5 minutes",
    slide_interval: str = "1 minute",
    window_type_label: str = "SLIDING_5MIN_1MIN",
) -> DataFrame:
    """
    Apply a sliding window aggregation on watermarked parking events.

    Parameters
    ----------
    df:
        Streaming DataFrame with watermarked `event_timestamp`.
    window_duration:
        Duration of the window (e.g. '5 minutes').
    slide_interval:
        Slide step duration (e.g. '1 minute').
    window_type_label:
        Descriptive label.

    Returns
    -------
    Aggregated streaming DataFrame.
    """
    return (
        df.groupBy(
            F.window(F.col("event_timestamp"), window_duration, slide_interval),
            F.col("lot_id"),
            F.col("zone_id"),
            F.col("city"),
        )
        .agg(
            F.count(F.lit(1)).alias("event_count"),
            F.sum(F.when(F.col("status") == "OCCUPIED", 1).otherwise(0)).alias("entries"),
            F.sum(F.when(F.col("status") == "FREE", 1).otherwise(0)).alias("exits"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.lit(window_type_label).alias("window_type"),
            F.col("lot_id"),
            F.col("zone_id"),
            F.col("city"),
            F.col("event_count"),
            F.col("entries"),
            F.col("exits"),
            (F.col("entries") - F.col("exits")).alias("net_change"),
            F.round(F.col("event_count") / 300.0, 2).alias("events_per_second"),
        )
    )


def create_all_window_aggregations(df: DataFrame) -> Dict[str, DataFrame]:
    """
    Creates all four window aggregation DataFrames specified in project requirements:
    1. 1-minute tumbling window
    2. 5-minute tumbling window
    3. 5-minute sliding window (1-min slide)
    4. 10-minute sliding window (2-min slide)
    """
    return {
        "tumbling_1min": apply_tumbling_window(
            df, window_duration="1 minute", window_type_label="TUMBLING_1MIN"
        ),
        "tumbling_5min": apply_tumbling_window(
            df, window_duration="5 minutes", window_type_label="TUMBLING_5MIN"
        ),
        "sliding_5min_1min": apply_sliding_window(
            df, window_duration="5 minutes", slide_interval="1 minute", window_type_label="SLIDING_5MIN_1MIN"
        ),
        "sliding_10min_2min": apply_sliding_window(
            df, window_duration="10 minutes", slide_interval="2 minutes", window_type_label="SLIDING_10MIN_2MIN"
        ),
    }
