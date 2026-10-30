"""
streaming/analytics.py
======================
Analytical Rollups and Streaming Metrics for Smart Parking.
Course: AI372IA — Stream Processing and Analytics, RV College of Engineering.

Calculations:
- Zone-level occupancy aggregation
- System-wide occupancy and availability totals
- End-to-end and processing latency percentiles
- Streaming throughput metrics
"""

from __future__ import annotations

import logging
from typing import Dict, Optional

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger("streaming.analytics")


def calculate_zone_aggregates(df: DataFrame) -> DataFrame:
    """
    Rolls up parking occupancy statistics to the geographical zone level.
    (e.g., ZONE_SOUTH, ZONE_EAST, ZONE_NORTH, ZONE_CENTRAL).
    """
    return (
        df.groupBy("zone_id", "city")
        .agg(
            F.sum("occupied_slots").alias("zone_occupied_slots"),
            F.sum("available_slots").alias("zone_available_slots"),
            F.sum("capacity").alias("zone_capacity"),
            F.round(
                (F.sum("occupied_slots") / F.when(F.sum("capacity") == 0, 1).otherwise(F.sum("capacity"))) * 100.0,
                2,
            ).alias("zone_occupancy_percentage"),
        )
    )


def calculate_system_totals(df: DataFrame) -> DataFrame:
    """
    Calculates city-wide system totals across all parking lots.
    """
    return df.agg(
        F.sum("occupied_slots").alias("total_occupied_slots"),
        F.sum("available_slots").alias("total_available_slots"),
        F.sum("capacity").alias("total_capacity"),
        F.round(
            (F.sum("occupied_slots") / F.when(F.sum("capacity") == 0, 1).otherwise(F.sum("capacity"))) * 100.0,
            2,
        ).alias("overall_occupancy_percentage"),
        F.countDistinct("lot_id").alias("active_lots_count"),
    )


def compute_batch_performance_metrics(
    batch_df: pd.DataFrame,
    batch_duration_seconds: float,
) -> Dict[str, float]:
    """
    Computes performance and latency metrics for a micro-batch.

    Parameters
    ----------
    batch_df:
        Pandas DataFrame of processed events in the batch.
    batch_duration_seconds:
        Measured execution duration of this micro-batch.

    Returns
    -------
    Dictionary of performance metrics.
    """
    if batch_df.empty:
        return {
            "events_processed": 0.0,
            "events_per_second": 0.0,
            "batch_duration_ms": batch_duration_seconds * 1000.0,
            "avg_delay_ms": 0.0,
            "p50_delay_ms": 0.0,
            "p95_delay_ms": 0.0,
            "p99_delay_ms": 0.0,
        }

    count = len(batch_df)
    throughput = count / batch_duration_seconds if batch_duration_seconds > 0 else 0.0

    # Calculate processing delay if timestamps present
    delays_ms = []
    if "event_timestamp" in batch_df.columns and "ingestion_timestamp" in batch_df.columns:
        try:
            evt_time = pd.to_datetime(batch_df["event_timestamp"], utc=True)
            ing_time = pd.to_datetime(batch_df["ingestion_timestamp"], utc=True)
            diffs = (ing_time - evt_time).dt.total_seconds() * 1000.0
            delays_ms = diffs.dropna().tolist()
        except Exception as e:
            logger.debug("Failed computing timestamp diff: %s", e)

    p50 = float(pd.Series(delays_ms).quantile(0.50)) if delays_ms else 0.0
    p95 = float(pd.Series(delays_ms).quantile(0.95)) if delays_ms else 0.0
    p99 = float(pd.Series(delays_ms).quantile(0.99)) if delays_ms else 0.0
    avg_delay = float(pd.Series(delays_ms).mean()) if delays_ms else 0.0

    return {
        "events_processed": float(count),
        "events_per_second": round(throughput, 2),
        "batch_duration_ms": round(batch_duration_seconds * 1000.0, 2),
        "avg_delay_ms": round(avg_delay, 2),
        "p50_delay_ms": round(p50, 2),
        "p95_delay_ms": round(p95, 2),
        "p99_delay_ms": round(p99, 2),
    }
