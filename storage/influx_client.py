"""
storage/influx_client.py
========================
InfluxDB 2.7 Client for Smart Parking Analytics.
Course: AI372IA — Stream Processing and Analytics, RV College of Engineering.

Handles time-series writes for:
- parking_occupancy
- parking_transitions
- parking_window_analytics
- stream_performance

Features:
- Primary support via `influxdb_client` SDK if installed.
- Zero-dependency HTTP fallback via `requests` (Line Protocol) if SDK is absent.
- Guaranteed idempotent upserts using UTC event timestamps.
- Exponential backoff retry handling on transient failures.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, List, Optional

import pandas as pd
import requests

logger = logging.getLogger("storage.influx_client")

# Attempt SDK import, fallback to HTTP REST line-protocol mode
try:
    from influxdb_client import InfluxDBClient, Point, WritePrecision
    from influxdb_client.client.exceptions import InfluxDBError
    from influxdb_client.client.write_api import SYNCHRONOUS, WriteOptions
    _HAS_INFLUX_SDK = True
except ImportError:
    _HAS_INFLUX_SDK = False

    class WritePrecision:
        NANOSECONDS = "ns"
        SECONDS = "s"

    class InfluxDBError(Exception):
        pass

    class Point:
        """Lightweight Line Protocol Point shim when influxdb-client is absent."""
        def __init__(self, measurement_name: str) -> None:
            self.measurement = measurement_name
            self.tags: dict[str, str] = {}
            self.fields: dict[str, Any] = {}
            self.time_ns: Optional[int] = None

        def tag(self, key: str, value: str) -> Point:
            self.tags[str(key)] = str(value).replace(" ", "\\ ").replace(",", "\\,")
            return self

        def field(self, key: str, value: Any) -> Point:
            self.fields[str(key)] = value
            return self

        def time(self, timestamp: Any, write_precision: str = "ns") -> Point:
            if isinstance(timestamp, (int, float)):
                self.time_ns = int(timestamp)
            elif isinstance(timestamp, datetime):
                self.time_ns = int(timestamp.timestamp() * 1_000_000_000)
            elif isinstance(timestamp, pd.Timestamp):
                self.time_ns = int(timestamp.value)
            return self

        def to_line_protocol(self) -> str:
            tags_str = ""
            if self.tags:
                sorted_tags = sorted(self.tags.items())
                tags_str = "," + ",".join(f"{k}={v}" for k, v in sorted_tags)

            fields_list = []
            for k, v in self.fields.items():
                if isinstance(v, (int,)):
                    fields_list.append(f"{k}={v}i")
                elif isinstance(v, (float,)):
                    fields_list.append(f"{k}={v}")
                elif isinstance(v, bool):
                    fields_list.append(f"{k}={'true' if v else 'false'}")
                else:
                    escaped = str(v).replace('"', '\\"')
                    fields_list.append(f'{k}="{escaped}"')
            fields_str = ",".join(fields_list)

            time_str = f" {self.time_ns}" if self.time_ns else ""
            return f"{self.measurement}{tags_str} {fields_str}{time_str}"


def _to_epoch_ns(dt: datetime) -> int:
    """Convert datetime to nanoseconds since UNIX epoch."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000_000)


@dataclass
class InfluxDBConfig:
    """Configuration for InfluxDB 2.7 connection."""
    url: str = "http://localhost:8086"
    token: str = "local-parking-token"
    org: str = "rvce"
    bucket: str = "parking"
    batch_size: int = 500
    flush_interval_ms: int = 1_000
    max_retries: int = 3


class ParkingInfluxClient:
    """
    High-performance InfluxDB client supporting both native SDK
    and lightweight HTTP line-protocol fallback.
    """

    def __init__(self, config: InfluxDBConfig) -> None:
        self._config = config
        self._writes_succeeded: int = 0
        self._writes_failed: int = 0
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Token {config.token}",
            "Content-Type": "text/plain; charset=utf-8",
            "Accept": "application/json",
        })

        if _HAS_INFLUX_SDK:
            try:
                self._sdk_client = InfluxDBClient(
                    url=config.url,
                    token=config.token,
                    org=config.org,
                )
                self._sdk_write_api = self._sdk_client.write_api(write_options=SYNCHRONOUS)
                logger.info("ParkingInfluxClient initialized using native influxdb-client SDK")
            except Exception as e:
                logger.warning("Failed initializing native SDK (%s); using HTTP REST fallback", e)
                self._sdk_client = None
                self._sdk_write_api = None
        else:
            self._sdk_client = None
            self._sdk_write_api = None
            logger.info("ParkingInfluxClient initialized using HTTP REST Line Protocol engine")

    @property
    def writes_succeeded(self) -> int:
        return self._writes_succeeded

    @property
    def writes_failed(self) -> int:
        return self._writes_failed

    def ping(self) -> bool:
        """Health check verifying InfluxDB responsiveness."""
        try:
            resp = self._session.get(f"{self._config.url.rstrip('/')}/health", timeout=3)
            return resp.status_code == 200
        except Exception as exc:
            logger.debug("Ping to InfluxDB failed: %s", exc)
            return False

    def close(self) -> None:
        """Close client sessions cleanly."""
        try:
            if self._sdk_client is not None:
                self._sdk_client.close()
            self._session.close()
        except Exception as e:
            logger.debug("Error closing InfluxDB client: %s", e)

    def __enter__(self) -> ParkingInfluxClient:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def _write_http_lines(self, lines: List[str]) -> bool:
        """Post line-protocol records directly to InfluxDB v2 write API."""
        if not lines:
            return True

        body = "\n".join(lines)
        url = f"{self._config.url.rstrip('/')}/api/v2/write"
        params = {
            "org": self._config.org,
            "bucket": self._config.bucket,
            "precision": "ns",
        }

        for attempt in range(self._config.max_retries + 1):
            try:
                resp = self._session.post(url, params=params, data=body.encode("utf-8"), timeout=5)
                if resp.status_code in (200, 204):
                    self._writes_succeeded += len(lines)
                    return True
                else:
                    logger.warning("InfluxDB write returned status %d: %s", resp.status_code, resp.text)
            except Exception as exc:
                delay = 2 ** attempt
                if attempt < self._config.max_retries:
                    logger.debug("Write retry %d in %ds (%s)", attempt + 1, delay, exc)
                    time.sleep(delay)
                else:
                    logger.error("InfluxDB write failed after %d retries: %s", self._config.max_retries, exc)

        self._writes_failed += len(lines)
        return False

    def write_occupancy(
        self,
        lot_id: str,
        zone_id: str,
        city: str,
        occupied_slots: int,
        available_slots: int,
        capacity: int,
        occupancy_pct: float,
        timestamp: datetime,
        source: str = "kafka-spark",
    ) -> bool:
        """Write parking_occupancy snapshot point."""
        point = (
            Point("parking_occupancy")
            .tag("lot_id", lot_id)
            .tag("zone_id", zone_id)
            .tag("city", city)
            .tag("source", source)
            .field("occupied_slots", int(occupied_slots))
            .field("available_slots", int(available_slots))
            .field("capacity", int(capacity))
            .field("occupancy_percentage", float(occupancy_pct))
            .time(timestamp, WritePrecision.NANOSECONDS)
        )
        return self.write_points([point])

    def write_transition(
        self,
        lot_id: str,
        zone_id: str,
        city: str,
        entries: int,
        exits: int,
        net_change: int,
        timestamp: datetime,
    ) -> bool:
        """Write parking_transitions delta point."""
        point = (
            Point("parking_transitions")
            .tag("lot_id", lot_id)
            .tag("zone_id", zone_id)
            .tag("city", city)
            .field("entries", int(entries))
            .field("exits", int(exits))
            .field("net_change", int(net_change))
            .time(timestamp, WritePrecision.NANOSECONDS)
        )
        return self.write_points([point])

    def write_window_analytics(
        self,
        lot_id: str,
        zone_id: str,
        city: str,
        window_type: str,
        window_start: datetime,
        window_end: datetime,
        occupied_slots: int,
        available_slots: int,
        capacity: int,
        occupancy_pct: float,
        entries: int,
        exits: int,
        event_count: int,
        avg_occ_pct: float,
        max_occ_pct: float,
        min_occ_pct: float,
        timestamp: datetime,
    ) -> bool:
        """Write parking_window_analytics metric point."""
        point = (
            Point("parking_window_analytics")
            .tag("lot_id", lot_id)
            .tag("zone_id", zone_id)
            .tag("city", city)
            .tag("window_type", window_type)
            .field("occupied_slots", int(occupied_slots))
            .field("available_slots", int(available_slots))
            .field("capacity", int(capacity))
            .field("occupancy_percentage", float(occupancy_pct))
            .field("entries", int(entries))
            .field("exits", int(exits))
            .field("event_count", int(event_count))
            .field("avg_occupancy_pct", float(avg_occ_pct))
            .field("max_occupancy_pct", float(max_occ_pct))
            .field("min_occupancy_pct", float(min_occ_pct))
            .time(timestamp, WritePrecision.NANOSECONDS)
        )
        return self.write_points([point])

    def write_performance_metric(
        self,
        events_per_second: float,
        processing_delay_ms: float,
        batch_duration_ms: float,
        invalid_count: int,
        duplicate_count: int,
        late_count: int,
        total_processed: int,
        kafka_lag: int,
        timestamp: datetime,
    ) -> bool:
        """Write pipeline stream_performance telemetry point."""
        point = (
            Point("stream_performance")
            .tag("source", "spark-streaming")
            .field("events_per_second", float(events_per_second))
            .field("processing_delay_ms", float(processing_delay_ms))
            .field("batch_duration_ms", float(batch_duration_ms))
            .field("invalid_event_count", int(invalid_count))
            .field("duplicate_event_count", int(duplicate_count))
            .field("late_event_count", int(late_count))
            .field("total_events_processed", int(total_processed))
            .field("kafka_consumer_lag", int(kafka_lag))
            .time(timestamp, WritePrecision.NANOSECONDS)
        )
        return self.write_points([point])

    def write_processing_telemetry(
        self,
        events_per_second: float,
        latency_ms: float,
        batch_duration_ms: float,
        input_rows: int,
        invalid_count: int = 0,
        duplicate_count: int = 0,
        late_count: int = 0,
        timestamp: Optional[datetime] = None,
    ) -> bool:
        """Write parking_processing measurement for Grafana dashboard compatibility."""
        ts = timestamp or datetime.now()
        point = (
            Point("parking_processing")
            .tag("pipeline", "spark_streaming")
            .field("events_per_second", float(events_per_second))
            .field("average_event_latency_ms", float(latency_ms))
            .field("batch_duration_ms", float(batch_duration_ms))
            .field("input_rows", int(input_rows))
            .field("invalid_events", int(invalid_count))
            .field("duplicate_events", int(duplicate_count))
            .field("late_events", int(late_count))
            .field("checkpoint_configured", 1)
            .time(ts, WritePrecision.NANOSECONDS)
        )
        return self.write_points([point])

    def write_points(self, points: List[Point]) -> bool:
        """Write a list of Point objects."""
        if not points:
            return True

        if self._sdk_write_api is not None:
            try:
                self._sdk_write_api.write(
                    bucket=self._config.bucket,
                    org=self._config.org,
                    record=points,
                )
                self._writes_succeeded += len(points)
                return True
            except Exception as e:
                logger.debug("SDK write failed (%s); attempting HTTP line-protocol fallback", e)

        # HTTP line protocol fallback
        lines = []
        for p in points:
            if hasattr(p, "to_line_protocol"):
                lines.append(p.to_line_protocol())
            else:
                lines.append(str(p))
        return self._write_http_lines(lines)

    def write_dataframe_batch(
        self,
        df: pd.DataFrame,
        measurement: str,
        tag_columns: list[str],
        field_columns: list[str],
        time_column: str,
    ) -> bool:
        """
        Primary sink method for Spark Structured Streaming's `foreachBatch`.
        Converts a Pandas DataFrame directly to line-protocol points.
        """
        if df.empty:
            return True

        points: list[Point] = []
        for _, row in df.iterrows():
            try:
                p = Point(measurement)
                for tag in tag_columns:
                    if tag in row and pd.notna(row[tag]):
                        p.tag(tag, str(row[tag]))
                for f in field_columns:
                    if f in row and pd.notna(row[f]):
                        val = row[f]
                        if isinstance(val, (int,)):
                            p.field(f, int(val))
                        else:
                            p.field(f, float(val))
                ts = row[time_column]
                p.time(ts, WritePrecision.NANOSECONDS)
                points.append(p)
            except Exception as exc:
                logger.debug("Error converting row to point: %s", exc)

        return self.write_points(points)


def create_client_from_env() -> ParkingInfluxClient:
    """Instantiate ParkingInfluxClient using environment variables."""
    config = InfluxDBConfig(
        url=os.getenv("INFLUXDB_URL", "http://localhost:8086"),
        token=os.getenv("INFLUXDB_TOKEN", "local-parking-token"),
        org=os.getenv("INFLUXDB_ORG", "rvce"),
        bucket=os.getenv("INFLUXDB_BUCKET", "parking"),
        batch_size=int(os.getenv("INFLUXDB_BATCH_SIZE", "500")),
        flush_interval_ms=int(os.getenv("INFLUXDB_FLUSH_INTERVAL_MS", "1000")),
        max_retries=int(os.getenv("INFLUXDB_MAX_RETRIES", "3")),
    )
    return ParkingInfluxClient(config)
