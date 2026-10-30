"""
ingestion/historical_replay.py — Historical Dataset Replay
===========================================================
Smart Parking Occupancy Analytics · AI372IA
RV College of Engineering, Bengaluru

Replays a historical parking occupancy CSV (e.g. Melbourne City Council Open
Data) through the ingestion pipeline at a configurable event rate, converting
each row into the canonical parking event schema v1.0.

Design notes
------------
* City is preserved as the **actual** source city from the dataset (Melbourne),
  NOT mapped to Bengaluru.  The ``source`` field is set to
  ``"HISTORICAL_REPLAY"`` so downstream consumers can distinguish replayed
  data from live sensor streams.
* Rate control is implemented with a token-bucket approach using
  ``time.perf_counter`` to minimise drift across the replay session.
* The replay loop respects a ``threading.Event`` stop signal so it can be
  halted cleanly from a parent thread or a signal handler.
* ``pandas`` is used for CSV loading; only the columns required by the
  schema are mandatory — all other columns are passed through as-is if
  they map to optional fields.
"""

from __future__ import annotations

import hashlib
import logging
import math
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from ingestion.producer import ParkingEventProducer

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SCHEMA_VERSION: str = "1.0"
_DEFAULT_RATE_PER_SECOND: float = 10.0
_DEFAULT_CITY: str = "Melbourne"
_DEFAULT_ZONE_ID: str = "UNKNOWN"
_DEFAULT_LOT_NAME: str = "Melbourne CBD"

# Column name aliases — keys are canonical schema field names,
# values are ordered lists of possible CSV column names (case-insensitive).
_COLUMN_ALIASES: dict[str, list[str]] = {
    "lot_id": [
        "StreetMarker", "street_marker", "parking_lot_id", "lot_id",
        "bay_id", "BayID", "LotID",
    ],
    "slot_id": [
        "BayID", "bay_id", "SlotID", "slot_id", "DeviceId", "device_id",
    ],
    "status": [
        "VehiclePresent", "vehicle_present", "OccupancyState",
        "occupancy_state", "status", "Status",
    ],
    "event_timestamp": [
        "ArrivalTime", "arrival_time", "DepartureTime", "departure_time",
        "Duration", "event_timestamp", "timestamp", "Timestamp",
        "detected_at", "DetectedAt",
    ],
    "zone_id": ["AreaName", "area_name", "zone", "Zone", "zone_id"],
    "lot_name": ["StreetName", "street_name", "lot_name", "LotName"],
    "latitude": ["Lat", "lat", "latitude", "Latitude"],
    "longitude": ["Lon", "lon", "longitude", "Longitude", "Long", "long"],
}


# ---------------------------------------------------------------------------
# Configuration dataclass
# ---------------------------------------------------------------------------


@dataclass
class HistoricalReplayConfig:
    """Configuration for a :class:`HistoricalReplayer` session.

    Attributes
    ----------
    source_file:
        Path to the CSV file containing the historical dataset.
    target_rate_per_second:
        Approximate number of events to publish per second.  ``0`` means
        publish as fast as possible (no rate limiting).
    time_column:
        CSV column whose value is used as ``event_timestamp``.  Auto-detected
        from :data:`_COLUMN_ALIASES` if ``None``.
    lot_id_column:
        CSV column mapped to ``lot_id``.  Auto-detected if ``None``.
    status_column:
        CSV column mapped to ``status``.  Auto-detected if ``None``.
    city_context:
        City name written into the ``city`` field of every event.
        Defaults to ``"Melbourne"`` — do **not** change to Bengaluru.
    replay_seed:
        Random seed for reproducible UUID generation during replay.  When
        ``None``, UUIDs are fully random per run.
    """

    source_file: str
    target_rate_per_second: float = _DEFAULT_RATE_PER_SECOND
    time_column: str | None = None
    lot_id_column: str | None = None
    status_column: str | None = None
    city_context: str = _DEFAULT_CITY
    replay_seed: int | None = None


# ---------------------------------------------------------------------------
# Replayer class
# ---------------------------------------------------------------------------


class HistoricalReplayer:
    """Reads a historical parking dataset and replays it through Kafka.

    Parameters
    ----------
    config:
        :class:`HistoricalReplayConfig` controlling file path, rate, and
        column mappings.

    Examples
    --------
    >>> cfg = HistoricalReplayConfig(
    ...     source_file="data/melbourne_parking.csv",
    ...     target_rate_per_second=50.0,
    ...     city_context="Melbourne",
    ... )
    >>> replayer = HistoricalReplayer(cfg)
    >>> with ParkingEventProducer("localhost:9092", "parking_events") as producer:
    ...     replayer.replay(producer)
    """

    def __init__(self, config: HistoricalReplayConfig) -> None:
        self._config = config
        self._event_sequence: int = 0
        self._resolved_columns: dict[str, str] = {}  # schema key → actual column
        logger.info(
            "HistoricalReplayer initialised | file=%r | rate=%.1f/s | city=%r",
            config.source_file,
            config.target_rate_per_second,
            config.city_context,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load_dataset(self, file_path: str) -> pd.DataFrame:
        """Load the historical CSV into a Pandas DataFrame.

        Performs basic type coercion and drops fully-empty rows.

        Parameters
        ----------
        file_path:
            Absolute or relative path to the CSV file.

        Returns
        -------
        pd.DataFrame
            Cleaned DataFrame ready for iteration.

        Raises
        ------
        FileNotFoundError
            If *file_path* does not exist.
        ValueError
            If the file cannot be parsed as CSV.
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(
                f"Historical dataset not found: {file_path!r}"
            )
        logger.info("Loading historical dataset from %r …", file_path)
        try:
            df = pd.read_csv(path, low_memory=False)
        except Exception as exc:
            raise ValueError(
                f"Failed to parse CSV at {file_path!r}: {exc}"
            ) from exc

        original_len = len(df)
        df.dropna(how="all", inplace=True)
        df.reset_index(drop=True, inplace=True)
        logger.info(
            "Dataset loaded | rows_raw=%d | rows_after_drop=%d | columns=%s",
            original_len,
            len(df),
            list(df.columns),
        )
        # Resolve column aliases once
        self._resolved_columns = self._resolve_column_aliases(df.columns.tolist())
        logger.debug("Resolved column mappings: %s", self._resolved_columns)
        return df

    def normalize_record(
        self,
        row: pd.Series,
        schema_version: str = _SCHEMA_VERSION,
    ) -> dict[str, Any]:
        """Convert a single historical DataFrame row to canonical schema v1.0.

        The conversion is intentionally transparent:
        * ``source`` is hard-coded to ``"HISTORICAL_REPLAY"`` so consumers
          can distinguish these events from live sensor data.
        * ``city`` is set to the *actual* source city from the dataset
          (defaults to ``"Melbourne"``), not to Bengaluru.
        * ``event_id`` is a deterministic UUID5 derived from the row content,
          making replays idempotent when *replay_seed* is ``None`` — or fully
          deterministic when a seed is provided.

        Parameters
        ----------
        row:
            A single row from the loaded DataFrame.
        schema_version:
            Schema version string to embed in the event; should be ``"1.0"``.

        Returns
        -------
        dict
            A parking event dict conforming to the canonical schema.
        """
        cfg = self._config
        cols = self._resolved_columns

        # ---- lot_id -------------------------------------------------------
        lot_id_col = cfg.lot_id_column or cols.get("lot_id")
        lot_id: str = (
            str(row[lot_id_col]).strip()
            if lot_id_col and lot_id_col in row.index and pd.notna(row[lot_id_col])
            else f"LOT_{row.name}"
        )

        # ---- slot_id ------------------------------------------------------
        slot_id_col = cols.get("slot_id")
        slot_id: str = (
            str(row[slot_id_col]).strip()
            if slot_id_col and slot_id_col in row.index and pd.notna(row[slot_id_col])
            else lot_id  # fall back to lot_id when no dedicated slot column
        )

        # ---- status -------------------------------------------------------
        status_col = cfg.status_column or cols.get("status")
        raw_status = (
            row[status_col]
            if status_col and status_col in row.index and pd.notna(row.get(status_col))
            else None
        )
        status: str = self._normalise_status(raw_status)

        # ---- event_timestamp ----------------------------------------------
        ts_col = cfg.time_column or cols.get("event_timestamp")
        raw_ts = (
            row[ts_col]
            if ts_col and ts_col in row.index and pd.notna(row.get(ts_col))
            else None
        )
        event_ts_str: str = self._normalise_timestamp(raw_ts)

        # ---- ingestion_timestamp ------------------------------------------
        ingestion_ts_str: str = datetime.now(tz=timezone.utc).isoformat()

        # ---- zone_id / lot_name -------------------------------------------
        zone_col = cols.get("zone_id")
        zone_id: str = (
            str(row[zone_col]).strip()
            if zone_col and zone_col in row.index and pd.notna(row.get(zone_col))
            else _DEFAULT_ZONE_ID
        )

        lot_name_col = cols.get("lot_name")
        lot_name: str = (
            str(row[lot_name_col]).strip()
            if lot_name_col and lot_name_col in row.index and pd.notna(row.get(lot_name_col))
            else _DEFAULT_LOT_NAME
        )

        # ---- coordinates --------------------------------------------------
        lat: float | None = self._safe_float(row, cols.get("latitude"))
        lon: float | None = self._safe_float(row, cols.get("longitude"))

        # ---- event_id — deterministic UUID5 based on content hash ---------
        fingerprint = f"{lot_id}:{slot_id}:{event_ts_str}:{self._event_sequence}"
        event_id: str = str(
            uuid.uuid5(uuid.NAMESPACE_DNS, fingerprint)
        )

        # ---- Assemble canonical event dict --------------------------------
        event: dict[str, Any] = {
            "event_id": event_id,
            "lot_id": lot_id,
            "slot_id": slot_id,
            "zone_id": zone_id,
            "city": cfg.city_context,       # Melbourne — not Bengaluru
            "status": status,
            "event_timestamp": event_ts_str,
            "ingestion_timestamp": ingestion_ts_str,
            "source": "HISTORICAL_REPLAY",  # always labelled as replay
            "event_sequence": self._event_sequence,
            "schema_version": schema_version,
            "lot_name": lot_name,
        }

        # Attach optional geo fields only if available
        if lat is not None:
            event["latitude"] = lat
        if lon is not None:
            event["longitude"] = lon

        self._event_sequence += 1
        return event

    def replay(
        self,
        producer: ParkingEventProducer,
        max_events: int | None = None,
        stop_event: threading.Event | None = None,
    ) -> dict[str, Any]:
        """Replay the historical dataset through *producer* at the configured rate.

        Parameters
        ----------
        producer:
            An open :class:`~ingestion.producer.ParkingEventProducer`.
        max_events:
            Stop after publishing this many events.  ``None`` → publish
            every row in the dataset.
        stop_event:
            A :class:`threading.Event` that, when set, causes the replay
            loop to exit cleanly after the current event.

        Returns
        -------
        dict
            Summary statistics: ``events_published``, ``events_failed``,
            ``elapsed_seconds``, ``actual_rate``.
        """
        file_path = self._config.source_file
        df = self.load_dataset(file_path)

        target_rate = self._config.target_rate_per_second
        rate_limited = target_rate > 0.0
        interval_s: float = 1.0 / target_rate if rate_limited else 0.0

        total_rows = len(df)
        limit = min(max_events, total_rows) if max_events is not None else total_rows

        logger.info(
            "Starting replay | rows=%d | limit=%d | rate=%.1f/s | interval_ms=%.1f",
            total_rows,
            limit,
            target_rate,
            interval_s * 1000,
        )

        published = 0
        failed = 0
        start_time = time.perf_counter()
        next_send_time = start_time

        for idx in range(limit):
            if stop_event is not None and stop_event.is_set():
                logger.info("Stop signal received — exiting replay loop at index %d.", idx)
                break

            row = df.iloc[idx]
            try:
                event = self.normalize_record(row)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "Failed to normalize row %d: %s | row_data=%s",
                    idx,
                    exc,
                    row.to_dict(),
                )
                failed += 1
                continue

            success = producer.send_event(event)
            if success:
                published += 1
            else:
                failed += 1

            # ---- Token-bucket rate limiter ---------------------------------
            if rate_limited:
                next_send_time += interval_s
                sleep_duration = next_send_time - time.perf_counter()
                if sleep_duration > 0:
                    time.sleep(sleep_duration)

            # ---- Progress logging every 1000 events -----------------------
            if (idx + 1) % 1_000 == 0:
                elapsed = time.perf_counter() - start_time
                actual_rate = published / elapsed if elapsed > 0 else 0.0
                logger.info(
                    "Replay progress | index=%d/%d | published=%d | "
                    "failed=%d | rate=%.1f/s",
                    idx + 1,
                    limit,
                    published,
                    failed,
                    actual_rate,
                )

        elapsed_total = time.perf_counter() - start_time
        actual_rate = published / elapsed_total if elapsed_total > 0 else 0.0

        summary = {
            "events_published": published,
            "events_failed": failed,
            "elapsed_seconds": round(elapsed_total, 3),
            "actual_rate_per_second": round(actual_rate, 2),
            "source_file": file_path,
            "city": self._config.city_context,
        }
        logger.info("Replay complete | %s", summary)
        producer.flush()
        return summary

    def estimate_replay_duration(self, file_path: str) -> dict[str, Any]:
        """Return dataset statistics without actually publishing events.

        Useful for pre-flight checks or capacity planning.

        Parameters
        ----------
        file_path:
            Path to the CSV file.

        Returns
        -------
        dict
            Keys: ``row_count``, ``column_count``, ``columns``,
            ``estimated_duration_seconds``, ``estimated_duration_human``,
            ``target_rate_per_second``, ``file_size_bytes``.
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Dataset not found: {file_path!r}")

        file_size_bytes = path.stat().st_size

        # Count rows cheaply without loading entire file into memory
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            row_count = sum(1 for _ in fh) - 1  # subtract header row

        # Read header only for column names
        header_df = pd.read_csv(path, nrows=0)
        columns = header_df.columns.tolist()

        rate = self._config.target_rate_per_second
        if rate > 0:
            est_seconds = row_count / rate
        else:
            est_seconds = 0.0  # unlimited rate — essentially instantaneous

        human_duration = self._format_duration(est_seconds)

        stats: dict[str, Any] = {
            "row_count": row_count,
            "column_count": len(columns),
            "columns": columns,
            "file_size_bytes": file_size_bytes,
            "file_size_mb": round(file_size_bytes / (1024 * 1024), 2),
            "target_rate_per_second": rate,
            "estimated_duration_seconds": round(est_seconds, 1),
            "estimated_duration_human": human_duration,
        }
        logger.info("Dataset estimate | %s", stats)
        return stats

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_column_aliases(
        actual_columns: list[str],
    ) -> dict[str, str]:
        """Map canonical schema field names to actual CSV column names.

        Parameters
        ----------
        actual_columns:
            List of column names present in the loaded DataFrame.

        Returns
        -------
        dict[str, str]
            Keys are canonical schema field names; values are matched CSV
            column names.  Fields without a match are absent from the dict.
        """
        lower_actual = {c.lower(): c for c in actual_columns}
        resolved: dict[str, str] = {}
        for schema_field, aliases in _COLUMN_ALIASES.items():
            for alias in aliases:
                if alias.lower() in lower_actual:
                    resolved[schema_field] = lower_actual[alias.lower()]
                    break
        return resolved

    @staticmethod
    def _normalise_status(raw: Any) -> str:
        """Convert raw sensor / CSV status values to ``OCCUPIED`` or ``FREE``.

        Handles boolean-like values (``True``/``1``/``"Present"`` → OCCUPIED),
        text strings, and unknown values (defaults to ``"FREE"``).
        """
        if raw is None or (isinstance(raw, float) and math.isnan(raw)):
            return "FREE"
        if isinstance(raw, bool):
            return "OCCUPIED" if raw else "FREE"
        if isinstance(raw, (int, float)):
            return "OCCUPIED" if int(raw) == 1 else "FREE"
        s = str(raw).strip().upper()
        if s in {"1", "TRUE", "OCCUPIED", "PRESENT", "VEHICLEPRESENT", "YES"}:
            return "OCCUPIED"
        if s in {"0", "FALSE", "FREE", "VACANT", "EMPTY", "NO", "UNOCCUPIED"}:
            return "FREE"
        logger.warning("Unknown status value %r — defaulting to 'FREE'.", raw)
        return "FREE"

    @staticmethod
    def _normalise_timestamp(raw: Any) -> str:
        """Return an ISO 8601 UTC string from various timestamp representations.

        Falls back to the current UTC time if *raw* cannot be parsed.
        """
        if raw is None or (isinstance(raw, float) and math.isnan(raw)):
            return datetime.now(tz=timezone.utc).isoformat()
        if isinstance(raw, (pd.Timestamp, datetime)):
            ts = pd.Timestamp(raw)
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            return ts.isoformat()
        try:
            ts = pd.to_datetime(str(raw), infer_datetime_format=True)
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            return ts.isoformat()
        except Exception:  # noqa: BLE001
            logger.warning(
                "Cannot parse timestamp %r — using current UTC time.", raw
            )
            return datetime.now(tz=timezone.utc).isoformat()

    @staticmethod
    def _safe_float(row: pd.Series, col: str | None) -> float | None:
        """Safely extract a float from *row[col]*, returning ``None`` on failure."""
        if col is None or col not in row.index:
            return None
        val = row[col]
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return None
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _format_duration(seconds: float) -> str:
        """Format a duration in seconds as a human-readable string."""
        if seconds <= 0:
            return "< 1 second (unlimited rate)"
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        parts = []
        if hours:
            parts.append(f"{hours}h")
        if minutes:
            parts.append(f"{minutes}m")
        parts.append(f"{secs}s")
        return " ".join(parts)


def main() -> None:
    """CLI Entry point for Historical Dataset Replayer."""
    import argparse

    parser = argparse.ArgumentParser(description="Historical Dataset Replayer (City of Melbourne 2019)")
    parser.add_argument(
        "--file",
        type=str,
        default="datasets/raw/melbourne_sensors_2019_sample.csv",
        help="Path to historical CSV file",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=25.0,
        help="Replay rate in events per second",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum events to replay",
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default="localhost:9092",
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="raw-parking-events",
        help="Destination Kafka topic",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger.info("Starting Historical Replayer from %s at %.1f ev/s", args.file, args.rate)

    cfg = HistoricalReplayConfig(
        source_file=args.file,
        target_rate_per_second=args.rate,
        city_context="Melbourne",
    )
    replayer = HistoricalReplayer(cfg)
    with ParkingEventProducer(bootstrap_servers=args.bootstrap_servers, topic=args.topic) as producer:
        summary = replayer.replay(producer, max_events=args.limit)
        print("\nReplay Summary:", summary)


if __name__ == "__main__":
    main()

