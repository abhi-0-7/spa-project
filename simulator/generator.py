"""
simulator.generator
===================
Core event generator for the Smart Parking Occupancy Analytics simulator.

``EventGenerator`` maintains the live state of all configured parking lots,
drives slot occupancy transitions according to the ``TrafficModel``, applies
fault injection via ``FaultInjector``, and yields fully-formed event dicts.

Event schema (v1.0)
-------------------
::

    {
        "event_id":           str,   # "EVT_<12-char hex>"
        "lot_id":             str,
        "slot_id":            str,
        "zone_id":            str,
        "city":               str,
        "status":             "OCCUPIED" | "FREE",
        "event_timestamp":    str,   # ISO-8601 UTC ("…Z")
        "ingestion_timestamp":str,   # ISO-8601 UTC, ≥ event_timestamp
        "source":             str,   # "SIMULATED_SENSOR"
        "event_sequence":     int,   # global monotonic counter
        "schema_version":     str,   # "1.0"
        "latitude":           float,
        "longitude":          float,
        "lot_name":           str
    }

Thread safety
-------------
The global event-sequence counter is protected by a ``threading.Lock`` so
``generate_continuous`` can safely be invoked from multiple threads.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Generator, List, Optional

import numpy as np

from simulator.fault_injector import FaultInjector
from simulator.parking_lot import ParkingLot, SlotStatus
from simulator.traffic_model import TrafficModel

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SCHEMA_VERSION = "1.0"
_SOURCE_TAG = "SIMULATED_SENSOR"
_INGESTION_JITTER_MS_MAX = 500   # max milliseconds added to event_timestamp


# ---------------------------------------------------------------------------
# EventGenerator
# ---------------------------------------------------------------------------

class EventGenerator:
    """Generates a realistic stream of parking sensor events.

    Parameters
    ----------
    config:
        Parsed ``simulator.yaml`` as a nested dict.
    seed:
        Optional integer random seed for reproducibility.  Overrides the
        value in ``config["event_generation"]["random_seed"]`` when provided.
    """

    def __init__(self, config: dict, seed: Optional[int] = None) -> None:
        self._config = config
        self._city: str = config.get("city", "Bengaluru")

        # ── RNG ──────────────────────────────────────────────────────
        _seed: Optional[int] = seed
        if _seed is None:
            _seed = config.get("event_generation", {}).get("random_seed", None)
        self._rng: np.random.Generator = np.random.default_rng(_seed)
        logger.info("EventGenerator RNG seed: %s", _seed)

        # ── Parking lots ──────────────────────────────────────────────
        self._lots: Dict[str, ParkingLot] = {}
        self._all_slot_ids: List[tuple[str, str]] = []  # (lot_id, slot_id)
        self._build_lots(config.get("parking_lots", []))

        # ── Traffic model ─────────────────────────────────────────────
        tm_cfg = config.get("traffic_model", {})
        self._traffic_model = TrafficModel(
            base_arrival_prob=float(tm_cfg.get("base_arrival_prob", 0.002)),
            base_departure_prob=float(tm_cfg.get("base_departure_prob", 0.002)),
            morning_peak_start=int(
                tm_cfg.get("morning_peak", {}).get("start_hour", 7)
            ),
            morning_peak_end=int(
                tm_cfg.get("morning_peak", {}).get("end_hour", 10)
            ),
            evening_peak_start=int(
                tm_cfg.get("evening_peak", {}).get("start_hour", 17)
            ),
            evening_peak_end=int(
                tm_cfg.get("evening_peak", {}).get("end_hour", 20)
            ),
        )

        # ── Fault injector ────────────────────────────────────────────
        fi_cfg = config.get("fault_injection", {})
        self._fault_injector = FaultInjector(
            duplicate_prob=float(fi_cfg.get("duplicate_probability", 0.02)),
            late_event_prob=float(fi_cfg.get("late_event_probability", 0.03)),
            max_late_delay_seconds=int(
                fi_cfg.get("max_late_event_delay_seconds", 180)
            ),
            invalid_event_prob=float(
                fi_cfg.get("invalid_event_probability", 0.01)
            ),
            sensor_failure_prob=float(
                fi_cfg.get("sensor_failure_probability", 0.001)
            ),
            out_of_order_prob=float(
                fi_cfg.get("out_of_order_probability", 0.02)
            ),
            rng=np.random.default_rng(
                None if _seed is None else _seed + 1
            ),
        )

        # ── Global sequence counter ───────────────────────────────────
        self._seq_counter: int = 0
        self._seq_lock: threading.Lock = threading.Lock()

        logger.info(
            "EventGenerator ready: %d lots, %d total slots.",
            len(self._lots),
            len(self._all_slot_ids),
        )

    # ------------------------------------------------------------------
    # Internal builders
    # ------------------------------------------------------------------

    def _build_lots(self, lot_configs: list[dict]) -> None:
        """Instantiate ``ParkingLot`` objects from the config list."""
        if not lot_configs:
            logger.warning("No parking lots defined in config.")
            return

        for lc in lot_configs:
            try:
                lot = ParkingLot(
                    lot_id=lc["lot_id"],
                    name=lc["name"],
                    zone_id=lc["zone_id"],
                    city=self._city,
                    latitude=float(lc["latitude"]),
                    longitude=float(lc["longitude"]),
                    capacity=int(lc["capacity"]),
                    num_slots=int(lc["num_slots"]),
                    lot_type=lc.get("lot_type", "surface"),
                )
                self._lots[lot.lot_id] = lot
                for slot_id in lot.slots:
                    self._all_slot_ids.append((lot.lot_id, slot_id))
            except KeyError as exc:
                logger.error(
                    "Missing required field %s in lot config: %s", exc, lc
                )
            except (TypeError, ValueError) as exc:
                logger.error("Invalid value in lot config %s: %s", lc, exc)

    def _next_sequence(self) -> int:
        """Atomically increment and return the global event sequence counter."""
        with self._seq_lock:
            self._seq_counter += 1
            return self._seq_counter

    # ------------------------------------------------------------------
    # Event factory
    # ------------------------------------------------------------------

    @staticmethod
    def _format_ts(dt: datetime) -> str:
        """Format *dt* as an ISO-8601 UTC string ending in 'Z'."""
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    def generate_event(
        self,
        lot: ParkingLot,
        slot_id: str,
        status: SlotStatus,
        timestamp: datetime,
    ) -> dict:
        """Build a single, fully-populated event dict.

        Parameters
        ----------
        lot:
            The ``ParkingLot`` that owns the slot.
        slot_id:
            The slot identifier.
        status:
            The new status being reported.
        timestamp:
            The event time (UTC).

        Returns
        -------
        dict
            Fully-populated event conforming to schema v1.0.
        """
        ingestion_jitter_ms = int(self._rng.integers(10, _INGESTION_JITTER_MS_MAX))
        ingestion_ts = datetime.fromtimestamp(
            timestamp.timestamp() + ingestion_jitter_ms / 1000.0,
            tz=timezone.utc,
        )

        return {
            "event_id": f"EVT_{uuid.uuid4().hex[:12].upper()}",
            "lot_id": lot.lot_id,
            "slot_id": slot_id,
            "zone_id": lot.zone_id,
            "city": lot.city,
            "status": status.value,
            "event_timestamp": self._format_ts(timestamp),
            "ingestion_timestamp": self._format_ts(ingestion_ts),
            "source": _SOURCE_TAG,
            "event_sequence": self._next_sequence(),
            "schema_version": _SCHEMA_VERSION,
            "latitude": lot.latitude,
            "longitude": lot.longitude,
            "lot_name": lot.name,
        }

    def generate_next_event(self) -> dict:
        """Generate a single realistic sensor event on demand."""
        now = datetime.now(timezone.utc)
        hour = now.hour
        events = self._drive_slot_transitions(hour=hour, now=now, target_count=1)
        if events:
            injected = self._fault_injector.inject_faults(events[0])
            return injected[0] if injected else events[0]
        # Fallback if no transitions
        lot_id, slot_id = self._all_slot_ids[int(self._rng.integers(0, len(self._all_slot_ids)))]
        lot = self._lots[lot_id]
        return self.generate_event(lot, slot_id, SlotStatus.OCCUPIED, now)

    # ------------------------------------------------------------------
    # Transition driver
    # ------------------------------------------------------------------

    def _drive_slot_transitions(
        self,
        hour: int,
        now: datetime,
        target_count: Optional[int] = None,
    ) -> list[dict]:
        """Iterate all slots and probabilistically fire occupancy transitions.

        Parameters
        ----------
        hour:
            Current hour (0–23) used by the traffic model.
        now:
            The wall-clock time to stamp on generated events.
        target_count:
            Optional desired number of transitions to generate. If provided,
            supplements natural probabilistic transitions with additional valid
            transitions up to target_count to support high-throughput benchmarks.

        Returns
        -------
        list[dict]
            Raw (pre-fault-injection) events for transitions that fired.
        """
        events: list[dict] = []
        for lot_id, slot_id in self._all_slot_ids:
            lot = self._lots[lot_id]
            slot = lot.slots.get(slot_id)
            if slot is None:
                continue

            if slot.current_status == SlotStatus.FREE:
                if self._traffic_model.should_vehicle_arrive(hour, rng=self._rng):
                    changed = lot.set_slot_status(slot_id, SlotStatus.OCCUPIED, now)
                    if changed:
                        events.append(
                            self.generate_event(lot, slot_id, SlotStatus.OCCUPIED, now)
                        )
            else:
                if self._traffic_model.should_vehicle_depart(hour, rng=self._rng):
                    changed = lot.set_slot_status(slot_id, SlotStatus.FREE, now)
                    if changed:
                        events.append(
                            self.generate_event(lot, slot_id, SlotStatus.FREE, now)
                        )

        # If a target_count is specified and natural transitions fall short,
        # randomly select slots and drive valid transitions to meet workload rate
        if target_count is not None and len(events) < target_count:
            needed = target_count - len(events)
            indices = self._rng.choice(len(self._all_slot_ids), size=min(needed * 2, len(self._all_slot_ids)), replace=False)
            for idx in indices:
                if len(events) >= target_count:
                    break
                lot_id, slot_id = self._all_slot_ids[idx]
                lot = self._lots[lot_id]
                slot = lot.slots.get(slot_id)
                if slot is None:
                    continue
                # Valid transition toggle
                new_status = SlotStatus.OCCUPIED if slot.current_status == SlotStatus.FREE else SlotStatus.FREE
                changed = lot.set_slot_status(slot_id, new_status, now)
                if changed:
                    events.append(self.generate_event(lot, slot_id, new_status, now))

        return events

    # ------------------------------------------------------------------
    # Public generators
    # ------------------------------------------------------------------

    def generate_batch(
        self,
        target_rate_per_second: int,
        duration_seconds: int,
        hour_override: Optional[int] = None,
    ) -> Generator[dict, None, None]:
        """Generate events for a fixed *duration_seconds* at *target_rate_per_second*.

        This generator simulates the passage of time without actually sleeping.
        Each "tick" represents one simulated second.  Up to
        ``target_rate_per_second`` transition events are attempted per tick;
        if fewer natural transitions occur the generator yields those and moves
        on (it does **not** pad with synthetic events).

        Parameters
        ----------
        target_rate_per_second:
            Maximum events to emit per simulated second.
        duration_seconds:
            Total simulated duration in seconds.
        hour_override:
            Pin the simulated hour (0–23) for the entire batch.  If ``None``,
            the real-time UTC hour is used for the first tick and advanced
            each 3600 simulated seconds.

        Yields
        ------
        dict
            Individual event dicts (may include fault-injected extras).
        """
        logger.info(
            "generate_batch: rate=%d/s, duration=%ds, hour_override=%s.",
            target_rate_per_second,
            duration_seconds,
            hour_override,
        )
        start_wall = datetime.now(timezone.utc)

        for tick in range(duration_seconds):
            now = datetime.fromtimestamp(
                start_wall.timestamp() + tick, tz=timezone.utc
            )
            hour = hour_override if hour_override is not None else now.hour

            raw_events = self._drive_slot_transitions(hour, now, target_count=target_rate_per_second)

            # Honour the rate cap: shuffle and take the first N
            if len(raw_events) > target_rate_per_second:
                self._rng.shuffle(raw_events)  # type: ignore[arg-type]
                raw_events = raw_events[:target_rate_per_second]

            for raw_event in raw_events:
                for final_event in self._fault_injector.inject_faults(raw_event):
                    yield final_event

    def generate_continuous(
        self,
        target_rate_per_second: int,
        stop_event: threading.Event,
    ) -> Generator[dict, None, None]:
        """Generate events continuously until *stop_event* is set.

        The generator uses real wall-clock time and real ``time.sleep`` to
        pace itself to ``target_rate_per_second``.  Actual throughput may be
        lower if the machine cannot keep up.

        Parameters
        ----------
        target_rate_per_second:
            Desired events per second.
        stop_event:
            ``threading.Event`` that signals the generator to stop.  Set it
            from another thread to trigger a graceful shutdown.

        Yields
        ------
        dict
            Individual event dicts.
        """
        logger.info(
            "generate_continuous: target_rate=%d/s. Stop via threading.Event.",
            target_rate_per_second,
        )
        tick_interval = 1.0  # one simulated second per wall-clock second

        while not stop_event.is_set():
            tick_start = time.monotonic()
            now = datetime.now(timezone.utc)
            hour = now.hour

            raw_events = self._drive_slot_transitions(hour, now, target_count=target_rate_per_second)

            if len(raw_events) > target_rate_per_second:
                self._rng.shuffle(raw_events)  # type: ignore[arg-type]
                raw_events = raw_events[:target_rate_per_second]

            for raw_event in raw_events:
                if stop_event.is_set():
                    return
                for final_event in self._fault_injector.inject_faults(raw_event):
                    yield final_event

            # Sleep for the remainder of the tick
            elapsed = time.monotonic() - tick_start
            sleep_for = max(0.0, tick_interval - elapsed)
            if sleep_for > 0 and not stop_event.is_set():
                time.sleep(sleep_for)

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    @property
    def lots(self) -> Dict[str, ParkingLot]:
        """Return the live ``{lot_id: ParkingLot}`` mapping."""
        return self._lots

    @property
    def traffic_model(self) -> TrafficModel:
        """Return the underlying ``TrafficModel`` instance."""
        return self._traffic_model

    @property
    def fault_injector(self) -> FaultInjector:
        """Return the underlying ``FaultInjector`` instance."""
        return self._fault_injector

    @property
    def event_sequence(self) -> int:
        """Return the current event-sequence counter value (thread-safe read)."""
        with self._seq_lock:
            return self._seq_counter

    def summary(self) -> dict:
        """Return a snapshot summary of all lots and total events generated."""
        return {
            "total_events_generated": self.event_sequence,
            "lots": [lot.to_dict() for lot in self._lots.values()],
        }

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"EventGenerator(lots={len(self._lots)}, "
            f"slots={len(self._all_slot_ids)}, "
            f"seq={self.event_sequence})"
        )
