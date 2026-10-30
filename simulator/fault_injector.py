"""
simulator.fault_injector
========================
Probabilistic fault injection for realistic IoT sensor simulation.

The ``FaultInjector`` class introduces the following failure modes:

+-----------------------+-------------------------------------------+
| Fault type            | Effect on the event stream                |
+=======================+===========================================+
| Duplicate             | Emits a second copy of the same event.    |
+-----------------------+-------------------------------------------+
| Late / delayed        | Emits a copy with an older timestamp.     |
+-----------------------+-------------------------------------------+
| Invalid / malformed   | Emits a structurally broken event.        |
+-----------------------+-------------------------------------------+
| Out-of-order          | Emits a copy with a timestamp slightly    |
|                       | *before* the original (OOO window).       |
+-----------------------+-------------------------------------------+
| Sensor failure        | Drops the event entirely (returns []).    |
+-----------------------+-------------------------------------------+

``inject_faults`` always places the *original valid event first* (unless a
sensor-failure is injected), followed by any additional fault events.  This
ensures the downstream stream-processing watermark logic can be tested with
known mixed sequences.
"""

from __future__ import annotations

import copy
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)


class FaultInjector:
    """Injects configurable faults into a stream of parking sensor events.

    Parameters
    ----------
    duplicate_prob:
        Probability [0, 1] that an event is duplicated.
    late_event_prob:
        Probability [0, 1] that a delayed copy is appended.
    max_late_delay_seconds:
        Maximum backward offset (seconds) for a late event.
    invalid_event_prob:
        Probability [0, 1] that a structurally invalid event is injected.
    sensor_failure_prob:
        Probability [0, 1] that a sensor failure is simulated (event dropped).
    out_of_order_prob:
        Probability [0, 1] that an out-of-order copy is injected.
    rng:
        NumPy random Generator for reproducible behaviour.  A fresh
        ``np.random.default_rng()`` is used when ``None``.
    """

    def __init__(
        self,
        duplicate_prob: float = 0.02,
        late_event_prob: float = 0.03,
        max_late_delay_seconds: int = 180,
        invalid_event_prob: float = 0.01,
        sensor_failure_prob: float = 0.001,
        out_of_order_prob: float = 0.02,
        rng: Optional[np.random.Generator] = None,
    ) -> None:
        self.duplicate_prob = float(duplicate_prob)
        self.late_event_prob = float(late_event_prob)
        self.max_late_delay_seconds = int(max_late_delay_seconds)
        self.invalid_event_prob = float(invalid_event_prob)
        self.sensor_failure_prob = float(sensor_failure_prob)
        self.out_of_order_prob = float(out_of_order_prob)
        self._rng: np.random.Generator = rng if rng is not None else np.random.default_rng()

        self._validate_probabilities()

        logger.debug(
            "FaultInjector initialised: dup=%.3f, late=%.3f, invalid=%.3f, "
            "failure=%.3f, ooo=%.3f, max_late_delay=%ds.",
            self.duplicate_prob,
            self.late_event_prob,
            self.invalid_event_prob,
            self.sensor_failure_prob,
            self.out_of_order_prob,
            self.max_late_delay_seconds,
        )

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_probabilities(self) -> None:
        """Raise ``ValueError`` if any probability is outside [0, 1]."""
        fields = {
            "duplicate_prob": self.duplicate_prob,
            "late_event_prob": self.late_event_prob,
            "invalid_event_prob": self.invalid_event_prob,
            "sensor_failure_prob": self.sensor_failure_prob,
            "out_of_order_prob": self.out_of_order_prob,
        }
        for name, value in fields.items():
            if not 0.0 <= value <= 1.0:
                raise ValueError(
                    f"Probability '{name}' must be in [0, 1], got {value!r}."
                )

    # ------------------------------------------------------------------
    # Individual fault predicates
    # ------------------------------------------------------------------

    def should_inject_duplicate(self, event: dict) -> bool:  # noqa: ARG002
        """Return ``True`` if a duplicate of *event* should be generated.

        Parameters
        ----------
        event:
            The source event dict (not currently inspected; reserved for
            future conditional logic, e.g. suppressing dups for certain lots).
        """
        return bool(self._rng.random() < self.duplicate_prob)

    def should_inject_late(self, event: dict) -> bool:  # noqa: ARG002
        """Return ``True`` if a late (delayed) copy of *event* should be added."""
        return bool(self._rng.random() < self.late_event_prob)

    def get_late_delay_seconds(self) -> int:
        """Sample a random late-event delay in seconds.

        Returns
        -------
        int
            An integer in the range [1, max_late_delay_seconds].
        """
        return int(self._rng.integers(1, self.max_late_delay_seconds + 1))

    def should_inject_invalid(self) -> bool:
        """Return ``True`` if a structurally invalid event should be emitted."""
        return bool(self._rng.random() < self.invalid_event_prob)

    def should_inject_sensor_failure(self) -> bool:
        """Return ``True`` if a sensor-failure (event drop) should be simulated."""
        return bool(self._rng.random() < self.sensor_failure_prob)

    def should_inject_out_of_order(self) -> bool:
        """Return ``True`` if an out-of-order event copy should be appended."""
        return bool(self._rng.random() < self.out_of_order_prob)

    # ------------------------------------------------------------------
    # Event factories
    # ------------------------------------------------------------------

    def _clone_with_new_id(self, event: dict) -> dict:
        """Deep-copy *event* and assign a fresh ``event_id``."""
        clone = copy.deepcopy(event)
        clone["event_id"] = f"EVT_{uuid.uuid4().hex[:12].upper()}"
        return clone

    def _shift_event_timestamp(self, event: dict, delta_seconds: int) -> dict:
        """Return a copy of *event* with ``event_timestamp`` shifted by *delta_seconds*.

        A *negative* delta moves the timestamp backward (late / OOO event).

        Parameters
        ----------
        event:
            Source event dict; must have an ``event_timestamp`` key in ISO-8601
            format.
        delta_seconds:
            Seconds to add (negative to subtract).
        """
        clone = self._clone_with_new_id(event)
        try:
            original_ts = datetime.fromisoformat(
                clone["event_timestamp"].replace("Z", "+00:00")
            )
            shifted_ts = original_ts + timedelta(seconds=delta_seconds)
            clone["event_timestamp"] = shifted_ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        except (KeyError, ValueError) as exc:
            logger.warning("Failed to shift event timestamp: %s", exc)
        clone["fault_type"] = "LATE" if delta_seconds < 0 else "FUTURE"
        return clone

    def create_invalid_event(self) -> dict:
        """Return a deliberately malformed event dict.

        The event is designed to trigger schema-validation failures in the
        ingestion layer.  It intentionally:

        * omits required fields (``lot_id``, ``slot_id``),
        * uses wrong types (``status`` as int),
        * includes an unparseable timestamp,
        * sets ``schema_version`` to a bogus value.

        Returns
        -------
        dict
            A structurally broken event.
        """
        invalid_variants = [
            # Missing required fields
            {
                "event_id": f"EVT_INVALID_{uuid.uuid4().hex[:8].upper()}",
                "status": 999,                          # wrong type
                "event_timestamp": "NOT_A_TIMESTAMP",   # unparseable
                "schema_version": "0.0",
                "fault_type": "INVALID_MISSING_FIELDS",
            },
            # Null / empty lot / slot ids
            {
                "event_id": f"EVT_INVALID_{uuid.uuid4().hex[:8].upper()}",
                "lot_id": "",
                "slot_id": None,
                "zone_id": "",
                "city": None,
                "status": "UNKNOWN_STATUS",
                "event_timestamp": datetime.now(timezone.utc).isoformat(),
                "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
                "source": None,
                "schema_version": "1.0",
                "fault_type": "INVALID_NULL_IDS",
            },
            # Impossible coordinates
            {
                "event_id": f"EVT_INVALID_{uuid.uuid4().hex[:8].upper()}",
                "lot_id": "LOT_GHOST_99",
                "slot_id": "LOT_GHOST_99-S000",
                "zone_id": "ZONE_GHOST",
                "city": "Bengaluru",
                "status": "OCCUPIED",
                "event_timestamp": datetime.now(timezone.utc).isoformat(),
                "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
                "source": "SIMULATED_SENSOR",
                "latitude": 999.0,    # impossible
                "longitude": -999.0,  # impossible
                "schema_version": "1.0",
                "fault_type": "INVALID_COORDS",
            },
        ]
        choice = int(self._rng.integers(0, len(invalid_variants)))
        return invalid_variants[choice]

    # ------------------------------------------------------------------
    # Main injection entry-point
    # ------------------------------------------------------------------

    def inject_faults(
        self,
        event: dict,
        rng: Optional[np.random.Generator] = None,  # noqa: ARG002 (reserved)
    ) -> list[dict]:
        """Apply all probabilistic fault rules to *event*.

        The returned list may contain:

        * Nothing            — if a sensor-failure is injected.
        * Only the original  — if no faults fire.
        * The original plus  — one or more fault events appended.

        The *original event is always first* in a non-empty list; fault events
        are appended after so downstream consumers see the valid event before
        the corrupted copies.

        Parameters
        ----------
        event:
            The clean, fully-formed event dict.
        rng:
            Ignored; kept for API compatibility.  The instance's internal RNG
            is always used to ensure reproducibility.

        Returns
        -------
        list[dict]
            Ordered list of events to publish.
        """
        # Sensor failure → drop everything
        if self.should_inject_sensor_failure():
            logger.debug(
                "Sensor failure injected for event_id=%s — dropping event.",
                event.get("event_id", "UNKNOWN"),
            )
            return []

        result: list[dict] = [event]

        # Duplicate
        if self.should_inject_duplicate(event):
            dup = self._clone_with_new_id(event)
            dup["fault_type"] = "DUPLICATE"
            result.append(dup)
            logger.debug("Duplicate injected for event_id=%s.", event.get("event_id"))

        # Late (delayed) event — timestamp shifted backward
        if self.should_inject_late(event):
            delay = self.get_late_delay_seconds()
            late = self._shift_event_timestamp(event, -delay)
            result.append(late)
            logger.debug(
                "Late event injected (-%ds) for event_id=%s.",
                delay,
                event.get("event_id"),
            )

        # Out-of-order — small backward jitter (1 – 30 s)
        if self.should_inject_out_of_order():
            ooo_delay = int(self._rng.integers(1, 31))
            ooo = self._shift_event_timestamp(event, -ooo_delay)
            ooo["fault_type"] = "OUT_OF_ORDER"
            result.append(ooo)
            logger.debug(
                "Out-of-order event injected (-%ds) for event_id=%s.",
                ooo_delay,
                event.get("event_id"),
            )

        # Invalid / malformed event injected alongside valid one
        if self.should_inject_invalid():
            result.append(self.create_invalid_event())
            logger.debug(
                "Invalid event injected alongside event_id=%s.",
                event.get("event_id"),
            )

        return result

    # ------------------------------------------------------------------
    # Stats / repr
    # ------------------------------------------------------------------

    def stats(self) -> dict:
        """Return a dict of configured fault probabilities (for logging)."""
        return {
            "duplicate_prob": self.duplicate_prob,
            "late_event_prob": self.late_event_prob,
            "max_late_delay_seconds": self.max_late_delay_seconds,
            "invalid_event_prob": self.invalid_event_prob,
            "sensor_failure_prob": self.sensor_failure_prob,
            "out_of_order_prob": self.out_of_order_prob,
        }

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"FaultInjector(dup={self.duplicate_prob}, "
            f"late={self.late_event_prob}, "
            f"invalid={self.invalid_event_prob}, "
            f"failure={self.sensor_failure_prob}, "
            f"ooo={self.out_of_order_prob})"
        )
