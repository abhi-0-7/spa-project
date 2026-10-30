"""
simulator.traffic_model
=======================
Time-of-day traffic patterns and probabilistic arrival / departure logic.

The model mirrors realistic Bengaluru urban parking demand:

    * **Morning peak** (07:00 – 10:00 IST)  — high arrivals, low departures.
    * **Evening peak** (17:00 – 20:00 IST)  — moderate arrivals, high departures.
    * **Off-peak** (10:00 – 17:00 IST)      — baseline rates.
    * **Night** (22:00 – 06:00 IST)          — very low rates.
    * **Weekend** — flatter profile, slight reduction in IT-park demand.

All probabilities are expressed as *per-slot-per-second* values so that the
generator can call ``should_vehicle_arrive`` / ``should_vehicle_depart`` once
per slot per simulated second without any additional scaling.
"""

from __future__ import annotations

import logging
from enum import Enum
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class TrafficPattern(str, Enum):
    """Time-of-day traffic pattern labels."""

    MORNING_PEAK = "MORNING_PEAK"
    EVENING_PEAK = "EVENING_PEAK"
    OFF_PEAK = "OFF_PEAK"
    NIGHT = "NIGHT"
    WEEKEND = "WEEKEND"


# ---------------------------------------------------------------------------
# Per-pattern multiplier table
# ---------------------------------------------------------------------------

# (arrival_multiplier, departure_multiplier)
_PATTERN_MULTIPLIERS: dict[TrafficPattern, tuple[float, float]] = {
    TrafficPattern.MORNING_PEAK: (3.0, 0.5),
    TrafficPattern.EVENING_PEAK: (1.5, 3.0),
    TrafficPattern.OFF_PEAK:     (1.0, 1.0),
    TrafficPattern.NIGHT:        (0.1, 0.1),
    TrafficPattern.WEEKEND:      (0.8, 0.8),
}


# ---------------------------------------------------------------------------
# TrafficModel
# ---------------------------------------------------------------------------

class TrafficModel:
    """Encapsulates time-of-day traffic logic.

    Parameters
    ----------
    base_arrival_prob:
        Probability (per slot per second) that a vehicle arrives at a *free*
        slot during off-peak hours.  Default: 0.002.
    base_departure_prob:
        Probability (per slot per second) that a vehicle departs an *occupied*
        slot during off-peak hours.  Default: 0.002.
    morning_peak_start:
        Start hour (inclusive) of morning peak.  Default: 7.
    morning_peak_end:
        End hour (exclusive) of morning peak.  Default: 10.
    evening_peak_start:
        Start hour (inclusive) of evening peak.  Default: 17.
    evening_peak_end:
        End hour (exclusive) of evening peak.  Default: 20.
    night_start:
        Start hour (inclusive) of night period.  Default: 22.
    night_end:
        End hour (exclusive, wraps past midnight) of night period.  Default: 6.
    is_weekend:
        If ``True``, use the WEEKEND pattern instead of the weekday patterns.
        Default: ``False``.
    """

    def __init__(
        self,
        base_arrival_prob: float = 0.002,
        base_departure_prob: float = 0.002,
        morning_peak_start: int = 7,
        morning_peak_end: int = 10,
        evening_peak_start: int = 17,
        evening_peak_end: int = 20,
        night_start: int = 22,
        night_end: int = 6,
        is_weekend: bool = False,
    ) -> None:
        self.base_arrival_prob: float = base_arrival_prob
        self.base_departure_prob: float = base_departure_prob
        self.morning_peak_start: int = morning_peak_start
        self.morning_peak_end: int = morning_peak_end
        self.evening_peak_start: int = evening_peak_start
        self.evening_peak_end: int = evening_peak_end
        self.night_start: int = night_start
        self.night_end: int = night_end
        self.is_weekend: bool = is_weekend

        logger.debug(
            "TrafficModel initialised: base_arrival=%.4f, base_departure=%.4f, "
            "morning_peak=%d-%d, evening_peak=%d-%d, night=%d-%d, weekend=%s.",
            base_arrival_prob,
            base_departure_prob,
            morning_peak_start,
            morning_peak_end,
            evening_peak_start,
            evening_peak_end,
            night_start,
            night_end,
            is_weekend,
        )

    # ------------------------------------------------------------------
    # Pattern detection
    # ------------------------------------------------------------------

    def get_current_pattern(self, hour: int) -> TrafficPattern:
        """Return the ``TrafficPattern`` that applies at *hour* (0 – 23).

        Parameters
        ----------
        hour:
            Current hour in 24-h format (0 = midnight, 23 = 11 pm).

        Returns
        -------
        TrafficPattern
        """
        if not 0 <= hour <= 23:
            raise ValueError(f"hour must be 0–23, got {hour!r}.")

        if self.is_weekend:
            return TrafficPattern.WEEKEND

        # Night spans midnight: 22 → 23 → 0 → … → 5
        in_night = hour >= self.night_start or hour < self.night_end
        if in_night:
            return TrafficPattern.NIGHT

        if self.morning_peak_start <= hour < self.morning_peak_end:
            return TrafficPattern.MORNING_PEAK

        if self.evening_peak_start <= hour < self.evening_peak_end:
            return TrafficPattern.EVENING_PEAK

        return TrafficPattern.OFF_PEAK

    def get_multipliers(self, hour: int) -> tuple[float, float]:
        """Return ``(arrival_multiplier, departure_multiplier)`` for *hour*.

        Parameters
        ----------
        hour:
            Current hour (0–23).

        Returns
        -------
        tuple[float, float]
        """
        pattern = self.get_current_pattern(hour)
        return _PATTERN_MULTIPLIERS[pattern]

    # ------------------------------------------------------------------
    # Probability helpers
    # ------------------------------------------------------------------

    def get_arrival_probability(self, hour: int, base_prob: Optional[float] = None) -> float:
        """Return the per-slot-per-second arrival probability at *hour*.

        Parameters
        ----------
        hour:
            Current hour (0–23).
        base_prob:
            Override the instance-level ``base_arrival_prob``.

        Returns
        -------
        float
            Probability clamped to [0.0, 1.0].
        """
        bp = base_prob if base_prob is not None else self.base_arrival_prob
        arrival_mult, _ = self.get_multipliers(hour)
        return min(1.0, bp * arrival_mult)

    def get_departure_probability(self, hour: int, base_prob: Optional[float] = None) -> float:
        """Return the per-slot-per-second departure probability at *hour*.

        Parameters
        ----------
        hour:
            Current hour (0–23).
        base_prob:
            Override the instance-level ``base_departure_prob``.

        Returns
        -------
        float
            Probability clamped to [0.0, 1.0].
        """
        bp = base_prob if base_prob is not None else self.base_departure_prob
        _, departure_mult = self.get_multipliers(hour)
        return min(1.0, bp * departure_mult)

    # ------------------------------------------------------------------
    # Decision helpers
    # ------------------------------------------------------------------

    def should_vehicle_arrive(
        self,
        hour: int,
        base_prob: Optional[float] = None,
        rng: Optional[np.random.Generator] = None,
    ) -> bool:
        """Decide probabilistically whether a vehicle arrives at a free slot.

        Parameters
        ----------
        hour:
            Current hour (0–23).
        base_prob:
            Override the instance-level ``base_arrival_prob``.
        rng:
            NumPy random Generator.  Falls back to ``np.random.default_rng()``
            when ``None``.

        Returns
        -------
        bool
            ``True`` → a vehicle should arrive.
        """
        _rng = rng if rng is not None else np.random.default_rng()
        prob = self.get_arrival_probability(hour, base_prob)
        return bool(_rng.random() < prob)

    def should_vehicle_depart(
        self,
        hour: int,
        base_prob: Optional[float] = None,
        rng: Optional[np.random.Generator] = None,
    ) -> bool:
        """Decide probabilistically whether a vehicle departs an occupied slot.

        Parameters
        ----------
        hour:
            Current hour (0–23).
        base_prob:
            Override the instance-level ``base_departure_prob``.
        rng:
            NumPy random Generator.  Falls back to ``np.random.default_rng()``
            when ``None``.

        Returns
        -------
        bool
            ``True`` → a vehicle should depart.
        """
        _rng = rng if rng is not None else np.random.default_rng()
        prob = self.get_departure_probability(hour, base_prob)
        return bool(_rng.random() < prob)

    # ------------------------------------------------------------------
    # Convenience / info
    # ------------------------------------------------------------------

    def describe(self, hour: int) -> dict:
        """Return a human-readable summary of the traffic state at *hour*.

        Returns
        -------
        dict
        """
        pattern = self.get_current_pattern(hour)
        arr_prob = self.get_arrival_probability(hour)
        dep_prob = self.get_departure_probability(hour)
        arr_mult, dep_mult = self.get_multipliers(hour)
        return {
            "hour": hour,
            "pattern": pattern.value,
            "arrival_rate_multiplier": arr_mult,
            "departure_rate_multiplier": dep_mult,
            "effective_arrival_prob": arr_prob,
            "effective_departure_prob": dep_prob,
        }

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"TrafficModel(base_arrival={self.base_arrival_prob}, "
            f"base_departure={self.base_departure_prob}, "
            f"is_weekend={self.is_weekend})"
        )
