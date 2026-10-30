"""
ingestion/validator.py — Event Validation & Dead-Letter Queue Record Builder
=============================================================================
Smart Parking Occupancy Analytics · AI372IA
RV College of Engineering, Bengaluru

Provides:
  ValidationResult      — immutable dataclass holding validation outcome.
  EventValidator        — validates individual events and builds DLQ records.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from ingestion.schema import (
    REQUIRED_FIELDS,
    VALID_SCHEMA_VERSIONS,
    VALID_SOURCES,
    VALID_STATUSES,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DLQ_SCHEMA_VERSION: str = "1.0"
_MAX_FUTURE_DELTA: timedelta = timedelta(hours=24)
_MAX_PAST_DELTA: timedelta = timedelta(days=7)


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ValidationResult:
    """Immutable result produced by :class:`EventValidator`.

    Attributes
    ----------
    is_valid:
        ``True`` when the event passed all mandatory checks.
    errors:
        List of human-readable error strings.  Non-empty when *is_valid* is
        ``False``.
    warnings:
        List of non-fatal advisory strings (event is still considered valid).
    raw_event:
        The original event dict, preserved for DLQ construction and logging.
    """

    is_valid: bool
    errors: list[str]
    warnings: list[str]
    raw_event: dict[str, Any]

    def __str__(self) -> str:  # noqa: D105
        status = "VALID" if self.is_valid else "INVALID"
        return (
            f"ValidationResult({status}, errors={self.errors}, "
            f"warnings={self.warnings})"
        )


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------


class EventValidator:
    """Validates parking events against the canonical schema v1.0.

    All checks are independent — the validator accumulates *all* errors in a
    single pass so that downstream teams can fix multiple issues at once.

    Usage
    -----
    >>> v = EventValidator()
    >>> result = v.validate(event_dict)
    >>> if not result.is_valid:
    ...     dlq = v.create_dlq_record(event_dict, result.errors, "ingestion")
    """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate(self, event: dict[str, Any]) -> ValidationResult:
        """Validate a single parking event dict.

        Parameters
        ----------
        event:
            A dict deserialized from the Kafka JSON message value.

        Returns
        -------
        ValidationResult
            Populated with all errors and/or warnings discovered.
        """
        errors: list[str] = []
        warnings: list[str] = []

        if not isinstance(event, dict):
            errors.append(
                f"Event must be a dict, got {type(event).__name__!r}."
            )
            return ValidationResult(
                is_valid=False,
                errors=errors,
                warnings=warnings,
                raw_event=event if isinstance(event, dict) else {},
            )

        # 1. event_id present and non-empty
        self._check_required_string(event, "event_id", errors)

        # 2. lot_id present and non-empty
        self._check_required_string(event, "lot_id", errors)

        # 3. slot_id present and non-empty
        self._check_required_string(event, "slot_id", errors)

        # 4. status ∈ {OCCUPIED, FREE}
        self._check_enum(event, "status", VALID_STATUSES, errors)

        # 5 & 6. Timestamp checks
        event_ts = self._check_iso8601_utc(event, "event_timestamp", errors)
        _ingest_ts = self._check_iso8601_utc(event, "ingestion_timestamp", errors)

        # 7. event_timestamp not more than 24 h in the future
        # 8. event_timestamp not more than 7 days in the past
        if event_ts is not None:
            now_utc = datetime.now(tz=timezone.utc)
            if event_ts > now_utc + _MAX_FUTURE_DELTA:
                errors.append(
                    f"'event_timestamp' is more than {_MAX_FUTURE_DELTA} "
                    f"in the future: {event_ts.isoformat()!r}."
                )
            if event_ts < now_utc - _MAX_PAST_DELTA:
                errors.append(
                    f"'event_timestamp' is more than {_MAX_PAST_DELTA} "
                    f"in the past: {event_ts.isoformat()!r}."
                )

        # 9. event_sequence >= 0
        self._check_non_negative_integer(event, "event_sequence", errors)

        # 10. source is a known value
        self._check_enum(event, "source", VALID_SOURCES, errors)

        # 11. schema_version is supported
        self._check_enum(event, "schema_version", VALID_SCHEMA_VERSIONS, errors)

        # 12. If latitude/longitude present, validate coordinate ranges
        self._check_coordinate(event, "latitude", -90.0, 90.0, errors, warnings)
        self._check_coordinate(event, "longitude", -180.0, 180.0, errors, warnings)

        # Advisory: all required fields present? (belt-and-suspenders)
        missing = [f for f in REQUIRED_FIELDS if f not in event]
        if missing:
            for m in missing:
                errors.append(f"Required field missing: '{m}'.")

        # Advisory: battery_level range (does not make event invalid by itself
        # if lat/lon pair is partial — log warning)
        lat_present = "latitude" in event and event["latitude"] is not None
        lon_present = "longitude" in event and event["longitude"] is not None
        if lat_present != lon_present:
            warnings.append(
                "Only one of 'latitude' / 'longitude' is present; "
                "both must be provided for geo-queries."
            )

        battery = event.get("battery_level")
        if battery is not None:
            try:
                b = float(battery)
                if b < 20.0:
                    warnings.append(
                        f"Sensor battery low: {b:.1f}% "
                        f"(sensor_id={event.get('sensor_id', 'unknown')!r})."
                    )
            except (TypeError, ValueError):
                errors.append(
                    f"'battery_level' must be a number, got {battery!r}."
                )

        is_valid = len(errors) == 0

        if not is_valid:
            logger.debug(
                "Event validation failed | event_id=%r | errors=%s",
                event.get("event_id"),
                errors,
            )

        return ValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            raw_event=event,
        )

    def validate_batch(
        self, events: list[dict[str, Any]]
    ) -> list[ValidationResult]:
        """Validate a list of events, returning one result per event.

        Parameters
        ----------
        events:
            Iterable of raw event dicts.

        Returns
        -------
        list[ValidationResult]
            Same length and order as *events*.
        """
        results: list[ValidationResult] = []
        for idx, event in enumerate(events):
            result = self.validate(event)
            if not result.is_valid:
                logger.warning(
                    "Batch validation: event[%d] invalid | event_id=%r | "
                    "error_count=%d",
                    idx,
                    event.get("event_id"),
                    len(result.errors),
                )
            results.append(result)

        valid_count = sum(1 for r in results if r.is_valid)
        logger.info(
            "Batch validation complete | total=%d | valid=%d | invalid=%d",
            len(results),
            valid_count,
            len(results) - valid_count,
        )
        return results

    @staticmethod
    def create_dlq_record(
        event: dict[str, Any],
        errors: list[str],
        stage: str,
    ) -> dict[str, Any]:
        """Build a dead-letter-queue (DLQ) record for a failed event.

        Parameters
        ----------
        event:
            The original (possibly malformed) event dict.
        errors:
            List of validation or processing error strings.
        stage:
            Pipeline stage where the failure occurred
            (e.g. ``"validation"``, ``"ingestion"``, ``"deserialization"``).

        Returns
        -------
        dict
            A DLQ record that can be serialised to JSON and sent to the
            DLQ Kafka topic or written to an error store.
        """
        return {
            "dlq_schema_version": _DLQ_SCHEMA_VERSION,
            "failure_timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "validation_stage": stage,
            "errors": errors,
            "original_payload": event,
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _check_required_string(
        event: dict[str, Any],
        field_name: str,
        errors: list[str],
    ) -> None:
        """Append an error if *field_name* is absent or an empty string."""
        value = event.get(field_name)
        if value is None:
            errors.append(f"Required field '{field_name}' is missing.")
        elif not isinstance(value, str):
            errors.append(
                f"'{field_name}' must be a string, got {type(value).__name__!r}."
            )
        elif not value.strip():
            errors.append(f"'{field_name}' must not be empty or whitespace-only.")

    @staticmethod
    def _check_enum(
        event: dict[str, Any],
        field_name: str,
        allowed: set[str],
        errors: list[str],
    ) -> None:
        """Append an error if *field_name* value is not in *allowed*."""
        value = event.get(field_name)
        if value is None:
            errors.append(f"Required field '{field_name}' is missing.")
        elif value not in allowed:
            errors.append(
                f"'{field_name}' must be one of {sorted(allowed)!r}, "
                f"got {value!r}."
            )

    @staticmethod
    def _check_iso8601_utc(
        event: dict[str, Any],
        field_name: str,
        errors: list[str],
    ) -> datetime | None:
        """Return parsed datetime (UTC) or ``None`` after appending an error."""
        raw = event.get(field_name)
        if raw is None:
            errors.append(f"Required field '{field_name}' is missing.")
            return None
        if not isinstance(raw, str):
            errors.append(
                f"'{field_name}' must be a string (ISO 8601), "
                f"got {type(raw).__name__!r}."
            )
            return None
        # Normalise common Z suffix → +00:00 for Python < 3.11 compatibility
        normalised = raw.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(normalised)
        except ValueError:
            errors.append(
                f"'{field_name}' is not a valid ISO 8601 datetime: {raw!r}."
            )
            return None
        if dt.tzinfo is None:
            errors.append(
                f"'{field_name}' must include a UTC timezone (e.g. 'Z' or "
                f"'+00:00'), got naive datetime: {raw!r}."
            )
            return None
        return dt.astimezone(timezone.utc)

    @staticmethod
    def _check_non_negative_integer(
        event: dict[str, Any],
        field_name: str,
        errors: list[str],
    ) -> None:
        """Append an error if *field_name* is not an integer >= 0."""
        value = event.get(field_name)
        if value is None:
            errors.append(f"Required field '{field_name}' is missing.")
            return
        if not isinstance(value, int) or isinstance(value, bool):
            errors.append(
                f"'{field_name}' must be an integer, got {type(value).__name__!r}."
            )
            return
        if value < 0:
            errors.append(
                f"'{field_name}' must be >= 0, got {value!r}."
            )

    @staticmethod
    def _check_coordinate(
        event: dict[str, Any],
        field_name: str,
        min_val: float,
        max_val: float,
        errors: list[str],
        warnings: list[str],  # noqa: ARG004  (reserved for future use)
    ) -> None:
        """Validate an optional geographic coordinate field."""
        value = event.get(field_name)
        if value is None:
            return  # optional field — absence is fine
        try:
            num = float(value)
        except (TypeError, ValueError):
            errors.append(
                f"'{field_name}' must be a number, got {value!r}."
            )
            return
        if not (min_val <= num <= max_val):
            errors.append(
                f"'{field_name}' must be in [{min_val}, {max_val}], got {num!r}."
            )
