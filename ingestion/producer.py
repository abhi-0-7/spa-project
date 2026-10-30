"""
ingestion/producer.py — Kafka Producer for Parking Events
==========================================================
Smart Parking Occupancy Analytics · AI372IA
RV College of Engineering, Bengaluru

Provides:
  ParkingEventProducer   — general-purpose producer for the main events topic.
  DLQProducer            — thin wrapper targeting the dead-letter-queue topic.

Design notes
------------
* Uses kafka-python 2.2.11 (pure-Python, no C extensions required).
* Compression is set to 'gzip' to avoid the optional snappy / lz4 C-library
  dependencies on Windows.
* All send operations are *synchronous-by-default* (future.get(timeout=10)),
  which simplifies error handling in an academic context. For higher
  throughput, swap to fire-and-forget with delivery callbacks.
* Producer state (messages_sent, messages_failed, bytes_sent) is tracked via
  simple integer counters — no external metrics library required.
"""

from __future__ import annotations

import json
import logging
from types import TracebackType
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default producer configuration
# ---------------------------------------------------------------------------

_DEFAULT_PRODUCER_CONFIG: dict[str, Any] = {
    "acks": "all",                  # wait for all in-sync replicas
    "retries": 3,
    "compression_type": "gzip",     # gzip: no native C libs required
    "linger_ms": 5,                 # batch micro-delay for throughput
    "request_timeout_ms": 30_000,
    "max_block_ms": 60_000,
    "retry_backoff_ms": 500,
    "max_in_flight_requests_per_connection": 1,  # preserve ordering with retries
}

# Timeout (seconds) when calling future.get() for synchronous confirmation
_SEND_TIMEOUT_SECONDS: int = 10


# ---------------------------------------------------------------------------
# ParkingEventProducer
# ---------------------------------------------------------------------------


class ParkingEventProducer:
    """Kafka producer scoped to a single topic for parking events.

    Parameters
    ----------
    bootstrap_servers:
        Comma-separated ``host:port`` string, e.g. ``"localhost:9092"``.
    topic:
        Kafka topic name to publish events to.
    config:
        Optional dict of additional/override KafkaProducer kwargs.  Values
        here are merged on top of the defaults — use to adjust timeouts,
        batch sizes, etc. without touching the class defaults.

    Examples
    --------
    >>> producer = ParkingEventProducer("localhost:9092", "parking_events")
    >>> producer.send_event({"lot_id": "L1", "slot_id": "S3", ...})
    True
    >>> producer.close()

    Or as a context manager:

    >>> with ParkingEventProducer("localhost:9092", "parking_events") as p:
    ...     p.send_batch(events)
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        config: dict[str, Any] | None = None,
    ) -> None:
        self._topic = topic
        self._bootstrap_servers = bootstrap_servers

        # Merge caller overrides on top of defaults
        producer_kwargs: dict[str, Any] = {**_DEFAULT_PRODUCER_CONFIG}
        if config:
            producer_kwargs.update(config)

        # Serialisers are not overridable via *config* to prevent accidental
        # breakage of the JSON wire format.
        producer_kwargs["bootstrap_servers"] = bootstrap_servers
        producer_kwargs["value_serializer"] = self._json_serializer
        producer_kwargs["key_serializer"] = self._key_serializer

        logger.info(
            "Initialising KafkaProducer | brokers=%r | topic=%r",
            bootstrap_servers,
            topic,
        )
        self._producer: KafkaProducer = KafkaProducer(**producer_kwargs)

        # Internal counters
        self._messages_sent: int = 0
        self._messages_failed: int = 0
        self._bytes_sent: int = 0

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def messages_sent(self) -> int:
        """Total messages successfully acknowledged by the broker."""
        return self._messages_sent

    @property
    def messages_failed(self) -> int:
        """Total messages that encountered a send error."""
        return self._messages_failed

    @property
    def bytes_sent(self) -> int:
        """Approximate total payload bytes sent (value bytes only)."""
        return self._bytes_sent

    # ------------------------------------------------------------------
    # Public send methods
    # ------------------------------------------------------------------

    def send_event(
        self,
        event: dict[str, Any],
        key: str | None = None,
    ) -> bool:
        """Publish a single parking event to the configured topic.

        Parameters
        ----------
        event:
            A validated parking event dict.
        key:
            Kafka message key string.  Defaults to ``event['lot_id']``.
            Using ``lot_id`` as the key ensures all events from the same
            lot land in the same partition (ordered processing per lot).

        Returns
        -------
        bool
            ``True`` if the broker acknowledged the message; ``False`` on
            any error.
        """
        message_key: str = key or event.get("lot_id", "unknown")
        try:
            future = self._producer.send(
                self._topic,
                value=event,
                key=message_key,
            )
            record_metadata = future.get(timeout=_SEND_TIMEOUT_SECONDS)
            payload_bytes = len(json.dumps(event).encode("utf-8"))
            self._messages_sent += 1
            self._bytes_sent += payload_bytes
            logger.debug(
                "Event sent | topic=%s | partition=%d | offset=%d | "
                "event_id=%r | bytes=%d",
                record_metadata.topic,
                record_metadata.partition,
                record_metadata.offset,
                event.get("event_id"),
                payload_bytes,
            )
            return True
        except KafkaTimeoutError as exc:
            self._messages_failed += 1
            logger.error(
                "Kafka timeout sending event | topic=%r | event_id=%r | "
                "lot_id=%r | error=%s",
                self._topic,
                event.get("event_id"),
                event.get("lot_id"),
                exc,
            )
            return False
        except KafkaError as exc:
            self._messages_failed += 1
            logger.error(
                "KafkaError sending event | topic=%r | event_id=%r | "
                "lot_id=%r | error=%s",
                self._topic,
                event.get("event_id"),
                event.get("lot_id"),
                exc,
            )
            return False
        except Exception as exc:  # noqa: BLE001
            self._messages_failed += 1
            logger.exception(
                "Unexpected error sending event | topic=%r | event_id=%r | "
                "error=%s",
                self._topic,
                event.get("event_id"),
                exc,
            )
            return False

    def send_batch(
        self,
        events: list[dict[str, Any]],
    ) -> tuple[int, int]:
        """Publish a list of events, returning success and error counts.

        Parameters
        ----------
        events:
            List of parking event dicts.

        Returns
        -------
        tuple[int, int]
            ``(success_count, error_count)`` after processing all events.
        """
        success_count = 0
        error_count = 0
        for event in events:
            if self.send_event(event):
                success_count += 1
            else:
                error_count += 1
        logger.info(
            "Batch send complete | topic=%r | total=%d | success=%d | error=%d",
            self._topic,
            len(events),
            success_count,
            error_count,
        )
        return success_count, error_count

    def send_dlq_event(self, dlq_record: dict[str, Any]) -> bool:
        """Publish a DLQ record using its ``original_payload.lot_id`` as key.

        This convenience method allows a :class:`ParkingEventProducer`
        targeting the DLQ topic to accept structured DLQ records directly.

        Parameters
        ----------
        dlq_record:
            A dict as returned by
            :meth:`ingestion.validator.EventValidator.create_dlq_record`.

        Returns
        -------
        bool
            ``True`` on success, ``False`` on error.
        """
        original = dlq_record.get("original_payload", {})
        key: str = original.get("lot_id", "dlq-unknown")
        return self.send_event(dlq_record, key=key)

    # ------------------------------------------------------------------
    # Lifecycle methods
    # ------------------------------------------------------------------

    def flush(self) -> None:
        """Flush all pending messages in the producer's internal buffer.

        Blocks until all outstanding send requests have completed or timed
        out.  Call before application shutdown to avoid data loss.
        """
        logger.debug("Flushing producer buffer | topic=%r", self._topic)
        self._producer.flush()

    def close(self) -> None:
        """Flush and close the underlying KafkaProducer gracefully."""
        logger.info(
            "Closing producer | topic=%r | sent=%d | failed=%d | bytes=%d",
            self._topic,
            self._messages_sent,
            self._messages_failed,
            self._bytes_sent,
        )
        try:
            self._producer.flush()
            self._producer.close()
        except KafkaError as exc:
            logger.error("Error closing KafkaProducer: %s", exc)

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "ParkingEventProducer":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        self.close()
        return False  # do not suppress exceptions

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def get_stats(self) -> dict[str, Any]:
        """Return a snapshot of producer metrics.

        Returns
        -------
        dict
            Keys: ``topic``, ``bootstrap_servers``, ``messages_sent``,
            ``messages_failed``, ``bytes_sent``, ``success_rate``.
        """
        total = self._messages_sent + self._messages_failed
        success_rate = (self._messages_sent / total * 100.0) if total > 0 else 0.0
        return {
            "topic": self._topic,
            "bootstrap_servers": self._bootstrap_servers,
            "messages_sent": self._messages_sent,
            "messages_failed": self._messages_failed,
            "bytes_sent": self._bytes_sent,
            "success_rate_pct": round(success_rate, 2),
        }

    # ------------------------------------------------------------------
    # Serialisers (static helpers)
    # ------------------------------------------------------------------

    @staticmethod
    def _json_serializer(value: Any) -> bytes:
        """Serialise *value* to compact UTF-8 JSON bytes."""
        return json.dumps(value, default=str, separators=(",", ":")).encode("utf-8")

    @staticmethod
    def _key_serializer(key: str) -> bytes:
        """Encode the string message key to UTF-8 bytes."""
        return key.encode("utf-8") if isinstance(key, str) else str(key).encode("utf-8")


# ---------------------------------------------------------------------------
# DLQProducer
# ---------------------------------------------------------------------------


class DLQProducer:
    """Dead-Letter Queue producer — wraps :class:`ParkingEventProducer`.

    Targets the DLQ topic specifically and exposes a focused API for
    publishing failed / invalid event records.

    Parameters
    ----------
    bootstrap_servers:
        Comma-separated ``host:port`` string.
    dlq_topic:
        DLQ Kafka topic name.
    config:
        Optional extra KafkaProducer configuration overrides.

    Examples
    --------
    >>> dlq = DLQProducer("localhost:9092", "parking_events_dlq")
    >>> dlq.publish(dlq_record)
    True
    >>> dlq.close()
    """

    def __init__(
        self,
        bootstrap_servers: str,
        dlq_topic: str,
        config: dict[str, Any] | None = None,
    ) -> None:
        self._dlq_topic = dlq_topic
        self._inner = ParkingEventProducer(
            bootstrap_servers=bootstrap_servers,
            topic=dlq_topic,
            config=config,
        )
        logger.info(
            "DLQProducer ready | topic=%r | brokers=%r",
            dlq_topic,
            bootstrap_servers,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def publish(self, dlq_record: dict[str, Any]) -> bool:
        """Send a DLQ record to the dead-letter topic.

        Parameters
        ----------
        dlq_record:
            Structured DLQ record (as built by
            :meth:`ingestion.validator.EventValidator.create_dlq_record`).

        Returns
        -------
        bool
            ``True`` on broker acknowledgement, ``False`` on failure.
        """
        return self._inner.send_dlq_event(dlq_record)

    def publish_batch(
        self,
        dlq_records: list[dict[str, Any]],
    ) -> tuple[int, int]:
        """Publish multiple DLQ records.

        Returns
        -------
        tuple[int, int]
            ``(success_count, error_count)``
        """
        success, error = 0, 0
        for record in dlq_records:
            if self.publish(record):
                success += 1
            else:
                error += 1
        logger.info(
            "DLQ batch complete | topic=%r | success=%d | error=%d",
            self._dlq_topic,
            success,
            error,
        )
        return success, error

    def flush(self) -> None:
        """Flush the underlying producer buffer."""
        self._inner.flush()

    def close(self) -> None:
        """Gracefully shut down the DLQ producer."""
        self._inner.close()

    def get_stats(self) -> dict[str, Any]:
        """Delegate stats retrieval to the inner producer."""
        stats = self._inner.get_stats()
        stats["producer_type"] = "DLQ"
        return stats

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "DLQProducer":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        self.close()
        return False
