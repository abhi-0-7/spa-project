"""
tests/integration/test_kafka_integration.py
============================================
Integration Tests for Apache Kafka Producer & DLQ Routing.
Course: AI372IA — Stream Processing and Analytics, RV College of Engineering.

Requires Kafka broker active at localhost:9092.
Tests are gracefully skipped if Kafka is unreachable.
"""

from __future__ import annotations

import unittest
from datetime import datetime, timezone

from ingestion.producer import DLQProducer, ParkingEventProducer
from monitoring.health_check import check_kafka_health


class TestKafkaIntegration(unittest.TestCase):
    """Kafka Integration Test Suite."""

    @classmethod
    def setUpClass(cls):
        status = check_kafka_health("localhost:9092")
        cls.kafka_available = status["status"] == "UP"
        if not cls.kafka_available:
            print("\n[INFO] Skipping Kafka integration tests (broker not reachable at localhost:9092).")

    def test_kafka_producer_single_event(self):
        """Verify that ParkingEventProducer connects and delivers a valid message."""
        if not self.kafka_available:
            self.skipTest("Kafka broker not available")

        producer = ParkingEventProducer(
            bootstrap_servers="localhost:9092",
            topic="raw-parking-events",
        )
        now_utc = datetime.now(timezone.utc).isoformat()
        event = {
            "event_id": "EVT_INT_001",
            "lot_id": "LOT_KORAMANGALA_01",
            "slot_id": "LOT_KORAMANGALA_01-S001",
            "zone_id": "ZONE_SOUTH",
            "city": "Bengaluru",
            "status": "OCCUPIED",
            "event_timestamp": now_utc,
            "ingestion_timestamp": now_utc,
            "source": "SIMULATED_SENSOR",
            "event_sequence": 999,
            "schema_version": "1.0",
        }
        success = producer.send_event(event, key="LOT_KORAMANGALA_01")
        producer.flush()
        producer.close()
        self.assertTrue(success, "Producer should successfully publish event to Kafka")

    def test_dlq_producer_delivery(self):
        """Verify that DLQProducer correctly routes quarantined records."""
        if not self.kafka_available:
            self.skipTest("Kafka broker not available")

        dlq_producer = DLQProducer(
            bootstrap_servers="localhost:9092",
            dlq_topic="parking-events-dlq",
        )
        malformed = {
            "original_payload": {"bad": "record"},
            "failure_reason": ["Missing event_id", "Invalid status"],
            "failure_timestamp": datetime.now(timezone.utc).isoformat(),
            "validation_stage": "SPARK_SCHEMA_VALIDATOR",
        }
        success = dlq_producer.publish(malformed)
        dlq_producer.flush()
        dlq_producer.close()
        self.assertTrue(success, "DLQ producer should successfully publish to DLQ topic")


if __name__ == "__main__":
    unittest.main()
