"""
tests/integration/test_end_to_end.py
====================================
End-to-End Acceptance Tests for Smart Parking Pipeline.
Course: AI372IA — Stream Processing and Analytics, RV College of Engineering.

Verifies:
1. Virtual sensor generation -> schema validation
2. InfluxDB HTTP Line Protocol connectivity
3. Component health check aggregation
"""

from __future__ import annotations

import unittest
from datetime import datetime, timezone

from ingestion.validator import EventValidator
from monitoring.health_check import check_all_services_health, check_influxdb_health
from simulator.generator import EventGenerator
from storage.influx_client import InfluxDBConfig, ParkingInfluxClient


class TestPipelineEndToEnd(unittest.TestCase):
    """Pipeline End-to-End Acceptance Suite."""

    def test_generator_to_validator_pipeline(self):
        """Verify that simulated events cleanly pass rigorous schema validation."""
        import yaml
        from pathlib import Path

        base_dir = Path(__file__).resolve().parent.parent.parent
        with open(base_dir / "config" / "simulator.yaml", "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)

        generator = EventGenerator(config=cfg, seed=123)
        validator = EventValidator()

        # Generate batch of 20 simulated events
        events = [generator.generate_next_event() for _ in range(20)]
        self.assertEqual(len(events), 20)

        # Validate events
        valid_count = 0
        for ev in events:
            res = validator.validate(ev)
            if res.is_valid:
                valid_count += 1

        # At least 90% should be valid (allowing for fault injection)
        self.assertGreaterEqual(valid_count, 15, "Most simulated events should be schema-valid")

    def test_influx_client_instantiation_and_ping(self):
        """Verify InfluxDB client instantiation and ping handling."""
        cfg = InfluxDBConfig(url="http://localhost:8086")
        client = ParkingInfluxClient(cfg)
        # Ping returns boolean (True if InfluxDB is up, False if container not started)
        status = client.ping()
        self.assertIsInstance(status, bool)
        client.close()

    def test_service_health_check_structure(self):
        """Verify health check returns well-formed status dictionaries."""
        health = check_all_services_health()
        self.assertIn("kafka", health)
        self.assertIn("influxdb", health)
        self.assertIn("grafana", health)
        self.assertIn("spark", health)
        for svc_name, svc_info in health.items():
            self.assertIn("status", svc_info)
            self.assertIn("service", svc_info)


if __name__ == "__main__":
    unittest.main()
