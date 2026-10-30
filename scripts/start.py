"""
scripts/start.py
================
Startup Coordinator for Smart Parking Analytics Infrastructure.
Course: AI372IA — Stream Processing and Analytics, RV College of Engineering.

Usage:
    python scripts/start.py [--skip-docker]
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path

from monitoring.health_check import check_all_services_health

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("scripts.start")


def main() -> None:
    parser = argparse.ArgumentParser(description="Start Smart Parking Infrastructure")
    parser.add_argument("--skip-docker", action="store_true", help="Skip running docker compose up")
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent.parent

    print("\n" + "=" * 65)
    print("      STARTING SMART PARKING ANALYTICS INFRASTRUCTURE")
    print("=" * 65)

    if not args.skip_docker:
        logger.info("Starting Docker Compose services (Kafka, InfluxDB, Grafana)...")
        try:
            cmd = ["docker", "compose", "up", "-d"]
            res = subprocess.run(cmd, cwd=base_dir, capture_output=True, text=True)
            if res.returncode == 0:
                logger.info("Docker Compose up command succeeded.")
            else:
                logger.warning("docker compose command failed (%s). If Docker is running manually, continuing...", res.stderr.strip())
        except Exception as exc:
            logger.warning("Could not invoke docker CLI directly (%s). Ensure Docker Desktop is running.", exc)

    logger.info("Waiting for infrastructure services to become healthy...")
    max_wait = 30
    start = time.time()
    while time.time() - start < max_wait:
        results = check_all_services_health()
        kafka_up = results["kafka"]["status"] == "UP"
        influx_up = results["influxdb"]["status"] == "UP"
        grafana_up = results["grafana"]["status"] == "UP"

        if kafka_up and influx_up and grafana_up:
            logger.info("All services (Kafka, InfluxDB, Grafana) are healthy and UP!")
            break
        time.sleep(3)

    print("\n" + "=" * 65)
    print("INFRASTRUCTURE READY")
    print("  • Grafana Dashboard:   http://localhost:3000 (admin / admin)")
    print("  • InfluxDB Web UI:     http://localhost:8086 (admin / parking-admin-password)")
    print("  • Kafka Broker:        localhost:9092")
    print("\nTo start real-time data streaming:")
    print("  Terminal 1 (Simulator): python -m simulator.main --mode normal --rate 50")
    print("  Terminal 2 (Spark):     python -m streaming.main")
    print("=" * 65 + "\n")


if __name__ == "__main__":
    main()
