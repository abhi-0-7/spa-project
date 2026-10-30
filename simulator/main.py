"""
simulator.main
==============
Command-line entry point for the Smart Parking Sensor Network Simulator.
Course: AI372IA — Stream Processing and Analytics, RV College of Engineering.

Usage examples:
    python -m simulator.main --mode normal --rate 50
    python -m simulator.main --mode high_load --rate 500
    python -m simulator.main --mode peak_hour
    python -m simulator.main --mode fault_injection
    python -m simulator.main --mode benchmark --seed 42 --duration 60
    python -m simulator.main --dry-run --rate 100 --duration 10
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from simulator.generator import EventGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("simulator.main")


def load_yaml_config(path: str | Path) -> dict:
    """Load configuration dictionary from YAML file."""
    config_file = Path(path)
    if not config_file.is_file():
        raise FileNotFoundError(f"Configuration file not found: {config_file.resolve()}")
    with open(config_file, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def parse_arguments() -> argparse.Namespace:
    """Parse CLI arguments for simulator execution."""
    parser = argparse.ArgumentParser(
        description="Smart Parking Virtual IoT Sensor Simulator (AI372IA)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        choices=["normal", "high_load", "peak_hour", "fault_injection", "benchmark"],
        default="normal",
        help="Operating simulation mode",
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=None,
        help="Target events per second (overrides mode setting)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=0,
        help="Simulation duration in seconds (0 = run indefinitely until Ctrl+C)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Reproducible random seed",
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config/simulator.yaml",
        help="Path to simulator YAML config",
    )
    parser.add_argument(
        "--kafka-config",
        type=str,
        default="config/kafka.yaml",
        help="Path to kafka YAML config",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate and print events locally without publishing to Kafka",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose DEBUG logging",
    )
    return parser.parse_args()


def run_simulator(args: argparse.Namespace) -> None:
    """Main simulation loop handling rates, publishing, and metrics."""
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("Initializing Smart Parking Simulator [Mode: %s]", args.mode)

    # Load configuration files
    base_dir = Path(__file__).resolve().parent.parent
    config_path = base_dir / args.config if not Path(args.config).is_absolute() else Path(args.config)
    sim_config = load_yaml_config(config_path)

    kafka_cfg_path = base_dir / args.kafka_config if not Path(args.kafka_config).is_absolute() else Path(args.kafka_config)
    kafka_config = load_yaml_config(kafka_cfg_path) if kafka_cfg_path.is_file() else {}

    # Determine mode overrides
    mode_cfg = sim_config.get("modes", {}).get(args.mode, {})
    target_rate = args.rate if args.rate is not None else mode_cfg.get("events_per_second", 50)
    seed = args.seed if args.seed is not None else mode_cfg.get("random_seed", sim_config.get("event_generation", {}).get("random_seed"))

    # Apply mode-specific fault injection overrides
    if args.mode == "fault_injection":
        fi = sim_config.setdefault("fault_injection", {})
        fi["duplicate_probability"] = mode_cfg.get("duplicate_probability", 0.10)
        fi["late_event_probability"] = mode_cfg.get("late_event_probability", 0.15)
        fi["invalid_event_probability"] = mode_cfg.get("invalid_event_probability", 0.05)

    # Initialize generator
    generator = EventGenerator(config=sim_config, seed=seed)

    # Initialize Kafka producer if not dry-run
    producer = None
    topic = kafka_config.get("topics", {}).get("raw_events", {}).get("name", "raw-parking-events")
    bootstrap_servers = kafka_config.get("bootstrap_servers", "localhost:9092")

    if not args.dry_run:
        try:
            from ingestion.producer import ParkingEventProducer
            logger.info("Connecting Kafka producer to %s (Topic: %s)", bootstrap_servers, topic)
            producer = ParkingEventProducer(
                bootstrap_servers=bootstrap_servers,
                topic=topic,
            )
            logger.info("Connected to Kafka successfully.")
        except Exception as e:
            logger.warning("Kafka not reachable (%s). Falling back to dry-run mode.", e)
            producer = None

    stop_event = threading.Event()

    def signal_handler(sig, frame):
        logger.info("Shutdown signal received. Stopping simulator...")
        stop_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    total_events = 0
    successful_sends = 0
    failed_sends = 0
    start_time = time.time()
    last_stat_time = start_time
    events_in_window = 0

    logger.info("Simulation running at target %d events/sec (Duration: %s)",
                target_rate, f"{args.duration}s" if args.duration > 0 else "infinite")

    try:
        # Use generator's continuous event stream
        for event in generator.generate_continuous(target_rate_per_second=target_rate, stop_event=stop_event):
            if stop_event.is_set():
                break

            total_events += 1
            events_in_window += 1

            if producer:
                key = event.get("lot_id", "DEFAULT_LOT")
                try:
                    producer.send_event(event, key=key)
                    successful_sends += 1
                except Exception as exc:
                    failed_sends += 1
                    logger.debug("Producer send error: %s", exc)
            else:
                successful_sends += 1
                if args.verbose or (total_events <= 5):
                    logger.info("EVENT #%d: [%s] Slot %s -> %s (Lat: %.4f, Lon: %.4f)",
                                total_events, event["lot_id"], event["slot_id"],
                                event["status"], event.get("latitude", 0.0), event.get("longitude", 0.0))

            # Duration check
            if args.duration > 0 and (time.time() - start_time) >= args.duration:
                logger.info("Configured duration of %d seconds reached.", args.duration)
                stop_event.set()
                break

            # Metrics reporting
            now = time.time()
            if now - last_stat_time >= 5.0:
                elapsed_win = now - last_stat_time
                current_rate = events_in_window / elapsed_win
                overall_rate = total_events / (now - start_time)
                logger.info("Rate: %.1f ev/s | Overall: %.1f ev/s | Total: %d | Sent: %d | Failed: %d",
                            current_rate, overall_rate, total_events, successful_sends, failed_sends)
                last_stat_time = now
                events_in_window = 0

    finally:
        total_time = time.time() - start_time
        avg_rate = total_events / total_time if total_time > 0 else 0
        logger.info("==================================================")
        logger.info("SIMULATION SUMMARY")
        logger.info("  Mode:               %s", args.mode)
        logger.info("  Total Runtime:      %.2f seconds", total_time)
        logger.info("  Total Generated:    %d events", total_events)
        logger.info("  Average Rate:       %.1f events/second", avg_rate)
        logger.info("  Successfully Sent:  %d events", successful_sends)
        logger.info("  Failed Deliveries:  %d events", failed_sends)
        logger.info("==================================================")

        if producer:
            try:
                producer.flush()
                producer.close()
            except Exception as e:
                logger.error("Error closing Kafka producer: %s", e)


def main():
    args = parse_arguments()
    run_simulator(args)


if __name__ == "__main__":
    main()
