"""
monitoring/health_check.py
==========================
Component Health Checks for Apache Kafka, InfluxDB, Grafana, and Spark UI.
Course: AI372IA — Stream Processing and Analytics, RV College of Engineering.

Usage:
    python -m monitoring.health_check
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Any, Dict

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("monitoring.health_check")


def check_kafka_health(bootstrap_servers: str = "localhost:9092") -> Dict[str, Any]:
    """Inspects Kafka broker availability and metadata with fast socket pre-check."""
    import socket

    host, port_str = bootstrap_servers.split(":")
    port = int(port_str)

    # Fast socket pre-check (1 second) to prevent blocking
    try:
        with socket.create_connection((host, port), timeout=1.0):
            pass
    except Exception as exc:
        return {
            "service": "Apache Kafka (KRaft)",
            "status": "DOWN",
            "endpoint": bootstrap_servers,
            "error": f"Broker connection failed ({exc})",
        }

    try:
        from kafka import KafkaAdminClient
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            request_timeout_ms=2000,
            api_version_auto_timeout_ms=2000,
        )
        topics = admin.list_topics()
        admin.close()
        return {
            "service": "Apache Kafka (KRaft)",
            "status": "UP",
            "endpoint": bootstrap_servers,
            "topics_found": len(topics),
            "topics": sorted(topics),
            "error": None,
        }
    except Exception as exc:
        return {
            "service": "Apache Kafka (KRaft)",
            "status": "DOWN",
            "endpoint": bootstrap_servers,
            "error": str(exc),
        }


def check_influxdb_health(
    url: str = "http://localhost:8086",
    token: str = "parking-super-secret-token",
) -> Dict[str, Any]:
    """Inspects InfluxDB 2.7 REST ping endpoint."""
    try:
        resp = requests.get(f"{url.rstrip('/')}/health", timeout=3)
        if resp.status_code == 200:
            data = resp.json()
            return {
                "service": "InfluxDB 2.7 OSS",
                "status": "UP",
                "endpoint": url,
                "version": data.get("version", "2.7"),
                "error": None,
            }
        return {
            "service": "InfluxDB 2.7 OSS",
            "status": "UNHEALTHY",
            "endpoint": url,
            "status_code": resp.status_code,
            "error": resp.text,
        }
    except Exception as exc:
        return {
            "service": "InfluxDB 2.7 OSS",
            "status": "DOWN",
            "endpoint": url,
            "error": str(exc),
        }


def check_grafana_health(url: str = "http://localhost:3000") -> Dict[str, Any]:
    """Inspects Grafana API health endpoint."""
    try:
        resp = requests.get(f"{url.rstrip('/')}/api/health", timeout=3)
        if resp.status_code == 200:
            data = resp.json()
            return {
                "service": "Grafana 10.4",
                "status": "UP",
                "endpoint": url,
                "version": data.get("version", "10.4"),
                "database": data.get("database", "ok"),
                "error": None,
            }
        return {
            "service": "Grafana 10.4",
            "status": "UNHEALTHY",
            "endpoint": url,
            "error": f"HTTP {resp.status_code}",
        }
    except Exception as exc:
        return {
            "service": "Grafana 10.4",
            "status": "DOWN",
            "endpoint": url,
            "error": str(exc),
        }


def check_spark_ui(url: str = "http://localhost:4040") -> Dict[str, Any]:
    """Inspects Apache Spark Web UI status."""
    try:
        resp = requests.get(f"{url.rstrip('/')}/api/v1/applications", timeout=3)
        if resp.status_code == 200:
            apps = resp.json()
            return {
                "service": "Spark Structured Streaming UI",
                "status": "UP",
                "endpoint": url,
                "active_apps": len(apps),
                "error": None,
            }
        return {
            "service": "Spark Structured Streaming UI",
            "status": "DOWN",
            "endpoint": url,
            "error": f"HTTP {resp.status_code}",
        }
    except Exception as exc:
        return {
            "service": "Spark Structured Streaming UI",
            "status": "INACTIVE (Not currently running)",
            "endpoint": url,
            "error": str(exc),
        }


def check_all_services_health() -> Dict[str, Dict[str, Any]]:
    """Runs health checks on all system services."""
    return {
        "kafka": check_kafka_health(),
        "influxdb": check_influxdb_health(),
        "grafana": check_grafana_health(),
        "spark": check_spark_ui(),
    }


def print_health_report() -> None:
    """Prints a structured ASCII report of service health."""
    results = check_all_services_health()

    print("\n" + "=" * 65)
    print("      SMART PARKING OCCUPANCY ANALYTICS — SYSTEM HEALTH")
    print("      Course: AI372IA — RV College of Engineering")
    print("=" * 65)

    all_green = True
    for key, res in results.items():
        status = res["status"]
        svc = res["service"]
        ep = res["endpoint"]
        mark = "OK" if status == "UP" else ("INFO" if "INACTIVE" in status else "DOWN")

        print(f"[{mark:<4}] {svc:<32} {ep:<18} Status: {status}")
        if res.get("error") and "INACTIVE" not in status:
            print(f"       Notice: {res['error']}")
            all_green = False

    print("=" * 65)
    if all_green:
        print("[OK] All infrastructure services are operational.")
    else:
        print("Notice: Start infrastructure containers with: docker compose up -d")
    print("=" * 65 + "\n")


if __name__ == "__main__":
    print_health_report()
