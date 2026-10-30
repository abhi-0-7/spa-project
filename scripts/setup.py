"""
scripts/setup.py
================
Environment Setup and Diagnostic Initializer for Smart Parking Analytics.
Course: AI372IA — Stream Processing and Analytics, RV College of Engineering.

Usage:
    python scripts/setup.py
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path


def main() -> None:
    print("=" * 65)
    print("      SMART PARKING OCCUPANCY ANALYTICS — SETUP (AI372IA)")
    print("      RV College of Engineering, Bengaluru")
    print("=" * 65)

    base_dir = Path(__file__).resolve().parent.parent
    if str(base_dir) not in sys.path:
        sys.path.insert(0, str(base_dir))

    # 1. Python Environment Check
    py_ver = sys.version_info
    print(f"[*] Python Version: {py_ver.major}.{py_ver.minor}.{py_ver.micro}")
    if py_ver < (3, 10):
        print("[!] Warning: Python 3.10+ recommended.")
    else:
        print("[OK] Python version verified.")

    # 2. Java Environment Check
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        print(f"[OK] JAVA_HOME found: {java_home}")
    else:
        temurin = r"C:\Program Files\Eclipse Adoptium\jdk-8.0.492.9-hotspot"
        if os.path.isdir(temurin):
            print(f"[OK] Detected OpenJDK at {temurin}")
            print(f"     Set in PowerShell: $env:JAVA_HOME = '{temurin}'")
        else:
            print("[WARN] JAVA_HOME not set. Spark requires Java 8, 11, or 17.")

    # 3. Environment Variables (.env)
    env_file = base_dir / ".env"
    env_example = base_dir / ".env.example"
    if not env_file.exists() and env_example.exists():
        shutil.copy(env_example, env_file)
        print("[OK] Created .env configuration from .env.example")
    elif env_file.exists():
        print("[OK] .env configuration already present")

    # 4. Create Runtime Directories
    for folder in ["checkpoints", "logs", "reports/benchmark_results", "reports/plots", "reports/generated_reports"]:
        (base_dir / folder).mkdir(parents=True, exist_ok=True)
    print("[OK] Runtime checkpoints, logs, and report directories created.")

    # 5. Core Module Import Test
    print("\n[*] Validating Core Pipeline Modules...")
    modules = [
        ("simulator.generator", "Virtual IoT Simulator"),
        ("ingestion.schema", "Event Schema & Validation"),
        ("ingestion.producer", "Kafka Producer"),
        ("streaming.state_manager", "Stateful Occupancy Engine"),
        ("streaming.windowing", "Window Aggregations"),
        ("storage.influx_client", "InfluxDB Time-Series Client"),
        ("monitoring.metrics", "Metrics Collector"),
    ]
    all_ok = True
    for mod_name, desc in modules:
        try:
            __import__(mod_name)
            print(f"    [OK] {desc:<30} ({mod_name})")
        except Exception as e:
            print(f"    [FAIL] {desc:<30} ERROR: {e}")
            all_ok = False

    print("\n" + "=" * 65)
    if all_ok:
        print("[SUCCESS] All project modules are installed and verified successfully!")
        print("\nNext steps to run the pipeline:")
        print("  1. Start Docker containers:   docker compose up -d")
        print("  2. Check service health:      python scripts/health_check.py")
        print("  3. Start Sensor Simulator:    python -m simulator.main --mode normal --rate 50")
        print("  4. Start Spark Streaming:     python -m streaming.main")
        print("  5. View Grafana Dashboard:    http://localhost:3000 (admin / parking123)")
    else:
        print("[!] Some modules failed verification. Check logs above.")
    print("=" * 65 + "\n")


if __name__ == "__main__":
    main()
