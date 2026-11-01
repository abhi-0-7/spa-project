"""
scripts/reset.py
================
Environment Reset and Checkpoint Cleanup Utility.
Course: AI372IA — Stream Processing and Analytics, RV College of Engineering.

Safely clears checkpoint state and logs after interactive confirmation.
Preserves persistent InfluxDB historical data by default unless explicitly asked.

Usage:
    python scripts/reset.py [--force] [--clear-data]
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Reset Smart Parking Checkpoints and Logs")
    parser.add_argument("--force", action="store_true", help="Bypass interactive confirmation prompt")
    parser.add_argument("--clear-data", action="store_true", help="Also clear persistent InfluxDB container data volumes")
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent.parent

    print("\n" + "=" * 65)
    print("      SMART PARKING ANALYTICS -- RESET UTILITY")
    print("=" * 65)

    if not args.force:
        confirm = input("Are you sure you want to delete checkpoints and runtime logs? [y/N]: ").strip().lower()
        if confirm != "y":
            print("Reset cancelled.")
            return

    # Clear checkpoints
    checkpoints_dir = base_dir / "checkpoints"
    if checkpoints_dir.exists():
        for item in checkpoints_dir.iterdir():
            if item.name == ".gitkeep":
                continue
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()
        print("[OK] Spark checkpoints directory reset.")

    # Clear logs
    logs_dir = base_dir / "logs"
    if logs_dir.exists():
        for item in logs_dir.iterdir():
            if item.name == ".gitkeep":
                continue
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()
        print("[OK] Runtime logs directory reset.")

    if args.clear_data:
        print("[!] Note: To completely reset InfluxDB Docker volumes, execute: docker compose down -v")

    print("[OK] Environment reset complete.\n")


if __name__ == "__main__":
    main()
