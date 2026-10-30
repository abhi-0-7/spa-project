"""
scripts/stop.py
===============
Graceful Teardown for Smart Parking Analytics Infrastructure.
Course: AI372IA — Stream Processing and Analytics, RV College of Engineering.

Usage:
    python scripts/stop.py
"""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("scripts.stop")


def main() -> None:
    base_dir = Path(__file__).resolve().parent.parent
    logger.info("Stopping Docker Compose infrastructure services...")
    try:
        res = subprocess.run(["docker", "compose", "down"], cwd=base_dir, capture_output=True, text=True)
        if res.returncode == 0:
            logger.info("Docker Compose services stopped cleanly.")
        else:
            logger.warning("docker compose down returned code %d: %s", res.returncode, res.stderr.strip())
    except Exception as e:
        logger.warning("Could not execute docker CLI (%s). Stop containers in Docker Desktop.", e)


if __name__ == "__main__":
    main()
