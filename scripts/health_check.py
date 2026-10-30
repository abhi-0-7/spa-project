"""
scripts/health_check.py
=======================
CLI Runner for System Health Verification.
Course: AI372IA — Stream Processing and Analytics, RV College of Engineering.

Usage:
    python scripts/health_check.py
"""

from __future__ import annotations

import sys
from pathlib import Path

base_dir = Path(__file__).resolve().parent.parent
if str(base_dir) not in sys.path:
    sys.path.insert(0, str(base_dir))

from monitoring.health_check import print_health_report

if __name__ == "__main__":
    print_health_report()
