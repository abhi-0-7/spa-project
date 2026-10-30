"""
tests/run_all_tests.py
======================
Automated Test Suite Runner for Smart Parking Analytics.
Course: AI372IA — Stream Processing and Analytics, RV College of Engineering.

Discovers and runs:
- tests/unit
- tests/integration
- tests/performance

Usage:
    python tests/run_all_tests.py
"""

from __future__ import annotations

import sys
import time
import unittest
from pathlib import Path


def main() -> None:
    test_dir = Path(__file__).resolve().parent
    base_dir = test_dir.parent

    # Ensure project root is in sys.path
    if str(base_dir) not in sys.path:
        sys.path.insert(0, str(base_dir))

    print("\n" + "=" * 65)
    print("      SMART PARKING OCCUPANCY ANALYTICS -- TEST SUITE")
    print("      Course: AI372IA -- RV College of Engineering")
    print("=" * 65)

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Discover tests across unit, integration, and performance directories
    discovered = loader.discover(start_dir=str(test_dir), pattern="test_*.py")
    suite.addTests(discovered)

    start_time = time.time()
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    elapsed = time.time() - start_time

    print("=" * 65)
    print(f"Tests Run:       {result.testsRun}")
    print(f"Errors:          {len(result.errors)}")
    print(f"Failures:        {len(result.failures)}")
    print(f"Skipped:         {len(result.skipped)}")
    print(f"Execution Time:  {elapsed:.2f} seconds")
    print("=" * 65)

    if result.wasSuccessful():
        print("[SUCCESS] ALL TESTS PASSED!\n")
        sys.exit(0)
    else:
        print("[FAILURE] FAILURES OR ERRORS DETECTED.\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
