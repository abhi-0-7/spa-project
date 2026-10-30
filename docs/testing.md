# Testing & Quality Assurance Documentation
## Smart Parking Occupancy Analytics
**Course:** AI372IA — Stream Processing and Analytics, RV College of Engineering  

---

## 1. Testing Strategy

The test suite in `tests/` validates the streaming pipeline across four distinct tiers:

1. **Unit Testing (`tests/unit/`):**
   - Pure functional and state logic tests.
   - Requires zero external services or JVM execution.
   - Evaluates:
     - `test_parking_lot.py`: Two-state Finite State Machine (`FREE <-> OCCUPIED`), duplicate event handling, capacity bounding, and availability formulas.
     - `test_validator.py`: Schema validation, mandatory fields, enum checks, timestamp bounding, coordinate validation, and DLQ record generation.
     - `test_generator.py`: Event schema conformance, sequence monotonicity, timestamp format, and reproducible random seed outputs.
     - `test_windowing.py`: Tumbling and sliding window properties, non-overlapping intervals, and slide-duration validation.

2. **Integration Testing (`tests/integration/`):**
   - Verifies inter-component communication.
   - `test_kafka_integration.py`: Validates Kafka producer delivery and dead-letter queue routing. Automatically skips gracefully if Kafka is offline.
   - `test_end_to_end.py`: Tests the full path from event generation to schema validation, InfluxDB ping, and service health check reporting.

3. **Performance Testing (`tests/performance/`):**
   - `test_simulator_throughput.py`: Asserts that the generator sustains 100 and 1,000 events/second with sub-millisecond generation times.

4. **Automated Suite Runner (`tests/run_all_tests.py`):**
   - Automatically discovers and executes all test suites, reporting aggregated success metrics.

---

## 2. Test Execution Commands

```powershell
# Run the complete automated test suite
python tests/run_all_tests.py

# Run unit tests only
python -m unittest discover -s tests/unit -p "test_*.py" -v

# Run performance benchmark tests
python -m unittest discover -s tests/performance -p "test_*.py" -v
```

---

## 3. Verified Test Results

```
=================================================================
      SMART PARKING OCCUPANCY ANALYTICS -- TEST SUITE
      Course: AI372IA -- RV College of Engineering
=================================================================

Tests Run:       43
Errors:          0
Failures:        0
Skipped:         2 (Kafka integration tests skipped when broker offline)
Execution Time:  0.89 seconds
=================================================================
[SUCCESS] ALL TESTS PASSED!
```
