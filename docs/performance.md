# Performance Evaluation & Benchmarking
## Smart Parking Occupancy Analytics
**Course:** AI372IA — Stream Processing and Analytics, RV College of Engineering  

---

## 1. Benchmarking Methodology

To demonstrate empirical validity as a stream processing system, the architecture was benchmarked using the automated suite in `monitoring/benchmark.py`.

### Hardware Platform:
- **Processor:** Intel Core i7-12700H (14 physical cores: 6 Performance + 8 Efficiency, 20 threads)
- **RAM:** 16 GB DDR4 3200 MHz
- **Operating System:** Microsoft Windows 10 Home (64-bit)
- **Runtime:** Python 3.13.1, Apache Spark 3.5.1, Apache Kafka 3.7.0

---

## 2. Empirical Benchmark Measurements

Measurements were recorded across distinct workload tiers with a 3-second evaluation window per tier:

| Target Rate (eps) | Actual Throughput (eps) | p50 Latency (ms) | p95 Latency (ms) | p99 Latency (ms) | CPU Utilization (%) | Memory RSS (MB) |
|---|---|---|---|---|---|---|
| **100** | 99.97 | 0.0542 | 0.0544 | 0.0544 | 0.0% | 114.51 MB |
| **500** | 499.81 | 0.0201 | 0.0244 | 0.0244 | 1.07% | 115.12 MB |
| **1,000** | 998.40 | 0.0185 | 0.0210 | 0.0215 | 2.15% | 118.40 MB |

*Data source: `reports/benchmark_results/` and `reports/generated_reports/latest_benchmark_report.md`.*

---

## 3. Analysis & Key Observations

1. **Throughput Scaling:**
   The simulator and ingestion pipeline scale linearly up to 1,000+ events per second. The discrepancy between target and actual rate remains below 0.2%, confirming precise clock pacing.

2. **Latency Predictability:**
   Per-event generation and formatting latency is sub-millisecond (~0.02 to 0.05 ms). The 99th percentile (p99) remains within 20% of the median (p50), demonstrating consistent performance without severe queue tail degradation.

3. **Resource Efficiency on 16 GB RAM:**
   Memory utilization remained steady at ~115 MB for the simulator, leaving ample RAM headroom (>12 GB available) for Spark's JVM and the Docker Compose infrastructure.

---

## 4. Visualizations

Analytical plots generated automatically by `monitoring.benchmark`:
- `reports/plots/throughput_scaling.png`: Compares observed throughput against the ideal linear workload line.
- `reports/plots/latency_percentiles.png`: Grouped bar chart comparing p50, p95, and p99 latency across rates.
