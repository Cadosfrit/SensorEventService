# Ingestion Benchmark Results

---

## System Specifications

- **CPU:** Intel i7-1167G7
- **RAM:** 16 GB
- **OS:** Windows 11

---

## Benchmark Command

The ingestion benchmark was executed using the following Maven test command:

```bash
mvn test -Dtest=com.company.sensor.events.benchmark.BenchmarkTest
```
or you can run it directly from your IDE by executing the `BenchmarkTest` class in the `src/test/java/com/cadosfrit/sensor/event/service/benchmark/BenchmarkTest.java` package.

**Note**
: The prerequisite setup for running benchmark is present in README.md file.

# Ingestion Benchmark Results

This benchmark runs an automated test that loads a batch of 1000 events into the database and measures timing and throughput for both V1 and V2 ingestion services.

---

## Measured Timing for 1000 Events

### EventIngestServiceV1

- **Total Events:** 1000
- **Accepted:** 750
- **Updated:** 175
- **Deduped:** 75
- **Rejected:** 0
- **Time Taken:** 233 ms
- **Throughput:** 4291.85 events/sec

### EventIngestServiceV2

- **Total Events:** 1000
- **Accepted:** 750
- **Updated:** 175
- **Deduped:** 75
- **Rejected:** 0
- **Time Taken:** 154 ms
- **Throughput:** 6493.51 events/sec

**Winner:** EventIngestServiceV2
- **Faster by:** 79 ms (33.91%)

---

## Notes / Optimizations Attempted

### V2 Optimizations Implemented

- Reduced database round-trips by batching writes.
- Optimized deduplication logic to minimize field comparisons.
- Used indexed columns for faster lookups.
- Improved stored procedure logic to handle updates more efficiently.

### Additional Observations

- V2 shows a significant increase in throughput (~1.5x faster).
- Deduplication and update handling are both faster with V2.
- Benchmark is fully transactional; all data is rolled back after completion.

**Note:** All benchmark data is rolled back after test completion, ensuring no permanent changes to the database.
