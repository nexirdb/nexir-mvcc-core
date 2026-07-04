# MVCC Core Benchmarking

This library includes a standalone performance benchmarking harness to measure the deterministic MVCC kernel operations.

These benchmarks intentionally measure **only** the pure CPU and in-memory cost of the MVCC rules engine: reads, validation, guards, batch validation, conflict checks, canonical codec serialization, and in-memory application.

**IMPORTANT**: These numbers are in-memory core metrics. They explicitly exclude network latency, durable persistence, consensus rounds, command parsing, and external idempotency caches. An adapter integrating this core will experience different total system throughput.

## Why Timing is Restricted to `benches/`

The MVCC core kernel is designed to be deterministic so it can execute inside an ordered storage or replicated-state-machine apply path. Consequently:
- The core library (`src/`) **never** calls `SystemTime` or `Instant`.
- Measuring operations and tracking elapsed time is strictly limited to the `benches/` test harness via the `criterion` framework.
- Timestamps are explicitly supplied to the API externally.

## How to Run Benchmarks

The benchmark suite leverages the [Criterion](https://github.com/bheisler/criterion.rs) framework for statistically rigorous measurements.

To run the full suite:
```bash
cargo bench
```

To run the release-readiness truth benchmark with a shortened local run:
```bash
cargo bench --bench perf_truth -- --sample-size 10 --warm-up-time 0.1 --measurement-time 0.1
```

To run a specific benchmark group from the older core benchmark harness (e.g., only reads):
```bash
cargo bench --bench core_bench -- Reads
```

To run the synthetic command-style mixed workload simulation:
```bash
cargo run --example command_load_sim --release
```

## Workloads Covered

1. **Reads**: Point reads of recent keys and historical iteration of hot keys with deep version chains.
2. **Intent Path**: Prewrite, commit, abort, and active intent conflict detection.
3. **Direct Batches**: Simulating blind writes. Tests scale across batch sizes (1, 10, 100, 1000) and varying payload sizes (0B, 32B, 1KiB, 64KiB) to observe throughput scaling.
4. **Guarded Batches**: Simulating read-modify-write and conditional-write commands. Covers both successful guard passes and fail-fast stale write paths.
5. **Garbage Collection**: Simulating the core scanning and pruning historical versions below safe points.
6. **Codec Speed**: Pure binary encoding/decoding overhead for intents and versions.

Benchmark results depend on hardware, compiler settings, and selected features. Keep local baseline snapshots outside the public documentation set unless they are generated as part of a formal release artifact.
