# Changelog

All notable changes to `nexir-mvcc-core` will be documented in this file.

The format is based on Keep a Changelog, and this project follows semantic versioning once published.

## [0.1.0] - 2026-07-04

### Added

- Deterministic MVCC engine over byte keys and values.
- Single-key intent lifecycle: `prewrite`, `commit`, and `abort`.
- Multi-key atomic intent batches: `prewrite_batch`, `commit_batch`, and `abort_batch`.
- Direct physical batch fast path via `apply_direct_batch`.
- Version-guarded batch fast path via `apply_guarded_batch`.
- Fast visible-version backend accessors.
- Incremental history-retention GC with cursor and budget support.
- Canonical binary codec for committed versions and intents.
- Reusable backend conformance suite behind the `conformance` feature.
- Property, model, conformance, failure, codec, batch, and GC tests.
- Criterion benchmark suites and synthetic mixed-workload simulation.
- MVCC design, adapter contract, batch/write-set, conflict semantics, GC, testing, and benchmarking documentation.

### Notes

- The core intentionally excludes durable storage engines, consensus implementations, database command parsing, async runtimes, wall-clock usage, and production metrics registries.
