# Changelog

All notable changes to `nexir-mvcc-core` will be documented in this file.

The format is based on Keep a Changelog, and this project follows semantic versioning once published.

## [0.3.0] - 2026-07-23

### Added

- Added `MvccEngine::plan_key_gc` for deterministic, read-only planning of one
  logical key at a caller-provided safe point.
- Added a per-key history-page budget, explicit obsolete timestamps, a
  separately identified final tombstone, and a conservative completion signal.
- Added coverage for bounded resumption, safe-point retention, tombstone
  collapse, deletion and recreation, opaque payloads, deterministic output, and
  `u128` timestamp ordering.

### Changed

- Added the planner as a public API and advanced the crate from `0.2.0` to
  `0.3.0`.

### Notes

- The public `Backend` trait is unchanged. Its existing bounded
  `get_visible_committed`, `get_latest_commit_ts`, `get_intent`, and
  `get_committed_timestamps_before` operations are sufficient for per-key
  planning without materializing an unbounded version chain.
- Candidate discovery, durable deletion batches, storage-engine integration,
  and payload interpretation remain adapter responsibilities.

## [0.2.0] - 2026-07-23

### Changed

- Widened the opaque ordered `Timestamp` from `u64` to `u128`.
- Encoded every timestamp as exactly 16-byte big-endian data.
- Advanced the record codec version and rejected the old eight-byte timestamp format.
- Updated backend ranges, conformance coverage, property tests, examples, and benchmarks for the wider timestamp domain.

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
