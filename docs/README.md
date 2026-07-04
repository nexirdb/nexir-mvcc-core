# Nexir MVCC Core Documentation

`nexir-mvcc-core` is the standalone MVCC engine used by Nexir. It provides deterministic versioning, intent handling, guarded writes, batch mutation rules, history retention, and backend conformance checks.

This repository is not the complete Nexir database. It does not include networking, command parsing, consensus, durable storage adapters, metrics backends, idempotency caches, query execution, or operational services. Those responsibilities belong to the database or storage adapter embedding this crate.

## Start Here

- [MVCC design](MVCC_DESIGN.md): core data model, timestamp rules, commit flow, read semantics, and garbage collection.
- [Adapter contract](ADAPTER_CONTRACT.md): exact requirements for durable backends and runtime adapters.
- [Batch/write-set design](BATCH_WRITE_SET_DESIGN.md): direct batches, guarded batches, and intent transactions.
- [Conflict semantics](CONFLICT_SEMANTICS.md): deterministic validation, errors, and retry behavior.
- [History retention and GC](HISTORY_RETENTION_AND_GC.md): safe points, incremental GC, and tombstone retention.
- [Testing strategy](TESTING.md): unit, property, failure, replay, and backend conformance tests.
- [Benchmarking](BENCHMARKING.md): how to run local core benchmarks and how to interpret them.

## Public Scope

The public documentation is intentionally small. It documents the stable core engine behavior and the contracts external adapters must uphold. Internal release notes, local benchmark snapshots, product orchestration notes, and exploratory design material are not part of the public documentation set.
