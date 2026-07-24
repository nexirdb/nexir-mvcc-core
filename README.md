# nexir-mvcc-core

A standalone, deterministic Multi-Version Concurrency Control (MVCC) core library used by Nexir.

`nexir-mvcc-core` provides the versioning, intent, guarded-write, batch mutation, and history-retention primitives required to build a transactional key-value storage layer. It is intentionally limited to the MVCC engine. This repository is not the complete Nexir database and does not include networking, command parsing, consensus, durable storage adapters, idempotency caches, query execution, metrics, or operational services.

## Key Features

- **Strict Separation of Concerns**: Core logic runs deterministically in memory. All disk/durable state is managed through a clean `Backend` trait.
- **Timestamp-Based Ordering**: Native support for caller-supplied, opaque `u128` logical timestamps (`start_ts`, `commit_ts`, `read_ts`).
- **Two-Phase Intent Transactions**: First-class support for single-key and multi-key intent transactions via `prewrite`, `commit`, `abort`, `prewrite_batch`, `commit_batch`, and `abort_batch`.
- **Atomic Batches**: Support for multi-key `prewrite_batch`, `commit_batch`, and `abort_batch` ensures all-or-nothing transactional guarantees.
- **Direct & Guarded Fast Paths**: `apply_direct_batch` and `apply_guarded_batch` bypass the intent system for high-performance single-roundtrip writes, complete with Compare-And-Swap (CAS) guard validation.
- **Incremental GC**: Deterministic history retention with `gc_incremental`, cursor resumption, and caller-supplied work budgets.
- **Per-Key GC Planning**: Read-only, bounded plans with explicit obsolete timestamps for adapters that own their own work discovery and atomic persistence.
- **Backend Conformance Suite**: Built-in test macros via the `conformance` feature to easily verify that your custom storage layer meets the logical backend contract (note: adapters must still provide their own crash atomicity tests).

## Scope and Integration

`nexir-mvcc-core` sits between an adapter/runtime layer and a durable backend implementation.

1. Implement the `Backend` trait for your storage engine or replicated state machine.
2. Wrap your `Backend` in the `MvccEngine`.
3. Map external reads, transactions, and conditional writes to the engine methods.

The core intentionally does not own:

- async runtimes or background tasks,
- consensus types,
- durable database bindings,
- command or document parsing,
- idempotency/replay caches,
- production metrics registries.

Adapters own those concerns and must serialize mutation apply for each ordered write domain.

The core treats every `CommittedVersion.value` as opaque bytes. It decides only
generic MVCC visibility, keeper, safe-point, and final-tombstone semantics.
Adapters own payload validation, candidate discovery, durable maintenance
queues, and the atomic storage batch that applies a plan.

## Quick Start

### Direct Batch (Single Roundtrip)

```rust
use nexir_mvcc_core::{InMemoryBackend, MvccEngine, PhysicalWrite, Timestamp};

let mut engine = MvccEngine::new(InMemoryBackend::new());

let writes = vec![PhysicalWrite {
    key: b"config_version".to_vec(),
    value: Some(b"1.0".to_vec()),
}];

// Apply directly at timestamp 10
engine.apply_direct_batch(Timestamp(10), writes).unwrap();

let value = engine.read(b"config_version", Timestamp(15)).unwrap();
assert_eq!(value.unwrap(), b"1.0");
```

### Multi-Key Intent Transaction

```rust
use nexir_mvcc_core::{InMemoryBackend, MvccEngine, PhysicalWrite, Timestamp, TxnId};

let mut engine = MvccEngine::new(InMemoryBackend::new());
let txn_id = TxnId(1);
let start_ts = Timestamp(10);

let writes = vec![PhysicalWrite {
    key: b"account_a".to_vec(),
    value: Some(b"900".to_vec()),
}];

// Step 1: prewrite all provisional writes.
engine.prewrite_batch(txn_id, start_ts, writes).unwrap();

// Step 2: commit and make the transaction visible.
engine.commit_batch(txn_id, start_ts, Timestamp(20), vec![b"account_a".to_vec()]).unwrap();
```

For more advanced use cases, including Read-Modify-Write and Abort idempotency, check the `examples/` directory.

## Documentation

- [Documentation index](docs/README.md)
- [MVCC design](docs/MVCC_DESIGN.md)
- [Adapter contract](docs/ADAPTER_CONTRACT.md)
- [Batch/write-set design](docs/BATCH_WRITE_SET_DESIGN.md)
- [Conflict semantics](docs/CONFLICT_SEMANTICS.md)
- [History retention and GC](docs/HISTORY_RETENTION_AND_GC.md)
- [Testing strategy](docs/TESTING.md)
- [Benchmarking](docs/BENCHMARKING.md)
- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)
- [Changelog](CHANGELOG.md)

## Testing and Conformance

The crate includes an in-memory backend and an extensive suite of property and failure tests.

To verify your own backend implementation, enable the `conformance` feature in your `Cargo.toml`:

```toml
[dependencies]
nexir-mvcc-core = { version = "0.2", features = ["conformance"] }
```

And run the macro in your tests:

```rust
#[cfg(test)]
mod tests {
    use my_crate::MyBackend;

    // Generates a suite of tests that assert atomicity, sorting, and tombstone rules
    nexir_mvcc_core::test_backend_conformance!(|| MyBackend::new_for_test());
}
```

## Release Checks

Before publishing or cutting a release, run:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo doc --no-deps --all-features
cargo package
```

Performance runs are intentionally separate from correctness gates:

```bash
cargo bench --bench perf_truth -- --sample-size 10 --warm-up-time 0.1 --measurement-time 0.1
cargo run --example command_load_sim --release
```

## License

Dual-licensed under either of:

- [MIT License](LICENSE-MIT)
- [Apache License, Version 2.0](LICENSE-APACHE)

at your option. See [NOTICE](NOTICE) for Apache-2.0 attribution notice information.
