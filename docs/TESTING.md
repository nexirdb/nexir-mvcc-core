# MVCC Core Testing Strategy

This standalone MVCC core is tested using multiple layers of rigor to ensure correctness, determinism, and robust API propagation independently of any database runtime. The core tests the crash-safety boundary and error propagation, but durable crash safety remains the responsibility of the adapter.

## 1. Standalone Core Tests
The primary testing layer includes standard unit and integration tests located in `tests/integration_tests.rs`, `tests/codec_tests.rs`, `tests/batch_tests.rs`, and `tests/gc_tests.rs`. 
These test specific fast-fail paths, correct isolation, incremental GC behavior (including cursor limits and budgets), and error taxonomies, utilizing an in-memory test backend.

## 2. Backend Conformance
A reusable macro `test_backend_conformance!` is defined in `src/conformance.rs` (available under the `conformance` feature flag). It tests any `Backend` implementation to ensure it upholds the strict requirements of the MVCC core:
- `get_committed_versions` returns versions sorted ascending by `commit_ts`.
- Fast lookup methods (`get_latest_committed`, `get_visible_committed`, and `get_latest_commit_ts`) match the canonical committed-version history.
- Incremental GC scan helpers (`keys_from` and `get_committed_timestamps_before`) return deterministic ordered results.
- `put_committed_batch`, `put_intents_batch`, `commit_intents_batch`, and `remove_intents_batch` operate fully all-or-nothing.
- Intents adhere to strict `(key, txn_id, start_ts)` identities.
- `all_keys` is correctly deduplicated and collated.

Adapters should use this conformance suite to prove they uphold the logical `Backend` contract. However, note that this macro cannot prove *crash atomicity* for external durable backends: adapters must still write their own failure/crash tests.

## 3. Deterministic Replay Model
Defined in `tests/model_tests.rs`, this suite proves that the MVCC engine is a pure state machine. Applying a deterministic schedule of actions (`DirectBatch`, `GuardedBatch`, `Prewrite`, `Commit`, `Abort`, `Gc`) against a fresh engine guarantees an identical resulting state snapshot.

## 4. Property & Generated Testing
Defined in `tests/property_tests.rs` (using the `proptest` crate), this suite generates hundreds of randomized schedules of concurrent operations, asserting the following mathematical invariants:
- **Strictly Increasing `commit_ts`**: Committed versions for any key never move backwards in time.
- **Failed State Isolation**: Failed batch operations do not mutate the snapshot.
- **GC Preservation**: Garbage collection preserves active intents and the required visible keeper version at the safe point.

## 5. Failure Simulation & Adapter Boundaries
The test suite explicitly simulates backend failures in `tests/failure_tests.rs` using a test-only `FailingBackend`. This proves:
- Validation logic completes *before* touching the backend.
- Backend errors correctly bubble up as `BatchError::Backend`.

### What the Core Proves vs. Adapter Responsibilities
- **The Core Proves**: Conflict detection, strict serialization, transaction invariants, deterministic replay, bounds checking, and correct API propagation.
- **The Adapter Proves**: It is the **backend adapter's** strict responsibility to ensure durability and crash safety. If the underlying database violates the all-or-nothing contract, for example through partial batch writes after power loss, the MVCC core cannot repair that state. Durable adapters must use their storage engine's atomic batch primitive to guarantee this boundary.
