# Batch/Write-Set Design

## Goal
Define how the MVCC core validates and stages multi-key atomic write sets without depending on a specific database runtime, consensus implementation, command protocol, or durable storage engine. The batch API provides the core primitives needed by adapters that execute ordered mutations.

## Core Batch Primitives

The MVCC kernel must support distinct mutation paths:

### 1. Direct Physical Batch
Used for blind single-key or same-shard multi-key writes.
- **Identity**: Requires only a `commit_ts`. No `txn_id` or `start_ts` is required because it does not use durable intents.
- **Operations**: An ordered list of `PhysicalWrite` operations (Put/Delete).
- **Validation**: 
  - Fails fast if any key is currently locked by an active intent.
  - Fails if duplicate keys are present in the batch's writes.
  - Fails if `commit_ts <= latest_commit_ts` for any key in the batch.
- **Execution**: Writes all committed versions atomically in a single backend call. No durable intents are created.

### 2. Version-Guarded Batch
Used for read-modify-write and conditional operations.
- **Identity**: Requires a `commit_ts`.
- **Read Guards**: The batch includes a set of `ReadGuard` preconditions (`ExpectedVersion` or `ExpectedValue`). 
  - `ExpectedVersion { expected_commit_ts: None }` ensures the key has no visible committed version at `read_ts`.
  - `ExpectedValue { expected_value: None }` ensures the visible logical value is absent (either unwritten or explicitly tombstoned) at `read_ts`.
- **Validation**:
  - Deterministically evaluates all read guards against the local MVCC state.
  - Fails if `commit_ts <= read_ts`.
  - Fails if any guard key has a committed version newer than `read_ts`.
  - Fails if the visible version/value does not exactly match the expected guard.
  - Fails if any affected key (read or write) is locked by an active intent.
- **Guard Coverage is the Caller's Semantic Obligation**: The engine does not enforce that written keys must be guarded. This allows flexible scenarios like "if A is ready, write B". However, for true read-modify-write commands, the caller *must* include a guard on the written key to prevent stale overwrites.
- **Execution**: If all guards pass, writes the physical mutations atomically directly as committed versions. If *any* guard fails, the batch is safely aborted and writes nothing.

### 3. Intent Batch
Used for multi-step distributed transactions requiring provisional locking.
- Retains the `txn_id`, `start_ts`, and `commit_ts` lifecycle.
- Validates via standard `prewrite_batch`, `commit_batch`, and `abort_batch` phases, writing durable intents before converting them to committed versions atomically.
- Enforces strict all-or-nothing validation, duplicate key rejection, and min_commit_ts invariants.
- Fully implemented with robust backend batching guarantees.

## Backend Atomicity Requirement
For multi-key batches to be truly durable and crash-safe, the `Backend` trait exposes atomic batch abstractions: `put_committed_batch`, `put_intents_batch`, `commit_intents_batch`, and `remove_intents_batch`. Durable adapters must guarantee that these sets of records are written or deleted atomically to the underlying hardware. `InMemoryBackend` provides a safe reference implementation.

## Error Taxonomy
Validation errors return precise error families, clearly indicating the failure mode:

Direct and guarded committed batches return `BatchError`:
- `EmptyBatch`
- `InvalidCommitTimestamp { read_ts, commit_ts }`
- `CommitTsTooOld { key, commit_ts, latest_commit_ts }`
- `NoReadGuards`
- `DuplicateKeyInBatch { key }`
- `KeyLocked { key, txn_id }`
- `GuardFailedNewerVersion { key, read_ts, actual_commit_ts }`
- `GuardFailedVersionMismatch { key, expected, actual }`
- `GuardFailedValueMismatch { key }`
- `Backend(String)`

Intent batches return `BatchPrewriteError`, `BatchCommitError`, or `BatchAbortError` depending on the lifecycle phase. These include empty-batch, duplicate-key, missing/wrong-intent, timestamp-bound, write-conflict, partial-replay, and backend failure variants.

## All-Or-Nothing Guarantees
All batch primitives are strictly all-or-nothing. Whether validating guards or checking active intents, a failure on a single key immediately aborts the entire batch without leaving partial state or stranded locks.

## Empty Batch Policy
To prevent bugs in adapter layers, the core library enforces a strict policy against empty writes:
- Empty `apply_direct_batch`, `apply_guarded_batch`, `prewrite_batch`, and `commit_batch` calls are instantly rejected with an `EmptyBatch` error.
- Empty `abort_batch` calls are permitted and return `Ok(())` because an empty abort inherently satisfies the idempotent post-condition that "the listed keys have no active intents for this transaction".
