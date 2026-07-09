# MVCC Design Document

## Overview

This crate implements the standalone MVCC engine used by Nexir. It provides deterministic timestamp-ordered committed versions, provisional intents, optimistic conflict detection, guarded writes, batch atomicity, and incremental history retention.

This repository contains only the MVCC core. It does not provide command parsing, networking, consensus, durable storage bindings, replay/idempotency caches, query execution, metrics, or operational services.

## Core Boundaries

The core owns:

- The concept of **intents** (uncommitted writes)
- The concept of **committed versions** with timestamps
- Optimistic conflict detection
- Direct and guarded batch application
- Deterministic incremental history retention

Adapters own timestamp assignment, durable storage, ordered mutation application, crash recovery, idempotent replay, and external request semantics.

## Timestamp Model

All timestamps are caller-supplied. The engine does **not** call `std::time` or any wall-clock source.

```rust
pub struct Timestamp(pub u64);
```

Supported timestamp roles:

- `start_ts` — when a transaction begins
- `read_ts` — when a read occurs
- `commit_ts` — when a transaction commits (**must be strictly greater than `start_ts`**)
- `safe_point_ts` — GC boundary

For adapter integration, timestamps may come from:

- Ordered log index / op ordinal
- A future TSO (Timestamp Oracle)
- Leader-assigned logical timestamps

The MVCC core is agnostic to the source.

## Data Model

### Committed Version

```rust
struct CommittedVersion {
    key: Vec<u8>,
    commit_ts: Timestamp,
    value: Option<Vec<u8>>, // None = tombstone
}
```

### Intent / Lock

```rust
struct Intent {
    key: Vec<u8>,
    txn_id: TxnId,
    start_ts: Timestamp,
    mutation: Mutation,
    min_commit_ts: Option<Timestamp>,
}
```

## Commit Model

1. `prewrite(txn_id, start_ts, key, mutation)` — place an intent
2. `commit(txn_id, key, start_ts, commit_ts)` — convert intent to committed version
3. `abort(txn_id, key, start_ts)` — remove intent

Commit is idempotent only in the sense that calling it twice with the same parameters is safe because the intent is removed after the first call. The second call returns `IntentNotFound`. Commit timestamps must be strictly greater than `start_ts` and strictly greater than the latest committed timestamp for that key. Timestamp collisions or retroactive commits are rejected.

## Conflict Rules

### Prewrite Conflict Detection

1. **KeyLocked**: If an intent exists for the key and belongs to a different `txn_id`, return `KeyLocked`.
2. **WriteConflict**: If the newest committed version has `commit_ts > start_ts`, return `WriteConflict`.
3. **Idempotent**: If an intent already exists with the same `txn_id`, `start_ts`, and `mutation`, return `Ok(())`.

### Read Semantics

- Return the newest committed version with `commit_ts <= read_ts`.
- Do not see uncommitted intents from other transactions.
- Read-your-own-write is supported via a separate `read_own_write` API.

### Lost-Update Invariant

The engine must ensure that a read-derived mutation cannot silently overwrite another concurrent committed mutation. The conflict rules above guarantee this:

- If another txn has an intent: `KeyLocked`
- If another txn has committed after our read: `WriteConflict`

## History Retention and GC

The core exposes both `gc_incremental(safe_point_ts, cursor, options: GcOptions)` and the compatibility helper `gc(safe_point_ts, options: GcOptions)`. The incremental API is the preferred production shape because callers can bound work per step using `GcBudget`.

GC removes old committed versions subject to:

1. Keep the newest committed version at or below `safe_point_ts`.
2. Keep all committed versions with `commit_ts > safe_point_ts`.
3. Never remove active intents.
4. Tombstones are committed versions. By default (`collapse_final_tombstones: false`), if a tombstone is the newest version at or below the safe point, it is preserved as the keeper.
5. Opt-in Collapse: If `collapse_final_tombstones: true` is passed, a final tombstone at or below the safe point is physically deleted rather than preserved as a keeper. This completely reclaims the row. Callers using this opt-in MUST guarantee no future operation reads or writes below the safe point, as opt-in collapse changes `read_with_version` and guard history semantics below the safe point.

The adapter is responsible for computing a safe `safe_point_ts` from active readers, snapshots, and retention policy. The core does not use wall clocks.

## Serialization

The codec uses an explicit version byte (`0x01`) and big-endian length-prefixed byte arrays. No reliance on Rust enum layout. Decoding is strictly canonical: the decoder enforces exact buffer consumption, explicitly rejecting records with trailing garbage bytes.

See `tests/codec_tests.rs` for golden byte fixtures.

## Adapter Integration

This crate is designed to be embedded by an adapter as follows:

- **Adapter apply** provides deterministic timestamps (for example, from an ordered log index).
- **MVCC core** handles conflict checks and versioned storage.
- **Command parsing** remains outside this crate.
- **Consensus** remains outside this crate.
- **Durable storage** is supplied through the `Backend` trait.


### Garbage Collection and Final Tombstones
By default, `nexir-mvcc-core` preserves the final tombstone of a key indefinitely as a keeper to maintain exact deletion timestamps. To reclaim this space and prevent unbounded growth on delete-heavy workloads, consumers can opt-in to final-tombstone collapse via `GcOptions { collapse_final_tombstones: true }`. When enabled, the GC will permanently remove the tombstone (and all its history) if its commit timestamp is safely bounded below the GC safe point. Consumers MUST guarantee that no future operations will read or write at a timestamp below the safe point, commonly enforced using a monotonic log-applied index.
