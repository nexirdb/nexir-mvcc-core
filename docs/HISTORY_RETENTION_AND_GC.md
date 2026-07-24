# Incremental GC & History Retention

`nexir-mvcc-core` uses an incremental history-retention garbage collection (GC) model to remove obsolete committed versions safely. The model provides deterministic, caller-budgeted work per invocation; it does not claim a strict wall-clock latency bound.

## Safe Point and History Cutoff
The core uses a `safe_point_ts` (history cutoff point) passed into `gc_incremental`. 
Any committed version strictly older than the `safe_point_ts` is a candidate for removal, EXCEPT the newest version at or below the `safe_point_ts` (the "keeper"). The keeper must be preserved because any reader querying at the `safe_point_ts` (or slightly above) needs to see that state. By default, the keeper is preserved even if it is a tombstone. However, if the caller explicitly opts-in via `GcOptions { collapse_final_tombstones: true }`, a final tombstone keeper and all its history will be physically deleted, completely reclaiming its storage footprint. Callers using this opt-in feature MUST ensure that the safe point is derived from a monotonic log index and that no future read or write will occur below this safe point, as collapsing final tombstones changes `read_with_version` and guard history semantics below the safe point.

## Determinism over Wall Clocks
The MVCC core purposefully avoids any internal dependency on wall clocks or `async` runtimes. `safe_point_ts` is supplied by the adapter, ensuring that GC logic is entirely deterministic, repeatable, and testable using fuzzing and model checking. Time-based retention barriers, active reader protections, and cluster-wide watermarks are all the responsibility of the adapter layer, which feeds a calculated `safe_point_ts` into the core engine.

## Incremental GC Semantics
Stop-the-world garbage collection can collapse throughput under hot-key workloads. The MVCC core avoids unbounded full-pass work by exposing `gc_incremental`.
Adapters define a `GcBudget` consisting of:
- `max_keys`: The maximum number of keys to examine in one invocation.
- `max_versions`: The maximum number of historical versions to physically remove in one invocation.

The `gc_incremental` method returns an `IncrementalGcCursor` which must be passed into the next invocation, allowing the core to spread cleanup across many small steps. Active transactional intents are never removed by this committed-history GC process.

## Per-Key Planning

Adapters that already own durable cleanup-work discovery can call
`plan_key_gc` instead of scanning the full logical keyspace. The planner:

- reads only the requested logical key;
- identifies the newest version visible at the safe point as the keeper;
- returns explicit older timestamps in newest-first order;
- never returns a version newer than the safe point;
- never mutates the backend;
- bounds the pre-keeper history page with
  `KeyGcOptions::max_versions_examined`; and
- reports whether it established that no eligible debt remains unplanned.

The fixed, bounded keeper/latest/intent point lookups do not consume the
history-page budget. If the history page exactly fills the budget, the result is
conservatively incomplete. The caller can atomically apply the explicit
removals and plan the same key again; repeated bounded planning eventually
drains the eligible history.

When `collapse_final_tombstones` is disabled, the keeper is always retained.
When enabled, a final tombstone is reported separately from the older
timestamps and only on a complete plan. Callers must remove that tombstone and
all returned older timestamps atomically. Callers using this opt-in feature MUST
ensure that the safe point is derived from a monotonic log index and that no
future read or write will occur below this safe point, as collapsing final
tombstones changes `read_with_version` and guard history semantics below the
safe point.

`CommittedVersion.value` remains opaque to this layer. Arbitrary byte payloads
are legal. Scalar records, candidate queues, RocksDB batches, consensus, and
other adapter-specific concerns must not enter the planner.

### Backend API decision

Per-key planning does not extend the public `Backend` trait. The existing
bounded point accessors (`get_visible_committed`, `get_latest_commit_ts`, and
`get_intent`) plus the bounded descending
`get_committed_timestamps_before` page express the required reads without
calling `get_committed_versions` or materializing an unbounded chain. The
planner itself is an additive public core API, reflected by the `0.3.0` minor
version.

## Mapping to Durable Compaction
For durable adapters:
- The core defines the precise semantics for version retention.
- The adapter implements these semantics physically, often integrating them into the storage engine's compaction or cleanup mechanism.
- The same logic applies: keep all active intents, keep the newest version at or below the retention cutoff, and discard older ones.
- The provided conformance tests ensure that any underlying implementation adheres identically to these rules.
