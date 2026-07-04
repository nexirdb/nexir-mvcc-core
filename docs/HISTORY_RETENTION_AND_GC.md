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

## Mapping to Durable Compaction
For durable adapters:
- The core defines the precise semantics for version retention.
- The adapter implements these semantics physically, often integrating them into the storage engine's compaction or cleanup mechanism.
- The same logic applies: keep all active intents, keep the newest version at or below the retention cutoff, and discard older ones.
- The provided conformance tests ensure that any underlying implementation adheres identically to these rules.
