//! Deterministic command-load simulation for the standalone MVCC core.
//!
//! This is not a network benchmark. It models the command mix and contention shape
//! of a command-oriented key-value workload directly against `MvccEngine`.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use nexir_mvcc_core::{
    BatchError, InMemoryBackend, MvccEngine, PhysicalWrite, ReadGuard, Timestamp, TxnId,
};

const KEYSPACE: u64 = 20_000;
const HOT_KEYS: u64 = 32;
const INITIAL_KEYS: u64 = 20_000;
const OPS: u64 = 100_000;
const GC_RETENTION: u64 = 2_000;

#[derive(Clone)]
struct PendingIncr {
    due_op: u64,
    key: Vec<u8>,
    read_ts: Timestamp,
    expected_value: Option<Vec<u8>>,
    new_value: Vec<u8>,
}

#[derive(Default)]
struct Stats {
    reads: u64,
    sets: u64,
    deletes: u64,
    msets: u64,
    incr_immediate: u64,
    incr_delayed: u64,
    txns: u64,
    guard_conflicts: u64,
    guard_retries: u64,
    applied_pending: u64,
    gc_runs: u64,
    gc_removed: usize,
    read_time: Duration,
    write_time: Duration,
    guard_time: Duration,
    txn_time: Duration,
    gc_time: Duration,
    max_gc_step_ns: u128,
}

struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        self.0
    }

    fn range(&mut self, upper: u64) -> u64 {
        self.next() % upper
    }
}

fn key(id: u64) -> Vec<u8> {
    format!("key:{id:08}").into_bytes()
}

#[derive(Clone)]
struct WorkloadMix {
    name: &'static str,
    enable_gc: bool,
    hot_key_percent: u64,
    read: u64,
    set: u64,
    del: u64,
    mset: u64,
    incr: u64,
    txn: u64,
}

fn choose_key(rng: &mut Lcg, hot_key_percent: u64) -> Vec<u8> {
    if rng.range(100) < hot_key_percent {
        key(rng.range(HOT_KEYS))
    } else {
        key(rng.range(KEYSPACE))
    }
}

fn parse_counter(value: Option<Vec<u8>>) -> u64 {
    value
        .and_then(|v| String::from_utf8(v).ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
}

fn apply_guarded_incr(
    engine: &mut MvccEngine<InMemoryBackend>,
    commit_ts: Timestamp,
    pending: PendingIncr,
) -> Result<(), BatchError> {
    engine.apply_guarded_batch(
        commit_ts,
        vec![ReadGuard::ExpectedValue {
            key: pending.key.clone(),
            read_ts: pending.read_ts,
            expected_value: pending.expected_value,
        }],
        vec![PhysicalWrite {
            key: pending.key,
            value: Some(pending.new_value),
        }],
    )
}

fn retry_incr(
    engine: &mut MvccEngine<InMemoryBackend>,
    key: Vec<u8>,
    read_ts: Timestamp,
    commit_ts: Timestamp,
) -> Result<(), BatchError> {
    let current = engine.read(&key, read_ts).unwrap();
    let next_value = (parse_counter(current.clone()) + 1)
        .to_string()
        .into_bytes();
    engine.apply_guarded_batch(
        commit_ts,
        vec![ReadGuard::ExpectedValue {
            key: key.clone(),
            read_ts,
            expected_value: current,
        }],
        vec![PhysicalWrite {
            key,
            value: Some(next_value),
        }],
    )
}

fn run_simulation(mix: WorkloadMix) {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    let mut rng = Lcg::new(0x5eed_1234_cafe_babe);
    let mut stats = Stats::default();
    let mut pending = VecDeque::new();
    let mut next_ts = 2u128;
    let mut cursor = None;
    let started = Instant::now();

    let initial = (0..INITIAL_KEYS)
        .map(|i| PhysicalWrite {
            key: key(i),
            value: Some(b"0".to_vec()),
        })
        .collect();
    engine.apply_direct_batch(Timestamp(1), initial).unwrap();

    let total_weight = mix.read + mix.set + mix.del + mix.mset + mix.incr + mix.txn;
    let r_thresh = mix.read;
    let s_thresh = r_thresh + mix.set;
    let d_thresh = s_thresh + mix.del;
    let m_thresh = d_thresh + mix.mset;
    let i_thresh = m_thresh + mix.incr;

    for op in 0..OPS {
        while pending
            .front()
            .map(|p: &PendingIncr| p.due_op <= op)
            .unwrap_or(false)
        {
            let pending_incr = pending.pop_front().unwrap();
            let key_for_retry = pending_incr.key.clone();
            let t0 = Instant::now();
            let commit_ts = Timestamp(next_ts);
            next_ts += 1;
            let result = apply_guarded_incr(&mut engine, commit_ts, pending_incr);
            stats.guard_time += t0.elapsed();
            stats.applied_pending += 1;

            if matches!(
                result,
                Err(BatchError::GuardFailedNewerVersion { .. })
                    | Err(BatchError::GuardFailedValueMismatch { .. })
                    | Err(BatchError::GuardFailedVersionMismatch { .. })
            ) {
                stats.guard_conflicts += 1;
                let retry_read_ts = Timestamp(next_ts - 1);
                let retry_commit_ts = Timestamp(next_ts);
                next_ts += 1;
                let retry_t0 = Instant::now();
                retry_incr(&mut engine, key_for_retry, retry_read_ts, retry_commit_ts).unwrap();
                stats.guard_time += retry_t0.elapsed();
                stats.guard_retries += 1;
            } else {
                result.unwrap();
            }
        }

        let roll = rng.range(total_weight);
        if roll < r_thresh {
            stats.reads += 1;
            let read_key = choose_key(&mut rng, mix.hot_key_percent);
            let t0 = Instant::now();
            let _ = engine.read(&read_key, Timestamp(next_ts - 1)).unwrap();
            stats.read_time += t0.elapsed();
        } else if roll < s_thresh {
            stats.sets += 1;
            let write_key = choose_key(&mut rng, mix.hot_key_percent);
            let value = format!("v:{op}").into_bytes();
            let t0 = Instant::now();
            engine
                .apply_direct_batch(
                    Timestamp(next_ts),
                    vec![PhysicalWrite {
                        key: write_key,
                        value: Some(value),
                    }],
                )
                .unwrap();
            next_ts += 1;
            stats.write_time += t0.elapsed();
        } else if roll < d_thresh {
            stats.deletes += 1;
            let write_key = choose_key(&mut rng, mix.hot_key_percent);
            let t0 = Instant::now();
            engine
                .apply_direct_batch(
                    Timestamp(next_ts),
                    vec![PhysicalWrite {
                        key: write_key,
                        value: None,
                    }],
                )
                .unwrap();
            next_ts += 1;
            stats.write_time += t0.elapsed();
        } else if roll < m_thresh {
            stats.msets += 1;
            let mut writes = Vec::with_capacity(4);
            let base = rng.range(KEYSPACE - 4);
            for i in 0..4 {
                writes.push(PhysicalWrite {
                    key: key(base + i),
                    value: Some(format!("m:{op}:{i}").into_bytes()),
                });
            }
            let t0 = Instant::now();
            engine
                .apply_direct_batch(Timestamp(next_ts), writes)
                .unwrap();
            next_ts += 1;
            stats.write_time += t0.elapsed();
        } else if roll < i_thresh {
            let incr_key = choose_key(&mut rng, mix.hot_key_percent);
            let read_ts = Timestamp(next_ts - 1);
            let current = engine.read(&incr_key, read_ts).unwrap();
            let next_value = (parse_counter(current.clone()) + 1)
                .to_string()
                .into_bytes();

            if rng.range(100) < 40 {
                stats.incr_delayed += 1;
                pending.push_back(PendingIncr {
                    due_op: op + 32 + rng.range(96),
                    key: incr_key,
                    read_ts,
                    expected_value: current,
                    new_value: next_value,
                });
            } else {
                stats.incr_immediate += 1;
                let t0 = Instant::now();
                engine
                    .apply_guarded_batch(
                        Timestamp(next_ts),
                        vec![ReadGuard::ExpectedValue {
                            key: incr_key.clone(),
                            read_ts,
                            expected_value: current,
                        }],
                        vec![PhysicalWrite {
                            key: incr_key,
                            value: Some(next_value),
                        }],
                    )
                    .unwrap();
                next_ts += 1;
                stats.guard_time += t0.elapsed();
            }
        } else {
            stats.txns += 1;
            let txn_id = TxnId(op + 1);
            let start_ts = Timestamp(next_ts);
            let a = choose_key(&mut rng, mix.hot_key_percent);
            let mut b = choose_key(&mut rng, mix.hot_key_percent);
            while a == b {
                b = key((rng.range(KEYSPACE) + HOT_KEYS) % KEYSPACE);
            }
            let writes = vec![
                PhysicalWrite {
                    key: a.clone(),
                    value: Some(format!("txn:{op}:a").into_bytes()),
                },
                PhysicalWrite {
                    key: b.clone(),
                    value: Some(format!("txn:{op}:b").into_bytes()),
                },
            ];
            let t0 = Instant::now();
            engine.prewrite_batch(txn_id, start_ts, writes).unwrap();
            next_ts += 1;
            engine
                .commit_batch(txn_id, start_ts, Timestamp(next_ts), vec![a, b])
                .unwrap();
            next_ts += 1;
            stats.txn_time += t0.elapsed();
        }

        if mix.enable_gc && op > 0 && op % 10 == 0 {
            let safe_point = Timestamp(next_ts.saturating_sub(GC_RETENTION.into()));
            let t0 = Instant::now();
            let res = engine
                .gc_incremental(
                    safe_point,
                    cursor.clone(),
                    nexir_mvcc_core::GcOptions {
                        budget: nexir_mvcc_core::GcBudget {
                            max_keys: 50,
                            max_versions: 200,
                        },
                        collapse_final_tombstones: false,
                    },
                )
                .unwrap();
            let elapsed = t0.elapsed();
            stats.gc_time += elapsed;
            stats.max_gc_step_ns = stats.max_gc_step_ns.max(elapsed.as_nanos());
            stats.gc_removed += res.versions_removed;
            stats.gc_runs += 1;

            if res.done {
                cursor = None;
            } else {
                cursor = Some(res.cursor);
            }
        }
    }

    while let Some(pending_incr) = pending.pop_front() {
        let key_for_retry = pending_incr.key.clone();
        let commit_ts = Timestamp(next_ts);
        next_ts += 1;
        let result = apply_guarded_incr(&mut engine, commit_ts, pending_incr);
        stats.applied_pending += 1;
        if result.is_err() {
            stats.guard_conflicts += 1;
            let retry_read_ts = Timestamp(next_ts - 1);
            let retry_commit_ts = Timestamp(next_ts);
            next_ts += 1;
            retry_incr(&mut engine, key_for_retry, retry_read_ts, retry_commit_ts).unwrap();
            stats.guard_retries += 1;
        }
    }

    let elapsed = started.elapsed();
    let logical_ops = OPS + stats.applied_pending + stats.guard_retries;
    println!("=== {} ===", mix.name);
    println!("elapsed_ms={}", elapsed.as_millis());
    println!("logical_ops={logical_ops}");
    println!(
        "throughput_ops_per_sec={:.0}",
        logical_ops as f64 / elapsed.as_secs_f64()
    );
    println!(
        "mix reads={} sets={} deletes={} msets={} incr_immediate={} incr_delayed={} txns={}",
        stats.reads,
        stats.sets,
        stats.deletes,
        stats.msets,
        stats.incr_immediate,
        stats.incr_delayed,
        stats.txns
    );
    println!(
        "guard_conflicts={} guard_retries={} applied_pending={}",
        stats.guard_conflicts, stats.guard_retries, stats.applied_pending
    );
    println!(
        "gc_runs={} gc_removed={} gc_time_ms={} avg_gc_step_ns={} max_gc_step_ns={}",
        stats.gc_runs,
        stats.gc_removed,
        stats.gc_time.as_millis(),
        stats.gc_time.as_nanos() / stats.gc_runs.max(1) as u128,
        stats.max_gc_step_ns
    );
    println!(
        "avg_read_ns={} avg_direct_write_ns={} avg_guard_ns={} avg_txn_ns={}",
        stats.read_time.as_nanos() / stats.reads.max(1) as u128,
        stats.write_time.as_nanos() / (stats.sets + stats.deletes + stats.msets).max(1) as u128,
        stats.guard_time.as_nanos()
            / (stats.incr_immediate + stats.applied_pending + stats.guard_retries).max(1) as u128,
        stats.txn_time.as_nanos() / stats.txns.max(1) as u128
    );
    println!();
}

fn main() {
    let default_mix = WorkloadMix {
        name: "no_gc",
        enable_gc: false,
        hot_key_percent: 85,
        read: 65,
        set: 12,
        del: 4,
        mset: 4,
        incr: 13,
        txn: 2,
    };

    run_simulation(default_mix.clone());

    let mut incremental_gc = default_mix.clone();
    incremental_gc.name = "incremental_gc";
    incremental_gc.enable_gc = true;
    run_simulation(incremental_gc);

    let mut hot_key_heavy = default_mix.clone();
    hot_key_heavy.name = "hot_key_heavy";
    hot_key_heavy.enable_gc = true;
    hot_key_heavy.hot_key_percent = 99;
    run_simulation(hot_key_heavy);

    let mut read_heavy = default_mix.clone();
    read_heavy.name = "read_heavy";
    read_heavy.enable_gc = true;
    read_heavy.read = 90;
    read_heavy.set = 5;
    read_heavy.del = 1;
    read_heavy.mset = 1;
    read_heavy.incr = 2;
    read_heavy.txn = 1;
    run_simulation(read_heavy);

    let mut write_heavy = default_mix.clone();
    write_heavy.name = "write_heavy";
    write_heavy.enable_gc = true;
    write_heavy.read = 10;
    write_heavy.set = 70;
    write_heavy.del = 5;
    write_heavy.mset = 10;
    write_heavy.incr = 3;
    write_heavy.txn = 2;
    run_simulation(write_heavy);

    let mut guarded_heavy = default_mix.clone();
    guarded_heavy.name = "guarded_heavy";
    guarded_heavy.enable_gc = true;
    guarded_heavy.read = 20;
    guarded_heavy.set = 5;
    guarded_heavy.del = 1;
    guarded_heavy.mset = 4;
    guarded_heavy.incr = 65;
    guarded_heavy.txn = 5;
    run_simulation(guarded_heavy);

    let mut txn_heavy = default_mix.clone();
    txn_heavy.name = "txn_heavy";
    txn_heavy.enable_gc = true;
    txn_heavy.read = 30;
    txn_heavy.set = 10;
    txn_heavy.del = 2;
    txn_heavy.mset = 3;
    txn_heavy.incr = 5;
    txn_heavy.txn = 50;
    run_simulation(txn_heavy);
}
