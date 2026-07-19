#![allow(deprecated)]
#![allow(dead_code)]

use nexir_mvcc_core::{
    Backend, CommittedVersion, Intent, Mutation, MvccEngine, PhysicalWrite, ReadGuard, Timestamp,
    TxnId,
};

#[derive(Clone, Debug)]
pub enum Action {
    DirectBatch(Timestamp, Vec<PhysicalWrite>),
    GuardedBatch(Timestamp, Vec<ReadGuard>, Vec<PhysicalWrite>),
    Prewrite(TxnId, Timestamp, Vec<u8>, Mutation),
    Commit(TxnId, Vec<u8>, Timestamp, Timestamp),
    Abort(TxnId, Vec<u8>, Timestamp),
    PrewriteBatch(TxnId, Timestamp, Vec<PhysicalWrite>),
    CommitBatch(TxnId, Timestamp, Timestamp, Vec<Vec<u8>>),
    AbortBatch(TxnId, Timestamp, Vec<Vec<u8>>),
    Gc(Timestamp),
}

pub fn apply_schedule<B: Backend>(engine: &mut MvccEngine<B>, schedule: &[Action]) {
    for action in schedule {
        match action {
            Action::DirectBatch(ts, writes) => {
                let _ = engine.apply_direct_batch(*ts, writes.clone());
            }
            Action::GuardedBatch(ts, guards, writes) => {
                let _ = engine.apply_guarded_batch(*ts, guards.clone(), writes.clone());
            }
            Action::Prewrite(txn, ts, key, muta) => {
                let _ = engine.prewrite(*txn, *ts, key.clone(), muta.clone());
            }
            Action::Commit(txn, key, start_ts, commit_ts) => {
                let _ = engine.commit(*txn, key, *start_ts, *commit_ts);
            }
            Action::Abort(txn, key, start_ts) => {
                let _ = engine.abort(*txn, key, *start_ts);
            }
            Action::PrewriteBatch(txn, ts, writes) => {
                let _ = engine.prewrite_batch(*txn, *ts, writes.clone());
            }
            Action::CommitBatch(txn, start_ts, commit_ts, keys) => {
                let _ = engine.commit_batch(*txn, *start_ts, *commit_ts, keys.clone());
            }
            Action::AbortBatch(txn, start_ts, keys) => {
                let _ = engine.abort_batch(*txn, *start_ts, keys.clone());
            }
            Action::Gc(ts) => {
                let _ = engine.gc(
                    *ts,
                    nexir_mvcc_core::GcOptions {
                        budget: nexir_mvcc_core::GcBudget {
                            max_keys: 10000,
                            max_versions: 10000,
                        },
                        collapse_final_tombstones: false,
                    },
                );
            }
        }
    }
}

pub fn apply_and_verify_schedule<B: Backend>(engine: &mut MvccEngine<B>, schedule: &[Action]) {
    for action in schedule {
        let snap_before = snapshot(engine);

        let result = match action {
            Action::DirectBatch(ts, writes) => engine
                .apply_direct_batch(*ts, writes.clone())
                .map_err(|_| ()),
            Action::GuardedBatch(ts, guards, writes) => engine
                .apply_guarded_batch(*ts, guards.clone(), writes.clone())
                .map_err(|_| ()),
            Action::Prewrite(txn, ts, key, muta) => engine
                .prewrite(*txn, *ts, key.clone(), muta.clone())
                .map_err(|_| ()),
            Action::Commit(txn, key, start_ts, commit_ts) => engine
                .commit(*txn, key, *start_ts, *commit_ts)
                .map_err(|_| ()),
            Action::Abort(txn, key, start_ts) => engine.abort(*txn, key, *start_ts).map_err(|_| ()),
            Action::PrewriteBatch(txn, ts, writes) => engine
                .prewrite_batch(*txn, *ts, writes.clone())
                .map_err(|_| ()),
            Action::CommitBatch(txn, start_ts, commit_ts, keys) => engine
                .commit_batch(*txn, *start_ts, *commit_ts, keys.clone())
                .map_err(|_| ()),
            Action::AbortBatch(txn, start_ts, keys) => engine
                .abort_batch(*txn, *start_ts, keys.clone())
                .map_err(|_| ()),
            Action::Gc(ts) => engine
                .gc(
                    *ts,
                    nexir_mvcc_core::GcOptions {
                        budget: nexir_mvcc_core::GcBudget {
                            max_keys: 10000,
                            max_versions: 10000,
                        },
                        collapse_final_tombstones: false,
                    },
                )
                .map(|_| ())
                .map_err(|_| ()),
        };

        let snap_after = snapshot(engine);

        // Assert: failed operations do not change snapshots
        if result.is_err() {
            assert_eq!(snap_before, snap_after, "Failed operation altered state!");
        }

        if let Action::Gc(_) = action {
            // Assert: GC does not remove active intents
            assert_eq!(snap_before.2, snap_after.2, "GC removed an active intent!");
        }
    }
}

pub fn snapshot<B: Backend>(
    engine: &MvccEngine<B>,
) -> (Vec<Vec<u8>>, Vec<CommittedVersion>, Vec<Intent>) {
    let backend = engine.backend();
    let mut all_keys = backend.all_keys().unwrap();
    all_keys.sort();

    let mut all_versions = Vec::new();
    let mut all_intents = Vec::new();
    for k in &all_keys {
        let mut versions = backend.get_committed_versions(k).unwrap();
        all_versions.append(&mut versions);

        if let Some(intent) = backend.get_intent(k).unwrap() {
            all_intents.push(intent);
        }
    }
    (all_keys, all_versions, all_intents)
}
