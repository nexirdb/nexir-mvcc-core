#![allow(deprecated)]

use nexir_mvcc_core::{
    Backend, CommittedVersion, InMemoryBackend, Mutation, MvccEngine, Timestamp, TxnId,
};

// ------------------------------------------------------------------
// A. Basic version reads
// ------------------------------------------------------------------
#[test]
fn test_basic_version_reads() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    // Put v1 at ts=1
    engine
        .prewrite(
            TxnId(1),
            Timestamp(5),
            b"k".to_vec(),
            Mutation::Put(b"v1".to_vec()),
        )
        .unwrap();
    engine
        .commit(TxnId(1), b"k", Timestamp(5), Timestamp(10))
        .unwrap();

    // Put v2 at ts=5
    engine
        .prewrite(
            TxnId(2),
            Timestamp(45),
            b"k".to_vec(),
            Mutation::Put(b"v2".to_vec()),
        )
        .unwrap();
    engine
        .commit(TxnId(2), b"k", Timestamp(45), Timestamp(50))
        .unwrap();

    assert_eq!(
        engine.read(b"k", Timestamp(10)).unwrap(),
        Some(b"v1".to_vec())
    );
    assert_eq!(
        engine.read(b"k", Timestamp(40)).unwrap(),
        Some(b"v1".to_vec())
    );
    assert_eq!(
        engine.read(b"k", Timestamp(50)).unwrap(),
        Some(b"v2".to_vec())
    );
    assert_eq!(
        engine.read(b"k", Timestamp(60)).unwrap(),
        Some(b"v2".to_vec())
    );
}

// ------------------------------------------------------------------
// B. Delete / tombstone
// ------------------------------------------------------------------
#[test]
fn test_delete_tombstone() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    // Put at ts=1
    engine
        .prewrite(
            TxnId(1),
            Timestamp(5),
            b"k".to_vec(),
            Mutation::Put(b"v1".to_vec()),
        )
        .unwrap();
    engine
        .commit(TxnId(1), b"k", Timestamp(5), Timestamp(10))
        .unwrap();

    // Delete at ts=3
    engine
        .prewrite(TxnId(2), Timestamp(25), b"k".to_vec(), Mutation::Delete)
        .unwrap();
    engine
        .commit(TxnId(2), b"k", Timestamp(25), Timestamp(30))
        .unwrap();

    assert_eq!(
        engine.read(b"k", Timestamp(20)).unwrap(),
        Some(b"v1".to_vec())
    );
    assert_eq!(engine.read(b"k", Timestamp(30)).unwrap(), None);
    assert_eq!(engine.read(b"k", Timestamp(40)).unwrap(), None);

    // GC at safe_point_ts=40 removes the old value but retains the final tombstone keeper by default
    let stats = engine
        .gc(
            Timestamp(40),
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 10000,
                    max_versions: 10000,
                },
                collapse_final_tombstones: false,
            },
        )
        .unwrap();
    assert_eq!(stats.versions_removed, 1); // v1 at ts=10 removed, tombstone at ts=30 kept because collapse_final_tombstones=false
    assert_eq!(stats.intents_preserved, 0);

    assert_eq!(engine.read(b"k", Timestamp(40)).unwrap(), None);
}

#[test]
fn test_gc_tombstone_removed_when_safe() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    engine
        .prewrite(
            TxnId(1),
            Timestamp(5),
            b"k".to_vec(),
            Mutation::Put(b"v1".to_vec()),
        )
        .unwrap();
    engine
        .commit(TxnId(1), b"k", Timestamp(5), Timestamp(10))
        .unwrap();

    engine
        .prewrite(TxnId(2), Timestamp(25), b"k".to_vec(), Mutation::Delete)
        .unwrap();
    engine
        .commit(TxnId(2), b"k", Timestamp(25), Timestamp(30))
        .unwrap();

    // GC at safe_point_ts=50 removes the old value but retains the final tombstone keeper by default
    let stats = engine
        .gc(
            Timestamp(50),
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 10000,
                    max_versions: 10000,
                },
                collapse_final_tombstones: false,
            },
        )
        .unwrap();
    assert_eq!(stats.versions_removed, 1); // v1 at ts=10 removed, tombstone at ts=30 kept because collapse_final_tombstones=false
    assert_eq!(engine.read(b"k", Timestamp(50)).unwrap(), None);
}

// ------------------------------------------------------------------
// C. Lock conflict
// ------------------------------------------------------------------
#[test]
fn test_lock_conflict() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    // Pre-populate with a committed version
    engine
        .prewrite(
            TxnId(0),
            Timestamp(5),
            b"k".to_vec(),
            Mutation::Put(b"v0".to_vec()),
        )
        .unwrap();
    engine
        .commit(TxnId(0), b"k", Timestamp(5), Timestamp(10))
        .unwrap();

    // txn A prewrites
    engine
        .prewrite(
            TxnId(10),
            Timestamp(95),
            b"k".to_vec(),
            Mutation::Put(b"va".to_vec()),
        )
        .unwrap();

    // txn B prewrite same key -> KeyLocked
    let err = engine
        .prewrite(
            TxnId(11),
            Timestamp(95),
            b"k".to_vec(),
            Mutation::Put(b"vb".to_vec()),
        )
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "key is locked by another transaction: txn_id=10"
    );
}

// ------------------------------------------------------------------
// D. Write conflict
// ------------------------------------------------------------------
#[test]
fn test_write_conflict() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    // Pre-populate
    engine
        .prewrite(
            TxnId(0),
            Timestamp(5),
            b"k".to_vec(),
            Mutation::Put(b"v0".to_vec()),
        )
        .unwrap();
    engine
        .commit(TxnId(0), b"k", Timestamp(5), Timestamp(10))
        .unwrap();

    // txn A starts at ts=10, reads k
    assert_eq!(
        engine.read(b"k", Timestamp(100)).unwrap(),
        Some(b"v0".to_vec())
    );

    // Another txn commits key at ts=11
    engine
        .prewrite(
            TxnId(1),
            Timestamp(105),
            b"k".to_vec(),
            Mutation::Put(b"v1".to_vec()),
        )
        .unwrap();
    engine
        .commit(TxnId(1), b"k", Timestamp(105), Timestamp(110))
        .unwrap();

    // txn A tries to prewrite -> WriteConflict
    let err = engine
        .prewrite(
            TxnId(10),
            Timestamp(95),
            b"k".to_vec(),
            Mutation::Put(b"va".to_vec()),
        )
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "write conflict: committed version after start_ts"
    );
}

// ------------------------------------------------------------------
// E. Commit validation
// ------------------------------------------------------------------
#[test]
fn test_commit_wrong_txn_id() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    engine
        .prewrite(
            TxnId(1),
            Timestamp(95),
            b"k".to_vec(),
            Mutation::Put(b"v".to_vec()),
        )
        .unwrap();

    let err = engine
        .commit(TxnId(2), b"k", Timestamp(100), Timestamp(200))
        .unwrap_err();
    assert_eq!(err.to_string(), "txn_id mismatch");
}

#[test]
fn test_commit_ts_too_old_single_key() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    // Intent created
    engine
        .prewrite(
            TxnId(1),
            Timestamp(5),
            b"k".to_vec(),
            Mutation::Put(b"v".to_vec()),
        )
        .unwrap();

    // Bypass engine to insert a newer committed version
    engine
        .backend_mut()
        .put_committed(CommittedVersion {
            key: b"k".to_vec(),
            commit_ts: Timestamp(20),
            value: Some(b"newer".to_vec()),
        })
        .unwrap();

    // Try to commit with ts=10, which is > start_ts (5) but <= latest_ts (20)
    let err = engine
        .commit(TxnId(1), b"k", Timestamp(5), Timestamp(10))
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "commit timestamp too old: commit_ts 10, latest is 20"
    );
}

#[test]
fn test_commit_wrong_start_ts() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    engine
        .prewrite(
            TxnId(1),
            Timestamp(95),
            b"k".to_vec(),
            Mutation::Put(b"v".to_vec()),
        )
        .unwrap();

    let err = engine
        .commit(TxnId(1), b"k", Timestamp(110), Timestamp(200))
        .unwrap_err();
    assert_eq!(err.to_string(), "start_ts mismatch");
}

#[test]
fn test_commit_missing_intent() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    let err = engine
        .commit(TxnId(1), b"k", Timestamp(100), Timestamp(200))
        .unwrap_err();
    assert_eq!(err.to_string(), "intent not found");
}

#[test]
fn test_commit_valid() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    engine
        .prewrite(
            TxnId(1),
            Timestamp(95),
            b"k".to_vec(),
            Mutation::Put(b"v".to_vec()),
        )
        .unwrap();
    engine
        .commit(TxnId(1), b"k", Timestamp(95), Timestamp(200))
        .unwrap();

    assert_eq!(
        engine.read(b"k", Timestamp(200)).unwrap(),
        Some(b"v".to_vec())
    );
}

// ------------------------------------------------------------------
// F. Abort validation
// ------------------------------------------------------------------
#[test]
fn test_abort_removes_intent() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    engine
        .prewrite(
            TxnId(1),
            Timestamp(95),
            b"k".to_vec(),
            Mutation::Put(b"v".to_vec()),
        )
        .unwrap();
    engine.abort(TxnId(1), b"k", Timestamp(95)).unwrap();

    assert_eq!(engine.read(b"k", Timestamp(200)).unwrap(), None);
}

#[test]
fn test_abort_does_not_remove_committed() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    engine
        .prewrite(
            TxnId(1),
            Timestamp(95),
            b"k".to_vec(),
            Mutation::Put(b"v".to_vec()),
        )
        .unwrap();
    engine
        .commit(TxnId(1), b"k", Timestamp(95), Timestamp(200))
        .unwrap();

    engine.abort(TxnId(1), b"k", Timestamp(95)).unwrap();

    assert_eq!(
        engine.read(b"k", Timestamp(200)).unwrap(),
        Some(b"v".to_vec())
    );
}

#[test]
fn test_abort_wrong_txn_does_not_remove() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    engine
        .prewrite(
            TxnId(1),
            Timestamp(95),
            b"k".to_vec(),
            Mutation::Put(b"v".to_vec()),
        )
        .unwrap();
    engine.abort(TxnId(2), b"k", Timestamp(95)).unwrap(); // no-op idempotent

    // Intent should still be there
    engine
        .commit(TxnId(1), b"k", Timestamp(95), Timestamp(200))
        .unwrap();
    assert_eq!(
        engine.read(b"k", Timestamp(200)).unwrap(),
        Some(b"v".to_vec())
    );
}

// ------------------------------------------------------------------
// G. Read-your-own-write
// ------------------------------------------------------------------
#[test]
fn test_read_own_write() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    // Pre-populate
    engine
        .prewrite(
            TxnId(0),
            Timestamp(5),
            b"k".to_vec(),
            Mutation::Put(b"old".to_vec()),
        )
        .unwrap();
    engine
        .commit(TxnId(0), b"k", Timestamp(5), Timestamp(10))
        .unwrap();

    // txn 10 reads old value via normal read
    assert_eq!(
        engine.read(b"k", Timestamp(100)).unwrap(),
        Some(b"old".to_vec())
    );

    // txn 10 prewrites new value
    engine
        .prewrite(
            TxnId(10),
            Timestamp(95),
            b"k".to_vec(),
            Mutation::Put(b"new".to_vec()),
        )
        .unwrap();

    // read_own_write sees the uncommitted value
    assert_eq!(
        engine
            .read_own_write(b"k", TxnId(10), Timestamp(95), Timestamp(100))
            .unwrap(),
        Some(b"new".to_vec())
    );

    // Normal read still does not see it
    assert_eq!(
        engine.read(b"k", Timestamp(100)).unwrap(),
        Some(b"old".to_vec())
    );
}

// ------------------------------------------------------------------
// H. GC
// ------------------------------------------------------------------
#[test]
fn test_gc_old_versions_removed() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    for i in 1..=5u64 {
        engine
            .prewrite(
                TxnId(i),
                Timestamp(i * 10 - 5),
                b"k".to_vec(),
                Mutation::Put(format!("v{}", i).into_bytes()),
            )
            .unwrap();
        engine
            .commit(TxnId(i), b"k", Timestamp(i * 10 - 5), Timestamp(i * 10))
            .unwrap();
    }

    let stats = engine
        .gc(
            Timestamp(40),
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 10000,
                    max_versions: 10000,
                },
                collapse_final_tombstones: false,
            },
        )
        .unwrap();
    assert_eq!(stats.versions_removed, 3); // v1, v2, v3 removed; v4 kept

    assert_eq!(
        engine.read(b"k", Timestamp(40)).unwrap(),
        Some(b"v4".to_vec())
    );
    assert_eq!(
        engine.read(b"k", Timestamp(50)).unwrap(),
        Some(b"v5".to_vec())
    );
}

#[test]
fn test_gc_preserves_intents() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    engine
        .prewrite(
            TxnId(1),
            Timestamp(5),
            b"k".to_vec(),
            Mutation::Put(b"v".to_vec()),
        )
        .unwrap();

    let stats = engine
        .gc(
            Timestamp(100),
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 10000,
                    max_versions: 10000,
                },
                collapse_final_tombstones: false,
            },
        )
        .unwrap();
    assert_eq!(stats.intents_preserved, 1);
    assert_eq!(stats.versions_removed, 0);

    // Intent still there
    engine
        .commit(TxnId(1), b"k", Timestamp(5), Timestamp(50))
        .unwrap();
    assert_eq!(
        engine.read(b"k", Timestamp(50)).unwrap(),
        Some(b"v".to_vec())
    );
}

#[test]
fn test_gc_keeps_newest_below_safe_point() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    engine
        .prewrite(
            TxnId(1),
            Timestamp(5),
            b"k".to_vec(),
            Mutation::Put(b"v1".to_vec()),
        )
        .unwrap();
    engine
        .commit(TxnId(1), b"k", Timestamp(5), Timestamp(10))
        .unwrap();

    engine
        .prewrite(
            TxnId(2),
            Timestamp(45),
            b"k".to_vec(),
            Mutation::Put(b"v5".to_vec()),
        )
        .unwrap();
    engine
        .commit(TxnId(2), b"k", Timestamp(45), Timestamp(50))
        .unwrap();

    let stats = engine
        .gc(
            Timestamp(30),
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 10000,
                    max_versions: 10000,
                },
                collapse_final_tombstones: false,
            },
        )
        .unwrap();
    assert_eq!(stats.versions_removed, 0); // v1 is newest below safe_point=3

    assert_eq!(
        engine.read(b"k", Timestamp(30)).unwrap(),
        Some(b"v1".to_vec())
    );
}

// ------------------------------------------------------------------
// I. Binary safety
// ------------------------------------------------------------------
#[test]
fn test_binary_keys_and_values() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    let key = vec![0x00, 0xFF, 0xAB, 0x00];
    let value = vec![0x00, 0x00, 0x01, 0x02];

    engine
        .prewrite(
            TxnId(1),
            Timestamp(5),
            key.clone(),
            Mutation::Put(value.clone()),
        )
        .unwrap();
    engine
        .commit(TxnId(1), &key, Timestamp(5), Timestamp(10))
        .unwrap();

    assert_eq!(engine.read(&key, Timestamp(10)).unwrap(), Some(value));
}

#[test]
fn test_empty_value() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    engine
        .prewrite(
            TxnId(1),
            Timestamp(5),
            b"k".to_vec(),
            Mutation::Put(b"".to_vec()),
        )
        .unwrap();
    engine
        .commit(TxnId(1), b"k", Timestamp(5), Timestamp(10))
        .unwrap();

    assert_eq!(
        engine.read(b"k", Timestamp(10)).unwrap(),
        Some(b"".to_vec())
    );
}

#[test]
fn test_large_value() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    let value = vec![0x42u8; 1_000_000];

    engine
        .prewrite(
            TxnId(1),
            Timestamp(5),
            b"k".to_vec(),
            Mutation::Put(value.clone()),
        )
        .unwrap();
    engine
        .commit(TxnId(1), b"k", Timestamp(5), Timestamp(10))
        .unwrap();

    assert_eq!(engine.read(b"k", Timestamp(10)).unwrap(), Some(value));
}

// ------------------------------------------------------------------
// J. Deterministic replay
// ------------------------------------------------------------------
#[test]
fn test_deterministic_replay() {
    let run = || {
        let backend = InMemoryBackend::new();
        let mut engine = MvccEngine::new(backend);

        engine
            .prewrite(
                TxnId(1),
                Timestamp(5),
                b"a".to_vec(),
                Mutation::Put(b"1".to_vec()),
            )
            .unwrap();
        engine
            .commit(TxnId(1), b"a", Timestamp(5), Timestamp(10))
            .unwrap();

        engine
            .prewrite(
                TxnId(2),
                Timestamp(15),
                b"b".to_vec(),
                Mutation::Put(b"2".to_vec()),
            )
            .unwrap();
        engine
            .commit(TxnId(2), b"b", Timestamp(15), Timestamp(20))
            .unwrap();

        let _ = engine.gc(
            Timestamp(10),
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 10000,
                    max_versions: 10000,
                },
                collapse_final_tombstones: false,
            },
        );

        let versions_a = engine.backend().get_committed_versions(b"a").unwrap();
        let versions_b = engine.backend().get_committed_versions(b"b").unwrap();
        let intent_a = engine.backend().get_intent(b"a").unwrap();
        let intent_b = engine.backend().get_intent(b"b").unwrap();

        (versions_a, versions_b, intent_a, intent_b)
    };

    let first = run();
    let second = run();
    assert_eq!(first, second);
}

// ------------------------------------------------------------------
// K. Lost-update proof
// ------------------------------------------------------------------
#[test]
fn test_lost_update_prevented() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    // initial: k = 10 at ts=1
    engine
        .prewrite(
            TxnId(0),
            Timestamp(5),
            b"k".to_vec(),
            Mutation::Put(b"10".to_vec()),
        )
        .unwrap();
    engine
        .commit(TxnId(0), b"k", Timestamp(5), Timestamp(10))
        .unwrap();

    // txn A start_ts=10 reads k => 10
    assert_eq!(
        engine.read(b"k", Timestamp(100)).unwrap(),
        Some(b"10".to_vec())
    );

    // txn B start_ts=10 reads k => 10
    assert_eq!(
        engine.read(b"k", Timestamp(100)).unwrap(),
        Some(b"10".to_vec())
    );

    // txn A prewrites k=11
    engine
        .prewrite(
            TxnId(10),
            Timestamp(95),
            b"k".to_vec(),
            Mutation::Put(b"11".to_vec()),
        )
        .unwrap();

    // txn B prewrites k=11 must not silently succeed
    let result = engine.prewrite(
        TxnId(11),
        Timestamp(95),
        b"k".to_vec(),
        Mutation::Put(b"11".to_vec()),
    );

    // Valid outcomes: KeyLocked, WriteConflict, or ReadRestart.
    assert!(
        result.is_err(),
        "txn B must not silently prewrite over txn A's intent"
    );

    // Even if B somehow succeeded, committing A then B should not silently overwrite.
    // Since B's prewrite failed in this engine, verify A can still commit.
    engine
        .commit(TxnId(10), b"k", Timestamp(95), Timestamp(200))
        .unwrap();

    assert_eq!(
        engine.read(b"k", Timestamp(200)).unwrap(),
        Some(b"11".to_vec())
    );
}

// ------------------------------------------------------------------
// L. Idempotent prewrite and commit
// ------------------------------------------------------------------
#[test]
fn test_idempotent_prewrite() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    engine
        .prewrite(
            TxnId(1),
            Timestamp(95),
            b"k".to_vec(),
            Mutation::Put(b"v".to_vec()),
        )
        .unwrap();
    engine
        .prewrite(
            TxnId(1),
            Timestamp(95),
            b"k".to_vec(),
            Mutation::Put(b"v".to_vec()),
        )
        .unwrap(); // idempotent

    engine
        .commit(TxnId(1), b"k", Timestamp(95), Timestamp(200))
        .unwrap();
    assert_eq!(
        engine.read(b"k", Timestamp(200)).unwrap(),
        Some(b"v".to_vec())
    );
}

#[test]
fn test_commit_ts_too_early() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    // Create an intent with min_commit_ts via direct backend manipulation
    // (engine API does not expose min_commit_ts on prewrite in this minimal model)
    let intent = nexir_mvcc_core::Intent {
        key: b"k".to_vec(),
        txn_id: TxnId(1),
        start_ts: Timestamp(100),
        mutation: Mutation::Put(b"v".to_vec()),
        min_commit_ts: Some(Timestamp(200)),
    };
    engine.backend_mut().put_intent(intent).unwrap();

    let err = engine
        .commit(TxnId(1), b"k", Timestamp(100), Timestamp(150))
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "commit_ts 150 is before required minimum 200"
    );
}

#[test]
fn test_commit_ts_equals_start_ts_rejected() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    engine
        .prewrite(
            TxnId(1),
            Timestamp(10),
            b"k".to_vec(),
            Mutation::Put(b"v".to_vec()),
        )
        .unwrap();
    let err = engine
        .commit(TxnId(1), b"k", Timestamp(10), Timestamp(10))
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "invalid commit timestamp: commit_ts 10 <= start_ts 10"
    );
}

#[test]
fn test_commit_ts_less_than_start_ts_rejected() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    engine
        .prewrite(
            TxnId(1),
            Timestamp(10),
            b"k".to_vec(),
            Mutation::Put(b"v".to_vec()),
        )
        .unwrap();
    let err = engine
        .commit(TxnId(1), b"k", Timestamp(10), Timestamp(9))
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "invalid commit timestamp: commit_ts 9 <= start_ts 10"
    );
}

#[test]
fn test_duplicate_commit_ts_rejected() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    // Txn 1 prewrites
    engine
        .prewrite(
            TxnId(1),
            Timestamp(10),
            b"k".to_vec(),
            Mutation::Put(b"v1".to_vec()),
        )
        .unwrap();

    // Directly insert a conflicting committed version at TS 20
    engine
        .backend_mut()
        .put_committed(nexir_mvcc_core::CommittedVersion {
            key: b"k".to_vec(),
            commit_ts: Timestamp(20),
            value: Some(b"v_old".to_vec()),
        })
        .unwrap();

    // Now try to commit Txn 1 at TS 20
    let err = engine
        .commit(TxnId(1), b"k", Timestamp(10), Timestamp(20))
        .unwrap_err();
    assert_eq!(err.to_string(), "duplicate commit timestamp: 20");
}

#[test]
fn test_lost_update_retry_after_lock_clears() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    // Initial value
    engine
        .prewrite(
            TxnId(0),
            Timestamp(1),
            b"k".to_vec(),
            Mutation::Put(b"init".to_vec()),
        )
        .unwrap();
    engine
        .commit(TxnId(0), b"k", Timestamp(1), Timestamp(5))
        .unwrap();

    // A and B both start at 10
    engine.read(b"k", Timestamp(10)).unwrap();
    engine.read(b"k", Timestamp(10)).unwrap();

    // A prewrites and commits
    engine
        .prewrite(
            TxnId(10),
            Timestamp(10),
            b"k".to_vec(),
            Mutation::Put(b"A".to_vec()),
        )
        .unwrap();
    engine
        .commit(TxnId(10), b"k", Timestamp(10), Timestamp(15))
        .unwrap();

    // B retries prewrite after A's intent is gone (lock cleared, but version exists at 15)
    let result = engine.prewrite(
        TxnId(11),
        Timestamp(10),
        b"k".to_vec(),
        Mutation::Put(b"B".to_vec()),
    );
    let err = result.unwrap_err();
    assert_eq!(
        err.to_string(),
        "write conflict: committed version after start_ts"
    );
}

#[test]
fn test_delete_tombstone_opt_in() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    // Put at ts=1
    engine
        .prewrite(
            TxnId(1),
            Timestamp(5),
            b"k".to_vec(),
            Mutation::Put(b"v1".to_vec()),
        )
        .unwrap();
    engine
        .commit(TxnId(1), b"k", Timestamp(5), Timestamp(10))
        .unwrap();

    // Delete at ts=3
    engine
        .prewrite(TxnId(2), Timestamp(25), b"k".to_vec(), Mutation::Delete)
        .unwrap();
    engine
        .commit(TxnId(2), b"k", Timestamp(25), Timestamp(30))
        .unwrap();

    assert_eq!(
        engine.read(b"k", Timestamp(20)).unwrap(),
        Some(b"v1".to_vec())
    );
    assert_eq!(engine.read(b"k", Timestamp(30)).unwrap(), None);
    assert_eq!(engine.read(b"k", Timestamp(40)).unwrap(), None);

    // GC at safe_point_ts=40 collapses the final tombstone as well
    let stats = engine
        .gc(
            Timestamp(40),
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 10000,
                    max_versions: 10000,
                },
                collapse_final_tombstones: true,
            },
        )
        .unwrap();
    assert_eq!(stats.versions_removed, 2);
    assert_eq!(stats.intents_preserved, 0);

    assert_eq!(engine.read(b"k", Timestamp(40)).unwrap(), None);
}
#[test]
fn test_gc_tombstone_removed_when_safe_opt_in() {
    let backend = InMemoryBackend::new();
    let mut engine = MvccEngine::new(backend);

    engine
        .prewrite(
            TxnId(1),
            Timestamp(5),
            b"k".to_vec(),
            Mutation::Put(b"v1".to_vec()),
        )
        .unwrap();
    engine
        .commit(TxnId(1), b"k", Timestamp(5), Timestamp(10))
        .unwrap();

    engine
        .prewrite(TxnId(2), Timestamp(25), b"k".to_vec(), Mutation::Delete)
        .unwrap();
    engine
        .commit(TxnId(2), b"k", Timestamp(25), Timestamp(30))
        .unwrap();

    // GC at safe_point_ts=50 removes everything (value and its final tombstone)
    let stats = engine
        .gc(
            Timestamp(50),
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 10000,
                    max_versions: 10000,
                },
                collapse_final_tombstones: true,
            },
        )
        .unwrap();
    assert_eq!(stats.versions_removed, 2);
    assert_eq!(engine.read(b"k", Timestamp(50)).unwrap(), None);
}
