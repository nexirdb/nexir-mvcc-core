use nexir_mvcc_core::backend::{Backend, InMemoryBackend};
use nexir_mvcc_core::engine::MvccEngine;
use nexir_mvcc_core::error::{BatchAbortError, BatchCommitError, BatchPrewriteError};
use nexir_mvcc_core::types::{PhysicalWrite, Timestamp, TxnId};

fn deterministic_key(id: u64) -> Vec<u8> {
    format!("key{:04}", id).into_bytes()
}

#[test]
fn test_successful_multi_key_prewrite_commit() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    let txn_id = TxnId(1);
    let start_ts = Timestamp(10);

    let writes = vec![
        PhysicalWrite {
            key: deterministic_key(1),
            value: Some(vec![1]),
        },
        PhysicalWrite {
            key: deterministic_key(2),
            value: Some(vec![2]),
        },
    ];

    assert!(engine.prewrite_batch(txn_id, start_ts, writes).is_ok());

    let keys = vec![deterministic_key(1), deterministic_key(2)];
    assert!(
        engine
            .commit_batch(txn_id, start_ts, Timestamp(20), keys)
            .is_ok()
    );

    assert_eq!(
        engine.read(&deterministic_key(1), Timestamp(25)).unwrap(),
        Some(vec![1])
    );
    assert_eq!(
        engine.read(&deterministic_key(2), Timestamp(25)).unwrap(),
        Some(vec![2])
    );
}

#[test]
fn test_prewrite_batch_empty() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    let res = engine.prewrite_batch(TxnId(1), Timestamp(10), vec![]);
    assert_eq!(res, Err(BatchPrewriteError::EmptyBatch));
}

#[test]
fn test_commit_batch_empty() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    let res = engine.commit_batch(TxnId(1), Timestamp(10), Timestamp(20), vec![]);
    assert_eq!(res, Err(BatchCommitError::EmptyBatch));
}

#[test]
fn test_all_existing_matching_prewrite_replay() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    let txn_id = TxnId(1);
    let start_ts = Timestamp(10);
    let writes = vec![
        PhysicalWrite {
            key: deterministic_key(1),
            value: Some(vec![1]),
        },
        PhysicalWrite {
            key: deterministic_key(2),
            value: Some(vec![2]),
        },
    ];

    assert!(
        engine
            .prewrite_batch(txn_id, start_ts, writes.clone())
            .is_ok()
    );
    assert!(engine.prewrite_batch(txn_id, start_ts, writes).is_ok());
}

#[test]
fn test_mixed_replay_fails_writes_nothing() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    let txn_id = TxnId(1);
    let start_ts = Timestamp(10);
    let writes1 = vec![PhysicalWrite {
        key: deterministic_key(1),
        value: Some(vec![1]),
    }];
    assert!(engine.prewrite_batch(txn_id, start_ts, writes1).is_ok());

    let writes2 = vec![
        PhysicalWrite {
            key: deterministic_key(1),
            value: Some(vec![1]),
        },
        PhysicalWrite {
            key: deterministic_key(2),
            value: Some(vec![2]),
        },
    ];
    assert_eq!(
        engine.prewrite_batch(txn_id, start_ts, writes2),
        Err(BatchPrewriteError::PartialBatchReplay)
    );
    assert!(
        engine
            .backend()
            .get_intent(&deterministic_key(2))
            .unwrap()
            .is_none()
    );
}

#[test]
fn test_conflict_on_one_key_writes_no_intents() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    let writes = vec![PhysicalWrite {
        key: deterministic_key(1),
        value: Some(vec![1]),
    }];
    assert!(
        engine
            .prewrite_batch(TxnId(1), Timestamp(10), writes)
            .is_ok()
    );

    let writes2 = vec![
        PhysicalWrite {
            key: deterministic_key(2),
            value: Some(vec![2]),
        },
        PhysicalWrite {
            key: deterministic_key(1),
            value: Some(vec![3]),
        },
    ];
    assert_eq!(
        engine.prewrite_batch(TxnId(2), Timestamp(15), writes2),
        Err(BatchPrewriteError::KeyLocked {
            key: deterministic_key(1),
            txn_id: TxnId(1)
        })
    );
    assert!(
        engine
            .backend()
            .get_intent(&deterministic_key(2))
            .unwrap()
            .is_none()
    );
}

#[test]
fn test_write_conflict_on_one_key_writes_no_intents() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    engine
        .apply_direct_batch(
            Timestamp(20),
            vec![PhysicalWrite {
                key: deterministic_key(1),
                value: Some(vec![1]),
            }],
        )
        .unwrap();

    let writes = vec![
        PhysicalWrite {
            key: deterministic_key(2),
            value: Some(vec![2]),
        },
        PhysicalWrite {
            key: deterministic_key(1),
            value: Some(vec![3]),
        },
    ];
    assert_eq!(
        engine.prewrite_batch(TxnId(1), Timestamp(10), writes),
        Err(BatchPrewriteError::WriteConflict {
            key: deterministic_key(1)
        })
    );
    assert!(
        engine
            .backend()
            .get_intent(&deterministic_key(2))
            .unwrap()
            .is_none()
    );
}

#[test]
fn test_missing_intent_on_commit() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    let txn_id = TxnId(1);
    let start_ts = Timestamp(10);
    let writes = vec![PhysicalWrite {
        key: deterministic_key(1),
        value: Some(vec![1]),
    }];
    assert!(engine.prewrite_batch(txn_id, start_ts, writes).is_ok());

    let keys = vec![deterministic_key(1), deterministic_key(2)];
    assert_eq!(
        engine.commit_batch(txn_id, start_ts, Timestamp(20), keys),
        Err(BatchCommitError::IntentNotFound {
            key: deterministic_key(2)
        })
    );

    assert!(
        engine
            .read(&deterministic_key(1), Timestamp(25))
            .unwrap()
            .is_none()
    );
    assert!(
        engine
            .backend()
            .get_intent(&deterministic_key(1))
            .unwrap()
            .is_some()
    );
}

#[test]
fn test_wrong_txn_start_ts_writes_nothing() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    let txn_id = TxnId(1);
    let start_ts = Timestamp(10);
    let writes = vec![PhysicalWrite {
        key: deterministic_key(1),
        value: Some(vec![1]),
    }];
    assert!(engine.prewrite_batch(txn_id, start_ts, writes).is_ok());

    assert_eq!(
        engine.commit_batch(
            TxnId(2),
            start_ts,
            Timestamp(20),
            vec![deterministic_key(1)]
        ),
        Err(BatchCommitError::TxnIdMismatch {
            key: deterministic_key(1)
        })
    );
    assert_eq!(
        engine.commit_batch(
            txn_id,
            Timestamp(15),
            Timestamp(20),
            vec![deterministic_key(1)]
        ),
        Err(BatchCommitError::StartTsMismatch {
            key: deterministic_key(1)
        })
    );
}

#[test]
fn test_commit_ts_too_old() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    let txn_id = TxnId(1);
    let start_ts = Timestamp(10);
    let writes = vec![PhysicalWrite {
        key: deterministic_key(1),
        value: Some(vec![1]),
    }];
    assert!(engine.prewrite_batch(txn_id, start_ts, writes).is_ok());

    // Simulate a newer write directly in backend bypassing the lock
    engine
        .backend_mut()
        .put_committed(nexir_mvcc_core::types::CommittedVersion {
            key: deterministic_key(1),
            commit_ts: Timestamp(25),
            value: Some(vec![2]),
        })
        .unwrap();

    assert_eq!(
        engine.commit_batch(txn_id, start_ts, Timestamp(20), vec![deterministic_key(1)]),
        Err(BatchCommitError::CommitTsTooOld {
            key: deterministic_key(1),
            commit_ts: Timestamp(20),
            latest_commit_ts: Timestamp(25)
        })
    );
}

#[test]
fn test_duplicate_keys_rejected() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    let writes = vec![
        PhysicalWrite {
            key: deterministic_key(1),
            value: Some(vec![1]),
        },
        PhysicalWrite {
            key: deterministic_key(1),
            value: Some(vec![2]),
        },
    ];
    assert_eq!(
        engine.prewrite_batch(TxnId(1), Timestamp(10), writes),
        Err(BatchPrewriteError::DuplicateKeyInBatch {
            key: deterministic_key(1)
        })
    );

    assert_eq!(
        engine.commit_batch(
            TxnId(1),
            Timestamp(10),
            Timestamp(20),
            vec![deterministic_key(1), deterministic_key(1)]
        ),
        Err(BatchCommitError::DuplicateKeyInBatch {
            key: deterministic_key(1)
        })
    );

    assert_eq!(
        engine.abort_batch(
            TxnId(1),
            Timestamp(10),
            vec![deterministic_key(1), deterministic_key(1)]
        ),
        Err(BatchAbortError::DuplicateKeyInBatch {
            key: deterministic_key(1)
        })
    );
}

#[test]
fn test_abort_removes_matching_ignores_unmatched() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    let txn_id = TxnId(1);
    let start_ts = Timestamp(10);
    let writes = vec![PhysicalWrite {
        key: deterministic_key(1),
        value: Some(vec![1]),
    }];
    assert!(engine.prewrite_batch(txn_id, start_ts, writes).is_ok());

    let keys = vec![deterministic_key(1), deterministic_key(2)];
    assert!(engine.abort_batch(txn_id, start_ts, keys).is_ok());

    assert!(
        engine
            .backend()
            .get_intent(&deterministic_key(1))
            .unwrap()
            .is_none()
    );
}
