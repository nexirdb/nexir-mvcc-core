use nexir_mvcc_core::{
    BatchError, InMemoryBackend, Mutation, MvccEngine, PhysicalWrite, ReadGuard, Timestamp, TxnId,
};

#[test]
fn test_direct_batch_multiple_keys() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    let writes = vec![
        PhysicalWrite {
            key: b"k1".to_vec(),
            value: Some(b"v1".to_vec()),
        },
        PhysicalWrite {
            key: b"k2".to_vec(),
            value: Some(b"v2".to_vec()),
        },
    ];
    engine.apply_direct_batch(Timestamp(10), writes).unwrap();

    assert_eq!(
        engine.read(b"k1", Timestamp(10)).unwrap(),
        Some(b"v1".to_vec())
    );
    assert_eq!(
        engine.read(b"k2", Timestamp(10)).unwrap(),
        Some(b"v2".to_vec())
    );
}

#[test]
fn test_direct_batch_empty() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    let res = engine.apply_direct_batch(Timestamp(10), vec![]);
    assert_eq!(res, Err(BatchError::EmptyBatch));
}

#[test]
fn test_direct_batch_rejects_retroactive_commit_ts() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    let writes1 = vec![PhysicalWrite {
        key: b"k1".to_vec(),
        value: Some(b"v1".to_vec()),
    }];
    engine.apply_direct_batch(Timestamp(10), writes1).unwrap();

    let writes2 = vec![PhysicalWrite {
        key: b"k1".to_vec(),
        value: Some(b"v2".to_vec()),
    }];
    let res = engine.apply_direct_batch(Timestamp(10), writes2); // exact duplicate
    assert_eq!(
        res,
        Err(BatchError::CommitTsTooOld {
            key: b"k1".to_vec(),
            commit_ts: Timestamp(10),
            latest_commit_ts: Timestamp(10)
        })
    );

    let writes3 = vec![PhysicalWrite {
        key: b"k1".to_vec(),
        value: Some(b"v3".to_vec()),
    }];
    let res = engine.apply_direct_batch(Timestamp(9), writes3); // older
    assert_eq!(
        res,
        Err(BatchError::CommitTsTooOld {
            key: b"k1".to_vec(),
            commit_ts: Timestamp(9),
            latest_commit_ts: Timestamp(10)
        })
    );
}

#[test]
fn test_direct_batch_conflicts_with_active_intent() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    engine
        .prewrite(
            TxnId(1),
            Timestamp(5),
            b"k1".to_vec(),
            Mutation::Put(b"i1".to_vec()),
        )
        .unwrap();

    let writes = vec![PhysicalWrite {
        key: b"k1".to_vec(),
        value: Some(b"v1".to_vec()),
    }];
    let res = engine.apply_direct_batch(Timestamp(10), writes);
    assert_eq!(
        res,
        Err(BatchError::KeyLocked {
            key: b"k1".to_vec(),
            txn_id: TxnId(1)
        })
    );
}

#[test]
fn test_direct_batch_rejects_duplicate_keys() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    let writes = vec![
        PhysicalWrite {
            key: b"k1".to_vec(),
            value: Some(b"v1".to_vec()),
        },
        PhysicalWrite {
            key: b"k1".to_vec(),
            value: Some(b"v2".to_vec()),
        },
    ];
    let res = engine.apply_direct_batch(Timestamp(10), writes);
    assert_eq!(
        res,
        Err(BatchError::DuplicateKeyInBatch {
            key: b"k1".to_vec()
        })
    );
}

#[test]
fn test_guarded_batch_succeeds_expected_version() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    engine
        .apply_direct_batch(
            Timestamp(5),
            vec![PhysicalWrite {
                key: b"k1".to_vec(),
                value: Some(b"v1".to_vec()),
            }],
        )
        .unwrap();

    let guards = vec![ReadGuard::ExpectedVersion {
        key: b"k1".to_vec(),
        read_ts: Timestamp(8),
        expected_commit_ts: Some(Timestamp(5)),
    }];
    let writes = vec![PhysicalWrite {
        key: b"k1".to_vec(),
        value: Some(b"v2".to_vec()),
    }];
    engine
        .apply_guarded_batch(Timestamp(10), guards, writes)
        .unwrap();

    assert_eq!(
        engine.read(b"k1", Timestamp(10)).unwrap(),
        Some(b"v2".to_vec())
    );
}

#[test]
fn test_guarded_batch_fails_expected_version_mismatch() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    engine
        .apply_direct_batch(
            Timestamp(5),
            vec![PhysicalWrite {
                key: b"k1".to_vec(),
                value: Some(b"v1".to_vec()),
            }],
        )
        .unwrap();

    let guards = vec![ReadGuard::ExpectedVersion {
        key: b"k1".to_vec(),
        read_ts: Timestamp(8),
        expected_commit_ts: Some(Timestamp(4)), // mismatch
    }];
    let writes = vec![PhysicalWrite {
        key: b"k1".to_vec(),
        value: Some(b"v2".to_vec()),
    }];
    let res = engine.apply_guarded_batch(Timestamp(10), guards, writes);
    assert_eq!(
        res,
        Err(BatchError::GuardFailedVersionMismatch {
            key: b"k1".to_vec(),
            expected: Some(Timestamp(4)),
            actual: Some(Timestamp(5)),
        })
    );
}

#[test]
fn test_guarded_batch_fails_newer_version_after_read_ts() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    engine
        .apply_direct_batch(
            Timestamp(5),
            vec![PhysicalWrite {
                key: b"k1".to_vec(),
                value: Some(b"v1".to_vec()),
            }],
        )
        .unwrap();
    engine
        .apply_direct_batch(
            Timestamp(9),
            vec![PhysicalWrite {
                key: b"k1".to_vec(),
                value: Some(b"v2".to_vec()),
            }],
        )
        .unwrap();

    let guards = vec![ReadGuard::ExpectedVersion {
        key: b"k1".to_vec(),
        read_ts: Timestamp(8),
        expected_commit_ts: Some(Timestamp(5)),
    }];
    let writes = vec![PhysicalWrite {
        key: b"k1".to_vec(),
        value: Some(b"v3".to_vec()),
    }];
    let res = engine.apply_guarded_batch(Timestamp(10), guards, writes);
    assert_eq!(
        res,
        Err(BatchError::GuardFailedNewerVersion {
            key: b"k1".to_vec(),
            read_ts: Timestamp(8),
            actual_commit_ts: Timestamp(9),
        })
    );
}

#[test]
fn test_guarded_batch_rejects_empty_guards() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    let writes = vec![PhysicalWrite {
        key: b"k1".to_vec(),
        value: Some(b"v1".to_vec()),
    }];
    let res = engine.apply_guarded_batch(Timestamp(10), vec![], writes);
    assert_eq!(res, Err(BatchError::NoReadGuards));
}

#[test]
fn test_guarded_batch_empty_writes() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    let guards = vec![ReadGuard::ExpectedVersion {
        key: b"k1".to_vec(),
        read_ts: Timestamp(8),
        expected_commit_ts: None,
    }];
    let res = engine.apply_guarded_batch(Timestamp(10), guards, vec![]);
    assert_eq!(res, Err(BatchError::EmptyBatch));
}

#[test]
fn test_guarded_batch_empty_guards_and_writes() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    let res = engine.apply_guarded_batch(Timestamp(10), vec![], vec![]);
    assert_eq!(res, Err(BatchError::EmptyBatch));
}

#[test]
fn test_guarded_batch_expected_absent_succeeds_fails() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());

    // Succeeds when absent
    let guards = vec![ReadGuard::ExpectedVersion {
        key: b"k1".to_vec(),
        read_ts: Timestamp(5),
        expected_commit_ts: None,
    }];
    let writes = vec![PhysicalWrite {
        key: b"k1".to_vec(),
        value: Some(b"v1".to_vec()),
    }];
    engine
        .apply_guarded_batch(Timestamp(10), guards, writes)
        .unwrap();

    // Fails when present
    let guards2 = vec![ReadGuard::ExpectedVersion {
        key: b"k1".to_vec(),
        read_ts: Timestamp(15),
        expected_commit_ts: None,
    }];
    let writes2 = vec![PhysicalWrite {
        key: b"k1".to_vec(),
        value: Some(b"v2".to_vec()),
    }];
    let res = engine.apply_guarded_batch(Timestamp(20), guards2, writes2);
    assert_eq!(
        res,
        Err(BatchError::GuardFailedVersionMismatch {
            key: b"k1".to_vec(),
            expected: None,
            actual: Some(Timestamp(10)),
        })
    );
}

#[test]
fn test_guarded_batch_expected_value_succeeds_fails() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    engine
        .apply_direct_batch(
            Timestamp(5),
            vec![PhysicalWrite {
                key: b"k1".to_vec(),
                value: Some(b"v1".to_vec()),
            }],
        )
        .unwrap();

    // Succeeds expected value
    let guards = vec![ReadGuard::ExpectedValue {
        key: b"k1".to_vec(),
        read_ts: Timestamp(8),
        expected_value: Some(b"v1".to_vec()),
    }];
    let writes = vec![PhysicalWrite {
        key: b"k1".to_vec(),
        value: Some(b"v2".to_vec()),
    }];
    engine
        .apply_guarded_batch(Timestamp(10), guards, writes)
        .unwrap();

    // Fails expected value
    let guards2 = vec![ReadGuard::ExpectedValue {
        key: b"k1".to_vec(),
        read_ts: Timestamp(15),
        expected_value: Some(b"v1".to_vec()), // expected old value
    }];
    let writes2 = vec![PhysicalWrite {
        key: b"k1".to_vec(),
        value: Some(b"v3".to_vec()),
    }];
    let res = engine.apply_guarded_batch(Timestamp(20), guards2, writes2);
    assert_eq!(
        res,
        Err(BatchError::GuardFailedValueMismatch {
            key: b"k1".to_vec()
        })
    );

    // Delete k1
    engine
        .apply_direct_batch(
            Timestamp(25),
            vec![PhysicalWrite {
                key: b"k1".to_vec(),
                value: None,
            }],
        )
        .unwrap();

    // Succeeds expected absent logical value
    let guards3 = vec![ReadGuard::ExpectedValue {
        key: b"k1".to_vec(),
        read_ts: Timestamp(28),
        expected_value: None,
    }];
    let writes3 = vec![PhysicalWrite {
        key: b"k1".to_vec(),
        value: Some(b"v4".to_vec()),
    }];
    engine
        .apply_guarded_batch(Timestamp(30), guards3, writes3)
        .unwrap();
    assert_eq!(
        engine.read(b"k1", Timestamp(30)).unwrap(),
        Some(b"v4".to_vec())
    );
}

#[test]
fn test_guarded_batch_writes_nothing_on_failure() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    engine
        .apply_direct_batch(
            Timestamp(5),
            vec![PhysicalWrite {
                key: b"k1".to_vec(),
                value: Some(b"v1".to_vec()),
            }],
        )
        .unwrap();

    let guards = vec![ReadGuard::ExpectedVersion {
        key: b"k1".to_vec(),
        read_ts: Timestamp(8),
        expected_commit_ts: Some(Timestamp(4)), // mismatch
    }];
    let writes = vec![
        PhysicalWrite {
            key: b"k1".to_vec(),
            value: Some(b"v2".to_vec()),
        },
        PhysicalWrite {
            key: b"k2".to_vec(),
            value: Some(b"v2".to_vec()),
        },
    ];
    assert!(engine
        .apply_guarded_batch(Timestamp(10), guards, writes)
        .is_err());

    // verify nothing was written
    assert_eq!(
        engine.read(b"k1", Timestamp(10)).unwrap(),
        Some(b"v1".to_vec())
    );
    assert_eq!(engine.read(b"k2", Timestamp(10)).unwrap(), None);
}

#[test]
fn test_stale_incr_scenario() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    engine
        .apply_direct_batch(
            Timestamp(5),
            vec![PhysicalWrite {
                key: b"counter".to_vec(),
                value: Some(b"10".to_vec()),
            }],
        )
        .unwrap();

    // Client A reads 10 at TS 8, computes 11, proposes commit at TS 10
    let guards_a = vec![ReadGuard::ExpectedVersion {
        key: b"counter".to_vec(),
        read_ts: Timestamp(8),
        expected_commit_ts: Some(Timestamp(5)),
    }];
    let writes_a = vec![PhysicalWrite {
        key: b"counter".to_vec(),
        value: Some(b"11".to_vec()),
    }];

    // Client B reads 10 at TS 9, computes 11, proposes commit at TS 12
    let guards_b = vec![ReadGuard::ExpectedVersion {
        key: b"counter".to_vec(),
        read_ts: Timestamp(9),
        expected_commit_ts: Some(Timestamp(5)),
    }];
    let writes_b = vec![PhysicalWrite {
        key: b"counter".to_vec(),
        value: Some(b"11".to_vec()),
    }];

    // Ordered apply processes A first
    engine
        .apply_guarded_batch(Timestamp(10), guards_a, writes_a)
        .unwrap();

    // Ordered apply processes B next -> MUST FAIL
    let res = engine.apply_guarded_batch(Timestamp(12), guards_b, writes_b);
    assert!(matches!(
        res,
        Err(BatchError::GuardFailedNewerVersion { .. })
    ));

    // Counter is exactly 11
    assert_eq!(
        engine.read(b"counter", Timestamp(20)).unwrap(),
        Some(b"11".to_vec())
    );
}

#[test]
fn test_guarded_batch_invalid_commit_timestamp() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    let guards = vec![ReadGuard::ExpectedVersion {
        key: b"k1".to_vec(),
        read_ts: Timestamp(10),
        expected_commit_ts: None,
    }];
    let writes = vec![PhysicalWrite {
        key: b"k1".to_vec(),
        value: Some(b"v1".to_vec()),
    }];

    // commit_ts == read_ts
    let res = engine.apply_guarded_batch(Timestamp(10), guards.clone(), writes.clone());
    assert_eq!(
        res,
        Err(BatchError::InvalidCommitTimestamp {
            read_ts: Timestamp(10),
            commit_ts: Timestamp(10)
        })
    );

    // commit_ts < read_ts
    let res2 = engine.apply_guarded_batch(Timestamp(9), guards, writes);
    assert_eq!(
        res2,
        Err(BatchError::InvalidCommitTimestamp {
            read_ts: Timestamp(10),
            commit_ts: Timestamp(9)
        })
    );
}

#[test]
fn test_guarded_batch_active_intent_conflicts() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    engine
        .prewrite(
            TxnId(1),
            Timestamp(5),
            b"k1".to_vec(),
            Mutation::Put(b"i1".to_vec()),
        )
        .unwrap();
    engine
        .prewrite(
            TxnId(2),
            Timestamp(5),
            b"k2".to_vec(),
            Mutation::Put(b"i2".to_vec()),
        )
        .unwrap();

    // Intent on guard key
    let guards1 = vec![ReadGuard::ExpectedVersion {
        key: b"k1".to_vec(),
        read_ts: Timestamp(8),
        expected_commit_ts: None,
    }];
    let writes1 = vec![PhysicalWrite {
        key: b"k3".to_vec(),
        value: Some(b"v3".to_vec()),
    }];
    let res1 = engine.apply_guarded_batch(Timestamp(10), guards1, writes1);
    assert_eq!(
        res1,
        Err(BatchError::KeyLocked {
            key: b"k1".to_vec(),
            txn_id: TxnId(1)
        })
    );

    // Intent on write key
    let guards2 = vec![ReadGuard::ExpectedVersion {
        key: b"k4".to_vec(),
        read_ts: Timestamp(8),
        expected_commit_ts: None,
    }];
    let writes2 = vec![PhysicalWrite {
        key: b"k2".to_vec(),
        value: Some(b"v2".to_vec()),
    }];
    let res2 = engine.apply_guarded_batch(Timestamp(10), guards2, writes2);
    assert_eq!(
        res2,
        Err(BatchError::KeyLocked {
            key: b"k2".to_vec(),
            txn_id: TxnId(2)
        })
    );
}

#[test]
fn test_guarded_batch_expected_version_none_after_tombstone() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    engine
        .apply_direct_batch(
            Timestamp(5),
            vec![PhysicalWrite {
                key: b"k1".to_vec(),
                value: Some(b"v1".to_vec()),
            }],
        )
        .unwrap();
    engine
        .apply_direct_batch(
            Timestamp(10),
            vec![PhysicalWrite {
                key: b"k1".to_vec(),
                value: None,
            }],
        )
        .unwrap();

    let guards = vec![ReadGuard::ExpectedVersion {
        key: b"k1".to_vec(),
        read_ts: Timestamp(15),
        expected_commit_ts: None, // This should fail! A tombstone is still a committed version.
    }];
    let writes = vec![PhysicalWrite {
        key: b"k1".to_vec(),
        value: Some(b"v2".to_vec()),
    }];

    // It should fail because there IS a committed version (the tombstone at 10)
    let res = engine.apply_guarded_batch(Timestamp(20), guards, writes);
    assert_eq!(
        res,
        Err(BatchError::GuardFailedVersionMismatch {
            key: b"k1".to_vec(),
            expected: None,
            actual: Some(Timestamp(10)),
        })
    );
}
