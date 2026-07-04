use nexir_mvcc_core::{
    error::GcError, Backend, InMemoryBackend, Mutation, MvccEngine, Timestamp, TxnId,
};

fn setup_engine() -> MvccEngine<InMemoryBackend> {
    MvccEngine::new(InMemoryBackend::new())
}

#[test]
fn incremental_gc_no_versions_noop() {
    let mut engine = setup_engine();
    let res = engine
        .gc_incremental(
            Timestamp(100),
            None,
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 10,
                    max_versions: 10,
                },
                collapse_final_tombstones: false,
            },
        )
        .unwrap();
    assert!(res.done);
    assert_eq!(res.keys_scanned, 0);
    assert_eq!(res.versions_removed, 0);
}

#[test]
fn incremental_gc_rejects_zero_budget() {
    let mut engine = setup_engine();
    let err = engine
        .gc_incremental(
            Timestamp(100),
            None,
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 0,
                    max_versions: 10,
                },
                collapse_final_tombstones: false,
            },
        )
        .unwrap_err();
    assert_eq!(err, GcError::InvalidGcBudget);

    let err2 = engine
        .gc_incremental(
            Timestamp(100),
            None,
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 10,
                    max_versions: 0,
                },
                collapse_final_tombstones: false,
            },
        )
        .unwrap_err();
    assert_eq!(err2, GcError::InvalidGcBudget);
}

#[test]
fn incremental_gc_removes_old_versions_preserves_cutoff_version() {
    let mut engine = setup_engine();
    engine
        .backend_mut()
        .put_committed(nexir_mvcc_core::CommittedVersion {
            key: b"k1".to_vec(),
            commit_ts: Timestamp(10),
            value: Some(b"v1".to_vec()),
        })
        .unwrap();
    engine
        .backend_mut()
        .put_committed(nexir_mvcc_core::CommittedVersion {
            key: b"k1".to_vec(),
            commit_ts: Timestamp(20),
            value: Some(b"v2".to_vec()),
        })
        .unwrap();
    engine
        .backend_mut()
        .put_committed(nexir_mvcc_core::CommittedVersion {
            key: b"k1".to_vec(),
            commit_ts: Timestamp(30),
            value: Some(b"v3".to_vec()),
        })
        .unwrap();

    let res = engine
        .gc_incremental(
            Timestamp(25),
            None,
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 10,
                    max_versions: 10,
                },
                collapse_final_tombstones: false,
            },
        )
        .unwrap();

    assert!(res.done);
    assert_eq!(res.versions_removed, 1); // Only 10 is removed, 20 is the keeper at 25

    let versions = engine.backend().get_committed_versions(b"k1").unwrap();
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0].commit_ts, Timestamp(20));
    assert_eq!(versions[1].commit_ts, Timestamp(30));
}

#[test]
fn incremental_gc_keeps_versions_newer_than_safe_point() {
    let mut engine = setup_engine();
    for ts in [10, 20, 30] {
        engine
            .backend_mut()
            .put_committed(nexir_mvcc_core::CommittedVersion {
                key: b"k1".to_vec(),
                commit_ts: Timestamp(ts),
                value: Some(vec![ts as u8]),
            })
            .unwrap();
    }

    let res = engine
        .gc_incremental(
            Timestamp(5),
            None,
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 10,
                    max_versions: 10,
                },
                collapse_final_tombstones: false,
            },
        )
        .unwrap();

    assert!(res.done);
    assert_eq!(res.versions_removed, 0); // All newer than 5
}

#[test]
fn incremental_gc_preserves_active_intents() {
    let mut engine = setup_engine();
    engine
        .prewrite(
            TxnId(1),
            Timestamp(5),
            b"k1".to_vec(),
            Mutation::Put(b"v1".to_vec()),
        )
        .unwrap();

    let res = engine
        .gc_incremental(
            Timestamp(100),
            None,
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 10,
                    max_versions: 10,
                },
                collapse_final_tombstones: false,
            },
        )
        .unwrap();

    assert!(res.done);
    assert_eq!(res.intents_preserved, 1);
    assert!(engine.backend().get_intent(b"k1").unwrap().is_some());
}

#[test]
fn incremental_gc_cursor_resumes_across_keys() {
    let mut engine = setup_engine();
    for key in [b"a", b"b", b"c"] {
        engine
            .backend_mut()
            .put_committed(nexir_mvcc_core::CommittedVersion {
                key: key.to_vec(),
                commit_ts: Timestamp(10),
                value: Some(b"v".to_vec()),
            })
            .unwrap();
        engine
            .backend_mut()
            .put_committed(nexir_mvcc_core::CommittedVersion {
                key: key.to_vec(),
                commit_ts: Timestamp(20),
                value: Some(b"v".to_vec()),
            })
            .unwrap();
    }

    // Budget: 1 key at a time
    let res1 = engine
        .gc_incremental(
            Timestamp(25),
            None,
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 1,
                    max_versions: 10,
                },
                collapse_final_tombstones: false,
            },
        )
        .unwrap();

    assert!(!res1.done);
    assert_eq!(res1.keys_scanned, 1);
    assert_eq!(res1.versions_removed, 1);
    assert_eq!(res1.cursor.next_key, Some(b"b".to_vec()));

    let res2 = engine
        .gc_incremental(
            Timestamp(25),
            Some(res1.cursor),
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 1,
                    max_versions: 10,
                },
                collapse_final_tombstones: false,
            },
        )
        .unwrap();

    assert!(!res2.done);
    assert_eq!(res2.keys_scanned, 1);
    assert_eq!(res2.versions_removed, 1);
    assert_eq!(res2.cursor.next_key, Some(b"c".to_vec()));

    let res3 = engine
        .gc_incremental(
            Timestamp(25),
            Some(res2.cursor),
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 1,
                    max_versions: 10,
                },
                collapse_final_tombstones: false,
            },
        )
        .unwrap();

    assert!(res3.done);
    assert_eq!(res3.keys_scanned, 1);
    assert_eq!(res3.versions_removed, 1);
    assert_eq!(res3.cursor.next_key, None);
}

#[test]
fn incremental_gc_budget_limits_versions_removed() {
    let mut engine = setup_engine();
    for ts in 1..=5 {
        engine
            .backend_mut()
            .put_committed(nexir_mvcc_core::CommittedVersion {
                key: b"k1".to_vec(),
                commit_ts: Timestamp(ts),
                value: Some(vec![ts as u8]),
            })
            .unwrap();
    }

    // GC at ts 10 should remove ts 1, 2, 3, 4 (keep 5)
    // Budget: max 2 versions
    let res1 = engine
        .gc_incremental(
            Timestamp(10),
            None,
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 10,
                    max_versions: 2,
                },
                collapse_final_tombstones: false,
            },
        )
        .unwrap();

    assert!(!res1.done);
    assert_eq!(res1.versions_removed, 2);
    assert_eq!(res1.cursor.next_key, Some(b"k1".to_vec())); // Paused at k1

    // 2nd pass
    let res2 = engine
        .gc_incremental(
            Timestamp(10),
            Some(res1.cursor),
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 10,
                    max_versions: 2,
                },
                collapse_final_tombstones: false,
            },
        )
        .unwrap();

    // Removes remaining 2. Is there more? No.
    // Wait: budget was exactly equal to remaining (2). It checks for more and finds 0.
    // So it finishes the key. There are no more keys. So done = true.
    assert!(res2.done);
    assert_eq!(res2.versions_removed, 2);
    assert_eq!(res2.cursor.next_key, None);
}

#[test]
fn full_gc_counts_intent_once_when_key_spans_steps() {
    let mut engine = setup_engine();

    for ts in 1..=10002 {
        engine
            .backend_mut()
            .put_committed(nexir_mvcc_core::CommittedVersion {
                key: b"k1".to_vec(),
                commit_ts: Timestamp(ts),
                value: Some(vec![1]),
            })
            .unwrap();
    }

    engine
        .prewrite(
            TxnId(1),
            Timestamp(10002),
            b"k1".to_vec(),
            Mutation::Put(b"next".to_vec()),
        )
        .unwrap();

    let stats = engine
        .gc(
            Timestamp(10003),
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 10000,
                    max_versions: 10000,
                },
                collapse_final_tombstones: false,
            },
        )
        .unwrap();
    assert_eq!(stats.versions_removed, 10001);
    assert_eq!(stats.intents_preserved, 1);
    assert!(engine.backend().get_intent(b"k1").unwrap().is_some());
}

#[test]
fn incremental_gc_full_pass_matches_existing_gc() {
    let mut engine1 = setup_engine();
    let mut engine2 = setup_engine();

    for key in [b"a", b"b"] {
        for ts in 1..=5 {
            let ver = nexir_mvcc_core::CommittedVersion {
                key: key.to_vec(),
                commit_ts: Timestamp(ts),
                value: Some(vec![ts as u8]),
            };
            engine1.backend_mut().put_committed(ver.clone()).unwrap();
            engine2.backend_mut().put_committed(ver).unwrap();
        }
    }

    // Old full GC
    engine1
        .gc(
            Timestamp(4),
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 10000,
                    max_versions: 10000,
                },
                collapse_final_tombstones: false,
            },
        )
        .unwrap();

    // Incremental full GC
    let mut cursor = None;
    loop {
        let res = engine2
            .gc_incremental(
                Timestamp(4),
                cursor,
                nexir_mvcc_core::GcOptions {
                    budget: nexir_mvcc_core::GcBudget {
                        max_keys: 1,
                        max_versions: 1,
                    },
                    collapse_final_tombstones: false,
                },
            )
            .unwrap();
        if res.done {
            break;
        }
        cursor = Some(res.cursor);
    }

    for key in [b"a", b"b"] {
        let v1 = engine1.backend().get_committed_versions(key).unwrap();
        let v2 = engine2.backend().get_committed_versions(key).unwrap();
        assert_eq!(v1, v2);
    }
}

#[test]
fn incremental_gc_is_deterministic_across_two_engines() {
    let mut engine1 = setup_engine();
    let mut engine2 = setup_engine();

    for ts in 1..=10 {
        let ver = nexir_mvcc_core::CommittedVersion {
            key: b"k1".to_vec(),
            commit_ts: Timestamp(ts),
            value: Some(vec![ts as u8]),
        };
        engine1.backend_mut().put_committed(ver.clone()).unwrap();
        engine2.backend_mut().put_committed(ver).unwrap();
    }

    // They should produce exact same results with same inputs
    let res1 = engine1
        .gc_incremental(
            Timestamp(10),
            None,
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 1,
                    max_versions: 5,
                },
                collapse_final_tombstones: false,
            },
        )
        .unwrap();

    let res2 = engine2
        .gc_incremental(
            Timestamp(10),
            None,
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 1,
                    max_versions: 5,
                },
                collapse_final_tombstones: false,
            },
        )
        .unwrap();

    assert_eq!(res1, res2);
}

#[test]
fn incremental_gc_binary_keys_ordering() {
    let mut engine = setup_engine();

    // Add keys in binary non-utf8 order
    let k1 = vec![0, 0, 0];
    let k2 = vec![0, 0, 1];
    let k3 = vec![0, 1, 0];

    for k in [&k1, &k2, &k3] {
        engine
            .backend_mut()
            .put_committed(nexir_mvcc_core::CommittedVersion {
                key: k.clone(),
                commit_ts: Timestamp(10),
                value: Some(b"v".to_vec()),
            })
            .unwrap();
    }

    let res = engine
        .gc_incremental(
            Timestamp(10),
            None,
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 1,
                    max_versions: 10,
                },
                collapse_final_tombstones: false,
            },
        )
        .unwrap();

    assert_eq!(res.cursor.next_key, Some(k2));
}

#[test]
fn incremental_gc_repeated_calls_eventually_done() {
    let mut engine = setup_engine();
    for key in 0..100u8 {
        engine
            .backend_mut()
            .put_committed(nexir_mvcc_core::CommittedVersion {
                key: vec![key],
                commit_ts: Timestamp(10),
                value: Some(b"v".to_vec()),
            })
            .unwrap();
    }

    let mut cursor = None;
    let mut calls = 0;
    loop {
        let res = engine
            .gc_incremental(
                Timestamp(10),
                cursor,
                nexir_mvcc_core::GcOptions {
                    budget: nexir_mvcc_core::GcBudget {
                        max_keys: 7,
                        max_versions: 10,
                    },
                    collapse_final_tombstones: false,
                },
            )
            .unwrap();
        calls += 1;
        if res.done {
            break;
        }
        cursor = Some(res.cursor);
        assert!(calls < 100); // Prevent infinite loops
    }

    assert_eq!(calls, 15); // 100 / 7 = 14 + 1
}

#[test]
fn gc_collapses_final_tombstone_when_safely_past_safe_point() {
    let mut engine = setup_engine();
    engine
        .backend_mut()
        .put_committed(nexir_mvcc_core::CommittedVersion {
            key: b"k1".to_vec(),
            commit_ts: Timestamp(10),
            value: Some(b"v1".to_vec()),
        })
        .unwrap();
    engine
        .backend_mut()
        .put_committed(nexir_mvcc_core::CommittedVersion {
            key: b"k1".to_vec(),
            commit_ts: Timestamp(20),
            value: None,
        })
        .unwrap();

    let res = engine
        .gc_incremental(
            Timestamp(25),
            None,
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 10,
                    max_versions: 10,
                },
                collapse_final_tombstones: true,
            },
        )
        .unwrap();
    assert!(res.done);
    assert_eq!(res.versions_removed, 2); // 1 for v1, 1 for tombstone

    let versions = engine.backend().get_committed_versions(b"k1").unwrap();
    assert!(versions.is_empty(), "final tombstone should be collapsed");
}

#[test]
fn gc_keeps_final_tombstone_if_active_read_pin_prevents_collapse() {
    let mut engine = setup_engine();
    engine
        .backend_mut()
        .put_committed(nexir_mvcc_core::CommittedVersion {
            key: b"k1".to_vec(),
            commit_ts: Timestamp(10),
            value: Some(b"v1".to_vec()),
        })
        .unwrap();
    engine
        .backend_mut()
        .put_committed(nexir_mvcc_core::CommittedVersion {
            key: b"k1".to_vec(),
            commit_ts: Timestamp(20),
            value: None,
        })
        .unwrap();

    // Safe point at 10: keeper is ts10 (the old value). The tombstone is newer and un-collected.
    let res = engine
        .gc_incremental(
            Timestamp(10),
            None,
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 10,
                    max_versions: 10,
                },
                collapse_final_tombstones: true,
            },
        )
        .unwrap();
    assert!(res.done);
    assert_eq!(res.versions_removed, 0);

    let versions = engine.backend().get_committed_versions(b"k1").unwrap();
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0].commit_ts, Timestamp(10));
    assert_eq!(versions[1].commit_ts, Timestamp(20));

    // The old value remains fully readable at the safe point
    let val = engine.read(b"k1", Timestamp(10)).unwrap();
    assert_eq!(val.as_deref(), Some(b"v1".as_ref()));
}

#[test]
fn gc_keeps_tombstone_if_newer_version_exists() {
    let mut engine = setup_engine();
    engine
        .backend_mut()
        .put_committed(nexir_mvcc_core::CommittedVersion {
            key: b"k1".to_vec(),
            commit_ts: Timestamp(20),
            value: None,
        })
        .unwrap();
    engine
        .backend_mut()
        .put_committed(nexir_mvcc_core::CommittedVersion {
            key: b"k1".to_vec(),
            commit_ts: Timestamp(30),
            value: Some(b"v2".to_vec()),
        })
        .unwrap();

    // Safe point at 25: keeper is ts20 (the tombstone). But ts30 exists!
    let res = engine
        .gc_incremental(
            Timestamp(25),
            None,
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 10,
                    max_versions: 10,
                },
                collapse_final_tombstones: false,
            },
        )
        .unwrap();
    assert!(res.done);
    assert_eq!(res.versions_removed, 0);

    let versions = engine.backend().get_committed_versions(b"k1").unwrap();
    assert_eq!(versions.len(), 2);
}

#[test]
fn gc_keeps_final_tombstone_if_active_intent_exists() {
    let mut engine = setup_engine();
    engine
        .backend_mut()
        .put_committed(nexir_mvcc_core::CommittedVersion {
            key: b"k1".to_vec(),
            commit_ts: Timestamp(20),
            value: None,
        })
        .unwrap();

    // Active intent on the same key
    engine
        .prewrite(
            TxnId(1),
            Timestamp(25),
            b"k1".to_vec(),
            Mutation::Put(b"v2".to_vec()),
        )
        .unwrap();

    let res = engine
        .gc_incremental(
            Timestamp(22),
            None,
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 10,
                    max_versions: 10,
                },
                collapse_final_tombstones: true,
            },
        )
        .unwrap();
    assert!(res.done);
    assert_eq!(res.versions_removed, 0); // Active intent prevents collapse

    let versions = engine.backend().get_committed_versions(b"k1").unwrap();
    assert_eq!(versions.len(), 1);
    assert!(engine.backend().get_intent(b"k1").unwrap().is_some());
}

#[derive(Clone)]
struct SpyBackend {
    inner: InMemoryBackend,
    collapse_tombstone_calls: CollapseTombstoneCalls,
    remove_committed_version_calls: RemoveCommittedVersionCalls,
}

type CollapseTombstoneCalls =
    std::sync::Arc<std::sync::Mutex<Vec<(Vec<u8>, Timestamp, Vec<Timestamp>)>>>;
type RemoveCommittedVersionCalls = std::sync::Arc<std::sync::Mutex<Vec<(Vec<u8>, Timestamp)>>>;

impl SpyBackend {
    fn new() -> Self {
        Self {
            inner: InMemoryBackend::new(),
            collapse_tombstone_calls: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            remove_committed_version_calls: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }
}

impl Backend for SpyBackend {
    fn get_latest_committed(
        &self,
        key: &[u8],
    ) -> Result<Option<nexir_mvcc_core::CommittedVersion>, String> {
        self.inner.get_latest_committed(key)
    }
    fn put_intent(&mut self, intent: nexir_mvcc_core::Intent) -> Result<(), String> {
        self.inner.put_intent(intent)
    }
    fn remove_intent(
        &mut self,
        key: &[u8],
        txn_id: TxnId,
        start_ts: Timestamp,
    ) -> Result<bool, String> {
        self.inner.remove_intent(key, txn_id, start_ts)
    }
    fn all_keys(&self) -> Result<Vec<Vec<u8>>, String> {
        self.inner.all_keys()
    }
    fn put_intents_batch(&mut self, intents: Vec<nexir_mvcc_core::Intent>) -> Result<(), String> {
        self.inner.put_intents_batch(intents)
    }
    fn commit_intents_batch(
        &mut self,
        commits: Vec<nexir_mvcc_core::CommittedVersion>,
        removed_intents: Vec<(Vec<u8>, TxnId, Timestamp)>,
    ) -> Result<(), String> {
        self.inner.commit_intents_batch(commits, removed_intents)
    }
    fn remove_intents_batch(
        &mut self,
        intents: Vec<(Vec<u8>, TxnId, Timestamp)>,
    ) -> Result<(), String> {
        self.inner.remove_intents_batch(intents)
    }
    fn keys_from(&self, start: Option<&[u8]>, limit: usize) -> Result<Vec<Vec<u8>>, String> {
        self.inner.keys_from(start, limit)
    }
    fn keys_from_prefix(
        &self,
        prefix: &[u8],
        start: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>, String> {
        self.inner.keys_from_prefix(prefix, start, limit)
    }
    fn get_committed_timestamps_before(
        &self,
        key: &[u8],
        before_ts: Timestamp,
        limit: usize,
    ) -> Result<Vec<Timestamp>, String> {
        self.inner
            .get_committed_timestamps_before(key, before_ts, limit)
    }
    fn get_latest_commit_ts(&self, key: &[u8]) -> Result<Option<Timestamp>, String> {
        self.inner.get_latest_commit_ts(key)
    }
    fn get_intent(&self, key: &[u8]) -> Result<Option<nexir_mvcc_core::Intent>, String> {
        self.inner.get_intent(key)
    }
    fn get_visible_committed(
        &self,
        key: &[u8],
        read_ts: Timestamp,
    ) -> Result<Option<nexir_mvcc_core::CommittedVersion>, String> {
        self.inner.get_visible_committed(key, read_ts)
    }
    fn get_committed_versions(
        &self,
        key: &[u8],
    ) -> Result<Vec<nexir_mvcc_core::CommittedVersion>, String> {
        self.inner.get_committed_versions(key)
    }
    fn put_committed(&mut self, version: nexir_mvcc_core::CommittedVersion) -> Result<(), String> {
        self.inner.put_committed(version)
    }
    fn put_committed_batch(
        &mut self,
        versions: Vec<nexir_mvcc_core::CommittedVersion>,
    ) -> Result<(), String> {
        self.inner.put_committed_batch(versions)
    }
    fn remove_committed_version(&mut self, key: &[u8], commit_ts: Timestamp) -> Result<(), String> {
        self.remove_committed_version_calls
            .lock()
            .unwrap()
            .push((key.to_vec(), commit_ts));
        self.inner.remove_committed_version(key, commit_ts)
    }
    fn collapse_tombstone(
        &mut self,
        key: &[u8],
        tombstone_ts: Timestamp,
        older_ts: Vec<Timestamp>,
    ) -> Result<(), String> {
        self.collapse_tombstone_calls.lock().unwrap().push((
            key.to_vec(),
            tombstone_ts,
            older_ts.clone(),
        ));
        self.inner.collapse_tombstone(key, tombstone_ts, older_ts)
    }
}

#[test]
fn test_spy_collapse_final_tombstone() {
    let mut spy = SpyBackend::new();
    spy.put_committed(nexir_mvcc_core::CommittedVersion {
        key: b"k1".to_vec(),
        commit_ts: Timestamp(10),
        value: Some(b"val".to_vec()),
    })
    .unwrap();
    spy.put_committed(nexir_mvcc_core::CommittedVersion {
        key: b"k1".to_vec(),
        commit_ts: Timestamp(20),
        value: None,
    })
    .unwrap();

    let collapse_calls = spy.collapse_tombstone_calls.clone();
    let remove_calls = spy.remove_committed_version_calls.clone();

    let mut engine = MvccEngine::new(spy);
    let res = engine
        .gc_incremental(
            Timestamp(25),
            None,
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 10,
                    max_versions: 10,
                },
                collapse_final_tombstones: true,
            },
        )
        .unwrap();

    assert!(res.done);
    assert_eq!(res.versions_removed, 2);

    let collapses = collapse_calls.lock().unwrap();
    assert_eq!(collapses.len(), 1);
    assert_eq!(collapses[0].0, b"k1");
    assert_eq!(collapses[0].1, Timestamp(20));
    assert_eq!(collapses[0].2, vec![Timestamp(10)]);

    let removes = remove_calls.lock().unwrap();
    assert!(removes.is_empty());
}

#[test]
fn test_spy_collapse_budget_exceeded() {
    let mut spy = SpyBackend::new();
    for ts in &[10, 12, 14] {
        spy.put_committed(nexir_mvcc_core::CommittedVersion {
            key: b"k1".to_vec(),
            commit_ts: Timestamp(*ts),
            value: Some(b"val".to_vec()),
        })
        .unwrap();
    }
    spy.put_committed(nexir_mvcc_core::CommittedVersion {
        key: b"k1".to_vec(),
        commit_ts: Timestamp(20),
        value: None,
    })
    .unwrap();

    let collapse_calls = spy.collapse_tombstone_calls.clone();
    let remove_calls = spy.remove_committed_version_calls.clone();

    let mut engine = MvccEngine::new(spy);
    let res = engine
        .gc_incremental(
            Timestamp(25),
            None,
            nexir_mvcc_core::GcOptions {
                budget: nexir_mvcc_core::GcBudget {
                    max_keys: 10,
                    max_versions: 2,
                },
                collapse_final_tombstones: true,
            },
        )
        .unwrap();

    assert!(!res.done);
    assert_eq!(res.versions_removed, 2);

    let collapses = collapse_calls.lock().unwrap();
    assert!(collapses.is_empty());

    let removes = remove_calls.lock().unwrap();
    assert_eq!(removes.len(), 2);
    assert_eq!(removes[0].0, b"k1");
    assert_eq!(removes[0].1, Timestamp(14));
    assert_eq!(removes[1].1, Timestamp(12));
}
