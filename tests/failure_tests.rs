use nexir_mvcc_core::{
    Backend, BatchError, CommittedVersion, InMemoryBackend, Intent, MvccEngine, PhysicalWrite,
    Timestamp, TxnId,
};

#[derive(Clone)]
struct FailingBackend {
    inner: InMemoryBackend,
    fail_on_put_batch: bool,
    put_batch_calls: usize,
    fail_on_put_intents: bool,
    put_intents_calls: usize,
    fail_on_commit_intents: bool,
    commit_intents_calls: usize,
    fail_on_remove_intents: bool,
    remove_intents_calls: usize,
    put_committed_calls: usize,
    remove_intent_calls: usize,
}

impl FailingBackend {
    fn new() -> Self {
        Self {
            inner: InMemoryBackend::new(),
            fail_on_put_batch: false,
            put_batch_calls: 0,
            fail_on_put_intents: false,
            put_intents_calls: 0,
            fail_on_commit_intents: false,
            commit_intents_calls: 0,
            fail_on_remove_intents: false,
            remove_intents_calls: 0,
            put_committed_calls: 0,
            remove_intent_calls: 0,
        }
    }
}

impl Backend for FailingBackend {
    fn get_committed_versions(&self, key: &[u8]) -> Result<Vec<CommittedVersion>, String> {
        self.inner.get_committed_versions(key)
    }

    fn get_latest_committed(&self, key: &[u8]) -> Result<Option<CommittedVersion>, String> {
        self.inner.get_latest_committed(key)
    }

    fn get_visible_committed(
        &self,
        key: &[u8],
        read_ts: Timestamp,
    ) -> Result<Option<CommittedVersion>, String> {
        self.inner.get_visible_committed(key, read_ts)
    }

    fn get_latest_commit_ts(&self, key: &[u8]) -> Result<Option<Timestamp>, String> {
        self.inner.get_latest_commit_ts(key)
    }

    fn get_intent(&self, key: &[u8]) -> Result<Option<Intent>, String> {
        self.inner.get_intent(key)
    }

    fn all_keys(&self) -> Result<Vec<Vec<u8>>, String> {
        self.inner.all_keys()
    }

    fn put_intent(&mut self, intent: Intent) -> Result<(), String> {
        self.inner.put_intent(intent)
    }

    fn remove_intent(
        &mut self,
        key: &[u8],
        txn_id: TxnId,
        start_ts: Timestamp,
    ) -> Result<bool, String> {
        self.remove_intent_calls += 1;
        self.inner.remove_intent(key, txn_id, start_ts)
    }

    fn put_committed(&mut self, version: CommittedVersion) -> Result<(), String> {
        self.put_committed_calls += 1;
        self.inner.put_committed(version)
    }

    fn remove_committed_version(&mut self, key: &[u8], commit_ts: Timestamp) -> Result<(), String> {
        self.inner.remove_committed_version(key, commit_ts)
    }

    fn put_committed_batch(&mut self, commits: Vec<CommittedVersion>) -> Result<(), String> {
        self.put_batch_calls += 1;
        if self.fail_on_put_batch {
            return Err("simulated backend failure".to_string());
        }
        self.inner.put_committed_batch(commits)
    }

    fn put_intents_batch(&mut self, intents: Vec<Intent>) -> Result<(), String> {
        self.put_intents_calls += 1;
        if self.fail_on_put_intents {
            return Err("simulated backend failure".to_string());
        }
        self.inner.put_intents_batch(intents)
    }

    fn commit_intents_batch(
        &mut self,
        commits: Vec<CommittedVersion>,
        removed_intents: Vec<(Vec<u8>, TxnId, Timestamp)>,
    ) -> Result<(), String> {
        self.commit_intents_calls += 1;
        if self.fail_on_commit_intents {
            return Err("simulated backend failure".to_string());
        }
        self.inner.commit_intents_batch(commits, removed_intents)
    }

    fn remove_intents_batch(
        &mut self,
        intents: Vec<(Vec<u8>, TxnId, Timestamp)>,
    ) -> Result<(), String> {
        self.remove_intents_calls += 1;
        if self.fail_on_remove_intents {
            return Err("simulated backend failure".to_string());
        }
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

    fn collapse_tombstone(
        &mut self,
        key: &[u8],
        tombstone_ts: Timestamp,
        older_ts: Vec<Timestamp>,
    ) -> Result<(), String> {
        self.inner.collapse_tombstone(key, tombstone_ts, older_ts)
    }
}

#[test]
fn test_validation_failure_bypasses_backend() {
    let mut backend = FailingBackend::new();
    backend.fail_on_put_batch = true;
    let mut engine = MvccEngine::new(backend);

    // Provide an invalid request (e.g. duplicate key)
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

    // It should fail with DuplicateKeyInBatch, NOT Backend error.
    assert_eq!(
        res,
        Err(BatchError::DuplicateKeyInBatch {
            key: b"k1".to_vec()
        })
    );
    // Validation failure should bypass the backend entirely.
    assert_eq!(engine.backend().put_batch_calls, 0);
}

#[test]
fn test_backend_failure_bubbles_up() {
    let mut backend = FailingBackend::new();
    backend.fail_on_put_batch = true;
    let mut engine = MvccEngine::new(backend);

    let writes = vec![PhysicalWrite {
        key: b"k1".to_vec(),
        value: Some(b"v1".to_vec()),
    }];
    let res = engine.apply_direct_batch(Timestamp(10), writes);

    // Engine does NOT report success
    assert_eq!(
        res,
        Err(BatchError::Backend("simulated backend failure".to_string()))
    );
}

#[test]
fn test_backend_failure_does_not_mutate_engine() {
    // If the backend violates the atomic write contract internally (e.g., partial writes during a crash),
    // the engine cannot repair that. But here, we prove that the engine explicitly propagates the failure
    // without applying partial state locally, trusting the backend trait abstraction.
    let mut backend = FailingBackend::new();
    backend.fail_on_put_batch = true;
    let mut engine = MvccEngine::new(backend);

    let writes = vec![PhysicalWrite {
        key: b"k1".to_vec(),
        value: Some(b"v1".to_vec()),
    }];
    let res = engine.apply_direct_batch(Timestamp(10), writes);
    assert!(res.is_err());

    // Check nothing was committed
    let read = engine.read(b"k1", Timestamp(10)).unwrap();
    assert_eq!(read, None);
}

#[test]
fn test_intent_validation_failure_bypasses_backend() {
    use nexir_mvcc_core::error::BatchPrewriteError;
    let mut backend = FailingBackend::new();
    backend.fail_on_put_intents = true;
    let mut engine = MvccEngine::new(backend);

    // Duplicate keys should fail validation
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
    let res = engine.prewrite_batch(TxnId(1), Timestamp(10), writes);

    assert_eq!(
        res,
        Err(BatchPrewriteError::DuplicateKeyInBatch {
            key: b"k1".to_vec()
        })
    );
    assert_eq!(engine.backend().put_intents_calls, 0);
}

#[test]
fn test_intent_backend_failure_bubbles_up() {
    use nexir_mvcc_core::error::BatchPrewriteError;
    let mut backend = FailingBackend::new();
    backend.fail_on_put_intents = true;
    let mut engine = MvccEngine::new(backend);

    let writes = vec![PhysicalWrite {
        key: b"k1".to_vec(),
        value: Some(b"v1".to_vec()),
    }];
    let res = engine.prewrite_batch(TxnId(1), Timestamp(10), writes);

    assert_eq!(
        res,
        Err(BatchPrewriteError::Backend(
            "simulated backend failure".to_string()
        ))
    );
}

#[test]
fn test_commit_intent_backend_failure_bubbles_up() {
    use nexir_mvcc_core::error::BatchCommitError;
    let backend = FailingBackend::new();
    let mut engine = MvccEngine::new(backend);

    let writes = vec![PhysicalWrite {
        key: b"k1".to_vec(),
        value: Some(b"v1".to_vec()),
    }];
    engine
        .prewrite_batch(TxnId(1), Timestamp(10), writes)
        .unwrap();

    engine.backend_mut().fail_on_commit_intents = true;

    let res = engine.commit_batch(TxnId(1), Timestamp(10), Timestamp(20), vec![b"k1".to_vec()]);
    assert_eq!(
        res,
        Err(BatchCommitError::Backend(
            "simulated backend failure".to_string()
        ))
    );
}

#[test]
fn test_single_key_commit_uses_atomic_backend_transition() {
    let backend = FailingBackend::new();
    let mut engine = MvccEngine::new(backend);

    engine
        .prewrite(
            TxnId(1),
            Timestamp(10),
            b"k1".to_vec(),
            nexir_mvcc_core::Mutation::Put(b"v1".to_vec()),
        )
        .unwrap();

    engine
        .commit(TxnId(1), b"k1", Timestamp(10), Timestamp(20))
        .unwrap();

    assert_eq!(engine.backend().commit_intents_calls, 1);
    assert_eq!(engine.backend().put_committed_calls, 0);
    assert_eq!(engine.backend().remove_intent_calls, 0);
    assert_eq!(
        engine
            .backend()
            .get_visible_committed(b"k1", Timestamp(20))
            .unwrap(),
        Some(CommittedVersion {
            key: b"k1".to_vec(),
            commit_ts: Timestamp(20),
            value: Some(b"v1".to_vec())
        })
    );
    assert!(engine.backend().get_intent(b"k1").unwrap().is_none());
}

#[test]
fn test_single_key_commit_backend_failure_is_atomic() {
    use nexir_mvcc_core::error::CommitError;

    let backend = FailingBackend::new();
    let mut engine = MvccEngine::new(backend);

    engine
        .prewrite(
            TxnId(1),
            Timestamp(10),
            b"k1".to_vec(),
            nexir_mvcc_core::Mutation::Put(b"v1".to_vec()),
        )
        .unwrap();

    engine.backend_mut().fail_on_commit_intents = true;

    let res = engine.commit(TxnId(1), b"k1", Timestamp(10), Timestamp(20));
    assert_eq!(
        res,
        Err(CommitError::Backend(
            "simulated backend failure".to_string()
        ))
    );
    assert_eq!(engine.backend().commit_intents_calls, 1);
    assert_eq!(engine.backend().put_committed_calls, 0);
    assert_eq!(engine.backend().remove_intent_calls, 0);
    assert!(
        engine
            .backend()
            .get_committed_versions(b"k1")
            .unwrap()
            .is_empty()
    );
    assert!(engine.backend().get_intent(b"k1").unwrap().is_some());
}

#[test]
fn test_abort_intent_backend_failure_bubbles_up() {
    use nexir_mvcc_core::error::BatchAbortError;
    let backend = FailingBackend::new();
    let mut engine = MvccEngine::new(backend);

    let writes = vec![PhysicalWrite {
        key: b"k1".to_vec(),
        value: Some(b"v1".to_vec()),
    }];
    engine
        .prewrite_batch(TxnId(1), Timestamp(10), writes)
        .unwrap();

    engine.backend_mut().fail_on_remove_intents = true;

    let res = engine.abort_batch(TxnId(1), Timestamp(10), vec![b"k1".to_vec()]);
    assert_eq!(
        res,
        Err(BatchAbortError::Backend(
            "simulated backend failure".to_string()
        ))
    );
}

#[test]
fn test_abort_batch_empty_succeeds_even_with_failing_backend() {
    let backend = FailingBackend::new();
    let mut engine = MvccEngine::new(backend);

    engine.backend_mut().fail_on_remove_intents = true;

    // An empty abort is completely idempotent and does not touch the backend.
    assert!(engine.abort_batch(TxnId(1), Timestamp(10), vec![]).is_ok());
    assert_eq!(engine.backend().remove_intents_calls, 0);
}
