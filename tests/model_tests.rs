mod support;

use nexir_mvcc_core::{
    InMemoryBackend, Mutation, MvccEngine, PhysicalWrite, ReadGuard, Timestamp, TxnId,
};
use support::model::*;

#[test]
fn test_deterministic_replay_simple_schedule() {
    let mut e1 = MvccEngine::new(InMemoryBackend::new());
    let mut e2 = MvccEngine::new(InMemoryBackend::new());

    let schedule = vec![
        Action::DirectBatch(
            Timestamp(10),
            vec![PhysicalWrite {
                key: b"k1".to_vec(),
                value: Some(b"v1".to_vec()),
            }],
        ),
        Action::Prewrite(
            TxnId(1),
            Timestamp(5),
            b"k2".to_vec(),
            Mutation::Put(b"v2".to_vec()),
        ),
        Action::Commit(TxnId(1), b"k2".to_vec(), Timestamp(5), Timestamp(15)),
        Action::GuardedBatch(
            Timestamp(20),
            vec![ReadGuard::ExpectedVersion {
                key: b"k1".to_vec(),
                read_ts: Timestamp(15),
                expected_commit_ts: Some(Timestamp(10)),
            }],
            vec![PhysicalWrite {
                key: b"k1".to_vec(),
                value: Some(b"v1_new".to_vec()),
            }],
        ),
        Action::Gc(Timestamp(12)),
    ];

    apply_schedule(&mut e1, &schedule);
    apply_schedule(&mut e2, &schedule);

    assert_eq!(snapshot(&e1), snapshot(&e2));
}

#[test]
fn test_deterministic_replay_with_failures() {
    let mut e1 = MvccEngine::new(InMemoryBackend::new());
    let mut e2 = MvccEngine::new(InMemoryBackend::new());

    let schedule = vec![
        Action::Commit(TxnId(1), b"k1".to_vec(), Timestamp(5), Timestamp(10)),
        Action::GuardedBatch(
            Timestamp(10),
            vec![ReadGuard::ExpectedVersion {
                key: b"k1".to_vec(),
                read_ts: Timestamp(5),
                expected_commit_ts: Some(Timestamp(1)),
            }],
            vec![PhysicalWrite {
                key: b"k1".to_vec(),
                value: Some(b"fail".to_vec()),
            }],
        ),
        Action::DirectBatch(
            Timestamp(20),
            vec![PhysicalWrite {
                key: b"k1".to_vec(),
                value: Some(b"success".to_vec()),
            }],
        ),
        Action::PrewriteBatch(
            TxnId(2),
            Timestamp(25),
            vec![
                PhysicalWrite {
                    key: b"k3".to_vec(),
                    value: Some(b"v3".to_vec()),
                },
                PhysicalWrite {
                    key: b"k4".to_vec(),
                    value: Some(b"v4".to_vec()),
                },
            ],
        ),
        Action::CommitBatch(
            TxnId(2),
            Timestamp(25),
            Timestamp(30),
            vec![b"k3".to_vec(), b"k4".to_vec()],
        ),
        Action::PrewriteBatch(
            TxnId(3),
            Timestamp(35),
            vec![
                PhysicalWrite {
                    key: b"k5".to_vec(),
                    value: Some(b"v5".to_vec()),
                },
                PhysicalWrite {
                    key: b"k6".to_vec(),
                    value: Some(b"v6".to_vec()),
                },
            ],
        ),
        Action::AbortBatch(
            TxnId(3),
            Timestamp(35),
            vec![b"k5".to_vec(), b"k6".to_vec()],
        ),
    ];

    apply_schedule(&mut e1, &schedule);
    apply_schedule(&mut e2, &schedule);

    assert_eq!(snapshot(&e1), snapshot(&e2));
}
