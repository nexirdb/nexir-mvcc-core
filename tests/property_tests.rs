use nexir_mvcc_core::{
    Backend, InMemoryBackend, Mutation, MvccEngine, PhysicalWrite, ReadGuard, Timestamp, TxnId,
};
use proptest::prelude::*;

mod support;
use support::model::*;

fn ts_strategy() -> impl Strategy<Value = Timestamp> {
    (1u64..100u64).prop_map(Timestamp)
}

fn txn_id_strategy() -> impl Strategy<Value = TxnId> {
    (1u64..20u64).prop_map(TxnId)
}

fn key_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(0u8..255u8, 1..8)
}

fn value_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(0u8..255u8, 0..16)
}

fn action_strategy() -> impl Strategy<Value = Action> {
    let ts = (1u64..20u64).prop_map(Timestamp);
    let txn = (1u64..5u64).prop_map(TxnId);
    let key = prop::collection::vec(0u8..3u8, 1..2);
    let val = prop::collection::vec(0u8..3u8, 1..2);

    prop_oneof![
        (ts.clone(), key.clone(), val.clone()).prop_map(|(t, k, v)| {
            Action::DirectBatch(
                t,
                vec![PhysicalWrite {
                    key: k,
                    value: Some(v),
                }],
            )
        }),
        (ts.clone(), key.clone(), val.clone(), ts.clone(), ts.clone()).prop_map(
            |(t, k, v, rt, e_ts)| {
                Action::GuardedBatch(
                    t,
                    vec![ReadGuard::ExpectedVersion {
                        key: k.clone(),
                        read_ts: rt,
                        expected_commit_ts: Some(e_ts),
                    }],
                    vec![PhysicalWrite {
                        key: k,
                        value: Some(v),
                    }],
                )
            }
        ),
        (txn.clone(), ts.clone(), key.clone(), val.clone())
            .prop_map(|(tx, t, k, v)| Action::Prewrite(tx, t, k, Mutation::Put(v))),
        (txn.clone(), key.clone(), ts.clone(), ts.clone())
            .prop_map(|(tx, k, st, ct)| Action::Commit(tx, k, st, ct)),
        (txn.clone(), key.clone(), ts.clone()).prop_map(|(tx, k, st)| Action::Abort(tx, k, st)),
        ts.clone().prop_map(Action::Gc),
    ]
}

fn schedule_strategy() -> impl Strategy<Value = Vec<Action>> {
    prop::collection::vec(action_strategy(), 0..15)
}

proptest! {
    #[test]
    fn prop_read_after_commit(
        key in key_strategy(),
        value in value_strategy(),
        start_ts in ts_strategy(),
        read_ts in ts_strategy(),
    ) {
        let backend = InMemoryBackend::new();
        let mut engine = MvccEngine::new(backend);

        let actual_commit_ts = Timestamp(start_ts.0 + 1);
        engine.prewrite(TxnId(1), start_ts, key.clone(), Mutation::Put(value.clone())).unwrap();
        engine.commit(TxnId(1), &key, start_ts, actual_commit_ts).unwrap();

        let result = engine.read(&key, read_ts).unwrap();
        if read_ts >= actual_commit_ts {
            prop_assert_eq!(result, Some(value));
        } else {
            prop_assert_eq!(result, None);
        }
    }

    #[test]
    fn prop_commit_requires_matching_intent(
        key in key_strategy(),
        value in value_strategy(),
        start_ts in ts_strategy(),
        bad_txn_id in txn_id_strategy(),
    ) {
        let backend = InMemoryBackend::new();
        let mut engine = MvccEngine::new(backend);

        let real_txn = TxnId(1);
        engine.prewrite(real_txn, start_ts, key.clone(), Mutation::Put(value)).unwrap();

        let result = engine.commit(bad_txn_id, &key, start_ts, Timestamp(start_ts.0 + 1));
        if bad_txn_id != real_txn {
            prop_assert!(result.is_err());
        }
    }

    #[test]
    fn prop_prewrite_idempotent(
        key in key_strategy(),
        value in value_strategy(),
        start_ts in ts_strategy(),
    ) {
        let backend = InMemoryBackend::new();
        let mut engine = MvccEngine::new(backend);

        let txn = TxnId(1);
        engine.prewrite(txn, start_ts, key.clone(), Mutation::Put(value.clone())).unwrap();
        engine.prewrite(txn, start_ts, key.clone(), Mutation::Put(value.clone())).unwrap(); // idempotent

        let intent = engine.backend().get_intent(&key).unwrap().unwrap();
        prop_assert_eq!(intent.mutation, Mutation::Put(value));
    }

    #[test]
    fn prop_abort_is_idempotent(
        key in key_strategy(),
        value in value_strategy(),
        start_ts in ts_strategy(),
    ) {
        let backend = InMemoryBackend::new();
        let mut engine = MvccEngine::new(backend);

        let txn = TxnId(1);
        engine.prewrite(txn, start_ts, key.clone(), Mutation::Put(value)).unwrap();
        engine.abort(txn, &key, start_ts).unwrap();
        engine.abort(txn, &key, start_ts).unwrap(); // idempotent no-op

        prop_assert!(engine.backend().get_intent(&key).unwrap().is_none());
    }

    #[test]
    fn prop_gc_does_not_remove_latest_below_safe_point(
        key in key_strategy(),
        v1 in value_strategy(),
        v2 in value_strategy(),
        ts1 in 1u64..50u64,
        ts2 in 51u64..100u64,
    ) {
        let backend = InMemoryBackend::new();
        let mut engine = MvccEngine::new(backend);

        let c1 = Timestamp(ts1 + 1);
        engine.prewrite(TxnId(1), Timestamp(ts1), key.clone(), Mutation::Put(v1)).unwrap();
        engine.commit(TxnId(1), &key, Timestamp(ts1), c1).unwrap();

        let c2 = Timestamp(ts2 + 1);
        engine.prewrite(TxnId(2), Timestamp(ts2), key.clone(), Mutation::Put(v2.clone())).unwrap();
        engine.commit(TxnId(2), &key, Timestamp(ts2), c2).unwrap();

        let safe_point = Timestamp(ts2 + 2);
        let stats = engine.gc(safe_point, nexir_mvcc_core::GcOptions { budget: nexir_mvcc_core::GcBudget { max_keys: 10000, max_versions: 10000 }, collapse_final_tombstones: false }).unwrap();
        // The latest version at or below safe_point is v2 at c2.
        // v1 at c1 is strictly below c2 and not the newest, so it can be removed.
        prop_assert_eq!(stats.versions_removed, 1);

        let read = engine.read(&key, safe_point).unwrap();
        prop_assert_eq!(read, Some(v2));
    }

    #[test]
    fn prop_lost_update_not_possible(
        key in key_strategy(),
        initial in value_strategy(),
        va in value_strategy(),
        vb in value_strategy(),
    ) {
        let backend = InMemoryBackend::new();
        let mut engine = MvccEngine::new(backend);

        // initial value
        engine.prewrite(TxnId(0), Timestamp(5), key.clone(), Mutation::Put(initial)).unwrap();
        engine.commit(TxnId(0), &key, Timestamp(5), Timestamp(10)).unwrap();

        // txn A and B both start at ts=10
        let start_ts = Timestamp(100);
        engine.read(&key, start_ts).unwrap(); // A reads
        engine.read(&key, start_ts).unwrap(); // B reads

        // A prewrites
        engine.prewrite(TxnId(10), start_ts, key.clone(), Mutation::Put(va.clone())).unwrap();

        // B prewrites -> must fail
        let b_result = engine.prewrite(TxnId(11), start_ts, key.clone(), Mutation::Put(vb.clone()));
        prop_assert!(b_result.is_err(), "B must not silently prewrite over A's intent");

        // A commits
        engine.commit(TxnId(10), &key, start_ts, Timestamp(200)).unwrap();

        // Now B tries to prewrite again -> WriteConflict because committed version at 20 > start_ts 10
        let b_result2 = engine.prewrite(TxnId(11), start_ts, key.clone(), Mutation::Put(vb));
        prop_assert!(b_result2.is_err(), "B must see write conflict after A commits");
    }

    #[test]
    fn prop_schedule_execution(schedule in schedule_strategy()) {
        let mut engine = MvccEngine::new(InMemoryBackend::new());
        apply_and_verify_schedule(&mut engine, &schedule);

        let (keys, versions, _) = snapshot(&engine);
        // Assert strictly increasing commit_ts for each key
        for k in &keys {
            let k_versions: Vec<_> = versions.iter().filter(|v| v.key == *k).collect();
            for i in 1..k_versions.len() {
                prop_assert!(k_versions[i - 1].commit_ts < k_versions[i].commit_ts);
            }
        }
    }
}
