//! Backend conformance test suite for adapter implementers.
//!
//! This module provides a macro to verify that a custom `Backend` implementation
//! correctly implements the atomic and semantic requirements of `nexir-mvcc-core`.

#![allow(dead_code)]

/// Reusable conformance suite for durable backends that store committed MVCC
/// versions but deliberately do not implement optional intent transactions.
#[macro_export]
macro_rules! test_committed_backend_conformance {
    ($factory:expr) => {
        #[test]
        fn committed_versions_are_ascending_and_cross_u64() {
            use $crate::Backend;
            let mut backend = $factory();
            let boundary = u64::MAX as u128;
            for timestamp in [boundary + 1, boundary - 1, boundary] {
                backend
                    .put_committed($crate::CommittedVersion {
                        key: b"k".to_vec(),
                        commit_ts: $crate::Timestamp(timestamp),
                        value: Some(timestamp.to_be_bytes().to_vec()),
                    })
                    .unwrap();
            }
            let timestamps: Vec<_> = backend
                .get_committed_versions(b"k")
                .unwrap()
                .into_iter()
                .map(|version| version.commit_ts)
                .collect();
            assert_eq!(
                timestamps,
                [
                    $crate::Timestamp(boundary - 1),
                    $crate::Timestamp(boundary),
                    $crate::Timestamp(boundary + 1),
                ]
            );
        }

        #[test]
        fn committed_batch_and_visibility_are_conformant() {
            use $crate::Backend;
            let mut backend = $factory();
            backend
                .put_committed_batch(vec![
                    $crate::CommittedVersion {
                        key: b"a".to_vec(),
                        commit_ts: $crate::Timestamp(10),
                        value: Some(b"old".to_vec()),
                    },
                    $crate::CommittedVersion {
                        key: b"a".to_vec(),
                        commit_ts: $crate::Timestamp(20),
                        value: Some(b"new".to_vec()),
                    },
                    $crate::CommittedVersion {
                        key: b"b".to_vec(),
                        commit_ts: $crate::Timestamp(10),
                        value: None,
                    },
                ])
                .unwrap();
            assert_eq!(
                backend
                    .get_visible_committed(b"a", $crate::Timestamp(15))
                    .unwrap()
                    .unwrap()
                    .value,
                Some(b"old".to_vec())
            );
            assert_eq!(
                backend.get_latest_commit_ts(b"a").unwrap(),
                Some($crate::Timestamp(20))
            );
            assert_eq!(
                backend.all_keys().unwrap(),
                vec![b"a".to_vec(), b"b".to_vec()]
            );
        }

        #[test]
        fn committed_key_scans_and_timestamp_pages_are_conformant() {
            use $crate::Backend;
            let mut backend = $factory();
            for (key, timestamp) in [
                (b"aa".as_slice(), 10),
                (b"aa".as_slice(), 20),
                (b"ab".as_slice(), 10),
                (b"b".as_slice(), 10),
            ] {
                backend
                    .put_committed($crate::CommittedVersion {
                        key: key.to_vec(),
                        commit_ts: $crate::Timestamp(timestamp),
                        value: Some(vec![timestamp as u8]),
                    })
                    .unwrap();
            }
            assert_eq!(
                backend.keys_from(Some(b"ab"), 2).unwrap(),
                vec![b"ab".to_vec(), b"b".to_vec()]
            );
            assert_eq!(
                backend.keys_from_prefix(b"a", None, 10).unwrap(),
                vec![b"aa".to_vec(), b"ab".to_vec()]
            );
            assert_eq!(
                backend
                    .get_committed_timestamps_before(b"aa", $crate::Timestamp(30), 2)
                    .unwrap(),
                vec![$crate::Timestamp(20), $crate::Timestamp(10)]
            );
        }

        #[test]
        fn committed_removal_and_tombstone_collapse_are_conformant() {
            use $crate::Backend;
            let mut backend = $factory();
            backend
                .put_committed_batch(vec![
                    $crate::CommittedVersion {
                        key: b"k".to_vec(),
                        commit_ts: $crate::Timestamp(10),
                        value: Some(b"value".to_vec()),
                    },
                    $crate::CommittedVersion {
                        key: b"k".to_vec(),
                        commit_ts: $crate::Timestamp(20),
                        value: None,
                    },
                ])
                .unwrap();
            backend
                .remove_committed_version(b"k", $crate::Timestamp(10))
                .unwrap();
            assert_eq!(backend.get_committed_versions(b"k").unwrap().len(), 1);
            backend
                .collapse_tombstone(b"k", $crate::Timestamp(20), Vec::new())
                .unwrap();
            assert!(backend.get_committed_versions(b"k").unwrap().is_empty());
        }
    };
}

/// Reusable macro to test a `Backend` implementation for MVCC conformance.
///
/// Adapters should invoke this in their test modules passing a factory closure
/// that returns a clean instance of their `Backend` implementation.
#[macro_export]
macro_rules! test_backend_conformance {
    ($factory:expr) => {
        #[test]
        fn test_ascending_commit_ts() {
            use $crate::Backend;
            let mut backend = $factory();
            // Insert out of order and check.
            backend
                .put_committed($crate::CommittedVersion {
                    key: b"k1".to_vec(),
                    commit_ts: $crate::Timestamp(20),
                    value: Some(b"v20".to_vec()),
                })
                .unwrap();
            backend
                .put_committed($crate::CommittedVersion {
                    key: b"k1".to_vec(),
                    commit_ts: $crate::Timestamp(10),
                    value: Some(b"v10".to_vec()),
                })
                .unwrap();
            backend
                .put_committed($crate::CommittedVersion {
                    key: b"k1".to_vec(),
                    commit_ts: $crate::Timestamp(30),
                    value: Some(b"v30".to_vec()),
                })
                .unwrap();

            let versions = backend.get_committed_versions(b"k1").unwrap();
            assert_eq!(versions.len(), 3);
            assert_eq!(versions[0].commit_ts, $crate::Timestamp(10));
            assert_eq!(versions[1].commit_ts, $crate::Timestamp(20));
            assert_eq!(versions[2].commit_ts, $crate::Timestamp(30));
        }

        #[test]
        fn test_all_or_nothing_batch_persistence() {
            use $crate::Backend;
            let mut backend = $factory();
            let commits = vec![
                $crate::CommittedVersion {
                    key: b"k1".to_vec(),
                    commit_ts: $crate::Timestamp(10),
                    value: Some(b"v1".to_vec()),
                },
                $crate::CommittedVersion {
                    key: b"k2".to_vec(),
                    commit_ts: $crate::Timestamp(10),
                    value: Some(b"v2".to_vec()),
                },
            ];
            backend.put_committed_batch(commits).unwrap();
            let v1 = backend.get_committed_versions(b"k1").unwrap();
            let v2 = backend.get_committed_versions(b"k2").unwrap();
            assert_eq!(v1.len(), 1);
            assert_eq!(v2.len(), 1);
        }

        #[test]
        fn test_intent_batch_persistence() {
            use $crate::{Backend, CommittedVersion, Intent, Mutation, Timestamp, TxnId};
            let mut backend = $factory();

            // 1. put_intents_batch
            let intents = vec![
                Intent {
                    key: b"kb1".to_vec(),
                    txn_id: TxnId(1),
                    start_ts: Timestamp(5),
                    mutation: Mutation::Put(b"v1".to_vec()),
                    min_commit_ts: None,
                },
                Intent {
                    key: b"kb2".to_vec(),
                    txn_id: TxnId(1),
                    start_ts: Timestamp(5),
                    mutation: Mutation::Delete,
                    min_commit_ts: None,
                },
            ];
            backend.put_intents_batch(intents).unwrap();

            assert!(backend.get_intent(b"kb1").unwrap().is_some());
            assert!(backend.get_intent(b"kb2").unwrap().is_some());

            // 2. commit_intents_batch
            let commits = vec![CommittedVersion {
                key: b"kb1".to_vec(),
                commit_ts: Timestamp(10),
                value: Some(b"v1".to_vec()),
            }];
            let removals = vec![(b"kb1".to_vec(), TxnId(1), Timestamp(5))];
            backend.commit_intents_batch(commits, removals).unwrap();

            // kb1 intent removed, version created
            assert!(backend.get_intent(b"kb1").unwrap().is_none());
            assert_eq!(backend.get_committed_versions(b"kb1").unwrap().len(), 1);

            // kb2 intent still there
            assert!(backend.get_intent(b"kb2").unwrap().is_some());

            // 3. remove_intents_batch
            let aborts = vec![(b"kb2".to_vec(), TxnId(1), Timestamp(5))];
            backend.remove_intents_batch(aborts).unwrap();

            assert!(backend.get_intent(b"kb2").unwrap().is_none());
        }

        #[test]
        fn test_strict_intent_identity() {
            use $crate::Backend;
            let mut backend = $factory();
            let intent = $crate::Intent {
                key: b"k1".to_vec(),
                txn_id: $crate::TxnId(1),
                start_ts: $crate::Timestamp(5),
                mutation: $crate::Mutation::Put(b"i1".to_vec()),
                min_commit_ts: None,
            };
            backend.put_intent(intent.clone()).unwrap();

            // Fetch
            let fetched = backend.get_intent(b"k1").unwrap().unwrap();
            assert_eq!(fetched, intent);

            // Remove with wrong txn_id - should fail or do nothing
            let removed_wrong_txn = backend
                .remove_intent(b"k1", $crate::TxnId(2), $crate::Timestamp(5))
                .unwrap();
            assert!(!removed_wrong_txn);

            // Remove with wrong start_ts
            let removed_wrong_ts = backend
                .remove_intent(b"k1", $crate::TxnId(1), $crate::Timestamp(6))
                .unwrap();
            assert!(!removed_wrong_ts);

            // Fetch should still be there
            assert!(backend.get_intent(b"k1").unwrap().is_some());

            // Remove correctly
            let removed_correct = backend
                .remove_intent(b"k1", $crate::TxnId(1), $crate::Timestamp(5))
                .unwrap();
            assert!(removed_correct);
            assert!(backend.get_intent(b"k1").unwrap().is_none());
        }

        #[test]
        fn test_all_keys_collation() {
            use $crate::Backend;
            let mut backend = $factory();
            backend
                .put_committed($crate::CommittedVersion {
                    key: b"a".to_vec(),
                    commit_ts: $crate::Timestamp(10),
                    value: Some(b"v".to_vec()),
                })
                .unwrap();
            backend
                .put_committed($crate::CommittedVersion {
                    key: b"b".to_vec(),
                    commit_ts: $crate::Timestamp(10),
                    value: Some(b"v".to_vec()),
                })
                .unwrap();
            backend
                .put_committed($crate::CommittedVersion {
                    key: b"b".to_vec(),
                    commit_ts: $crate::Timestamp(20),
                    value: Some(b"v".to_vec()),
                })
                .unwrap();

            let intent = $crate::Intent {
                key: b"c".to_vec(),
                txn_id: $crate::TxnId(1),
                start_ts: $crate::Timestamp(5),
                mutation: $crate::Mutation::Put(b"i".to_vec()),
                min_commit_ts: None,
            };
            backend.put_intent(intent).unwrap();

            let keys = backend.all_keys().unwrap();
            // The backend contract: sorted and deduplicated.
            assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        }

        #[test]
        fn test_tombstone_preservation() {
            use $crate::Backend;
            let mut backend = $factory();
            backend
                .put_committed($crate::CommittedVersion {
                    key: b"t1".to_vec(),
                    commit_ts: $crate::Timestamp(10),
                    value: None,
                })
                .unwrap();

            let versions = backend.get_committed_versions(b"t1").unwrap();
            assert_eq!(versions.len(), 1);
            assert_eq!(versions[0].value, None);
        }

        #[test]
        fn test_fast_lookups() {
            use $crate::Backend;
            let mut backend = $factory();

            // Initial state: no versions
            assert_eq!(backend.get_latest_committed(b"f1").unwrap(), None);
            assert_eq!(backend.get_latest_commit_ts(b"f1").unwrap(), None);
            assert_eq!(
                backend
                    .get_visible_committed(b"f1", $crate::Timestamp(100))
                    .unwrap(),
                None
            );

            // Insert versions: 10 (value), 20 (tombstone), 30 (value)
            backend
                .put_committed($crate::CommittedVersion {
                    key: b"f1".to_vec(),
                    commit_ts: $crate::Timestamp(10),
                    value: Some(b"v10".to_vec()),
                })
                .unwrap();
            backend
                .put_committed($crate::CommittedVersion {
                    key: b"f1".to_vec(),
                    commit_ts: $crate::Timestamp(20),
                    value: None,
                })
                .unwrap();
            backend
                .put_committed($crate::CommittedVersion {
                    key: b"f1".to_vec(),
                    commit_ts: $crate::Timestamp(30),
                    value: Some(b"v30".to_vec()),
                })
                .unwrap();

            // test_latest
            let latest = backend.get_latest_committed(b"f1").unwrap().unwrap();
            assert_eq!(latest.commit_ts, $crate::Timestamp(30));
            assert_eq!(latest.value, Some(b"v30".to_vec()));

            assert_eq!(
                backend.get_latest_commit_ts(b"f1").unwrap(),
                Some($crate::Timestamp(30))
            );

            // test_visible_committed: before first
            let vis_5 = backend
                .get_visible_committed(b"f1", $crate::Timestamp(5))
                .unwrap();
            assert_eq!(vis_5, None);

            // test_visible_committed: exactly at first
            let vis_10 = backend
                .get_visible_committed(b"f1", $crate::Timestamp(10))
                .unwrap()
                .unwrap();
            assert_eq!(vis_10.commit_ts, $crate::Timestamp(10));
            assert_eq!(vis_10.value, Some(b"v10".to_vec()));

            // test_visible_committed: between 10 and 20
            let vis_15 = backend
                .get_visible_committed(b"f1", $crate::Timestamp(15))
                .unwrap()
                .unwrap();
            assert_eq!(vis_15.commit_ts, $crate::Timestamp(10));
            assert_eq!(vis_15.value, Some(b"v10".to_vec()));

            // test_visible_committed: exactly at tombstone
            let vis_20 = backend
                .get_visible_committed(b"f1", $crate::Timestamp(20))
                .unwrap()
                .unwrap();
            assert_eq!(vis_20.commit_ts, $crate::Timestamp(20));
            assert_eq!(vis_20.value, None);

            // test_visible_committed: exactly at latest
            let vis_30 = backend
                .get_visible_committed(b"f1", $crate::Timestamp(30))
                .unwrap()
                .unwrap();
            assert_eq!(vis_30.commit_ts, $crate::Timestamp(30));

            // test_visible_committed: after latest
            let vis_100 = backend
                .get_visible_committed(b"f1", $crate::Timestamp(100))
                .unwrap()
                .unwrap();
            assert_eq!(vis_100.commit_ts, $crate::Timestamp(30));
            assert_eq!(vis_100.value, Some(b"v30".to_vec()));
        }

        #[test]
        fn test_binary_handling() {
            use $crate::Backend;
            let mut backend = $factory();
            let binary_key = vec![0, 255, 128, 64];
            let binary_val = vec![1, 254, 127, 63];
            backend
                .put_committed($crate::CommittedVersion {
                    key: binary_key.clone(),
                    commit_ts: $crate::Timestamp(10),
                    value: Some(binary_val.clone()),
                })
                .unwrap();

            let versions = backend.get_committed_versions(&binary_key).unwrap();
            assert_eq!(versions[0].key, binary_key);
            assert_eq!(versions[0].value, Some(binary_val));
        }
        #[test]
        fn test_keys_from() {
            use $crate::Backend;
            let mut backend = $factory();

            backend
                .put_committed($crate::CommittedVersion {
                    key: b"a".to_vec(),
                    commit_ts: $crate::Timestamp(10),
                    value: Some(b"v".to_vec()),
                })
                .unwrap();

            let intent = $crate::Intent {
                key: b"b".to_vec(),
                txn_id: $crate::TxnId(1),
                start_ts: $crate::Timestamp(5),
                mutation: $crate::Mutation::Put(b"i".to_vec()),
                min_commit_ts: None,
            };
            backend.put_intent(intent).unwrap();

            backend
                .put_committed($crate::CommittedVersion {
                    key: b"c".to_vec(),
                    commit_ts: $crate::Timestamp(10),
                    value: Some(b"v".to_vec()),
                })
                .unwrap();

            backend
                .put_committed($crate::CommittedVersion {
                    key: b"c".to_vec(),
                    commit_ts: $crate::Timestamp(20),
                    value: Some(b"v".to_vec()),
                })
                .unwrap();

            // All keys with no start
            let keys = backend.keys_from(None, 10).unwrap();
            assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);

            // All keys with limit
            let keys = backend.keys_from(None, 2).unwrap();
            assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);

            // Resume from "b"
            let keys = backend.keys_from(Some(b"b"), 10).unwrap();
            assert_eq!(keys, vec![b"b".to_vec(), b"c".to_vec()]);

            // Resume from "b" with limit 1
            let keys = backend.keys_from(Some(b"b"), 1).unwrap();
            assert_eq!(keys, vec![b"b".to_vec()]);

            // Resume from key not present
            let keys = backend.keys_from(Some(b"ab"), 10).unwrap();
            assert_eq!(keys, vec![b"b".to_vec(), b"c".to_vec()]);
        }

        #[test]
        fn test_keys_from_prefix() {
            use $crate::Backend;
            let mut backend = $factory();

            backend
                .put_committed($crate::CommittedVersion {
                    key: b"a1".to_vec(),
                    commit_ts: $crate::Timestamp(10),
                    value: Some(b"v1".to_vec()),
                })
                .unwrap();
            backend
                .put_committed($crate::CommittedVersion {
                    key: b"a1".to_vec(),
                    commit_ts: $crate::Timestamp(20),
                    value: Some(b"v2".to_vec()),
                })
                .unwrap();
            backend
                .put_committed($crate::CommittedVersion {
                    key: b"a2".to_vec(),
                    commit_ts: $crate::Timestamp(10),
                    value: Some(b"v1".to_vec()),
                })
                .unwrap();

            // Intent should not be returned by keys_from_prefix
            let intent = $crate::Intent {
                key: b"a3".to_vec(),
                txn_id: $crate::TxnId(1),
                start_ts: $crate::Timestamp(5),
                mutation: $crate::Mutation::Put(b"i".to_vec()),
                min_commit_ts: None,
            };
            backend.put_intent(intent).unwrap();

            backend
                .put_committed($crate::CommittedVersion {
                    key: b"b1".to_vec(),
                    commit_ts: $crate::Timestamp(10),
                    value: Some(b"v1".to_vec()),
                })
                .unwrap();

            // 1. Scan with prefix b"a"
            let keys = backend.keys_from_prefix(b"a", None, 10).unwrap();
            assert_eq!(keys, vec![b"a1".to_vec(), b"a2".to_vec()]); // Excludes b"a3" (intent) and b"b1"

            // 2. Scan with prefix and limit
            let keys = backend.keys_from_prefix(b"a", None, 1).unwrap();
            assert_eq!(keys, vec![b"a1".to_vec()]);

            // 3. Scan with prefix and start cursor
            let keys = backend.keys_from_prefix(b"a", Some(b"a2"), 10).unwrap();
            assert_eq!(keys, vec![b"a2".to_vec()]);

            // 4. Scan with empty prefix should error
            assert!(backend.keys_from_prefix(b"", None, 10).is_err());

            // 5. Scan with start cursor not starting with prefix should error
            assert!(backend.keys_from_prefix(b"a", Some(b"b1"), 10).is_err());
        }

        #[test]
        fn test_get_committed_timestamps_before() {
            use $crate::Backend;
            let mut backend = $factory();

            // Insert 10, 20, 30
            for ts in [10, 20, 30] {
                backend
                    .put_committed($crate::CommittedVersion {
                        key: b"k1".to_vec(),
                        commit_ts: $crate::Timestamp(ts),
                        value: Some(vec![1]),
                    })
                    .unwrap();
            }

            // Before 30, limit 10
            let ts = backend
                .get_committed_timestamps_before(b"k1", $crate::Timestamp(30), 10)
                .unwrap();
            assert_eq!(ts, vec![$crate::Timestamp(20), $crate::Timestamp(10)]);

            // Before 30, limit 1
            let ts = backend
                .get_committed_timestamps_before(b"k1", $crate::Timestamp(30), 1)
                .unwrap();
            assert_eq!(ts, vec![$crate::Timestamp(20)]);

            // Before 10
            let ts = backend
                .get_committed_timestamps_before(b"k1", $crate::Timestamp(10), 10)
                .unwrap();
            assert!(ts.is_empty());
        }

        #[test]
        fn test_collapse_tombstone() {
            use $crate::Backend;
            let mut backend = $factory();

            for ts in [10, 20] {
                backend
                    .put_committed($crate::CommittedVersion {
                        key: b"k1".to_vec(),
                        commit_ts: $crate::Timestamp(ts),
                        value: Some(vec![1]),
                    })
                    .unwrap();
            }
            backend
                .put_committed($crate::CommittedVersion {
                    key: b"k1".to_vec(),
                    commit_ts: $crate::Timestamp(30),
                    value: None,
                })
                .unwrap();
            backend
                .put_committed($crate::CommittedVersion {
                    key: b"k2".to_vec(),
                    commit_ts: $crate::Timestamp(10),
                    value: Some(vec![2]),
                })
                .unwrap();

            backend
                .collapse_tombstone(
                    b"k1",
                    $crate::Timestamp(30),
                    vec![$crate::Timestamp(20), $crate::Timestamp(10)],
                )
                .unwrap();

            assert!(backend.get_committed_versions(b"k1").unwrap().is_empty());
            assert_eq!(backend.get_latest_committed(b"k1").unwrap(), None);
            assert_eq!(backend.all_keys().unwrap(), vec![b"k2".to_vec()]);
            assert_eq!(backend.get_committed_versions(b"k2").unwrap().len(), 1);
        }
    };
}
