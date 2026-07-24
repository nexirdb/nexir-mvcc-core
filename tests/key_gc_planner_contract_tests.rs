//! Executable contract fixtures for bounded, read-only per-key GC planning.
//!
//! These fixtures deliberately describe plans independently of the production
//! planner API. The implementation commit binds the planner to this same
//! contract matrix without weakening these safety assertions.

use nexir_mvcc_core::{
    Backend, CommittedVersion, InMemoryBackend, KeyGcOptions, KeyGcPlan, Mutation, MvccEngine,
    Timestamp, TxnId, error::GcError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VersionKind {
    Value,
    Tombstone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Version {
    timestamp: Timestamp,
    kind: VersionKind,
}

#[derive(Debug)]
struct ExpectedPlan<'a> {
    versions: &'a [Version],
    safe_point: Timestamp,
    versions_to_remove: &'a [Timestamp],
    collapse_tombstone: Option<Timestamp>,
    max_versions_examined: usize,
    versions_examined: usize,
    complete: bool,
    collapse_final_tombstones: bool,
}

fn assert_plan_is_safe(plan: &ExpectedPlan<'_>) {
    assert!(
        plan.versions_examined <= plan.max_versions_examined,
        "planning must remain within its version budget"
    );
    assert!(
        plan.versions_to_remove.len() <= plan.versions_examined,
        "every removal must correspond to an examined historical version"
    );

    let keeper = plan
        .versions
        .iter()
        .filter(|version| version.timestamp <= plan.safe_point)
        .max_by_key(|version| version.timestamp);

    for timestamp in plan.versions_to_remove {
        let keeper = keeper.expect("a removal requires a safe-point-visible keeper");
        assert!(
            *timestamp < keeper.timestamp,
            "ordinary removal must stay strictly below the keeper"
        );
        assert!(
            plan.versions
                .iter()
                .any(|version| version.timestamp == *timestamp),
            "a plan may remove only an explicit existing version"
        );
    }

    if let Some(tombstone_timestamp) = plan.collapse_tombstone {
        let keeper = keeper.expect("tombstone collapse requires a keeper");
        assert_eq!(tombstone_timestamp, keeper.timestamp);
        assert_eq!(keeper.kind, VersionKind::Tombstone);
        assert_eq!(
            Some(tombstone_timestamp),
            plan.versions.iter().map(|version| version.timestamp).max(),
            "only a final tombstone may collapse"
        );
        assert!(
            plan.complete,
            "the final tombstone may collapse only in a complete plan"
        );
    } else if let Some(keeper) = keeper {
        assert!(
            !plan.versions_to_remove.contains(&keeper.timestamp),
            "the safe-point-visible version must remain"
        );
    }

    assert!(
        plan.versions_to_remove
            .iter()
            .all(|timestamp| *timestamp <= plan.safe_point),
        "versions newer than the safe point must remain"
    );

    let key = b"contract-key";
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    for version in plan.versions {
        engine
            .backend_mut()
            .put_committed(CommittedVersion {
                key: key.to_vec(),
                commit_ts: version.timestamp,
                value: match version.kind {
                    VersionKind::Value => Some(version.timestamp.0.to_be_bytes().to_vec()),
                    VersionKind::Tombstone => None,
                },
            })
            .unwrap();
    }
    let before = engine.backend().get_committed_versions(key).unwrap();

    let actual = engine
        .plan_key_gc(
            key,
            plan.safe_point,
            KeyGcOptions {
                max_versions_examined: plan.max_versions_examined,
                collapse_final_tombstones: plan.collapse_final_tombstones,
            },
        )
        .unwrap();

    assert_eq!(actual.key, key);
    assert_eq!(actual.versions_to_remove, plan.versions_to_remove);
    assert_eq!(actual.collapse_tombstone, plan.collapse_tombstone);
    assert_eq!(actual.versions_examined, plan.versions_examined);
    assert_eq!(actual.complete, plan.complete);
    assert_eq!(
        engine.backend().get_committed_versions(key).unwrap(),
        before,
        "planning must not mutate the backend"
    );
}

#[test]
fn safe_point_keeper_and_newer_versions_are_never_ordinary_removals() {
    let versions = [
        Version {
            timestamp: Timestamp(10),
            kind: VersionKind::Value,
        },
        Version {
            timestamp: Timestamp(20),
            kind: VersionKind::Value,
        },
        Version {
            timestamp: Timestamp(30),
            kind: VersionKind::Value,
        },
    ];
    let removals = [Timestamp(10)];

    assert_plan_is_safe(&ExpectedPlan {
        versions: &versions,
        safe_point: Timestamp(25),
        versions_to_remove: &removals,
        collapse_tombstone: None,
        max_versions_examined: 8,
        versions_examined: 1,
        complete: true,
        collapse_final_tombstones: false,
    });
}

#[test]
fn final_tombstone_collapse_is_explicit_and_requires_a_complete_plan() {
    let versions = [
        Version {
            timestamp: Timestamp(10),
            kind: VersionKind::Value,
        },
        Version {
            timestamp: Timestamp(20),
            kind: VersionKind::Tombstone,
        },
    ];
    let removals = [Timestamp(10)];

    assert_plan_is_safe(&ExpectedPlan {
        versions: &versions,
        safe_point: Timestamp(25),
        versions_to_remove: &removals,
        collapse_tombstone: Some(Timestamp(20)),
        max_versions_examined: 8,
        versions_examined: 1,
        complete: true,
        collapse_final_tombstones: true,
    });
}

#[test]
fn exhausted_budget_is_incomplete_and_names_only_examined_versions() {
    let versions = [
        Version {
            timestamp: Timestamp(1),
            kind: VersionKind::Value,
        },
        Version {
            timestamp: Timestamp(2),
            kind: VersionKind::Value,
        },
        Version {
            timestamp: Timestamp(3),
            kind: VersionKind::Value,
        },
        Version {
            timestamp: Timestamp(4),
            kind: VersionKind::Value,
        },
    ];
    let removals = [Timestamp(3), Timestamp(2)];

    assert_plan_is_safe(&ExpectedPlan {
        versions: &versions,
        safe_point: Timestamp(5),
        versions_to_remove: &removals,
        collapse_tombstone: None,
        max_versions_examined: 2,
        versions_examined: 2,
        complete: false,
        collapse_final_tombstones: false,
    });
}

#[test]
fn timestamp_contract_spans_the_u64_boundary() {
    let low = Timestamp(u64::MAX as u128);
    let keeper = Timestamp(u64::MAX as u128 + 1);
    let newer = Timestamp(u64::MAX as u128 + 2);
    let versions = [
        Version {
            timestamp: low,
            kind: VersionKind::Value,
        },
        Version {
            timestamp: keeper,
            kind: VersionKind::Value,
        },
        Version {
            timestamp: newer,
            kind: VersionKind::Value,
        },
    ];
    let removals = [low];

    assert_plan_is_safe(&ExpectedPlan {
        versions: &versions,
        safe_point: keeper,
        versions_to_remove: &removals,
        collapse_tombstone: None,
        max_versions_examined: 4,
        versions_examined: 1,
        complete: true,
        collapse_final_tombstones: false,
    });
}

fn put_version(
    engine: &mut MvccEngine<InMemoryBackend>,
    key: &[u8],
    timestamp: u128,
    value: Option<&[u8]>,
) {
    engine
        .backend_mut()
        .put_committed(CommittedVersion {
            key: key.to_vec(),
            commit_ts: Timestamp(timestamp),
            value: value.map(<[u8]>::to_vec),
        })
        .unwrap();
}

fn options(max_versions_examined: usize, collapse_final_tombstones: bool) -> KeyGcOptions {
    KeyGcOptions {
        max_versions_examined,
        collapse_final_tombstones,
    }
}

fn apply_plan(engine: &mut MvccEngine<InMemoryBackend>, plan: &KeyGcPlan) -> Result<(), String> {
    if let Some(tombstone_timestamp) = plan.collapse_tombstone {
        engine.backend_mut().collapse_tombstone(
            &plan.key,
            tombstone_timestamp,
            plan.versions_to_remove.clone(),
        )
    } else {
        for timestamp in &plan.versions_to_remove {
            engine
                .backend_mut()
                .remove_committed_version(&plan.key, *timestamp)?;
        }
        Ok(())
    }
}

#[test]
fn zero_planning_budget_is_rejected() {
    let engine = MvccEngine::new(InMemoryBackend::new());

    assert_eq!(
        engine
            .plan_key_gc(b"k", Timestamp(10), options(0, false))
            .unwrap_err(),
        GcError::InvalidKeyGcBudget
    );
}

#[test]
fn empty_and_single_version_keys_have_no_unsafe_removals() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());

    let empty = engine
        .plan_key_gc(b"empty", Timestamp(10), options(2, false))
        .unwrap();
    assert!(empty.complete);
    assert!(empty.versions_to_remove.is_empty());
    assert_eq!(empty.collapse_tombstone, None);

    put_version(&mut engine, b"single", 5, Some(b"value"));
    let single = engine
        .plan_key_gc(b"single", Timestamp(10), options(2, false))
        .unwrap();
    assert!(single.complete);
    assert!(single.versions_to_remove.is_empty());
    assert_eq!(single.collapse_tombstone, None);
}

#[test]
fn opaque_payload_bytes_are_not_interpreted_by_planning() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    let key = b"opaque";
    put_version(&mut engine, key, 1, Some(&[0, 0xff, 3, 0x80]));
    put_version(&mut engine, key, 2, Some(&[]));

    let plan = engine
        .plan_key_gc(key, Timestamp(3), options(4, false))
        .unwrap();

    assert_eq!(plan.versions_to_remove, [Timestamp(1)]);
    assert!(plan.complete);
}

#[test]
fn bounded_replanning_eventually_drains_all_eligible_history() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    let key = b"bounded";
    for timestamp in 1..=5 {
        put_version(&mut engine, key, timestamp, Some(&[timestamp as u8]));
    }

    let first = engine
        .plan_key_gc(key, Timestamp(10), options(2, false))
        .unwrap();
    assert_eq!(first.versions_to_remove, [Timestamp(4), Timestamp(3)]);
    assert!(!first.complete);
    apply_plan(&mut engine, &first).unwrap();

    let second = engine
        .plan_key_gc(key, Timestamp(10), options(2, false))
        .unwrap();
    assert_eq!(second.versions_to_remove, [Timestamp(2), Timestamp(1)]);
    assert!(!second.complete);
    apply_plan(&mut engine, &second).unwrap();

    let final_plan = engine
        .plan_key_gc(key, Timestamp(10), options(2, false))
        .unwrap();
    assert!(final_plan.complete);
    assert!(final_plan.versions_to_remove.is_empty());
    assert_eq!(
        engine.backend().get_committed_versions(key).unwrap(),
        [CommittedVersion {
            key: key.to_vec(),
            commit_ts: Timestamp(5),
            value: Some(vec![5]),
        }]
    );
}

#[test]
fn final_tombstone_waits_until_its_full_history_fits() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    let key = b"tombstone-budget";
    put_version(&mut engine, key, 1, Some(b"one"));
    put_version(&mut engine, key, 2, Some(b"two"));
    put_version(&mut engine, key, 3, None);

    let first = engine
        .plan_key_gc(key, Timestamp(4), options(2, true))
        .unwrap();
    assert_eq!(first.versions_to_remove, [Timestamp(2), Timestamp(1)]);
    assert_eq!(first.collapse_tombstone, None);
    assert!(!first.complete);
    apply_plan(&mut engine, &first).unwrap();

    let second = engine
        .plan_key_gc(key, Timestamp(4), options(2, true))
        .unwrap();
    assert!(second.versions_to_remove.is_empty());
    assert_eq!(second.collapse_tombstone, Some(Timestamp(3)));
    assert!(second.complete);
    apply_plan(&mut engine, &second).unwrap();
    assert!(
        engine
            .backend()
            .get_committed_versions(key)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn tombstone_collapse_obeys_option_intent_and_finality_guards() {
    let mut disabled = MvccEngine::new(InMemoryBackend::new());
    put_version(&mut disabled, b"disabled", 1, Some(b"old"));
    put_version(&mut disabled, b"disabled", 2, None);
    let disabled_plan = disabled
        .plan_key_gc(b"disabled", Timestamp(3), options(4, false))
        .unwrap();
    assert_eq!(disabled_plan.collapse_tombstone, None);

    let mut intent = MvccEngine::new(InMemoryBackend::new());
    put_version(&mut intent, b"intent", 1, Some(b"old"));
    put_version(&mut intent, b"intent", 2, None);
    intent
        .prewrite(
            TxnId(1),
            Timestamp(3),
            b"intent".to_vec(),
            Mutation::Put(b"future".to_vec()),
        )
        .unwrap();
    let intent_plan = intent
        .plan_key_gc(b"intent", Timestamp(4), options(4, true))
        .unwrap();
    assert_eq!(intent_plan.collapse_tombstone, None);

    let mut shadowed = MvccEngine::new(InMemoryBackend::new());
    put_version(&mut shadowed, b"shadowed", 1, Some(b"old"));
    put_version(&mut shadowed, b"shadowed", 2, None);
    put_version(&mut shadowed, b"shadowed", 3, Some(b"new"));
    let shadowed_plan = shadowed
        .plan_key_gc(b"shadowed", Timestamp(2), options(4, true))
        .unwrap();
    assert_eq!(shadowed_plan.collapse_tombstone, None);
    assert_eq!(shadowed_plan.versions_to_remove, [Timestamp(1)]);
}

#[test]
fn deletion_recreation_and_replanning_remain_coherent() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    let key = b"recreate";
    put_version(&mut engine, key, 10, Some(b"value"));
    put_version(&mut engine, key, 20, None);

    let deletion = engine
        .plan_key_gc(key, Timestamp(30), options(4, true))
        .unwrap();
    assert_eq!(deletion.versions_to_remove, [Timestamp(10)]);
    assert_eq!(deletion.collapse_tombstone, Some(Timestamp(20)));
    apply_plan(&mut engine, &deletion).unwrap();

    put_version(&mut engine, key, 40, Some(b"recreated"));
    let recreation = engine
        .plan_key_gc(key, Timestamp(50), options(4, true))
        .unwrap();
    assert!(recreation.complete);
    assert!(recreation.versions_to_remove.is_empty());
    assert_eq!(recreation.collapse_tombstone, None);
    assert_eq!(
        engine.read(key, Timestamp(50)).unwrap().as_deref(),
        Some(b"recreated".as_slice())
    );
}

#[test]
fn identical_inputs_produce_identical_plans() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    let key = b"deterministic";
    put_version(&mut engine, key, 1, Some(b"a"));
    put_version(&mut engine, key, 2, Some(b"b"));
    put_version(&mut engine, key, 3, Some(b"c"));

    let first = engine
        .plan_key_gc(key, Timestamp(4), options(8, false))
        .unwrap();
    let second = engine
        .plan_key_gc(key, Timestamp(4), options(8, false))
        .unwrap();

    assert_eq!(first, second);
}
