//! Executable contract fixtures for bounded, read-only per-key GC planning.
//!
//! These fixtures deliberately describe plans independently of the production
//! planner API. The implementation commit binds the planner to this same
//! contract matrix without weakening these safety assertions.

use nexir_mvcc_core::Timestamp;

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
    });
}
