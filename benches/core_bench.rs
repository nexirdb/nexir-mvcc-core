use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use nexir_mvcc_core::{
    codec, CommittedVersion, InMemoryBackend, Intent, Mutation, MvccEngine, PhysicalWrite,
    ReadGuard, Timestamp, TxnId,
};

fn deterministic_key(i: u64) -> Vec<u8> {
    format!("key_{:08}", i).into_bytes()
}

fn deterministic_value(size: usize) -> Vec<u8> {
    vec![42u8; size]
}

fn bench_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("Reads");

    // Latest read on cold keys
    let mut engine = MvccEngine::new(InMemoryBackend::new());
    let mut precomputed_keys = Vec::with_capacity(1000);
    for i in 0..1000 {
        let key = deterministic_key(i);
        precomputed_keys.push(key.clone());
        let _ = engine.apply_direct_batch(
            Timestamp(10),
            vec![PhysicalWrite {
                key,
                value: Some(vec![1]),
            }],
        );
    }

    group.bench_function("latest_read_cold_keys", |b| {
        let mut i = 0;
        b.iter(|| {
            let key = &precomputed_keys[i % 1000];
            black_box(engine.read(key, Timestamp(20)).unwrap());
            i += 1;
        })
    });

    group.finish();
}

fn bench_hot_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("Hot Key");

    for size in [1, 100, 1_000, 100_000].iter() {
        group.bench_with_input(BenchmarkId::new("latest_read", size), size, |b, &size| {
            b.iter_batched(
                || {
                    let mut engine = MvccEngine::new(InMemoryBackend::new());
                    let key = b"hot".to_vec();
                    for i in 1..=size {
                        let _ = engine.apply_direct_batch(
                            Timestamp(i),
                            vec![PhysicalWrite {
                                key: key.clone(),
                                value: Some(vec![1]),
                            }],
                        );
                    }
                    (engine, key)
                },
                |(engine, key)| {
                    black_box(engine.read(&key, Timestamp(size + 1)).unwrap());
                },
                BatchSize::SmallInput,
            )
        });

        group.bench_with_input(
            BenchmarkId::new("historical_read", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let mut engine = MvccEngine::new(InMemoryBackend::new());
                        let key = b"hot".to_vec();
                        for i in 1..=size {
                            let _ = engine.apply_direct_batch(
                                Timestamp(i),
                                vec![PhysicalWrite {
                                    key: key.clone(),
                                    value: Some(vec![1]),
                                }],
                            );
                        }
                        (engine, key)
                    },
                    |(engine, key)| {
                        black_box(engine.read(&key, Timestamp(size / 2 + 1)).unwrap());
                    },
                    BatchSize::SmallInput,
                )
            },
        );

        group.bench_with_input(
            BenchmarkId::new("guarded_incr_success", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let mut engine = MvccEngine::new(InMemoryBackend::new());
                        let key = b"hot".to_vec();
                        for i in 1..=size {
                            let _ = engine.apply_direct_batch(
                                Timestamp(i),
                                vec![PhysicalWrite {
                                    key: key.clone(),
                                    value: Some(vec![1]),
                                }],
                            );
                        }
                        (engine, key)
                    },
                    |(mut engine, key)| {
                        let guards = vec![ReadGuard::ExpectedVersion {
                            key: key.clone(),
                            read_ts: Timestamp(size),
                            expected_commit_ts: Some(Timestamp(size)),
                        }];
                        let writes = vec![PhysicalWrite {
                            key,
                            value: Some(vec![2]),
                        }];
                        engine
                            .apply_guarded_batch(Timestamp(size + 1), guards, writes)
                            .unwrap()
                    },
                    BatchSize::SmallInput,
                )
            },
        );

        group.bench_with_input(
            BenchmarkId::new("guarded_incr_stale_fail", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let mut engine = MvccEngine::new(InMemoryBackend::new());
                        let key = b"hot".to_vec();
                        for i in 1..=size {
                            let _ = engine.apply_direct_batch(
                                Timestamp(i),
                                vec![PhysicalWrite {
                                    key: key.clone(),
                                    value: Some(vec![1]),
                                }],
                            );
                        }
                        (engine, key)
                    },
                    |(mut engine, key)| {
                        let guards = vec![ReadGuard::ExpectedVersion {
                            key: key.clone(),
                            read_ts: Timestamp(size - 1),
                            expected_commit_ts: Some(Timestamp(size - 1)),
                        }];
                        let writes = vec![PhysicalWrite {
                            key,
                            value: Some(vec![2]),
                        }];
                        let res = engine.apply_guarded_batch(Timestamp(size + 1), guards, writes);
                        assert!(res.is_err());
                    },
                    BatchSize::SmallInput,
                )
            },
        );

        group.bench_with_input(BenchmarkId::new("direct_set", size), size, |b, &size| {
            b.iter_batched(
                || {
                    let mut engine = MvccEngine::new(InMemoryBackend::new());
                    let key = b"hot".to_vec();
                    for i in 1..=size {
                        let _ = engine.apply_direct_batch(
                            Timestamp(i),
                            vec![PhysicalWrite {
                                key: key.clone(),
                                value: Some(vec![1]),
                            }],
                        );
                    }
                    (engine, key)
                },
                |(mut engine, key)| {
                    engine
                        .apply_direct_batch(
                            Timestamp(size + 1),
                            vec![PhysicalWrite {
                                key,
                                value: Some(vec![2]),
                            }],
                        )
                        .unwrap();
                },
                BatchSize::SmallInput,
            )
        });
    }
}

fn bench_intent_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("Intent Path");

    let val = vec![1, 2, 3];
    group.bench_function("prewrite_success", |b| {
        b.iter_batched(
            || {
                (
                    MvccEngine::new(InMemoryBackend::new()),
                    deterministic_key(1),
                )
            },
            |(mut engine, key)| {
                engine
                    .prewrite(TxnId(1), Timestamp(10), key, Mutation::Put(val.clone()))
                    .unwrap()
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("commit_success", |b| {
        b.iter_batched(
            || {
                let mut engine = MvccEngine::new(InMemoryBackend::new());
                let key = deterministic_key(2);
                engine
                    .prewrite(
                        TxnId(1),
                        Timestamp(10),
                        key.clone(),
                        Mutation::Put(val.clone()),
                    )
                    .unwrap();
                (engine, key)
            },
            |(mut engine, key)| {
                engine
                    .commit(TxnId(1), &key, Timestamp(10), Timestamp(20))
                    .unwrap()
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("abort", |b| {
        b.iter_batched(
            || {
                let mut engine = MvccEngine::new(InMemoryBackend::new());
                let key = deterministic_key(3);
                engine
                    .prewrite(
                        TxnId(1),
                        Timestamp(10),
                        key.clone(),
                        Mutation::Put(val.clone()),
                    )
                    .unwrap();
                (engine, key)
            },
            |(mut engine, key)| engine.abort(TxnId(1), &key, Timestamp(10)).unwrap(),
            BatchSize::SmallInput,
        )
    });

    group.bench_function("active_intent_conflict", |b| {
        b.iter_batched(
            || {
                let mut engine = MvccEngine::new(InMemoryBackend::new());
                let key = deterministic_key(4);
                engine
                    .prewrite(
                        TxnId(1),
                        Timestamp(10),
                        key.clone(),
                        Mutation::Put(val.clone()),
                    )
                    .unwrap();
                (engine, key)
            },
            |(mut engine, key)| {
                let res = engine.prewrite(TxnId(2), Timestamp(15), key, Mutation::Put(val.clone()));
                assert!(res.is_err());
                res
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_direct_batches(c: &mut Criterion) {
    let mut group = c.benchmark_group("Direct Batches");

    let batch_sizes = [1, 10, 100, 1000];
    let value_sizes = [0, 32, 1024, 65536];

    for &bs in &batch_sizes {
        for &vs in &value_sizes {
            // Avoid cartesian explosion by limiting large values to small batches
            if vs == 65536 && bs > 10 {
                continue;
            }
            if vs == 1024 && bs > 100 {
                continue;
            }

            group.bench_with_input(
                BenchmarkId::new(format!("batch_{}", bs), format!("val_{}B", vs)),
                &(bs, vs),
                |b, &(bs, vs)| {
                    b.iter_batched(
                        || {
                            let engine = MvccEngine::new(InMemoryBackend::new());
                            let val = if vs == 0 {
                                Some(Vec::new())
                            } else {
                                Some(deterministic_value(vs))
                            };
                            let mut writes = Vec::with_capacity(bs);
                            for i in 0..bs {
                                writes.push(PhysicalWrite {
                                    key: deterministic_key(i as u64),
                                    value: val.clone(),
                                });
                            }
                            (engine, writes)
                        },
                        |(mut engine, writes)| {
                            engine.apply_direct_batch(Timestamp(10), writes).unwrap()
                        },
                        BatchSize::SmallInput,
                    )
                },
            );
        }
    }
    group.finish();
}

fn bench_guarded_batches(c: &mut Criterion) {
    let mut group = c.benchmark_group("Guarded Batches");

    group.bench_function("incr_style_success", |b| {
        b.iter_batched(
            || {
                let mut engine = MvccEngine::new(InMemoryBackend::new());
                let key = b"counter".to_vec();
                engine
                    .apply_direct_batch(
                        Timestamp(10),
                        vec![PhysicalWrite {
                            key: key.clone(),
                            value: Some(vec![1]),
                        }],
                    )
                    .unwrap();
                (engine, key)
            },
            |(mut engine, key)| {
                let guards = vec![ReadGuard::ExpectedVersion {
                    key: key.clone(),
                    read_ts: Timestamp(15),
                    expected_commit_ts: Some(Timestamp(10)),
                }];
                let writes = vec![PhysicalWrite {
                    key,
                    value: Some(vec![2]),
                }];
                engine
                    .apply_guarded_batch(Timestamp(20), guards, writes)
                    .unwrap()
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("stale_guard_fail", |b| {
        b.iter_batched(
            || {
                let mut engine = MvccEngine::new(InMemoryBackend::new());
                let key = b"counter".to_vec();
                engine
                    .apply_direct_batch(
                        Timestamp(10),
                        vec![PhysicalWrite {
                            key: key.clone(),
                            value: Some(vec![1]),
                        }],
                    )
                    .unwrap();
                engine
                    .apply_direct_batch(
                        Timestamp(15),
                        vec![PhysicalWrite {
                            key: key.clone(),
                            value: Some(vec![2]),
                        }],
                    )
                    .unwrap();
                (engine, key)
            },
            |(mut engine, key)| {
                // Read at 12 (saw commit 10), but actual latest is 15.
                let guards = vec![ReadGuard::ExpectedVersion {
                    key: key.clone(),
                    read_ts: Timestamp(12),
                    expected_commit_ts: Some(Timestamp(10)),
                }];
                let writes = vec![PhysicalWrite {
                    key,
                    value: Some(vec![2]),
                }];
                let res = engine.apply_guarded_batch(Timestamp(20), guards, writes);
                assert!(res.is_err());
                res
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_gc(c: &mut Criterion) {
    let mut group = c.benchmark_group("GC");

    group.bench_function("gc_full_many_versions_one_key", |b| {
        b.iter_batched(
            || {
                let mut engine = MvccEngine::new(InMemoryBackend::new());
                let key = b"hot_key".to_vec();
                for i in 1..=1000 {
                    engine
                        .apply_direct_batch(
                            Timestamp(i),
                            vec![PhysicalWrite {
                                key: key.clone(),
                                value: Some(vec![1]),
                            }],
                        )
                        .unwrap();
                }
                engine
            },
            |mut engine| {
                engine
                    .gc(
                        Timestamp(900),
                        nexir_mvcc_core::GcOptions {
                            budget: nexir_mvcc_core::GcBudget {
                                max_keys: 10000,
                                max_versions: 10000,
                            },
                            collapse_final_tombstones: false,
                        },
                    )
                    .unwrap()
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("gc_incremental_many_versions_one_key", |b| {
        b.iter_batched(
            || {
                let mut engine = MvccEngine::new(InMemoryBackend::new());
                let key = b"hot_key".to_vec();
                for i in 1..=1000 {
                    engine
                        .apply_direct_batch(
                            Timestamp(i),
                            vec![PhysicalWrite {
                                key: key.clone(),
                                value: Some(vec![1]),
                            }],
                        )
                        .unwrap();
                }
                engine
            },
            |mut engine| {
                let mut cursor = None;
                loop {
                    let res = engine
                        .gc_incremental(
                            Timestamp(900),
                            cursor,
                            nexir_mvcc_core::GcOptions {
                                budget: nexir_mvcc_core::GcBudget {
                                    max_keys: 10,
                                    max_versions: 100,
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
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("gc_incremental_many_hot_keys_small_budget", |b| {
        b.iter_batched(
            || {
                let mut engine = MvccEngine::new(InMemoryBackend::new());
                for i in 1..=100 {
                    let key = deterministic_key(i);
                    for j in 1..=10 {
                        engine
                            .apply_direct_batch(
                                Timestamp(j),
                                vec![PhysicalWrite {
                                    key: key.clone(),
                                    value: Some(vec![1]),
                                }],
                            )
                            .unwrap();
                    }
                }
                engine
            },
            |mut engine| {
                let mut cursor = None;
                loop {
                    let res = engine
                        .gc_incremental(
                            Timestamp(8),
                            cursor,
                            nexir_mvcc_core::GcOptions {
                                budget: nexir_mvcc_core::GcBudget {
                                    max_keys: 5,
                                    max_versions: 5,
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
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("gc_incremental_resume_over_large_keyspace", |b| {
        b.iter_batched(
            || {
                let mut engine = MvccEngine::new(InMemoryBackend::new());
                for i in 1..=1000 {
                    let key = deterministic_key(i);
                    engine
                        .apply_direct_batch(
                            Timestamp(i),
                            vec![PhysicalWrite {
                                key,
                                value: Some(vec![1]),
                            }],
                        )
                        .unwrap();
                }
                engine
            },
            |mut engine| {
                let mut cursor = None;
                loop {
                    let res = engine
                        .gc_incremental(
                            Timestamp(900),
                            cursor,
                            nexir_mvcc_core::GcOptions {
                                budget: nexir_mvcc_core::GcBudget {
                                    max_keys: 50,
                                    max_versions: 100,
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
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("Codec");

    let cv = CommittedVersion {
        key: b"some_key_string".to_vec(),
        commit_ts: Timestamp(123456),
        value: Some(deterministic_value(64)),
    };
    let cv_buf = codec::encode_committed(&cv).unwrap();

    group.bench_function("encode_committed", |b| {
        b.iter(|| {
            black_box(codec::encode_committed(black_box(&cv)).unwrap());
        })
    });

    group.bench_function("decode_committed", |b| {
        b.iter(|| {
            black_box(codec::decode_committed(black_box(&cv_buf)).unwrap());
        })
    });

    let intent = Intent {
        key: b"some_key_string".to_vec(),
        txn_id: TxnId(999),
        start_ts: Timestamp(123456),
        mutation: Mutation::Put(deterministic_value(64)),
        min_commit_ts: Some(Timestamp(123457)),
    };
    let intent_buf = codec::encode_intent(&intent).unwrap();

    group.bench_function("encode_intent", |b| {
        b.iter(|| {
            black_box(codec::encode_intent(black_box(&intent)).unwrap());
        })
    });

    group.bench_function("decode_intent", |b| {
        b.iter(|| {
            black_box(codec::decode_intent(black_box(&intent_buf)).unwrap());
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_reads,
    bench_hot_key,
    bench_intent_path,
    bench_direct_batches,
    bench_guarded_batches,
    bench_gc,
    bench_codec
);
criterion_main!(benches);
