use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use nexir_mvcc_core::{
    CommittedVersion, InMemoryBackend, Mutation, MvccEngine, PhysicalWrite, ReadGuard, Timestamp,
    TxnId,
};

fn key(i: u64) -> Vec<u8> {
    format!("key_{i:08}").into_bytes()
}

fn value(size: usize) -> Vec<u8> {
    vec![42u8; size]
}

fn bench_pure_read_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("A. Pure Read Path");

    group.bench_function("latest_read_same_hot_key", |b| {
        let mut engine = MvccEngine::new(InMemoryBackend::new());
        let k = b"hot_key".to_vec();
        engine
            .apply_direct_batch(
                Timestamp(10),
                vec![PhysicalWrite {
                    key: k.clone(),
                    value: Some(vec![1]),
                }],
            )
            .unwrap();

        b.iter(|| {
            black_box(engine.read(black_box(&k), Timestamp(20)).unwrap());
        })
    });

    group.bench_function("latest_read_many_keys", |b| {
        let mut engine = MvccEngine::new(InMemoryBackend::new());
        let keys: Vec<_> = (0..10_000).map(key).collect();
        for (i, k) in keys.iter().enumerate() {
            engine
                .apply_direct_batch(
                    Timestamp(i as u64 + 1),
                    vec![PhysicalWrite {
                        key: k.clone(),
                        value: Some(vec![1]),
                    }],
                )
                .unwrap();
        }

        let mut idx = 0;
        b.iter(|| {
            let k = &keys[idx % 10_000];
            black_box(engine.read(black_box(k), Timestamp(20_000)).unwrap());
            idx += 1;
        })
    });

    group.bench_function("historical_read_hot_key_1k_versions", |b| {
        let mut engine = MvccEngine::new(InMemoryBackend::new());
        let k = b"hot_key".to_vec();
        for i in 1..=1000 {
            engine
                .apply_direct_batch(
                    Timestamp(i),
                    vec![PhysicalWrite {
                        key: k.clone(),
                        value: Some(vec![1]),
                    }],
                )
                .unwrap();
        }

        b.iter(|| {
            black_box(engine.read(black_box(&k), Timestamp(500)).unwrap());
        })
    });

    group.bench_function("historical_read_hot_key_100k_versions", |b| {
        let mut engine = MvccEngine::new(InMemoryBackend::new());
        let k = b"hot_key".to_vec();
        for i in 1..=100_000 {
            engine
                .apply_direct_batch(
                    Timestamp(i),
                    vec![PhysicalWrite {
                        key: k.clone(),
                        value: Some(vec![1]),
                    }],
                )
                .unwrap();
        }

        b.iter(|| {
            black_box(engine.read(black_box(&k), Timestamp(50_000)).unwrap());
        })
    });

    group.finish();
}

fn bench_direct_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("B. Direct Physical Writes");

    group.bench_function("direct_set_same_key_increasing_ts", |b| {
        let mut engine = MvccEngine::new(InMemoryBackend::new());
        let k = b"hot_key".to_vec();
        let val = value(32);
        let mut ts = 1;

        b.iter(|| {
            engine
                .apply_direct_batch(
                    Timestamp(ts),
                    vec![PhysicalWrite {
                        key: k.clone(),
                        value: Some(val.clone()),
                    }],
                )
                .unwrap();
            ts += 1;
        })
    });

    group.bench_function("direct_set_many_keys_increasing_ts", |b| {
        let mut engine = MvccEngine::new(InMemoryBackend::new());
        let keys: Vec<_> = (0..10_000).map(key).collect();
        let val = value(32);
        let mut ts = 1;

        b.iter(|| {
            let k = &keys[(ts as usize) % 10_000];
            engine
                .apply_direct_batch(
                    Timestamp(ts),
                    vec![PhysicalWrite {
                        key: k.clone(),
                        value: Some(val.clone()),
                    }],
                )
                .unwrap();
            ts += 1;
        })
    });

    for &size in &[1, 10, 100, 1000] {
        group.bench_with_input(
            criterion::BenchmarkId::new("direct_batch", size),
            &size,
            |b, &s| {
                let keys: Vec<_> = (0..s).map(|i| key(i as u64)).collect();
                let val = value(32);
                let mut ts = 1;

                b.iter_batched(
                    || {
                        let engine = MvccEngine::new(InMemoryBackend::new());
                        let mut writes = Vec::with_capacity(s);
                        for k in &keys {
                            writes.push(PhysicalWrite {
                                key: k.clone(),
                                value: Some(val.clone()),
                            });
                        }
                        (engine, writes)
                    },
                    |(mut engine, writes)| {
                        engine.apply_direct_batch(Timestamp(ts), writes).unwrap();
                        ts += 1;
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

fn bench_guarded_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("C. Version-Guarded Writes");

    group.bench_function("guarded_update_same_key_success", |b| {
        let mut engine = MvccEngine::new(InMemoryBackend::new());
        let k = b"hot_key".to_vec();
        engine
            .apply_direct_batch(
                Timestamp(1),
                vec![PhysicalWrite {
                    key: k.clone(),
                    value: Some(vec![1]),
                }],
            )
            .unwrap();
        let mut ts = 2;

        b.iter(|| {
            engine
                .apply_guarded_batch(
                    Timestamp(ts),
                    vec![ReadGuard::ExpectedVersion {
                        key: k.clone(),
                        read_ts: Timestamp(ts - 1),
                        expected_commit_ts: Some(Timestamp(ts - 1)),
                    }],
                    vec![PhysicalWrite {
                        key: k.clone(),
                        value: Some(vec![1]),
                    }],
                )
                .unwrap();
            ts += 1;
        })
    });

    group.bench_function("guarded_update_same_key_stale_fail", |b| {
        let mut engine = MvccEngine::new(InMemoryBackend::new());
        let k = b"hot_key".to_vec();
        engine
            .apply_direct_batch(
                Timestamp(1),
                vec![PhysicalWrite {
                    key: k.clone(),
                    value: Some(vec![1]),
                }],
            )
            .unwrap();

        b.iter(|| {
            let res = engine.apply_guarded_batch(
                Timestamp(10),
                vec![ReadGuard::ExpectedVersion {
                    key: k.clone(),
                    read_ts: Timestamp(0),
                    expected_commit_ts: None,
                }],
                vec![PhysicalWrite {
                    key: k.clone(),
                    value: Some(vec![1]),
                }],
            );
            assert!(res.is_err());
        })
    });

    group.bench_function("guarded_update_many_keys_success", |b| {
        let mut engine = MvccEngine::new(InMemoryBackend::new());
        let keys: Vec<_> = (0..10_000).map(key).collect();
        let mut latest_ts = Vec::with_capacity(keys.len());
        for (i, k) in keys.iter().enumerate() {
            let commit_ts = Timestamp(i as u64 + 1);
            engine
                .apply_direct_batch(
                    commit_ts,
                    vec![PhysicalWrite {
                        key: k.clone(),
                        value: Some(vec![1]),
                    }],
                )
                .unwrap();
            latest_ts.push(commit_ts);
        }
        let mut next_commit_ts = Timestamp(20_000);

        b.iter(|| {
            let idx = (next_commit_ts.0 as usize) % keys.len();
            let k = &keys[idx];
            let expected_commit_ts = latest_ts[idx];
            engine
                .apply_guarded_batch(
                    next_commit_ts,
                    vec![ReadGuard::ExpectedVersion {
                        key: k.clone(),
                        read_ts: expected_commit_ts,
                        expected_commit_ts: Some(expected_commit_ts),
                    }],
                    vec![PhysicalWrite {
                        key: k.clone(),
                        value: Some(vec![1]),
                    }],
                )
                .unwrap();
            latest_ts[idx] = next_commit_ts;
            next_commit_ts = Timestamp(next_commit_ts.0 + 1);
        });
    });

    group.bench_function("guarded_expected_absent_success", |b| {
        b.iter_batched(
            || {
                let engine = MvccEngine::new(InMemoryBackend::new());
                let k = b"absent".to_vec();
                (engine, k)
            },
            |(mut engine, k)| {
                engine
                    .apply_guarded_batch(
                        Timestamp(1),
                        vec![ReadGuard::ExpectedVersion {
                            key: k.clone(),
                            read_ts: Timestamp(0),
                            expected_commit_ts: None,
                        }],
                        vec![PhysicalWrite {
                            key: k.clone(),
                            value: Some(vec![1]),
                        }],
                    )
                    .unwrap();
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("guarded_expected_absent_fail", |b| {
        let mut engine = MvccEngine::new(InMemoryBackend::new());
        let k = b"present".to_vec();
        engine
            .apply_direct_batch(
                Timestamp(1),
                vec![PhysicalWrite {
                    key: k.clone(),
                    value: Some(vec![1]),
                }],
            )
            .unwrap();
        b.iter(|| {
            let res = engine.apply_guarded_batch(
                Timestamp(2),
                vec![ReadGuard::ExpectedVersion {
                    key: k.clone(),
                    read_ts: Timestamp(1),
                    expected_commit_ts: None,
                }],
                vec![PhysicalWrite {
                    key: k.clone(),
                    value: Some(vec![2]),
                }],
            );
            assert!(res.is_err());
        })
    });

    group.finish();
}

fn bench_intent_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("D. Intent Path");

    group.bench_function("prewrite_single_key", |b| {
        b.iter_batched(
            || {
                let engine = MvccEngine::new(InMemoryBackend::new());
                let k = b"key1".to_vec();
                (engine, k)
            },
            |(mut engine, k)| {
                engine
                    .prewrite(TxnId(1), Timestamp(10), k, Mutation::Put(vec![1]))
                    .unwrap();
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("commit_single_key", |b| {
        b.iter_batched(
            || {
                let mut engine = MvccEngine::new(InMemoryBackend::new());
                let k = b"key1".to_vec();
                engine
                    .prewrite(TxnId(1), Timestamp(10), k.clone(), Mutation::Put(vec![1]))
                    .unwrap();
                (engine, k)
            },
            |(mut engine, k)| {
                engine
                    .commit(TxnId(1), &k, Timestamp(10), Timestamp(20))
                    .unwrap();
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("abort_single_key", |b| {
        b.iter_batched(
            || {
                let mut engine = MvccEngine::new(InMemoryBackend::new());
                let k = b"key1".to_vec();
                engine
                    .prewrite(TxnId(1), Timestamp(10), k.clone(), Mutation::Put(vec![1]))
                    .unwrap();
                (engine, k)
            },
            |(mut engine, k)| {
                engine.abort(TxnId(1), &k, Timestamp(10)).unwrap();
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("prewrite_commit_single_key", |b| {
        b.iter_batched(
            || {
                let engine = MvccEngine::new(InMemoryBackend::new());
                let k = b"key1".to_vec();
                (engine, k)
            },
            |(mut engine, k)| {
                engine
                    .prewrite(TxnId(1), Timestamp(10), k.clone(), Mutation::Put(vec![1]))
                    .unwrap();
                engine
                    .commit(TxnId(1), &k, Timestamp(10), Timestamp(20))
                    .unwrap();
            },
            BatchSize::SmallInput,
        )
    });

    for &size in &[10, 100] {
        group.bench_with_input(
            criterion::BenchmarkId::new("prewrite_batch", size),
            &size,
            |b, &s| {
                b.iter_batched(
                    || {
                        let engine = MvccEngine::new(InMemoryBackend::new());
                        let writes = (0..s)
                            .map(|i| PhysicalWrite {
                                key: key(i as u64),
                                value: Some(vec![1]),
                            })
                            .collect();
                        (engine, writes)
                    },
                    |(mut engine, writes)| {
                        engine
                            .prewrite_batch(TxnId(1), Timestamp(10), writes)
                            .unwrap();
                    },
                    BatchSize::SmallInput,
                )
            },
        );

        group.bench_with_input(
            criterion::BenchmarkId::new("commit_batch", size),
            &size,
            |b, &s| {
                b.iter_batched(
                    || {
                        let mut engine = MvccEngine::new(InMemoryBackend::new());
                        let writes: Vec<_> = (0..s)
                            .map(|i| PhysicalWrite {
                                key: key(i as u64),
                                value: Some(vec![1]),
                            })
                            .collect();
                        engine
                            .prewrite_batch(TxnId(1), Timestamp(10), writes)
                            .unwrap();
                        let keys = (0..s).map(|i| key(i as u64)).collect();
                        (engine, keys)
                    },
                    |(mut engine, keys)| {
                        engine
                            .commit_batch(TxnId(1), Timestamp(10), Timestamp(20), keys)
                            .unwrap();
                    },
                    BatchSize::SmallInput,
                )
            },
        );

        group.bench_with_input(
            criterion::BenchmarkId::new("abort_batch", size),
            &size,
            |b, &s| {
                b.iter_batched(
                    || {
                        let mut engine = MvccEngine::new(InMemoryBackend::new());
                        let writes: Vec<_> = (0..s)
                            .map(|i| PhysicalWrite {
                                key: key(i as u64),
                                value: Some(vec![1]),
                            })
                            .collect();
                        engine
                            .prewrite_batch(TxnId(1), Timestamp(10), writes)
                            .unwrap();
                        let keys = (0..s).map(|i| key(i as u64)).collect();
                        (engine, keys)
                    },
                    |(mut engine, keys)| {
                        engine.abort_batch(TxnId(1), Timestamp(10), keys).unwrap();
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

fn bench_narrow_in_memory(c: &mut Criterion) {
    let mut group = c.benchmark_group("E. Narrow In-Memory Microbenchmarks");

    // same_tx_repeated_update_no_commit is basically overwriting the intent? Core rejects duplicate keys in a batch.
    // And prewrite fails if active intent exists for diff txn. If same txn, it overwrites.
    group.bench_function("same_tx_repeated_update_no_commit", |b| {
        let mut engine = MvccEngine::new(InMemoryBackend::new());
        let k = b"key1".to_vec();
        b.iter(|| {
            engine
                .prewrite(TxnId(1), Timestamp(10), k.clone(), Mutation::Put(vec![1]))
                .unwrap();
        })
    });

    group.bench_function("repeated_direct_update_no_gc", |b| {
        let mut engine = MvccEngine::new(InMemoryBackend::new());
        let k = b"key1".to_vec();
        let mut ts = 1;
        b.iter(|| {
            engine
                .apply_direct_batch(
                    Timestamp(ts),
                    vec![PhysicalWrite {
                        key: k.clone(),
                        value: Some(vec![1]),
                    }],
                )
                .unwrap();
            ts += 1;
        })
    });

    group.bench_function("repeated_guarded_update_no_gc", |b| {
        let mut engine = MvccEngine::new(InMemoryBackend::new());
        let k = b"key1".to_vec();
        engine
            .apply_direct_batch(
                Timestamp(1),
                vec![PhysicalWrite {
                    key: k.clone(),
                    value: Some(vec![1]),
                }],
            )
            .unwrap();
        let mut ts = 2;
        b.iter(|| {
            engine
                .apply_guarded_batch(
                    Timestamp(ts),
                    vec![ReadGuard::ExpectedVersion {
                        key: k.clone(),
                        read_ts: Timestamp(ts - 1),
                        expected_commit_ts: Some(Timestamp(ts - 1)),
                    }],
                    vec![PhysicalWrite {
                        key: k.clone(),
                        value: Some(vec![2]),
                    }],
                )
                .unwrap();
            ts += 1;
        })
    });

    group.bench_function("begin_update_commit_equivalent", |b| {
        b.iter_batched(
            || {
                let engine = MvccEngine::new(InMemoryBackend::new());
                let k = b"key1".to_vec();
                (engine, k)
            },
            |(mut engine, k)| {
                engine
                    .prewrite(TxnId(1), Timestamp(10), k.clone(), Mutation::Put(vec![1]))
                    .unwrap();
                engine
                    .commit(TxnId(1), &k, Timestamp(10), Timestamp(20))
                    .unwrap();
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_allocation_cost_probes(c: &mut Criterion) {
    let mut group = c.benchmark_group("F. Allocation & Clone Cost Probes");

    let small_key = b"short_string".to_vec();
    group.bench_function("clone_small_key", |b| {
        b.iter(|| {
            black_box(small_key.clone());
        })
    });

    let large_value = vec![42u8; 1024];
    group.bench_function("clone_1kb_value", |b| {
        b.iter(|| {
            black_box(large_value.clone());
        })
    });

    group.bench_function("physical_write_construction", |b| {
        b.iter(|| {
            black_box(PhysicalWrite {
                key: b"key".to_vec(),
                value: Some(vec![1, 2, 3]),
            });
        })
    });

    group.bench_function("committed_version_construction", |b| {
        b.iter(|| {
            black_box(CommittedVersion {
                key: b"key".to_vec(),
                commit_ts: Timestamp(10),
                value: Some(vec![1, 2, 3]),
            });
        })
    });

    group.bench_function("read_guard_construction", |b| {
        b.iter(|| {
            black_box(ReadGuard::ExpectedVersion {
                key: b"key".to_vec(),
                read_ts: Timestamp(10),
                expected_commit_ts: Some(Timestamp(5)),
            });
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_pure_read_path,
    bench_direct_writes,
    bench_guarded_writes,
    bench_intent_path,
    bench_narrow_in_memory,
    bench_allocation_cost_probes
);
criterion_main!(benches);
