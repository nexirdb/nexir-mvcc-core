//! This example demonstrates an atomic `INCR` semantic using a Read-Modify-Write
//! pattern via `apply_guarded_batch`.
//!
//! We use `ExpectedValue` because `engine.read()` returns the logical value.
//! While `ExpectedVersion` is stricter against ABA problems, deriving the visible
//! `commit_ts` requires querying the backend directly. `ExpectedValue` is simpler
//! and sufficient for commutative operations like INCR.

use nexir_mvcc_core::{InMemoryBackend, MvccEngine, PhysicalWrite, ReadGuard, Timestamp};

fn main() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());

    // Initialize the counter directly
    engine
        .apply_direct_batch(
            Timestamp(5),
            vec![PhysicalWrite {
                key: b"counter".to_vec(),
                value: Some(b"10".to_vec()),
            }],
        )
        .unwrap();

    let read_ts = Timestamp(8);
    // 1. Read
    let current_val = engine.read(b"counter", read_ts).unwrap().unwrap();
    let current_num: u64 = String::from_utf8(current_val.clone())
        .unwrap()
        .parse()
        .unwrap();

    // 2. Modify
    let new_num = current_num + 1;
    let new_val = new_num.to_string().into_bytes();

    // 3. Write Conditionally
    let guards = vec![ReadGuard::ExpectedValue {
        key: b"counter".to_vec(),
        read_ts,
        expected_value: Some(current_val),
    }];
    let writes = vec![PhysicalWrite {
        key: b"counter".to_vec(),
        value: Some(new_val),
    }];

    println!(
        "Attempting to increment counter from {} to {} at TS 10",
        current_num, new_num
    );

    let commit_ts = Timestamp(10);
    match engine.apply_guarded_batch(commit_ts, guards, writes) {
        Ok(()) => println!("Increment successful!"),
        Err(e) => println!("Increment failed: {:?}", e),
    }

    let final_val = engine.read(b"counter", Timestamp(15)).unwrap();
    println!(
        "Final value: {:?}",
        final_val.map(|v| String::from_utf8(v).unwrap())
    );
}
