//! This example demonstrates how `abort_batch` handles retries idempotently.
//!
//! In a distributed system, a client might issue an abort, experience a timeout,
//! and retry the abort. `abort_batch` safely ignores keys where the intent is
//! already removed.

use nexir_mvcc_core::{InMemoryBackend, MvccEngine, PhysicalWrite, Timestamp, TxnId};

fn main() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());

    let txn_id = TxnId(42);
    let start_ts = Timestamp(10);
    let keys = vec![b"key1".to_vec(), b"key2".to_vec()];

    // Write some intents
    let writes = vec![
        PhysicalWrite {
            key: keys[0].clone(),
            value: Some(b"v1".to_vec()),
        },
        PhysicalWrite {
            key: keys[1].clone(),
            value: Some(b"v2".to_vec()),
        },
    ];
    engine.prewrite_batch(txn_id, start_ts, writes).unwrap();
    println!(
        "Intents written for {:?}",
        keys.iter()
            .map(|k| String::from_utf8(k.clone()).unwrap())
            .collect::<Vec<_>>()
    );

    // First Abort (e.g. original request)
    println!("Executing first abort request...");
    engine.abort_batch(txn_id, start_ts, keys.clone()).unwrap();
    println!("First abort successful.");

    // Second Abort (e.g. retry after network timeout)
    println!("Executing second abort request (retry)...");
    engine.abort_batch(txn_id, start_ts, keys).unwrap();
    println!("Second abort successful. Idempotency maintained.");
}
