//! This example demonstrates an atomic multi-key local batch intent transaction using
//! the batch intent primitives. This simulates a classic two-phase transaction
//! executed entirely through batch operations.

use nexir_mvcc_core::{InMemoryBackend, MvccEngine, PhysicalWrite, Timestamp, TxnId};

fn main() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());

    let txn_id = TxnId(100);
    let start_ts = Timestamp(10);

    // Step 1: prewrite all intents atomically.
    println!("Step 1: prewriting intents for account_a and account_b at TS 10");
    let writes = vec![
        PhysicalWrite {
            key: b"account_a".to_vec(),
            value: Some(b"900".to_vec()), // A transfers 100 to B
        },
        PhysicalWrite {
            key: b"account_b".to_vec(),
            value: Some(b"1100".to_vec()), // B receives 100
        },
    ];

    match engine.prewrite_batch(txn_id, start_ts, writes) {
        Ok(()) => println!("Prewrite successful! Locks acquired."),
        Err(e) => {
            println!("Prewrite failed: {:?}", e);
            return;
        }
    }

    // A reader at TS 15 cannot see the intents.
    let read_a = engine.read(b"account_a", Timestamp(15)).unwrap();
    println!("Reader at TS 15 sees account_a: {:?}", read_a);

    // Step 2: commit all intents atomically.
    let commit_ts = Timestamp(20);
    let keys = vec![b"account_a".to_vec(), b"account_b".to_vec()];

    println!("Step 2: committing transaction at TS 20");
    match engine.commit_batch(txn_id, start_ts, commit_ts, keys) {
        Ok(()) => println!("Commit successful!"),
        Err(e) => println!("Commit failed: {:?}", e),
    }

    // A reader at TS 25 now sees the committed transaction.
    let final_a = engine.read(b"account_a", Timestamp(25)).unwrap().unwrap();
    let final_b = engine.read(b"account_b", Timestamp(25)).unwrap().unwrap();

    println!(
        "Reader at TS 25 sees account_a: {:?}",
        String::from_utf8(final_a).unwrap()
    );
    println!(
        "Reader at TS 25 sees account_b: {:?}",
        String::from_utf8(final_b).unwrap()
    );
}
