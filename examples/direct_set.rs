//! This example demonstrates how to use `apply_direct_batch` to perform
//! a simple `SET` operation on a key. Direct batches bypass the two-phase
//! intent system entirely and are useful for single-roundtrip writes when
//! transactions are not required.

use nexir_mvcc_core::{InMemoryBackend, MvccEngine, PhysicalWrite, Timestamp};

fn main() {
    let mut engine = MvccEngine::new(InMemoryBackend::new());

    let write = PhysicalWrite {
        key: b"config_version".to_vec(),
        value: Some(b"1.0".to_vec()),
    };

    println!("Executing SET config_version = '1.0' at TS 10");
    engine
        .apply_direct_batch(Timestamp(10), vec![write])
        .unwrap();

    let value = engine.read(b"config_version", Timestamp(15)).unwrap();
    println!(
        "Read at TS 15: {:?}",
        value.map(|v| String::from_utf8(v).unwrap())
    );
}
