use nexir_mvcc_core::{
    CommittedVersion, Intent, Mutation, Timestamp, TxnId, decode_committed, decode_intent,
    encode_committed, encode_intent,
};

#[test]
fn test_roundtrip_committed_put() {
    let original = CommittedVersion {
        key: b"testkey".to_vec(),
        commit_ts: Timestamp(42),
        value: Some(b"hello".to_vec()),
    };
    let encoded = encode_committed(&original).unwrap();
    let decoded = decode_committed(&encoded).unwrap();
    assert_eq!(original, decoded);
}

#[test]
fn test_roundtrip_timestamp_beyond_u64() {
    let original = CommittedVersion {
        key: b"wide".to_vec(),
        commit_ts: Timestamp((u64::MAX as u128) + 42),
        value: Some(b"value".to_vec()),
    };
    let encoded = encode_committed(&original).unwrap();
    let decoded = decode_committed(&encoded).unwrap();
    assert_eq!(original, decoded);
}

#[test]
fn test_roundtrip_committed_tombstone() {
    let original = CommittedVersion {
        key: b"delkey".to_vec(),
        commit_ts: Timestamp(99),
        value: None,
    };
    let encoded = encode_committed(&original).unwrap();
    let decoded = decode_committed(&encoded).unwrap();
    assert_eq!(original, decoded);
}

#[test]
fn test_roundtrip_intent_put() {
    let original = Intent {
        key: b"mykey".to_vec(),
        txn_id: TxnId(7),
        start_ts: Timestamp(12),
        mutation: Mutation::Put(b"val".to_vec()),
        min_commit_ts: Some(Timestamp(15)),
    };
    let encoded = encode_intent(&original).unwrap();
    let decoded = decode_intent(&encoded).unwrap();
    assert_eq!(original, decoded);
}

#[test]
fn test_roundtrip_intent_delete() {
    let original = Intent {
        key: b"x".to_vec(),
        txn_id: TxnId(1),
        start_ts: Timestamp(2),
        mutation: Mutation::Delete,
        min_commit_ts: None,
    };
    let encoded = encode_intent(&original).unwrap();
    let decoded = decode_intent(&encoded).unwrap();
    assert_eq!(original, decoded);
}

#[test]
fn test_golden_committed_put() {
    let v = CommittedVersion {
        key: b"k".to_vec(),
        commit_ts: Timestamp(1),
        value: Some(b"v".to_vec()),
    };
    let encoded = encode_committed(&v).unwrap();
    // 0x02 = codec version
    // 0x01 = committed record
    // 0x00 00 00 01 = key len, then "k"
    // 16-byte big-endian commit_ts=1
    // 0x01 = present flag
    // 0x00 00 00 01 = value len, then "v"
    assert_eq!(
        encoded,
        vec![
            0x02, 0x01, // version, record type
            0x00, 0x00, 0x00, 0x01, b'k', // key
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // commit_ts
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, // present
            0x00, 0x00, 0x00, 0x01, b'v', // value
        ]
    );
}

#[test]
fn test_golden_committed_tombstone() {
    let v = CommittedVersion {
        key: b"k".to_vec(),
        commit_ts: Timestamp(5),
        value: None,
    };
    let encoded = encode_committed(&v).unwrap();
    assert_eq!(
        encoded,
        vec![
            0x02, 0x01, // version, record type
            0x00, 0x00, 0x00, 0x01, b'k', // key
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // commit_ts
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, // absent (tombstone)
        ]
    );
}

#[test]
fn test_golden_intent_delete() {
    let intent = Intent {
        key: b"k".to_vec(),
        txn_id: TxnId(10),
        start_ts: Timestamp(20),
        mutation: Mutation::Delete,
        min_commit_ts: None,
    };
    let encoded = encode_intent(&intent).unwrap();
    assert_eq!(
        encoded,
        vec![
            0x02, 0x02, // version, record type
            0x00, 0x00, 0x00, 0x01, b'k', // key
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A, // txn_id=10
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // start_ts=20
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x00, // min_commit_ts absent
            0x02, // mutation = Delete
        ]
    );
}

#[test]
fn test_decode_empty_fails() {
    assert!(decode_committed(&[]).is_err());
    assert!(decode_intent(&[]).is_err());
}

#[test]
fn test_decode_bad_version_fails() {
    assert!(decode_committed(&[0xFF]).is_err());
    assert!(decode_intent(&[0xFF]).is_err());
}

#[test]
fn test_old_eight_byte_timestamp_codec_is_rejected() {
    let old_committed = vec![
        0x01, 0x01, // old version, committed record
        0x00, 0x00, 0x00, 0x01, b'k', // key
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // old timestamp
        0x00, // tombstone
    ];
    assert!(decode_committed(&old_committed).is_err());
}

#[test]
fn test_truncated_sixteen_byte_timestamps_are_rejected() {
    let mut truncated_committed = vec![
        0x02, 0x01, // version, committed record
        0x00, 0x00, 0x00, 0x01, b'k', // key
    ];
    truncated_committed.extend_from_slice(&[0; 15]);
    assert!(decode_committed(&truncated_committed).is_err());

    let mut truncated_intent = vec![
        0x02, 0x02, // version, intent record
        0x00, 0x00, 0x00, 0x01, b'k', // key
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // txn_id
    ];
    truncated_intent.extend_from_slice(&[0; 15]);
    assert!(decode_intent(&truncated_intent).is_err());
}

#[test]
fn test_timestamp_encoding_preserves_numerical_order() {
    let low = CommittedVersion {
        key: b"k".to_vec(),
        commit_ts: Timestamp(u64::MAX as u128),
        value: None,
    };
    let high = CommittedVersion {
        key: b"k".to_vec(),
        commit_ts: Timestamp((u64::MAX as u128) + 1),
        value: None,
    };
    let low_encoded = encode_committed(&low).unwrap();
    let high_encoded = encode_committed(&high).unwrap();

    assert!(low_encoded[7..23] < high_encoded[7..23]);
}

#[test]
fn test_decode_wrong_record_type_fails() {
    // valid version, but record type 0xFF is unknown
    let bad = vec![0x01, 0xFF, 0x00, 0x00, 0x00, 0x00];
    assert!(decode_committed(&bad).is_err());
    assert!(decode_intent(&bad).is_err());
}

#[test]
fn test_decode_trailing_garbage_fails() {
    let v = CommittedVersion {
        key: b"k".to_vec(),
        commit_ts: Timestamp(1),
        value: Some(b"v".to_vec()),
    };
    let mut encoded = encode_committed(&v).unwrap();
    encoded.push(0xFF); // trailing garbage
    assert!(decode_committed(&encoded).is_err());

    let intent = Intent {
        key: b"k".to_vec(),
        txn_id: TxnId(10),
        start_ts: Timestamp(20),
        mutation: Mutation::Delete,
        min_commit_ts: None,
    };
    let mut encoded_intent = encode_intent(&intent).unwrap();
    encoded_intent.push(0xFF);
    assert!(decode_intent(&encoded_intent).is_err());
}
