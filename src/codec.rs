use crate::error::CodecError;
use crate::types::{CommittedVersion, Intent, Mutation, Timestamp, TxnId};

// Codec version byte. Only 0x02 is defined.
const CODEC_VERSION: u8 = 0x02;
const RECORD_COMMITTED: u8 = 0x01;
const RECORD_INTENT: u8 = 0x02;
const FLAG_PRESENT: u8 = 0x01;
const FLAG_ABSENT: u8 = 0x00;
const MUTATION_PUT: u8 = 0x01;
const MUTATION_DELETE: u8 = 0x02;

fn encode_u32(v: u32) -> [u8; 4] {
    v.to_be_bytes()
}

fn decode_u32(buf: &[u8], offset: &mut usize) -> Result<u32, CodecError> {
    if buf.len() < *offset + 4 {
        return Err(CodecError::Decode("truncated u32".into()));
    }
    let val = u32::from_be_bytes([
        buf[*offset],
        buf[*offset + 1],
        buf[*offset + 2],
        buf[*offset + 3],
    ]);
    *offset += 4;
    Ok(val)
}

fn encode_u64(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

fn decode_u64(buf: &[u8], offset: &mut usize) -> Result<u64, CodecError> {
    if buf.len() < *offset + 8 {
        return Err(CodecError::Decode("truncated u64".into()));
    }
    let val = u64::from_be_bytes([
        buf[*offset],
        buf[*offset + 1],
        buf[*offset + 2],
        buf[*offset + 3],
        buf[*offset + 4],
        buf[*offset + 5],
        buf[*offset + 6],
        buf[*offset + 7],
    ]);
    *offset += 8;
    Ok(val)
}

fn encode_u128(v: u128) -> [u8; 16] {
    v.to_be_bytes()
}

fn decode_u128(buf: &[u8], offset: &mut usize) -> Result<u128, CodecError> {
    if buf.len() < *offset + 16 {
        return Err(CodecError::Decode("truncated u128".into()));
    }
    let bytes: [u8; 16] = buf[*offset..*offset + 16]
        .try_into()
        .map_err(|_| CodecError::Decode("truncated u128".into()))?;
    *offset += 16;
    Ok(u128::from_be_bytes(bytes))
}

fn encode_bytes(buf: &mut Vec<u8>, bytes: &[u8]) -> Result<(), CodecError> {
    let len =
        u32::try_from(bytes.len()).map_err(|_| CodecError::Encode("byte slice too long".into()))?;
    buf.extend_from_slice(&encode_u32(len));
    buf.extend_from_slice(bytes);
    Ok(())
}

fn decode_bytes(buf: &[u8], offset: &mut usize) -> Result<Vec<u8>, CodecError> {
    let len = decode_u32(buf, offset)? as usize;
    if buf.len() < *offset + len {
        return Err(CodecError::Decode("truncated byte slice".into()));
    }
    let bytes = buf[*offset..*offset + len].to_vec();
    *offset += len;
    Ok(bytes)
}

/// Encodes a committed version to bytes.
pub fn encode_committed(version: &CommittedVersion) -> Result<Vec<u8>, CodecError> {
    let mut buf = Vec::new();
    buf.push(CODEC_VERSION);
    buf.push(RECORD_COMMITTED);
    encode_bytes(&mut buf, &version.key)?;
    buf.extend_from_slice(&encode_u128(version.commit_ts.0));
    match &version.value {
        Some(v) => {
            buf.push(FLAG_PRESENT);
            encode_bytes(&mut buf, v)?;
        }
        None => {
            buf.push(FLAG_ABSENT);
        }
    }
    Ok(buf)
}

/// Decodes a committed version from bytes.
pub fn decode_committed(buf: &[u8]) -> Result<CommittedVersion, CodecError> {
    let mut offset = 0;
    if buf.is_empty() {
        return Err(CodecError::Decode("empty buffer".into()));
    }
    let version = buf[offset];
    offset += 1;
    if version != CODEC_VERSION {
        return Err(CodecError::Decode(format!(
            "unknown codec version {}",
            version
        )));
    }
    if buf.len() < offset + 1 {
        return Err(CodecError::Decode("missing record type".into()));
    }
    let record_type = buf[offset];
    offset += 1;
    if record_type != RECORD_COMMITTED {
        return Err(CodecError::Decode(format!(
            "expected committed record type {}, got {}",
            RECORD_COMMITTED, record_type
        )));
    }
    let key = decode_bytes(buf, &mut offset)?;
    let commit_ts = Timestamp(decode_u128(buf, &mut offset)?);
    if buf.len() < offset + 1 {
        return Err(CodecError::Decode("missing value flag".into()));
    }
    let flag = buf[offset];
    offset += 1;
    let value = if flag == FLAG_PRESENT {
        Some(decode_bytes(buf, &mut offset)?)
    } else if flag == FLAG_ABSENT {
        None
    } else {
        return Err(CodecError::Decode(format!("unknown value flag {}", flag)));
    };
    if offset != buf.len() {
        return Err(CodecError::Decode("trailing garbage".into()));
    }
    Ok(CommittedVersion {
        key,
        commit_ts,
        value,
    })
}

/// Encodes an intent to bytes.
pub fn encode_intent(intent: &Intent) -> Result<Vec<u8>, CodecError> {
    let mut buf = Vec::new();
    buf.push(CODEC_VERSION);
    buf.push(RECORD_INTENT);
    encode_bytes(&mut buf, &intent.key)?;
    buf.extend_from_slice(&encode_u64(intent.txn_id.0));
    buf.extend_from_slice(&encode_u128(intent.start_ts.0));
    match intent.min_commit_ts {
        Some(ts) => {
            buf.push(FLAG_PRESENT);
            buf.extend_from_slice(&encode_u128(ts.0));
        }
        None => {
            buf.push(FLAG_ABSENT);
        }
    }
    match &intent.mutation {
        Mutation::Put(v) => {
            buf.push(MUTATION_PUT);
            encode_bytes(&mut buf, v)?;
        }
        Mutation::Delete => {
            buf.push(MUTATION_DELETE);
        }
    }
    Ok(buf)
}

/// Decodes an intent from bytes.
pub fn decode_intent(buf: &[u8]) -> Result<Intent, CodecError> {
    let mut offset = 0;
    if buf.is_empty() {
        return Err(CodecError::Decode("empty buffer".into()));
    }
    let version = buf[offset];
    offset += 1;
    if version != CODEC_VERSION {
        return Err(CodecError::Decode(format!(
            "unknown codec version {}",
            version
        )));
    }
    if buf.len() < offset + 1 {
        return Err(CodecError::Decode("missing record type".into()));
    }
    let record_type = buf[offset];
    offset += 1;
    if record_type != RECORD_INTENT {
        return Err(CodecError::Decode(format!(
            "expected intent record type {}, got {}",
            RECORD_INTENT, record_type
        )));
    }
    let key = decode_bytes(buf, &mut offset)?;
    let txn_id = TxnId(decode_u64(buf, &mut offset)?);
    let start_ts = Timestamp(decode_u128(buf, &mut offset)?);
    if buf.len() < offset + 1 {
        return Err(CodecError::Decode("missing min_commit_ts flag".into()));
    }
    let flag = buf[offset];
    offset += 1;
    let min_commit_ts = if flag == FLAG_PRESENT {
        Some(Timestamp(decode_u128(buf, &mut offset)?))
    } else if flag == FLAG_ABSENT {
        None
    } else {
        return Err(CodecError::Decode(format!(
            "unknown min_commit_ts flag {}",
            flag
        )));
    };
    if buf.len() < offset + 1 {
        return Err(CodecError::Decode("missing mutation type".into()));
    }
    let mutation_type = buf[offset];
    offset += 1;
    let mutation = if mutation_type == MUTATION_PUT {
        Mutation::Put(decode_bytes(buf, &mut offset)?)
    } else if mutation_type == MUTATION_DELETE {
        Mutation::Delete
    } else {
        return Err(CodecError::Decode(format!(
            "unknown mutation type {}",
            mutation_type
        )));
    };
    if offset != buf.len() {
        return Err(CodecError::Decode("trailing garbage".into()));
    }
    Ok(Intent {
        key,
        txn_id,
        start_ts,
        mutation,
        min_commit_ts,
    })
}
