use thiserror::Error;

use crate::types::{Timestamp, TxnId};

/// Error during a read operation.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum ReadError {
    /// An error returned by the backend storage.
    #[error("backend error: {0}")]
    Backend(String),
}

/// Error during a single-key prewrite operation.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum PrewriteError {
    /// The key is locked by another active transaction.
    #[error("key is locked by another transaction: txn_id={txn_id}")]
    KeyLocked {
        /// The transaction ID currently holding the lock.
        txn_id: TxnId,
    },
    /// A committed version exists that is newer than the transaction's start_ts.
    #[error("write conflict: committed version after start_ts")]
    WriteConflict,
    /// An error returned by the backend storage.
    #[error("backend error: {0}")]
    Backend(String),
    /// An intent already exists for the same transaction but with different parameters.
    #[error("intent already exists with different parameters")]
    IntentAlreadyExists,
}

/// Error during a single-key commit operation.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum CommitError {
    /// The intent to be committed was not found.
    #[error("intent not found")]
    IntentNotFound,
    /// The transaction ID on the intent does not match the commit request.
    #[error("txn_id mismatch")]
    TxnIdMismatch,
    /// The start timestamp on the intent does not match the commit request.
    #[error("start_ts mismatch")]
    StartTsMismatch,
    /// The commit timestamp is less than or equal to the start timestamp.
    #[error("invalid commit timestamp: commit_ts {commit_ts} <= start_ts {start_ts}")]
    InvalidCommitTimestamp {
        /// The start timestamp of the transaction.
        start_ts: Timestamp,
        /// The invalid commit timestamp.
        commit_ts: Timestamp,
    },
    /// A committed version already exists exactly at this commit timestamp.
    #[error("duplicate commit timestamp: {commit_ts}")]
    DuplicateCommitTimestamp {
        /// The duplicate commit timestamp.
        commit_ts: Timestamp,
    },
    /// The commit timestamp is older than the latest committed version for a key.
    #[error("commit timestamp too old: commit_ts {commit_ts}, latest is {latest_commit_ts}")]
    CommitTsTooOld {
        /// The requested commit timestamp.
        commit_ts: Timestamp,
        /// The latest committed version's timestamp.
        latest_commit_ts: Timestamp,
    },
    /// The commit timestamp is earlier than the intent's required minimum commit timestamp.
    #[error("commit_ts {commit_ts} is before required minimum {min_commit_ts}")]
    CommitTsTooEarly {
        /// The requested commit timestamp.
        commit_ts: Timestamp,
        /// The minimum allowed commit timestamp.
        min_commit_ts: Timestamp,
    },
    /// An error returned by the backend storage.
    #[error("backend error: {0}")]
    Backend(String),
}

/// Error during a single-key abort operation.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum AbortError {
    /// An error returned by the backend storage.
    #[error("backend error: {0}")]
    Backend(String),
}

/// Error during garbage collection.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum GcError {
    /// An error returned by the backend storage.
    #[error("backend error: {0}")]
    Backend(String),
    /// The provided GC budget is invalid (e.g., max_keys or max_versions is 0).
    #[error("invalid gc budget: max_keys and max_versions must be > 0")]
    InvalidGcBudget,
}

/// Error during codec operations.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    /// An error occurred during encoding.
    #[error("encode error: {0}")]
    Encode(String),
    /// An error occurred during decoding.
    #[error("decode error: {0}")]
    Decode(String),
}

/// Error during a direct or guarded batch.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum BatchError {
    /// The batch contained no write operations.
    #[error("batch is empty")]
    EmptyBatch,
    /// The commit timestamp is less than or equal to a guard's read timestamp.
    #[error("invalid commit timestamp: commit_ts {commit_ts} <= read_ts {read_ts}")]
    InvalidCommitTimestamp {
        /// The read timestamp of the guard.
        read_ts: Timestamp,
        /// The invalid commit timestamp.
        commit_ts: Timestamp,
    },
    /// The commit timestamp is older than the latest committed version for a key.
    #[error("commit timestamp too old: key {key:?} at {commit_ts}, latest is {latest_commit_ts}")]
    CommitTsTooOld {
        /// The key that caused the error.
        key: Vec<u8>,
        /// The requested commit timestamp.
        commit_ts: Timestamp,
        /// The latest committed version's timestamp.
        latest_commit_ts: Timestamp,
    },
    /// A guarded batch was submitted without any read guards.
    #[error("guarded batch requires at least one read guard")]
    NoReadGuards,
    /// The batch contains multiple physical writes for the same key.
    #[error("duplicate key in batch: {key:?}")]
    DuplicateKeyInBatch {
        /// The duplicate key.
        key: Vec<u8>,
    },
    /// A key in the batch is currently locked by an active intent.
    #[error("key is locked by an active intent: key {key:?}, txn_id {txn_id}")]
    KeyLocked {
        /// The locked key.
        key: Vec<u8>,
        /// The transaction ID holding the lock.
        txn_id: TxnId,
    },
    /// A read guard failed because a newer version exists after the `read_ts`.
    #[error("read guard failed: newer version exists after read_ts {read_ts} for key {key:?} (actual_commit_ts: {actual_commit_ts})")]
    GuardFailedNewerVersion {
        /// The key that failed the guard.
        key: Vec<u8>,
        /// The read timestamp of the guard.
        read_ts: Timestamp,
        /// The timestamp of the newer version.
        actual_commit_ts: Timestamp,
    },
    /// A read guard failed because the actual version did not match the expected version.
    #[error(
        "read guard failed: expected commit_ts {expected:?} but found {actual:?} for key {key:?}"
    )]
    GuardFailedVersionMismatch {
        /// The key that failed the guard.
        key: Vec<u8>,
        /// The expected commit timestamp (or None if expected absent).
        expected: Option<Timestamp>,
        /// The actual commit timestamp found (or None if actually absent).
        actual: Option<Timestamp>,
    },
    /// A read guard failed because the actual logical value did not match the expected value.
    #[error("read guard failed: expected value mismatch for key {key:?}")]
    GuardFailedValueMismatch {
        /// The key that failed the guard.
        key: Vec<u8>,
    },
    /// An error returned by the backend storage.
    #[error("backend error: {0}")]
    Backend(String),
}

/// Error during a prewrite batch.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum BatchPrewriteError {
    /// The batch contained no write operations.
    #[error("batch is empty")]
    EmptyBatch,
    /// The batch contains multiple writes for the same key.
    #[error("duplicate key in batch: {key:?}")]
    DuplicateKeyInBatch {
        /// The duplicate key.
        key: Vec<u8>,
    },
    /// A key in the batch is currently locked by another active transaction.
    #[error("key is locked by another transaction: key {key:?}, txn_id {txn_id}")]
    KeyLocked {
        /// The locked key.
        key: Vec<u8>,
        /// The transaction ID holding the lock.
        txn_id: TxnId,
    },
    /// A committed version exists that is newer than the transaction's start_ts.
    #[error("write conflict: committed version after start_ts for key {key:?}")]
    WriteConflict {
        /// The key that caused the conflict.
        key: Vec<u8>,
    },
    /// An intent already exists for the same transaction but with different parameters.
    #[error("intent already exists with different parameters for key {key:?}")]
    IntentAlreadyExists {
        /// The key with the conflicting intent.
        key: Vec<u8>,
    },
    /// The batch is a partial replay, meaning some intents exist while others do not.
    #[error("partial batch replay detected: some keys have intents, others are missing")]
    PartialBatchReplay,
    /// An error returned by the backend storage.
    #[error("backend error: {0}")]
    Backend(String),
}

/// Error during a commit batch.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum BatchCommitError {
    /// The batch contained no commit operations.
    #[error("batch is empty")]
    EmptyBatch,
    /// The batch contains multiple commits for the same key.
    #[error("duplicate key in batch: {key:?}")]
    DuplicateKeyInBatch {
        /// The duplicate key.
        key: Vec<u8>,
    },
    /// The intent to be committed was not found.
    #[error("intent not found for key {key:?}")]
    IntentNotFound {
        /// The key whose intent is missing.
        key: Vec<u8>,
    },
    /// The transaction ID on the intent does not match the commit request.
    #[error("txn_id mismatch for key {key:?}")]
    TxnIdMismatch {
        /// The key with the mismatched transaction ID.
        key: Vec<u8>,
    },
    /// The start timestamp on the intent does not match the commit request.
    #[error("start_ts mismatch for key {key:?}")]
    StartTsMismatch {
        /// The key with the mismatched start timestamp.
        key: Vec<u8>,
    },
    /// The commit timestamp is less than or equal to the start timestamp.
    #[error("invalid commit timestamp: commit_ts {commit_ts} <= start_ts {start_ts}")]
    InvalidCommitTimestamp {
        /// The start timestamp of the transaction.
        start_ts: Timestamp,
        /// The invalid commit timestamp.
        commit_ts: Timestamp,
    },
    /// The commit timestamp is earlier than the intent's required minimum commit timestamp.
    #[error("commit_ts {commit_ts} is before required minimum {min_commit_ts} for key {key:?}")]
    CommitTsTooEarly {
        /// The key failing the minimum commit timestamp check.
        key: Vec<u8>,
        /// The requested commit timestamp.
        commit_ts: Timestamp,
        /// The minimum allowed commit timestamp.
        min_commit_ts: Timestamp,
    },
    /// The commit timestamp is older than the latest committed version for a key.
    #[error("commit timestamp too old: key {key:?} at {commit_ts}, latest is {latest_commit_ts}")]
    CommitTsTooOld {
        /// The key that caused the error.
        key: Vec<u8>,
        /// The requested commit timestamp.
        commit_ts: Timestamp,
        /// The latest committed version's timestamp.
        latest_commit_ts: Timestamp,
    },
    /// An error returned by the backend storage.
    #[error("backend error: {0}")]
    Backend(String),
}

/// Error during a batch abort operation.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum BatchAbortError {
    /// The batch contains multiple aborts for the same key.
    #[error("duplicate key in batch: {key:?}")]
    DuplicateKeyInBatch {
        /// The duplicate key.
        key: Vec<u8>,
    },
    /// An error returned by the backend storage.
    #[error("backend error: {0}")]
    Backend(String),
}
