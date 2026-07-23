use std::fmt;

/// A monotonic logical timestamp used for ordering versions and intents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(pub u128);

/// A unique identifier for a distributed or local transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxnId(pub u64);

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Alias for a physical key.
pub type Key = Vec<u8>;
/// Alias for a physical value.
pub type Value = Vec<u8>;

/// Represents a logical mutation to a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mutation {
    /// A logical put/write.
    Put(Value),
    /// A logical delete/tombstone.
    Delete,
}

impl Mutation {
    /// Returns the optional value of the mutation.
    pub fn value(&self) -> Option<Value> {
        match self {
            Mutation::Put(v) => Some(v.clone()),
            Mutation::Delete => None,
        }
    }

    /// Returns whether this mutation is a delete.
    pub fn is_delete(&self) -> bool {
        matches!(self, Mutation::Delete)
    }
}

/// A durable, provisional lock representing an uncommitted transactional write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Intent {
    /// The key being modified.
    pub key: Key,
    /// The transaction holding this intent.
    pub txn_id: TxnId,
    /// The start timestamp of the transaction.
    pub start_ts: Timestamp,
    /// The physical mutation (Put or Delete).
    pub mutation: Mutation,
    /// An optional minimum commit timestamp required for this intent.
    pub min_commit_ts: Option<Timestamp>,
}

/// A fully committed version of a key, visible to readers at or after `commit_ts`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedVersion {
    /// The key.
    pub key: Key,
    /// The timestamp at which this version became visible.
    pub commit_ts: Timestamp,
    /// The physical value, or None if it's a tombstone.
    pub value: Option<Value>, // None means tombstone
}

/// A raw physical write instruction used in direct and guarded batches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalWrite {
    /// The key to write.
    pub key: Key,
    /// The value to write, or None for a delete/tombstone.
    pub value: Option<Value>, // None means tombstone/delete
}

/// A precondition guard evaluated against the MVCC state before applying a guarded batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadGuard {
    /// Guard based on an expected specific version (commit timestamp).
    ExpectedVersion {
        /// The key to check.
        key: Key,
        /// The read timestamp at which to evaluate the guard.
        read_ts: Timestamp,
        /// The exact `commit_ts` expected, or None if expecting absence.
        expected_commit_ts: Option<Timestamp>, // None means expected absent
    },
    /// Guard based on an expected logical value.
    ExpectedValue {
        /// The key to check.
        key: Key,
        /// The read timestamp at which to evaluate the guard.
        read_ts: Timestamp,
        /// The exact value expected, or None if expecting logical absence.
        expected_value: Option<Value>, // None means the visible logical value is absent
    },
}
