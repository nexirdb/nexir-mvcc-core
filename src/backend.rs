use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::types::{CommittedVersion, Intent, Key, Timestamp, TxnId, Value};

/// The storage backend contract.
/// - `get_committed_versions` must return versions for the key sorted by ascending `commit_ts`.
/// - A durable backend must make commit atomic with respect to committed-version creation and intent removal.
/// - `put_committed_batch` must be strictly all-or-nothing for durable backends.
pub trait Backend {
    /// Returns all committed versions for a key, ordered ascending by `commit_ts`.
    fn get_committed_versions(&self, key: &[u8]) -> Result<Vec<CommittedVersion>, String>;
    /// Returns the most recent committed version for a key, if any.
    fn get_latest_committed(&self, key: &[u8]) -> Result<Option<CommittedVersion>, String>;
    /// Returns the most recent committed version for a key that is visible at or before `read_ts`.
    fn get_visible_committed(
        &self,
        key: &[u8],
        read_ts: Timestamp,
    ) -> Result<Option<CommittedVersion>, String>;
    /// Returns the timestamp of the most recent committed version for a key, if any.
    fn get_latest_commit_ts(&self, key: &[u8]) -> Result<Option<Timestamp>, String>;
    /// Fetches the active intent for a given key, if any exists.
    fn get_intent(&self, key: &[u8]) -> Result<Option<Intent>, String>;
    /// Writes a single intent to the backend.
    fn put_intent(&mut self, intent: Intent) -> Result<(), String>;
    /// Removes an intent from the backend if it matches the given `txn_id` and `start_ts`.
    /// Returns `true` if removed, `false` otherwise.
    fn remove_intent(
        &mut self,
        key: &[u8],
        txn_id: TxnId,
        start_ts: Timestamp,
    ) -> Result<bool, String>;
    /// Writes a single committed version to the backend.
    fn put_committed(&mut self, version: CommittedVersion) -> Result<(), String>;
    /// Writes multiple committed versions atomically. Must be all-or-nothing.
    fn put_committed_batch(&mut self, versions: Vec<CommittedVersion>) -> Result<(), String>;
    /// Removes a specific committed version during garbage collection.
    fn remove_committed_version(&mut self, key: &[u8], commit_ts: Timestamp) -> Result<(), String>;
    /// Returns a deduplicated, sorted list of all keys currently managed by the backend.
    fn all_keys(&self) -> Result<Vec<Key>, String>;
    /// Writes multiple intents atomically. Must be all-or-nothing.
    fn put_intents_batch(&mut self, intents: Vec<Intent>) -> Result<(), String>;
    /// Converts multiple intents into committed versions atomically.
    /// Must create the versions and remove the intents in a single durable transaction.
    fn commit_intents_batch(
        &mut self,
        commits: Vec<CommittedVersion>,
        removed_intents: Vec<(Key, TxnId, Timestamp)>,
    ) -> Result<(), String>;
    /// Removes multiple intents atomically. Must be all-or-nothing.
    fn remove_intents_batch(&mut self, intents: Vec<(Key, TxnId, Timestamp)>)
    -> Result<(), String>;
    /// Returns up to `limit` keys strictly ordered, starting from `start` (inclusive if provided).
    fn keys_from(&self, start: Option<&[u8]>, limit: usize) -> Result<Vec<Key>, String>;
    /// Returns up to `limit` keys starting with `prefix`, strictly ordered, starting from `start` if provided.
    /// Excludes intents and returns committed keys only.
    fn keys_from_prefix(
        &self,
        prefix: &[u8],
        start: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Key>, String>;
    /// Returns the `limit` newest commit timestamps strictly before `before_ts` for the given key.
    /// Ordered descending (newest first).
    fn get_committed_timestamps_before(
        &self,
        key: &[u8],
        before_ts: Timestamp,
        limit: usize,
    ) -> Result<Vec<Timestamp>, String>;
    /// Atomically removes a tombstone version and every supplied older version.
    /// Callers must pass the complete older version set for a final tombstone collapse.
    fn collapse_tombstone(
        &mut self,
        key: &[u8],
        tombstone_ts: Timestamp,
        older_ts: Vec<Timestamp>,
    ) -> Result<(), String>;
}

/// A simple, non-durable in-memory implementation of the `Backend` trait.
/// Intended for testing, examples, and rapid prototyping.
#[derive(Debug, Clone, Default)]
pub struct InMemoryBackend {
    committed: BTreeMap<(Key, Timestamp), Option<Value>>,
    intents: HashMap<Key, Intent>,
    all_keys_set: BTreeSet<Key>,
}

impl InMemoryBackend {
    /// Creates a new, empty in-memory backend.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Backend for InMemoryBackend {
    fn get_committed_versions(&self, key: &[u8]) -> Result<Vec<CommittedVersion>, String> {
        let mut result = Vec::new();
        let start = (key.to_vec(), Timestamp(0));
        for ((k, ts), value) in self.committed.range(start..) {
            if k.as_slice() != key {
                break;
            }
            result.push(CommittedVersion {
                key: k.clone(),
                commit_ts: *ts,
                value: value.clone(),
            });
        }
        Ok(result)
    }

    fn get_latest_committed(&self, key: &[u8]) -> Result<Option<CommittedVersion>, String> {
        let range = (key.to_vec(), Timestamp(0))..=(key.to_vec(), Timestamp(u64::MAX));
        if let Some(((k, ts), value)) = self.committed.range(range).next_back()
            && k.as_slice() == key
        {
            return Ok(Some(CommittedVersion {
                key: k.clone(),
                commit_ts: *ts,
                value: value.clone(),
            }));
        }
        Ok(None)
    }

    fn get_visible_committed(
        &self,
        key: &[u8],
        read_ts: Timestamp,
    ) -> Result<Option<CommittedVersion>, String> {
        let range = (key.to_vec(), Timestamp(0))..=(key.to_vec(), read_ts);
        if let Some(((k, ts), value)) = self.committed.range(range).next_back()
            && k.as_slice() == key
        {
            return Ok(Some(CommittedVersion {
                key: k.clone(),
                commit_ts: *ts,
                value: value.clone(),
            }));
        }
        Ok(None)
    }

    fn get_latest_commit_ts(&self, key: &[u8]) -> Result<Option<Timestamp>, String> {
        let range = (key.to_vec(), Timestamp(0))..=(key.to_vec(), Timestamp(u64::MAX));
        if let Some(((k, ts), _)) = self.committed.range(range).next_back()
            && k.as_slice() == key
        {
            return Ok(Some(*ts));
        }
        Ok(None)
    }

    fn get_intent(&self, key: &[u8]) -> Result<Option<Intent>, String> {
        Ok(self.intents.get(key).cloned())
    }

    fn put_intent(&mut self, intent: Intent) -> Result<(), String> {
        self.all_keys_set.insert(intent.key.clone());
        self.intents.insert(intent.key.clone(), intent);
        Ok(())
    }

    fn remove_intent(
        &mut self,
        key: &[u8],
        txn_id: TxnId,
        start_ts: Timestamp,
    ) -> Result<bool, String> {
        if let Some(intent) = self.intents.get(key)
            && intent.txn_id == txn_id
            && intent.start_ts == start_ts
        {
            self.intents.remove(key);
            self.maybe_remove_from_keys(key);
            return Ok(true);
        }
        Ok(false)
    }

    fn put_committed(&mut self, version: CommittedVersion) -> Result<(), String> {
        self.all_keys_set.insert(version.key.clone());
        self.committed.insert(
            (version.key.clone(), version.commit_ts),
            version.value.clone(),
        );
        Ok(())
    }

    fn put_committed_batch(&mut self, versions: Vec<CommittedVersion>) -> Result<(), String> {
        for version in versions {
            self.all_keys_set.insert(version.key.clone());
            self.committed.insert(
                (version.key.clone(), version.commit_ts),
                version.value.clone(),
            );
        }
        Ok(())
    }

    fn remove_committed_version(&mut self, key: &[u8], commit_ts: Timestamp) -> Result<(), String> {
        self.committed.remove(&(key.to_vec(), commit_ts));
        self.maybe_remove_from_keys(key);
        Ok(())
    }

    fn all_keys(&self) -> Result<Vec<Key>, String> {
        Ok(self.all_keys_set.iter().cloned().collect())
    }

    fn put_intents_batch(&mut self, intents: Vec<Intent>) -> Result<(), String> {
        for intent in intents {
            self.all_keys_set.insert(intent.key.clone());
            self.intents.insert(intent.key.clone(), intent);
        }
        Ok(())
    }

    fn commit_intents_batch(
        &mut self,
        commits: Vec<CommittedVersion>,
        removed_intents: Vec<(Key, TxnId, Timestamp)>,
    ) -> Result<(), String> {
        for version in commits {
            self.all_keys_set.insert(version.key.clone());
            self.committed.insert(
                (version.key.clone(), version.commit_ts),
                version.value.clone(),
            );
        }
        for (key, txn_id, start_ts) in removed_intents {
            if let Some(intent) = self.intents.get(&key)
                && intent.txn_id == txn_id
                && intent.start_ts == start_ts
            {
                self.intents.remove(&key);
                self.maybe_remove_from_keys(&key);
            }
        }
        Ok(())
    }

    fn remove_intents_batch(
        &mut self,
        intents: Vec<(Key, TxnId, Timestamp)>,
    ) -> Result<(), String> {
        for (key, txn_id, start_ts) in intents {
            if let Some(intent) = self.intents.get(&key)
                && intent.txn_id == txn_id
                && intent.start_ts == start_ts
            {
                self.intents.remove(&key);
                self.maybe_remove_from_keys(&key);
            }
        }
        Ok(())
    }

    fn keys_from(&self, start: Option<&[u8]>, limit: usize) -> Result<Vec<Key>, String> {
        if let Some(s) = start {
            Ok(self
                .all_keys_set
                .range(s.to_vec()..)
                .take(limit)
                .cloned()
                .collect())
        } else {
            Ok(self.all_keys_set.iter().take(limit).cloned().collect())
        }
    }

    fn keys_from_prefix(
        &self,
        prefix: &[u8],
        start: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Key>, String> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        if prefix.is_empty() {
            return Err("Prefix cannot be empty".to_string());
        }
        if let Some(s) = start
            && !s.starts_with(prefix)
        {
            return Err("Start cursor must start with the prefix".to_string());
        }
        let scan_start = start.unwrap_or(prefix);
        let mut result = Vec::new();
        let range_start = (scan_start.to_vec(), Timestamp(0));
        for ((k, _), _) in self.committed.range(range_start..) {
            if !k.starts_with(prefix) {
                break;
            }
            if result.last() != Some(k) {
                result.push(k.clone());
                if result.len() == limit {
                    break;
                }
            }
        }
        Ok(result)
    }

    fn get_committed_timestamps_before(
        &self,
        key: &[u8],
        before_ts: Timestamp,
        limit: usize,
    ) -> Result<Vec<Timestamp>, String> {
        let range = (key.to_vec(), Timestamp(0))..(key.to_vec(), before_ts);
        let mut result = Vec::new();
        for ((k, ts), _) in self.committed.range(range).rev().take(limit) {
            if k.as_slice() == key {
                result.push(*ts);
            }
        }
        Ok(result)
    }

    fn collapse_tombstone(
        &mut self,
        key: &[u8],
        tombstone_ts: Timestamp,
        older_ts: Vec<Timestamp>,
    ) -> Result<(), String> {
        self.committed.remove(&(key.to_vec(), tombstone_ts));
        for ts in older_ts {
            self.committed.remove(&(key.to_vec(), ts));
        }
        self.maybe_remove_from_keys(key);
        Ok(())
    }
}

impl InMemoryBackend {
    fn maybe_remove_from_keys(&mut self, key: &[u8]) {
        if !self.intents.contains_key(key) {
            let range = (key.to_vec(), Timestamp(0))..=(key.to_vec(), Timestamp(u64::MAX));
            if self.committed.range(range).next().is_none() {
                self.all_keys_set.remove(key);
            }
        }
    }
}
