use crate::backend::Backend;
use crate::error::{
    AbortError, BatchAbortError, BatchCommitError, BatchError, BatchPrewriteError, CommitError,
    GcError, PrewriteError, ReadError,
};
use crate::types::{
    CommittedVersion, Intent, Mutation, PhysicalWrite, ReadGuard, Timestamp, TxnId,
};

/// Statistics produced by garbage collection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcStats {
    /// The number of obsolete versions physically removed.
    pub versions_removed: usize,
    /// The number of intents safely ignored and preserved.
    pub intents_preserved: usize,
}

/// Budget for incremental garbage collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcBudget {
    /// Maximum number of keys to process in one step.
    pub max_keys: usize,
    /// Maximum number of historical versions to remove in one step.
    pub max_versions: usize,
}

/// Options for garbage collection operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcOptions {
    /// Budget parameters limiting the scope of one step.
    pub budget: GcBudget,
    /// Explicit opt-in retention policy: if true, a final unshadowed tombstone
    /// will be physically collapsed. This is ONLY safe if the caller guarantees
    /// a strict low-watermark where no future reads, prewrites, or guards will
    /// ever be issued at or below the collapsed tombstone timestamp.
    pub collapse_final_tombstones: bool,
}

/// Cursor to resume incremental garbage collection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalGcCursor {
    /// The next key to evaluate.
    pub next_key: Option<Vec<u8>>,
}

/// Result of an incremental garbage collection step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalGcResult {
    /// The cursor to resume GC from.
    pub cursor: IncrementalGcCursor,
    /// True if a full pass over the keyspace has completed.
    pub done: bool,
    /// Number of keys scanned in this step.
    pub keys_scanned: usize,
    /// Number of versions scanned in this step.
    pub versions_scanned: usize,
    /// Number of versions physically removed in this step.
    pub versions_removed: usize,
    /// Number of active intents safely ignored and preserved.
    pub intents_preserved: usize,
}

/// The central state machine for executing MVCC operations.
///
/// `MvccEngine` wraps a durable `Backend` and provides high-level APIs for
/// single-key and multi-key reads, transactional intents, and direct batches.
pub struct MvccEngine<B: Backend> {
    backend: B,
}

impl<B: Backend> MvccEngine<B> {
    /// Creates a new `MvccEngine` with the given backend.
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    /// Returns an immutable reference to the underlying backend.
    pub fn backend(&self) -> &B {
        &self.backend
    }

    /// Returns a mutable reference to the underlying backend.
    pub fn backend_mut(&mut self) -> &mut B {
        &mut self.backend
    }

    /// Reads the visible value for a key at a given logical `read_ts`.
    ///
    /// This method ignores uncommitted intents and finds the newest committed
    /// version that has a `commit_ts` <= `read_ts`.
    pub fn read(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Vec<u8>>, ReadError> {
        let version = self
            .backend
            .get_visible_committed(key, read_ts)
            .map_err(ReadError::Backend)?;
        Ok(version.and_then(|v| v.value))
    }

    /// Reads the visible value for a key at `read_ts` and returns its commit timestamp and value.
    ///
    /// If no version is visible at or before `read_ts`, returns `Ok(None)`.
    /// If a tombstone is visible, returns `Ok(Some((ts, None)))`.
    /// If a value is visible, returns `Ok(Some((ts, Some(value))))`.
    #[allow(clippy::type_complexity)]
    pub fn read_with_version(
        &self,
        key: &[u8],
        read_ts: Timestamp,
    ) -> Result<Option<(Timestamp, Option<Vec<u8>>)>, ReadError> {
        let version = self
            .backend
            .get_visible_committed(key, read_ts)
            .map_err(ReadError::Backend)?;
        Ok(version.map(|v| (v.commit_ts, v.value)))
    }

    /// Reads the visible value for a key, prioritizing the transaction's own active intent.
    ///
    /// If the transaction has an active intent on the key, its value is returned.
    /// Otherwise, it falls back to a normal historical read at `read_ts`.
    pub fn read_own_write(
        &self,
        key: &[u8],
        txn_id: TxnId,
        start_ts: Timestamp,
        read_ts: Timestamp,
    ) -> Result<Option<Vec<u8>>, ReadError> {
        // First, check if this txn has an intent on the key.
        if let Some(intent) = self.backend.get_intent(key).map_err(ReadError::Backend)?
            && intent.txn_id == txn_id
            && intent.start_ts == start_ts
        {
            return Ok(intent.mutation.value());
        }
        // Fall back to committed versions.
        self.read(key, read_ts)
    }

    /// Prewrites a single intent for a distributed transaction.
    ///
    /// Fails if another transaction holds an intent on the key, or if a newer
    /// committed version exists (write conflict). It is idempotent for the same transaction
    /// and `start_ts`.
    pub fn prewrite(
        &mut self,
        txn_id: TxnId,
        start_ts: Timestamp,
        key: Vec<u8>,
        mutation: Mutation,
    ) -> Result<(), PrewriteError> {
        // Check for an existing intent by another txn.
        if let Some(intent) = self
            .backend
            .get_intent(&key)
            .map_err(PrewriteError::Backend)?
        {
            if intent.txn_id != txn_id {
                return Err(PrewriteError::KeyLocked {
                    txn_id: intent.txn_id,
                });
            }
            // Same txn: if start_ts matches, this is a re-prewrite (idempotent).
            // If start_ts differs, it's a conflicting intent from the same txn
            // (should not happen in correct usage).
            if intent.start_ts != start_ts {
                return Err(PrewriteError::IntentAlreadyExists);
            }
            // Same txn, same start_ts: already prewritten. Verify mutation matches.
            if intent.mutation != mutation {
                return Err(PrewriteError::IntentAlreadyExists);
            }
            return Ok(());
        }

        // Check for write conflict: any committed version with commit_ts > start_ts.
        if let Some(latest_ts) = self
            .backend
            .get_latest_commit_ts(&key)
            .map_err(PrewriteError::Backend)?
            && latest_ts > start_ts
        {
            return Err(PrewriteError::WriteConflict);
        }

        let intent = Intent {
            key: key.clone(),
            txn_id,
            start_ts,
            mutation,
            min_commit_ts: None,
        };
        self.backend
            .put_intent(intent)
            .map_err(PrewriteError::Backend)?;
        Ok(())
    }

    /// Commits a single intent, creating a durable version.
    ///
    /// Converts the intent at `start_ts` into a committed version at `commit_ts`.
    /// The backend must execute this atomically (create version and remove intent).
    pub fn commit(
        &mut self,
        txn_id: TxnId,
        key: &[u8],
        start_ts: Timestamp,
        commit_ts: Timestamp,
    ) -> Result<(), CommitError> {
        let intent = self
            .backend
            .get_intent(key)
            .map_err(CommitError::Backend)?
            .ok_or(CommitError::IntentNotFound)?;

        if intent.txn_id != txn_id {
            return Err(CommitError::TxnIdMismatch);
        }
        if intent.start_ts != start_ts {
            return Err(CommitError::StartTsMismatch);
        }

        if commit_ts <= start_ts {
            return Err(CommitError::InvalidCommitTimestamp {
                start_ts,
                commit_ts,
            });
        }

        if let Some(min_ts) = intent.min_commit_ts
            && commit_ts < min_ts
        {
            return Err(CommitError::CommitTsTooEarly {
                commit_ts,
                min_commit_ts: min_ts,
            });
        }

        // Check for retroactive commit
        if let Some(latest_ts) = self
            .backend
            .get_latest_commit_ts(key)
            .map_err(CommitError::Backend)?
            && commit_ts <= latest_ts
        {
            if commit_ts == latest_ts {
                return Err(CommitError::DuplicateCommitTimestamp { commit_ts });
            } else {
                return Err(CommitError::CommitTsTooOld {
                    commit_ts,
                    latest_commit_ts: latest_ts,
                });
            }
        }

        // Create committed version and remove the intent in one backend transition.
        let version = CommittedVersion {
            key: key.to_vec(),
            commit_ts,
            value: intent.mutation.value(),
        };
        self.backend
            .commit_intents_batch(vec![version], vec![(key.to_vec(), txn_id, start_ts)])
            .map_err(CommitError::Backend)?;
        Ok(())
    }

    /// Aborts a single intent, removing it from the backend.
    ///
    /// This method is idempotent: if the intent is not found, it returns `Ok(())`.
    pub fn abort(
        &mut self,
        txn_id: TxnId,
        key: &[u8],
        start_ts: Timestamp,
    ) -> Result<(), AbortError> {
        let removed = self
            .backend
            .remove_intent(key, txn_id, start_ts)
            .map_err(AbortError::Backend)?;
        // If the intent was not found or did not match, it's a no-op.
        // This makes abort idempotent.
        let _ = removed;
        Ok(())
    }

    /// Prewrites multiple intents atomically for a transaction.
    ///
    /// Rejects empty batches with `EmptyBatch`. Identical replay semantics apply
    /// as in single-key prewrite.
    pub fn prewrite_batch(
        &mut self,
        txn_id: TxnId,
        start_ts: Timestamp,
        writes: Vec<PhysicalWrite>,
    ) -> Result<(), BatchPrewriteError> {
        if writes.is_empty() {
            return Err(BatchPrewriteError::EmptyBatch);
        }

        let mut key_set = std::collections::HashSet::new();
        for w in &writes {
            if !key_set.insert(w.key.clone()) {
                return Err(BatchPrewriteError::DuplicateKeyInBatch { key: w.key.clone() });
            }
        }

        let mut existing_count = 0;
        for w in &writes {
            if let Some(intent) = self
                .backend
                .get_intent(&w.key)
                .map_err(BatchPrewriteError::Backend)?
            {
                if intent.txn_id != txn_id {
                    return Err(BatchPrewriteError::KeyLocked {
                        key: w.key.clone(),
                        txn_id: intent.txn_id,
                    });
                }
                let expected_mutation = if let Some(v) = &w.value {
                    Mutation::Put(v.clone())
                } else {
                    Mutation::Delete
                };
                if intent.start_ts != start_ts || intent.mutation != expected_mutation {
                    return Err(BatchPrewriteError::IntentAlreadyExists { key: w.key.clone() });
                }
                existing_count += 1;
            } else if let Some(latest_ts) = self
                .backend
                .get_latest_commit_ts(&w.key)
                .map_err(BatchPrewriteError::Backend)?
                && latest_ts > start_ts
            {
                return Err(BatchPrewriteError::WriteConflict { key: w.key.clone() });
            }
        }

        if existing_count == writes.len() {
            return Ok(());
        } else if existing_count > 0 {
            return Err(BatchPrewriteError::PartialBatchReplay);
        }

        let mut intents = Vec::with_capacity(writes.len());
        for w in writes {
            intents.push(Intent {
                key: w.key,
                txn_id,
                start_ts,
                mutation: if let Some(v) = w.value {
                    Mutation::Put(v)
                } else {
                    Mutation::Delete
                },
                min_commit_ts: None,
            });
        }
        self.backend
            .put_intents_batch(intents)
            .map_err(BatchPrewriteError::Backend)?;
        Ok(())
    }

    /// Commits multiple intents atomically, creating durable versions.
    ///
    /// Rejects empty batches with `EmptyBatch`.
    pub fn commit_batch(
        &mut self,
        txn_id: TxnId,
        start_ts: Timestamp,
        commit_ts: Timestamp,
        keys: Vec<Vec<u8>>,
    ) -> Result<(), BatchCommitError> {
        if keys.is_empty() {
            return Err(BatchCommitError::EmptyBatch);
        }

        let mut key_set = std::collections::HashSet::new();
        for key in &keys {
            if !key_set.insert(key.clone()) {
                return Err(BatchCommitError::DuplicateKeyInBatch { key: key.clone() });
            }
        }

        if commit_ts <= start_ts {
            return Err(BatchCommitError::InvalidCommitTimestamp {
                start_ts,
                commit_ts,
            });
        }

        let mut commits = Vec::with_capacity(keys.len());
        let mut removed_intents = Vec::with_capacity(keys.len());

        for key in &keys {
            let intent = self
                .backend
                .get_intent(key)
                .map_err(BatchCommitError::Backend)?
                .ok_or_else(|| BatchCommitError::IntentNotFound { key: key.clone() })?;

            if intent.txn_id != txn_id {
                return Err(BatchCommitError::TxnIdMismatch { key: key.clone() });
            }
            if intent.start_ts != start_ts {
                return Err(BatchCommitError::StartTsMismatch { key: key.clone() });
            }
            if let Some(min_ts) = intent.min_commit_ts
                && commit_ts < min_ts
            {
                return Err(BatchCommitError::CommitTsTooEarly {
                    key: key.clone(),
                    commit_ts,
                    min_commit_ts: min_ts,
                });
            }

            if let Some(latest_ts) = self
                .backend
                .get_latest_commit_ts(key)
                .map_err(BatchCommitError::Backend)?
                && commit_ts <= latest_ts
            {
                return Err(BatchCommitError::CommitTsTooOld {
                    key: key.clone(),
                    commit_ts,
                    latest_commit_ts: latest_ts,
                });
            }

            commits.push(CommittedVersion {
                key: key.clone(),
                commit_ts,
                value: intent.mutation.value(),
            });
            removed_intents.push((key.clone(), txn_id, start_ts));
        }

        self.backend
            .commit_intents_batch(commits, removed_intents)
            .map_err(BatchCommitError::Backend)?;
        Ok(())
    }

    /// Aborts multiple intents, removing them atomically.
    ///
    /// If the key list is empty, it returns `Ok(())` (idempotent).
    pub fn abort_batch(
        &mut self,
        txn_id: TxnId,
        start_ts: Timestamp,
        keys: Vec<Vec<u8>>,
    ) -> Result<(), BatchAbortError> {
        if keys.is_empty() {
            return Ok(());
        }

        let mut key_set = std::collections::HashSet::new();
        for key in &keys {
            if !key_set.insert(key.clone()) {
                return Err(BatchAbortError::DuplicateKeyInBatch { key: key.clone() });
            }
        }

        let mut removed_intents = Vec::with_capacity(keys.len());
        for key in &keys {
            if let Some(intent) = self
                .backend
                .get_intent(key)
                .map_err(BatchAbortError::Backend)?
                && intent.txn_id == txn_id
                && intent.start_ts == start_ts
            {
                removed_intents.push((key.clone(), txn_id, start_ts));
            }
        }

        self.backend
            .remove_intents_batch(removed_intents)
            .map_err(BatchAbortError::Backend)?;
        Ok(())
    }

    /// Applies a batch of writes directly, bypassing intents.
    ///
    /// Rejects empty batches with `EmptyBatch`. Fails if any key has an active intent
    /// or if `commit_ts` is not strictly greater than the latest committed version.
    pub fn apply_direct_batch(
        &mut self,
        commit_ts: Timestamp,
        writes: Vec<PhysicalWrite>,
    ) -> Result<(), BatchError> {
        if writes.is_empty() {
            return Err(BatchError::EmptyBatch);
        }

        let mut key_set = std::collections::HashSet::new();
        for w in &writes {
            if !key_set.insert(w.key.clone()) {
                return Err(BatchError::DuplicateKeyInBatch { key: w.key.clone() });
            }
        }

        // Validate all writes
        for w in &writes {
            // Reject active intents
            if let Some(intent) = self
                .backend
                .get_intent(&w.key)
                .map_err(BatchError::Backend)?
            {
                return Err(BatchError::KeyLocked {
                    key: w.key.clone(),
                    txn_id: intent.txn_id,
                });
            }

            // Reject commit_ts <= latest_commit_ts
            if let Some(latest_ts) = self
                .backend
                .get_latest_commit_ts(&w.key)
                .map_err(BatchError::Backend)?
                && commit_ts <= latest_ts
            {
                return Err(BatchError::CommitTsTooOld {
                    key: w.key.clone(),
                    commit_ts,
                    latest_commit_ts: latest_ts,
                });
            }
        }

        // Apply all-or-nothing
        let mut commits = Vec::with_capacity(writes.len());
        for w in writes {
            commits.push(CommittedVersion {
                key: w.key,
                commit_ts,
                value: w.value,
            });
        }

        self.backend
            .put_committed_batch(commits)
            .map_err(BatchError::Backend)?;
        Ok(())
    }

    /// Applies a batch of writes conditionally, validating read guards first.
    ///
    /// Useful for Read-Modify-Write operations (e.g. Compare-And-Swap) without
    /// interactive two-phase commit transactions. Rejects empty writes or empty guards.
    pub fn apply_guarded_batch(
        &mut self,
        commit_ts: Timestamp,
        guards: Vec<ReadGuard>,
        writes: Vec<PhysicalWrite>,
    ) -> Result<(), BatchError> {
        if writes.is_empty() {
            return Err(BatchError::EmptyBatch);
        }
        if guards.is_empty() {
            return Err(BatchError::NoReadGuards);
        }

        let mut write_keys = std::collections::HashSet::new();
        for w in &writes {
            if !write_keys.insert(w.key.clone()) {
                return Err(BatchError::DuplicateKeyInBatch { key: w.key.clone() });
            }
        }

        // Validate all guards
        for guard in &guards {
            let (guard_key, guard_read_ts) = match guard {
                ReadGuard::ExpectedVersion { key, read_ts, .. } => (key, read_ts),
                ReadGuard::ExpectedValue { key, read_ts, .. } => (key, read_ts),
            };

            if commit_ts <= *guard_read_ts {
                return Err(BatchError::InvalidCommitTimestamp {
                    read_ts: *guard_read_ts,
                    commit_ts,
                });
            }

            // Check active intents
            if let Some(intent) = self
                .backend
                .get_intent(guard_key)
                .map_err(BatchError::Backend)?
            {
                return Err(BatchError::KeyLocked {
                    key: guard_key.clone(),
                    txn_id: intent.txn_id,
                });
            }

            if let Some(latest_ts) = self
                .backend
                .get_latest_commit_ts(guard_key)
                .map_err(BatchError::Backend)?
                && latest_ts > *guard_read_ts
            {
                return Err(BatchError::GuardFailedNewerVersion {
                    key: guard_key.clone(),
                    read_ts: *guard_read_ts,
                    actual_commit_ts: latest_ts,
                });
            }

            let visible_version = self
                .backend
                .get_visible_committed(guard_key, *guard_read_ts)
                .map_err(BatchError::Backend)?;

            match guard {
                ReadGuard::ExpectedVersion {
                    expected_commit_ts, ..
                } => {
                    let actual_commit_ts = visible_version.as_ref().map(|v| v.commit_ts);
                    if actual_commit_ts != *expected_commit_ts {
                        return Err(BatchError::GuardFailedVersionMismatch {
                            key: guard_key.clone(),
                            expected: *expected_commit_ts,
                            actual: actual_commit_ts,
                        });
                    }
                }
                ReadGuard::ExpectedValue { expected_value, .. } => {
                    let actual_value = visible_version.as_ref().and_then(|v| v.value.as_ref());
                    if actual_value != expected_value.as_ref() {
                        return Err(BatchError::GuardFailedValueMismatch {
                            key: guard_key.clone(),
                        });
                    }
                }
            }
        }

        // Validate write keys against intents and duplicate versions
        for w in &writes {
            if let Some(intent) = self
                .backend
                .get_intent(&w.key)
                .map_err(BatchError::Backend)?
            {
                return Err(BatchError::KeyLocked {
                    key: w.key.clone(),
                    txn_id: intent.txn_id,
                });
            }

            if let Some(latest_ts) = self
                .backend
                .get_latest_commit_ts(&w.key)
                .map_err(BatchError::Backend)?
                && commit_ts <= latest_ts
            {
                return Err(BatchError::CommitTsTooOld {
                    key: w.key.clone(),
                    commit_ts,
                    latest_commit_ts: latest_ts,
                });
            }
        }

        // Apply all-or-nothing
        let mut commits = Vec::with_capacity(writes.len());
        for w in writes {
            commits.push(CommittedVersion {
                key: w.key,
                commit_ts,
                value: w.value,
            });
        }

        self.backend
            .put_committed_batch(commits)
            .map_err(BatchError::Backend)?;
        Ok(())
    }

    /// Performs an incremental step of garbage collection.
    ///
    /// Obsolete versions older than `safe_point_ts` are removed up to the `budget`.
    pub fn gc_incremental(
        &mut self,
        safe_point_ts: Timestamp,
        cursor: Option<IncrementalGcCursor>,
        options: GcOptions,
    ) -> Result<IncrementalGcResult, GcError> {
        if options.budget.max_keys == 0 || options.budget.max_versions == 0 {
            return Err(GcError::InvalidGcBudget);
        }

        let start_key = cursor.and_then(|c| c.next_key);

        let keys = self
            .backend
            .keys_from(start_key.as_deref(), options.budget.max_keys + 1)
            .map_err(GcError::Backend)?;

        let mut keys_scanned = 0;
        let mut versions_scanned = 0;
        let mut versions_removed = 0;
        let mut intents_preserved = 0;

        let mut next_cursor_key = None;
        let mut done = false;
        let mut exhausted_versions = false;

        let num_keys_to_process = std::cmp::min(keys.len(), options.budget.max_keys);

        for key in keys.iter().take(num_keys_to_process) {
            keys_scanned += 1;

            let has_intent = self
                .backend
                .get_intent(key)
                .map_err(GcError::Backend)?
                .is_some();
            if has_intent {
                intents_preserved += 1;
            }

            let keeper = self
                .backend
                .get_visible_committed(key, safe_point_ts)
                .map_err(GcError::Backend)?;

            if let Some(keeper_ver) = keeper {
                versions_scanned += 1; // Count the keeper lookup

                let limit = options.budget.max_versions - versions_removed;
                if limit == 0 {
                    // We check if there are actually any versions to remove before breaking
                    let check_more = self
                        .backend
                        .get_committed_timestamps_before(key, keeper_ver.commit_ts, 1)
                        .map_err(GcError::Backend)?;

                    if !check_more.is_empty() {
                        next_cursor_key = Some(key.clone());
                        exhausted_versions = true;
                        break;
                    }

                    // Out of budget, but maybe it's a final tombstone?
                    // We need at least 1 budget unit to remove it, so we must revisit next time.
                    if options.collapse_final_tombstones
                        && keeper_ver.value.is_none()
                        && !has_intent
                        && let Some(latest_ts) = self
                            .backend
                            .get_latest_commit_ts(key)
                            .map_err(GcError::Backend)?
                        && latest_ts == keeper_ver.commit_ts
                    {
                        next_cursor_key = Some(key.clone());
                        exhausted_versions = true;
                        break;
                    }
                    continue;
                }

                // Check if this is a final tombstone collapse case
                let mut is_final_tombstone = false;
                if options.collapse_final_tombstones
                    && keeper_ver.value.is_none()
                    && !has_intent
                    && let Some(latest_ts) = self
                        .backend
                        .get_latest_commit_ts(key)
                        .map_err(GcError::Backend)?
                    && latest_ts == keeper_ver.commit_ts
                {
                    is_final_tombstone = true;
                }

                if is_final_tombstone {
                    // Final tombstone case: we must remove the tombstone and all older versions atomically.
                    // We query up to limit + 1 older versions to see if they all fit in the remaining budget.
                    let mut older_versions = self
                        .backend
                        .get_committed_timestamps_before(key, keeper_ver.commit_ts, limit + 1)
                        .map_err(GcError::Backend)?;

                    versions_scanned += older_versions.len();

                    let has_more = older_versions.len() > limit;
                    if has_more {
                        // There are more older versions than the remaining budget.
                        // Pop the extra to only remove `limit` older versions.
                        older_versions.pop();
                        versions_scanned -= 1;

                        // We cannot delete the tombstone yet, because the remaining older versions
                        // exceed the budget. We only delete the older versions.
                        for ts in older_versions {
                            self.backend
                                .remove_committed_version(key, ts)
                                .map_err(GcError::Backend)?;
                            versions_removed += 1;
                        }

                        next_cursor_key = Some(key.clone());
                        exhausted_versions = true;
                        break;
                    } else {
                        // older_versions.len() <= limit.
                        // The total versions to remove (older versions + tombstone) is older_versions.len() + 1.
                        let older_len = older_versions.len();
                        if older_len < limit {
                            // Fits in the remaining budget! Collapse them atomically.
                            self.backend
                                .collapse_tombstone(key, keeper_ver.commit_ts, older_versions)
                                .map_err(GcError::Backend)?;

                            versions_removed += older_len + 1;
                        } else {
                            // older_versions.len() == limit.
                            // The total versions to remove (older_versions.len() + 1) exceeds the remaining budget (limit).
                            // We can only remove the older versions in this pass, leaving the tombstone.
                            for ts in older_versions {
                                self.backend
                                    .remove_committed_version(key, ts)
                                    .map_err(GcError::Backend)?;
                                versions_removed += 1;
                            }

                            next_cursor_key = Some(key.clone());
                            exhausted_versions = true;
                            break;
                        }
                    }
                } else {
                    // Normal GC case (not a final tombstone): we only remove older versions.
                    let mut to_remove = self
                        .backend
                        .get_committed_timestamps_before(key, keeper_ver.commit_ts, limit + 1)
                        .map_err(GcError::Backend)?;

                    versions_scanned += to_remove.len();

                    let has_more = to_remove.len() > limit;
                    if has_more {
                        to_remove.pop();
                        versions_scanned -= 1;
                    }

                    for ts in to_remove {
                        self.backend
                            .remove_committed_version(key, ts)
                            .map_err(GcError::Backend)?;
                        versions_removed += 1;
                    }

                    if has_more {
                        next_cursor_key = Some(key.clone());
                        exhausted_versions = true;
                        break;
                    }
                }
            }
        }

        if !exhausted_versions {
            if keys.len() > options.budget.max_keys {
                next_cursor_key = Some(keys[options.budget.max_keys].clone());
            } else {
                done = true;
            }
        }

        Ok(IncrementalGcResult {
            cursor: IncrementalGcCursor {
                next_key: next_cursor_key,
            },
            done,
            keys_scanned,
            versions_scanned,
            versions_removed,
            intents_preserved,
        })
    }

    /// Performs a full garbage collection by repeatedly calling `gc_incremental`.
    ///
    /// This is maintained for compatibility.
    //
    // When removing the following method, you have to transform the following tests
    //
    // Test                        Lines
    // ----                        -----
    // tests/integration_tests.rs  97, 140, 514, 552, 606, 729, 1041, 1083
    // tests/gc_tests.rs           343, 378
    // tests/support/model.rs      50, 93
    // tests/property_tests.rs     168
    // benches/core_bench.rs       457
    #[deprecated(
        note = "unbounded: loops gc_incremental to completion and materializes the whole \
                keyspace via all_keys(); production must use budgeted gc_incremental \
                (see MVCC_GC_FIRST_CLASS_DESIGN.md §5.4)"
    )]
    pub fn gc(&mut self, safe_point_ts: Timestamp, options: GcOptions) -> Result<GcStats, GcError> {
        let mut total_versions_removed = 0;

        let mut cursor = None;

        loop {
            let res = self.gc_incremental(safe_point_ts, cursor.take(), options)?;
            total_versions_removed += res.versions_removed;

            if res.done {
                break;
            }
            cursor = Some(res.cursor);
        }

        let mut total_intents_preserved = 0;
        for key in self.backend.all_keys().map_err(GcError::Backend)? {
            if self
                .backend
                .get_intent(&key)
                .map_err(GcError::Backend)?
                .is_some()
            {
                total_intents_preserved += 1;
            }
        }

        Ok(GcStats {
            versions_removed: total_versions_removed,
            intents_preserved: total_intents_preserved,
        })
    }
}
