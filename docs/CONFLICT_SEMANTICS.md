# Conflict Semantics

## Prewrite

When a transaction attempts to `prewrite` a key, the engine checks in order:

1. **Intent check** — Is there an active intent on the key?
   - **Different txn**: return `PrewriteError::KeyLocked { txn_id }`
   - **Same txn, same start_ts, same mutation**: idempotent success
   - **Same txn, different start_ts or mutation**: `PrewriteError::IntentAlreadyExists`

2. **Write conflict check** — Is there a committed version with `commit_ts > start_ts`?
   - Yes: return `PrewriteError::WriteConflict`
   - No: proceed

3. **Create intent** — Store the intent in the backend.

## Commit

When a transaction attempts to `commit`:

1. **Find intent** — Must exist. If not: `CommitError::IntentNotFound`.
2. **Verify txn_id** — Must match. If not: `CommitError::TxnIdMismatch`.
3. **Verify start_ts** — Must match. If not: `CommitError::StartTsMismatch`.
4. **Verify commit_ts bounds** — Must be strictly greater than `start_ts`. If not: `CommitError::InvalidCommitTimestamp`. Also must be `>= min_commit_ts` if set. If not: `CommitError::CommitTsTooEarly`.
5. **Check latest committed timestamp** — `commit_ts` must be strictly greater than the latest committed version for this key.
   - If `commit_ts <= latest_commit_ts`: return a timestamp collision/retroactive commit error. In normal latest-version validation this is `CommitError::CommitTsTooOld`; exact duplicate detection is retained for defensive single-key collision reporting.
6. **Atomically commit intent** — Ask the backend to create the `CommittedVersion` at `commit_ts` and remove the matching intent in one durable transition.

## Abort

When a transaction attempts to `abort`:

1. **Find intent matching (key, txn_id, start_ts)**.
2. If found: remove it.
3. If not found or mismatch: **no-op** (idempotent).

Abort never removes committed versions.

## Read

When reading at `read_ts`:

1. Ask the backend for the visible committed version at `read_ts`.
2. Select the version with the highest `commit_ts` such that `commit_ts <= read_ts`.
3. Return `Some(value)` or `None` if tombstone or no version found.
4. Intents from other transactions are invisible.

## Read-Your-Own-Write

When a transaction wants to see its own uncommitted writes:

1. Check if this `(txn_id, start_ts)` has an intent on the key.
2. If yes, return the intent's mutation value.
3. If no, fall back to normal read semantics.

## Lost-Update Prevention

Consider:

```
initial: k = 10 at ts=1

txn A start_ts=10 reads k => 10
txn B start_ts=10 reads k => 10

txn A prewrite k=11 succeeds
txn B prewrite k=11
```

If `txn B` tries to prewrite after `txn A`, the engine will find `txn A`'s intent and return `KeyLocked`. If `txn A` commits first, `txn B` will see a committed version with `commit_ts=20 > start_ts=10` and return `WriteConflict`.

The invalid outcome — both commit `k=11` silently — is impossible.
