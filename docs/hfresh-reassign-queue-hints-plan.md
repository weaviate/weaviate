# HFresh Reassign Queue Hints Plan

## Goal

Make HFresh reassign recovery robust after crashes without adding high-volume
per-reassign LSM writes. The queue record should carry enough information to
recover useful reassign hints, while duplicate suppression remains best-effort
and in memory.

## Current Problem

Reassign queue records currently persist only the vector ID. The latest known
posting ID is stored in a separate in-memory dedup map and flushed as one large
blob only during normal shutdown. After a crash, queued reassign tasks can
survive without their posting hints. Those recovered tasks decode with posting
ID zero and bypass the `RNGSelect` skip hint, causing unnecessary reassign work,
version churn, and reassign cascades.

The dedup map also grows very large during imports because it stores one
`vecID -> postingID` entry per pending unique vector reassign.

## Design Direction

- Encode the posting hint directly in each reassign queue record.
- Keep duplicate suppression as a non-persisted in-memory structure keyed by
  vector ID.
- Use first-hint-wins semantics while a vector is already in flight.
- Treat hints as optimization state, not correctness state.
- Make every slice backward compatible with existing queue/shared-bucket data.

## Compatibility Rule

Every slice must be able to start from data produced by the previous code and
must either:

- read the old data format directly, or
- migrate it explicitly before depending on the new format.

For queue records specifically, new code must support pre-existing reassign
records that contain only `op + vecID`. Those legacy records should decode as
`{vecID, hintPostingID: 0}` rather than failing queue recovery.

Downgrades must also be considered. If a newer version writes tasks and the user
then starts an older version, the older decoder should still be able to process
or safely ignore those records. For reassign tasks, the new record must keep the
old byte prefix exactly:

```text
op + vecID + optional trailing bytes
```

The older decoder reads `op`, then reads the first eight bytes as `vecID`; it
does not validate exact record length. Appending `hintPostingID` after the old
payload should therefore be downgrade-readable by older code. On downgrade, the
hint is lost and old code falls back to its existing in-memory/shared-bucket
hint behavior, but queue recovery should not fail because of the record format.

## Slices

### Slice 1: Queue Record Format

Change reassign queue records to encode:

```text
op + vecID + hintPostingID
```

Keep analyze, split, and merge task record formats unchanged.

Compatibility and migration:

- Keep the old prefix byte-compatible: byte 0 is still `taskQueueReassignOp`,
  bytes 1-8 are still `vecID`.
- Append `hintPostingID` after the old payload. This lets older versions decode
  the vector ID and ignore the trailing hint bytes during downgrade.
- Decode legacy reassign records with length 9 as `vecID` plus hint `0`.
- Decode new reassign records with length 17 or greater as `vecID` plus
  persisted hint, ignoring any future trailing fields.
- Reject malformed reassign records shorter than the legacy 9-byte prefix.
- Do not rewrite existing queue chunks in place.

Tests:

- Encode/decode new reassign record with hint.
- Decode legacy reassign record.
- Reject malformed reassign record.
- Verify analyze/split/merge task decoding remains unchanged.

### Slice 2: In-Memory Reassign Dedup

Replace the persisted `reassignDeduplicator` with a best-effort in-memory
in-flight set keyed by vector ID.

Initial implementation can use a simple map/set if that is the smallest safe
step. A denser `GroupedPagedArray`-style structure can follow if the map still
shows up in heap profiles.

Compatibility and migration:

- Keep reading any existing shared-bucket reassign blob if needed only for
  startup compatibility, but do not require it for queue decoding.
- Prefer treating old blob data as obsolete once queue records are self-contained.
- If removing the old blob, leave old data harmless and ignored rather than
  requiring a destructive migration.

Tests:

- Duplicate enqueue for the same vector is suppressed while in flight.
- Enqueue after completion succeeds.
- Startup with an old shared-bucket reassign blob does not fail.

### Slice 3: Explicit Reassign Completion

Stop deferring `ReassignDone` at the top of `doReassign`. Clear the in-flight
marker only after the operation reaches a terminal state.

Terminal states include:

- vector is deleted,
- vector no longer exists,
- `RNGSelect` determines no reassign is needed,
- reassign completes successfully,
- version changed concurrently and the task is no longer current.

Compatibility and migration:

- No data migration.
- Existing queue records, including legacy records, must still clear in-flight
  state after terminal handling.

Tests:

- In-flight marker is cleared after skip.
- In-flight marker is cleared after successful reassign.
- In-flight marker is cleared after deleted vector.
- In-flight marker is not cleared prematurely when retry/requeue is required.

### Slice 4: Deleted Target During Reassign

Handle the case where a selected posting is deleted between `RNGSelect` and
`append`.

Preferred shape:

- Make `append` or `doReassign` surface a retry/reselect signal when a target
  posting disappeared.
- Retry RNG selection inside the same reassign task with a small bound, or
  clear in-flight before re-enqueueing the vector.

Compatibility and migration:

- No data migration.
- Behavior must remain safe for both legacy and new queue records.

Tests:

- Deleted append target does not swallow a needed retry.
- Retry is bounded.
- The vector remains eligible for future reassign if retry cannot complete.

### Slice 5: Metrics and Observability

Add lightweight visibility around hint usefulness and stale-hint behavior.

Candidate counters:

- reassign task decoded with legacy format,
- reassign task decoded with embedded hint,
- duplicate reassign enqueue suppressed,
- hint skipped reassign,
- hint posting missing or deleted when processed,
- reassign retry due to deleted target posting.

Compatibility and migration:

- Metrics are additive only.
- No persisted data changes.

Tests:

- Unit tests can assert counters only where existing metrics patterns make that
  straightforward. Otherwise this slice can rely on compile coverage.

### Slice 6: Cleanup

Remove or quarantine obsolete persisted reassign-dedup code after the new queue
format and in-memory dedup are proven in tests.

Persisted old-behavior data to handle:

- `reassignBucketKey = []byte{sharedBucketVersionV1, 4, 0}` in the HFresh
  shared bucket.
- The value is a single blob containing repeated 16-byte entries:
  `vectorID uint64` followed by `postingID uint64`, little-endian.
- This is the only persisted shared-bucket state found for the old reassign
  dedup/hint behavior. The durable task queue chunks themselves must not be
  deleted as part of this cleanup.

Compatibility and migration:

- Old shared-bucket reassign blob must not break startup.
- New code should delete `reassignBucketKey` once queue records are
  self-contained and the in-memory dedup no longer reads it.
- Keep the key reserved in code after deleting its data. Do not remove the
  constant/symbol in a way that makes the namespace look available.
- Rename or comment it as legacy/reserved, for example:

```go
// reassignBucketKey is a legacy shared-bucket key used by the old persisted
// reassign deduplicator. It is intentionally reserved forever so no future
// metadata can accidentally read stale reassign blobs as a different format.
// New code must not write to this key.
var reassignBucketKey = []byte{sharedBucketVersionV1, 4, 0}
```

- Deleting this key is safe for forward operation because the new queue records
  carry their own hints and the in-memory dedup is best-effort only.
- Downgrade caveat: deleting the old blob means older versions will decode any
  new-format queue records by vector ID only and will not recover posting hints
  from the shared bucket. This is accepted because the new task record remains
  byte-compatible and downgrade should not corrupt or fail queue processing; it
  may only lose the hint optimization.

Tests:

- Startup with legacy blob.
- Startup deletes or ignores legacy blob according to the chosen cleanup path.
- Startup with legacy queue records.
- Startup with mixed legacy/new queue records.

## Open Questions

- Should first-hint-wins be instrumented before considering duplicate records
  with newer hints?
- Should the in-memory set start as a map for simplicity, then move to a dense
  paged bitset after behavior is stable?
- Do we need a one-time cleanup of the old `reassignBucketKey`, or is harmless
  orphaned metadata acceptable?

## Changelog

### 2026-06-19

- Created branch `codex/hfresh-reassign-queue-hints` from `stable/v1.37`.
- Agreed on queue-embedded posting hints to avoid per-reassign LSM writes.
- Added this plan and changelog.
- Implemented and verified Slice 1:
  - New reassign task records keep the old `op + vecID` prefix and append
    `hintPostingID`.
  - Decoder accepts legacy 9-byte records, new 17-byte records, and future
    records with trailing fields.
  - Added focused task queue format tests.
  - Verified with `go test ./adapters/repos/db/vector/hfresh` and
    `go test ./adapters/repos/db/queue`.
- Implemented Slice 2:
  - Replaced the persisted reassign deduplicator with the existing in-memory
    vector-ID deduplicator.
  - Removed shutdown flushing of pending reassign hints to the shared bucket.
  - Kept `reassignBucketKey` reserved in code with a legacy warning comment.
  - Added a regression test that closing the task queue does not persist the
    legacy reassign dedup blob.

### 2026-06-22

- Implemented the agreed Slice 3 direction:
  - `append` no longer owns the missing-posting re-enqueue decision; it returns
    `false, nil` when the target posting disappeared.
  - `Add` now re-enqueues after `append` reports a missing posting.
  - `doReassign` uses a split-style deferred `ReassignDone`, but clears the
    in-flight marker before re-enqueueing a vector whose selected posting
    disappeared.
  - Added regression coverage that `append` does not enqueue reassign tasks by
    itself and that terminal reassign paths clear the in-memory dedup marker.
  - Verified with `go test ./adapters/repos/db/vector/hfresh` and
    `go test ./adapters/repos/db/queue`.
- Implemented the legacy shared-bucket cleanup:
  - Startup checks the old `reassignBucketKey` with `Bucket.Exists` so the
    legacy blob is not loaded into memory, then deletes it only when present.
  - The reserved key remains documented in code and is still not reused.
  - Added startup coverage that the legacy blob is removed while unrelated
    shared-bucket data is preserved.
  - Added explicit mixed legacy/new reassign queue record decode coverage.
  - Verified with `go test ./adapters/repos/db/vector/hfresh` and
    `go test ./adapters/repos/db/queue`.
