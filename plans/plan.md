# Deterministic Replica Movement via Change Capture Log

## Context

Replica movement today catches up writes that land on the source during HYDRATING via hashtree-based async replication with a fixed timing window (`asyncReplicationUpperTimeBound = now+5s` in FINALIZING; `asyncReplicationMinimumWait = 100s` in DEHYDRATING). This is probabilistic: writes that slip past these windows are caught only by the next ~30s hashbeat cycle, and the core correctness test `TestReplicaMovementTenantParallelWrites` is commented out as flaky (`TODO fix flake and uncomment`). RFC is at `plans/rfc.md`.

We replace this for **movements only** with a temporary, append-only change capture log per in-flight op. The source stamps every post-snapshot write with a monotonic LSN. The target tails the log over gRPC and replays in LSN order. Every write committed on the source is delivered to the target at least once — either via the file snapshot, via the log, or via the normal 2PC path after RAFT-add. Non-movement async replication (hashtree) is untouched, and this PR does NOT change Weaviate's underlying leaderless-2PC replication semantics — only the movement-catchup mechanism is refactored.

### Decisions already made (do not revisit)

- **Full replacement** of the hashtree/timing catchup inside `processFinalizingOp` / `processDehydratingOp`. No feature flag; no fallback path.
- **Abort-on-source-restart in V1**. The log does not need to survive a source crash. Operator reissues the movement. Crash-survival is a follow-up.
- **Brief write-pause during Finalize** on the source to get a deterministic final LSN. `~ms`-scale blocking; trading a short pause for eliminating the flake.
- **N concurrent movements per shard** supported from day one via a per-op log map.

## Approach summary

- Each in-flight movement op maintains a dedicated append-only log file at `<shard>/changelog/<op_id>.log` on the source.
- A per-shard `ChangeLogSet` (atomic pointer on `Shard`) routes every committed write to every active log.
- New gRPC RPCs on `FileReplicationService` let the target stream entries, freeze the LSN, and tear down.
- `processFinalizingOp` and `processDehydratingOp` are rewritten to tail-then-freeze-then-drain instead of start-async-then-wait.
- A new `OverwriteObjectsFromChangeLog` replay path skips the `StaleUpdateTime` conflict check and does pure last-write-wins by timestamp.

## Core correctness invariant

**"Every write committed on the source is delivered to the target at least once."** Delivery is via (a) the file snapshot for writes before the log activated, (b) the change-log for writes after activation and during the dual-write window, or (c) the normal replication path once the target is in the sharding state.

Per-UUID consistency under concurrent same-UUID writes from different coordinators is a property of Weaviate's existing leaderless-2PC replication path (see `usecases/replica/coordinator.go`, `adapters/repos/db/shard_write_put.go:372-413`). The change-capture feature is a refactor of the movement catchup mechanism only — it builds on Weaviate's existing replication semantics and does not attempt to strengthen them.

The log's per-entry guarantees are tractable and preserved:
1. LSN allocation happens **after** `upsertObjectDataLSM` / `bucket.Delete{With}` succeeds, **inside** `docIdLock[poolId]`. Per-UUID LSN order follows per-UUID source commit order at that lock.
2. A single `ChangeLog.mu` serializes all appends to one log file, so LSN assignment and file write are atomic with respect to other tee calls.
3. The target-side replay in `OverwriteObjectsFromChangeLog` uses a strict-`>` LWW check so a replayed older entry can never overwrite a newer locally-present state — this is the seam that keeps the dual-write window safe regardless of how normal 2PC interleaves.

## Log storage: append-only file per op

Chosen over an lsmkv bucket or in-memory ring.

- Single producer, single consumer, monotonic keys, seconds-to-minutes lifetime, deleted after READY. lsmkv would drag in compaction, segment lifecycle, bloom tuning, tombstones — all unneeded.
- Layout: `<shard_path>/changelog/<op_id>.log`.
- Per-append frame:
  ```
  [8B  LSN, big-endian]
  [1B  flags]                    // bit 0: isDelete
  [8B  updateTimeUnixMillis, LE]
  [16B UUID binary]
  [4B  payloadLen, LE]
  [N   payload bytes]            // raw storobj.Object bytes; deletes carry no payload (flag + updateTime are sufficient)
  [4B  CRC32 of frame]
  ```
- In-memory `offsetIndex []int64` maps `lsn-snapshotLSN-1 → file offset` so tailers can seek to `fromLSN` without scanning.
- `bufio.Writer` for append throughput; `fd.Sync()` only on `Deactivate` (V1 does not need crash-durability — see decisions).
- Cleanup on Deactivate: close fd, `os.Remove(path)`, drop in-memory state.

## Log entry payload: raw `storobj.Object` bytes

Use the already-marshalled `storobj.Object` bytes that every PUT / MERGE call site computes just before its bucket write (`objBinary` at `shard_write_put.go:276`, `objBytes` at `shard_write_merge.go:168` and `:267`). The tee reuses that slice verbatim — no additional marshal, no wrapper object. The target decodes via `storobj.FromBinary` and feeds the result to `PutObjectBatch` / `DeleteObject` from `OverwriteObjectsFromChangeLog`.

Deletes carry **no payload**: the frame header's `isDelete` flag and `updateTimeUnixMillis` are sufficient for the target's LWW replay. The `updateTimeUnixMillis` field in the header is the only ordering key; tombstoning happens at replay-time by calling `DeleteObject(id, ts)`.

Why not `VObject.MarshalBinaryV2`? An earlier draft proposed wrapping the payload in `objects.VObject` for uniformity with the existing `OverwriteObjects` call shape. It didn't pull its weight:

- `VObject.Deleted` duplicates information the frame header already carries.
- `VObject.StaleUpdateTime` is used only by the conflict-detection path of `OverwriteObjects`; changelog replay is pure LWW-by-timestamp and doesn't need it.
- `VObject.LatestObject` wraps the same `models.Object` that `storobj` embeds.
- Each tee call site already computes `storobj` bytes for the bucket write — wrapping as `VObject` would add a second marshal for every tee'd write.
- The target-side replay would unwrap `VObject → models.Object → storobj.Object` anyway before inserting.

Using raw `storobj` bytes is smaller, faster on the hot path, and keeps the replay path aligned with the DB's existing primitives (`storobj.FromBinary`, `PutObjectBatch`).

## Tee points (single Shard helper, called from 5 sites)

Add on `Shard`:

```go
// changeLogs holds the set of active change-log sinks for in-flight movements.
// Loaded under atomic.Pointer to avoid adding a mutex on the write hot path.
changeLogs atomic.Pointer[changelog.Set]

func (s *Shard) AppendChangeLogPut(idBytes []byte, updateTimeMillis int64, objBinary []byte)
func (s *Shard) AppendChangeLogDelete(idBytes []byte, updateTimeMillis int64)
```

Both return immediately when `changeLogs.Load() == nil`, which is the common production case. When active, they iterate all logs in the set and append.

Neither helper returns `error`: the tee is contractually infallible from the caller's perspective — user writes must never fail because of tee issues. Any internal error is handled inline per the retry-and-deactivate policy below. An `error` return would force every write-path call site to write a dead `return err` branch that rots and misleads readers. See the Tee-error semantics section for the failure path.

The PUT helper takes the already-marshalled `objBinary` slice (the bytes produced by `obj.MarshalBinary()` at the call site just before the bucket write) — the payload format is raw `storobj.Object` bytes, so the helper reuses the slice verbatim. Zero marshal overhead on the tee. See §"Log entry payload" above for why we use `storobj` bytes instead of a wrapper format.

**Tee placement: before hashtree, after bucket.** Each of the five tees fires immediately after the bucket write succeeds and *before* the hashtree update (`mayUpsertObjectHashTree` / `mayDeleteObjectHashTree`). The object bucket is the single source of truth for replica-movement catchup; the hashtree is a derived structure used by the non-movement async-replication path and continues to serve that path unchanged. Placing the tee before hashtree ensures that any write committed to the bucket is also logged, independent of hashtree outcomes.

Tee call sites (all under `docIdLock[poolId]`, inside existing `asyncReplicationRWMux.RLock`, and inside the new `quiesceMux.RLock` added in Phase 2):

| Site | File:line | Placement |
|---|---|---|
| PUT (covers batch) | `adapters/repos/db/shard_write_put.go` (inside `putObjectLSM`) | after `upsertObjectDataLSM` success, before `mayUpsertObjectHashTree`; skip when `status.skipUpsert` is true |
| MERGE non-mutable | `adapters/repos/db/shard_write_merge.go` (inside `mergeObjectInStorage`) | after its own `upsertObjectDataLSM`, before `mayUpsertObjectHashTree` |
| MERGE mutable | `adapters/repos/db/shard_write_merge.go` (inside `mutableMergeObjectLSM`) | after its own `upsertObjectDataLSM`, before `mayUpsertObjectHashTree` |
| DELETE single | `adapters/repos/db/shard_write_delete.go` (inside `DeleteObject`) | after `bucket.Delete` / `bucket.DeleteWith` success, before `mayDeleteObjectHashTree`; skip when `existing == nil` |
| DELETE batch | `adapters/repos/db/shard_read.go` (inside `batchDeleteObject`) | after bucket delete success, before `mayDeleteObjectHashTree` |

Batch PUTs funnel through `putObjectLSM` via `objectsBatcher`, so one tee covers them. **Both merge paths need their own tees**: `mergeObjectInStorage` calls `upsertObjectDataLSM` directly (it does NOT funnel through `putObjectLSM`), and `mutableMergeObjectLSM` writes through a different path. That's 5 tee sites total.

### Tee-error semantics

The tee never fails the user write. On error:

- **I/O errors on append** (anything other than `ErrLogFinalized` / `ErrLogDeactivated`) are treated as transient and retried up to 3 total attempts with short backoff (1 ms, 5 ms). All retries happen under the locks the write path already holds, so the backoff cap stays small.
- There is no separate marshal-error branch: the payload (`objBinary`) is already marshalled by the caller before the bucket write, so by the time the tee runs the payload slice is known-good. Any marshal failure already surfaced to the user before the tee was reached.
- On retry exhaustion: log at ERROR, call `changelog.Unregister` + `log.Deactivate()` inline for the affected op. The target's tailer observes `ErrLogDeactivated` on its next call, and the Phase 5 consumer translates that into an aborted movement op. Other ops in the `Set` are unaffected.

User-visible writes are never failed by tee issues, regardless of movement state.

Skip cases are correct no-ops:
- `status.skipUpsert == true`: verified via `compareObjsForInsertStatus` (`shard_write_put.go:545-573`) — this flag is set only when geo props, legacy vector, target vectors, multi-vectors, additional props, AND regular properties are all byte-identical to the stored object. No bucket write, no inverted index update, no hashtree update happens on the source. Source state is unchanged; target (which has the matching state via the file snapshot or a prior LSN replay) is also unchanged. No log entry needed.
- **Important:** only skip on `skipUpsert == true`. The `docIDPreserved` branch (`preserve=true, skip=false`, when additional props OR regular props differ while vectors are equal) DOES rewrite the object bucket and update the inverted index — the tee must fire in that case. Check specifically `if !status.skipUpsert { AppendChangeLogPut(...) }`, not `if compareObjsForInsertStatus returned anything`.
- `DeleteObject` with `existing == nil`: nothing was deleted.

## LSN generation

Inside `ChangeLog.AppendPut` / `AppendDelete`:
```go
cl.mu.Lock()
defer cl.mu.Unlock()
if cl.finalized {
    return ErrLogFinalized
}
cl.lsn++                       // monotonic, protected by cl.mu
lsn := cl.lsn
frame := encodeFrame(lsn, ...)
offset := cl.writer.Buffered() + fileSize
cl.writer.Write(frame)
cl.offsetIndex = append(cl.offsetIndex, offset)
cl.cond.Broadcast()            // wake tailers
```

Per-UUID ordering is preserved because `docIdLock[poolId]` serializes the commit sequence; the tee happens inside that lock; `cl.mu` then serializes the LSN assignment.

## Activation, finalize, deactivation lifecycle

Source-side RPCs (all added to `FileReplicationService`):

| RPC | When | Effect |
|---|---|---|
| `StartChangeCapture(index, shard, op_id)` | Source receives it from the target-side consumer before file listing. Under `backupLock.Lock(shard)`: opportunistically sweep the shard's `changelog/` directory of any `.log` files whose op-id is not currently registered (covers orphans left by prior failed movements on a long-lived HOT shard — see "Orphan cleanup" below), flush memtable, create log file + counter (snapshotLSN = 0), register it in `shard.changeLogs`. Returns `{op_id, snapshot_lsn}`. |
| `ListFiles` | Unchanged — `FlushMemtables` is idempotent. Log already active. |
| `GetChangeLog(index, shard, op_id) → stream ChangeLogStreamEntry` | Target tails from the beginning. Server blocks on `cond.Wait` when no new entries; on each Append the producer wakes the streamer. Closes when finalized AND `lastSentLSN >= finalLSN`. |
| `FinalizeChangeLog(index, shard, op_id) → {final_lsn}` | Acquires a **per-shard write-quiesce latch** (new), takes `cl.mu`, sets `cl.finalized = true`, snapshots `final_lsn = cl.lsn`, broadcasts. Writes attempting to append after finalize return `ErrLogFinalized`; the quiesce latch returns `storagestate.ErrStatusReadOnly`-style error to clients, who retry against the cluster (which by now has the target in the replica set via RAFT). Expected duration: milliseconds. |
| `StopChangeCapture(index, shard, op_id)` | Close fd, remove file, deregister from `shard.changeLogs`, release the quiesce latch. |

Activation is *driven by the target-side consumer in `processHydratingOp`* immediately before `CopyReplicaFiles`. Concretely: the consumer calls a new copier method `StartChangeCapture` which wraps the RPC to the source. Then `CopyReplicaFiles` proceeds unchanged; the file snapshot is captured with the log already active, so `snapshotLSN = 0` and every post-listing write lands in the log.

### `Shard.ActivateChangeLog` return shape

The in-process method returns `(*changelog.ChangeLog, error)`, not just `error`. The pointer is for in-process callers (Phase 2 tests that want to append directly). The Phase 4 gRPC handlers do NOT cache per-op state: every `GetChangeLog`/`FinalizeChangeLog`/`StopChangeCapture` call re-resolves via `shard.GetChangeLog(opID)`, which is an O(1) atomic load + map lookup. This keeps the server stateless and avoids a second source of truth for "is this op active?" that could drift from `shard.changeLogs`.

### Per-op concurrency

`shard.changeLogs` is an `atomic.Pointer[changelog.Set]` where `Set` wraps a `sync.Map` keyed by `op_id`. The tee iterates the set. Appending to N logs under an op is `O(N)` file writes; in practice `N <= 2` (COPY-to-two-targets). Activation atomically CAS-updates the set pointer with a new set containing the added op. Deactivation does the inverse.

### V1 restart behavior

On source restart, all `changeLogs` are gone from memory (not reloaded). Any inflight movement ops transition to failed — the consumer will re-issue from REGISTERED on the next tick. This matches today's behavior where replica movement is not checkpoint-resumable.

### Orphan cleanup

The `.log` file for an op-id is removed in three ways, in order of how soon they catch a leak:

1. **On a clean exit path** — `StopChangeCapture` closes and removes the file.
2. **On a shard restart** — `NewShard` runs `sweepChangelogDir()` with an empty "keep" set, removing every file in the directory (V1 never resumes in-flight movements, so anything at init is by construction orphaned).
3. **On the next `ActivateChangeLog` call against the same shard** — before opening the new op's file, `ActivateChangeLog` removes every `.log` file whose basename is not in the currently-registered `changeLogs`. This is the safety net that bounds disk usage on a long-lived HOT shard which cycles through repeated failed COPY ops without ever restarting.

Path 3 is independent of path 2 and runs at a much finer cadence. It is cheap (one `os.ReadDir` per activation, which is a rare movement-driven event), requires no new goroutine, and has no TTL to tune — liveness comes from the authoritative `changeLogs` atomic pointer.

Phase 5's consumer is also expected to call `StopChangeCapture` on error paths so path 1 handles the common case; paths 2 and 3 exist for the process-crash and consumer-bug cases respectively.

## Replay path on target

New function in `adapters/repos/db/replication.go`, adjacent to `OverwriteObjects`:

```go
// ChangeLogReplayEntry is the decoded form of a single changelog frame: the
// header fields needed for LWW ordering plus the raw storobj payload for
// PUTs. The Phase 4 copier client decodes gRPC stream entries into this
// shape and hands a batch to OverwriteObjectsFromChangeLog.
type ChangeLogReplayEntry struct {
    ID                      strfmt.UUID
    LastUpdateTimeUnixMilli int64
    IsDelete                bool
    Payload                 []byte // raw storobj.Object bytes for PUTs; empty for deletes
}

// OverwriteObjectsFromChangeLog applies change-log entries to the local shard
// with pure last-write-wins semantics by LastUpdateTimeUnixMilli. Unlike
// OverwriteObjects it does NOT emit conflicts on StaleUpdateTime mismatch;
// the source-of-truth invariant is the LSN ordering on the source, and the
// target silently skips any entry for which the local object is already
// newer (the dual-write window can produce this legitimately).
func (idx *Index) OverwriteObjectsFromChangeLog(
    ctx context.Context,
    shard string,
    updates []ChangeLogReplayEntry,
) error
```

Semantics per entry, applied **strictly in LSN order, one call per entry** (no deferred PUT batching):
- If `u.IsDelete` and local update time > `u.LastUpdateTimeUnixMilli`: skip.
- Else if `u.IsDelete`: `DeleteObject(ctx, u.ID, time.UnixMilli(u.LastUpdateTimeUnixMilli))`.
- Else if local update time > `u.LastUpdateTimeUnixMilli`: skip.
- Else: decode payload via `storobj.FromBinary(u.Payload)` and call `PutObjectBatch(ctx, []*storobj.Object{decoded})`. Decoding errors abort the movement at the offending entry; no subsequent entries are applied.

The local update time comes from `Shard.ObjectDigestErrDeleted`, which surfaces `lsmkv.Deleted` (use the tombstone's `DeletionTime().UnixMilli()` as `currUpdateTime`) and `lsmkv.NotFound` (leave `currUpdateTime = 0` so the incoming entry always wins).

One-call-per-entry is load-bearing: mirroring `OverwriteObjects`'s "defer PUTs, fire DELETEs inline" pattern would mis-order a PUT → DELETE sequence for the same UUID within a single batch, which Phase 4's in-LSN-order tailer can legitimately produce. Phase 3's caller therefore does not need to group or pre-sort entries beyond preserving the LSN order they arrived in.

No `RepairResponse` return — disk errors bubble up and abort the movement. Unlike `OverwriteObjects`, replay does NOT consult `idx.DeletionStrategy()` and does NOT emit `StaleUpdateTime` conflicts: pure LWW by `LastUpdateTimeUnixMilli`.

## State machine integration

### `processHydratingOp` (new prefix)

Before `CopyReplicaFiles`: call `replicaCopier.StartChangeCapture(ctx, srcNode, collection, shard, opID)`. Then proceed unchanged. `snapshotLSN` is implicit (0) and tracked by the tailer.

### `processFinalizingOp` — full replacement of lines 628–666

The consumer uses the 4-primitive copier API (`StartChangeCapture`, `TailAndApply`, `FinalizeChangeLog`, `StopChangeCapture`). There is no threshold-based catch-up primitive; the stream terminates deterministically on server-side `io.EOF` (which the server emits once the tailer has drained through finalLSN).

```go
// Start a drain goroutine. TailAndApply blocks until io.EOF (log finalized
// and drained through finalLSN), ctx is cancelled, or a fatal error occurs.
drainCtx, cancelDrain := context.WithCancel(ctx)
drainDone := make(chan error, 1)
enterrors.GoWrapper(func() {
    _, err := c.replicaCopier.TailAndApply(drainCtx, src, coll, shard, op.Op.ID)
    drainDone <- err
}, c.logger)

// Commit target to sharding state via RAFT. After this, incoming writes
// double-write to the target (log + normal 2PC path). The LWW check on the
// replay side keeps older log entries from overwriting newer local state.
if !replicaExists {
    if _, err := c.leaderClient.ReplicationAddReplicaToShard(ctx, ...); err != nil { cancelDrain; <-drainDone; return "", err }
}

// Freeze the log. Once this returns, no new entries will be appended and the
// server-side tailer will hit io.EOF as soon as it drains through finalLSN.
if _, err := c.replicaCopier.FinalizeChangeLog(ctx, src, coll, shard, op.Op.ID); err != nil {
    cancelDrain; <-drainDone; return "", err
}

// Wait for the drain goroutine to observe io.EOF.
if err := <-drainDone; err != nil { return "", err }

switch op.Op.TransferType {
case api.COPY:
    if err := c.replicaCopier.StopChangeCapture(ctx, src, coll, shard, op.Op.ID); err != nil { ... }
    if err := c.sync(ctx, op); err != nil { return "", err }
    return api.READY, nil
case api.MOVE:
    // Do NOT stop the log here. DEHYDRATING will activate a fresh op and
    // finalize/drain again after DeleteReplicaFromShard has propagated.
    return api.DEHYDRATING, nil
}
```

During the window between RAFT-add and drain-complete, the target may receive the same logical write on both channels (the log replay and the normal 2PC path). The strict-`>` LWW check on the replay side prevents older log entries from clobbering newer local state established via normal replication. Beyond that, writes during the window are subject to whatever semantics Weaviate's existing leaderless-2PC path provides for concurrent writes — this PR does not change those semantics.

### `processDehydratingOp` — full replacement of lines 697–736

Same drain-goroutine-plus-Finalize pattern as FINALIZING, with a fresh op activated at the start of DEHYDRATING:

```go
// Activate a fresh log window for the DEHYDRATING phase. The source is still
// in the sharding state and can still receive writes during RAFT propagation.
if err := c.replicaCopier.StartChangeCapture(ctx, src, coll, shard, op.Op.ID+"-dehydrate"); err != nil { ... }

drainCtx, cancelDrain := context.WithCancel(ctx)
drainDone := make(chan error, 1)
enterrors.GoWrapper(func() {
    _, err := c.replicaCopier.TailAndApply(drainCtx, src, coll, shard, op.Op.ID+"-dehydrate")
    drainDone <- err
}, c.logger)

// Remove source from sharding state. After RAFT propagates, source stops
// receiving writes.
if _, err := c.leaderClient.DeleteReplicaFromShard(ctx, ...); err != nil { cancelDrain; <-drainDone; return "", err }

// Finalize the log. Once io.EOF arrives on the drain goroutine we know we
// have applied every entry the source ever committed into this log.
if _, err := c.replicaCopier.FinalizeChangeLog(ctx, src, coll, shard, op.Op.ID+"-dehydrate"); err != nil { cancelDrain; <-drainDone; return "", err }

if err := <-drainDone; err != nil { return "", err }

if err := c.replicaCopier.StopChangeCapture(ctx, src, coll, shard, op.Op.ID+"-dehydrate"); err != nil { ... }
if err := c.sync(ctx, op); err != nil { return "", err }
return api.READY, nil
```

Note the COPY-vs-MOVE asymmetry: FINALIZING-COPY finalizes before the dual-write window because target is already write-visible via RAFT; FINALIZING-MOVE defers DEHYDRATING teardown so a fresh log can catch in-flight writes that arrive during RAFT propagation.

### What is removed

- `startAsyncReplication` and `waitForAsyncReplication` calls inside `processFinalizingOp` and `processDehydratingOp`. The helper functions remain (they are used by the non-movement async replication path via other call sites in the shard package).
- `asyncReplicationUpperTimeBoundUnixMillis` and the `asyncReplicationMinimumWait` fetch in those two functions.

## Concrete files

### New

- `cluster/replication/changelog/log.go` — `ChangeLog` struct, `Set`, `Activate`, `AppendPut`, `AppendDelete`, `Finalize`, `Deactivate`, `Tail`, `offsetIndex`.
- `cluster/replication/changelog/entry.go` — `Entry`, `Encode`, `DecodeFrame`, CRC helpers. Uses `usecases/byteops`.
- `cluster/replication/changelog/log_test.go` — encode/decode roundtrip, CRC mismatch, torn frame at tail, concurrent append monotonicity, finalize-then-append-errors, tailer cond wake-up.
- `cluster/replication/copier/copier_changelog.go` — 4 client primitives: `StartChangeCapture`, `TailAndApply`, `FinalizeChangeLog`, `StopChangeCapture`. `TailAndApply` blocks until the server stream hits `io.EOF`, ctx is cancelled, or a fatal error occurs. No `ReplayUntilCaughtUp`, no `ReplayUntilLSN`, no threshold. Delegates decoding to the `changelog` package; delegates replay to `Index.OverwriteObjectsFromChangeLog`.

### Modified

- `adapters/repos/db/shard.go` — add `changeLogs atomic.Pointer[changelog.Set]` field in the atomics block around line 272. Add `ActivateChangeLog`, `FinalizeChangeLog`, `StopChangeCapture`, `AppendChangeLogPut`, `AppendChangeLogDelete` methods.
- `adapters/repos/db/shard_write_put.go` — tee at line 281 (after `upsertObjectDataLSM`), still under `docIdLock` and `asyncReplicationRWMux.RLock`.
- `adapters/repos/db/shard_write_delete.go` — tee at line 78 (after `bucket.Delete`/`DeleteWith`).
- `adapters/repos/db/shard_write_merge.go` — tee inside `mutableMergeObjectLSM` after bucket put. (The `mergeObjectInStorage` path funnels through `putObjectLSM` and is covered.)
- `adapters/repos/db/shard_read.go` — tee inside `batchDeleteObject` after bucket delete.
- `adapters/repos/db/replication.go`:
  - Add `OverwriteObjectsFromChangeLog`.
  - Add `IncomingStartChangeCapture`, `IncomingGetChangeLog` (server-stream adapter), `IncomingFinalizeChangeLog`, `IncomingStopChangeCapture`.
- `cluster/replication/consumer.go` — rewrite `processHydratingOp`, `processFinalizingOp`, `processDehydratingOp` per the flows above. `startAsyncReplication` / `waitForAsyncReplication` / `stopAsyncReplication` are no longer called from these three functions.
- `adapters/handlers/rest/clusterapi/grpc/protocol/file_replication.proto` — add RPCs (`StartChangeCapture`, `GetChangeLog`, `FinalizeChangeLog`, `StopChangeCapture`) and messages (`StartChangeCaptureRequest/Response`, `GetChangeLogRequest`, `ChangeLogStreamEntry`, `FinalizeChangeLogRequest/Response`, `StopChangeCaptureRequest/Response`).
- `adapters/handlers/rest/clusterapi/grpc/file_replication_service.go` — add handlers. `GetChangeLog` follows the `GetFile` streaming pattern (line 130 onward).
- `adapters/handlers/rest/clusterapi/grpc/generated/protocol/*.pb.go` — regenerated via `make grpc` (do not hand-edit).
- `test/acceptance/replication/replica_replication/fast/replica_replication_test.go` — uncomment `TestReplicaMovementTenantParallelWrites` (line 443), remove the `REPLICA_MOVEMENT_MINIMUM_ASYNC_WAIT=10s` env (no longer meaningful for movement catchup).

## Code primitives to reuse (do not re-implement)

- `usecases/byteops` — all frame encoding on hot paths. Avoid `binary.Read`.
- `storobj.Object.MarshalBinary` — already called at every PUT / MERGE tee site (`objBinary` / `objBytes` in scope); the slice is reused verbatim as the log payload, so the tee adds no marshal cost.
- `storobj.FromBinary` — used on the target replay side to decode the payload back into a `*storobj.Object` for `PutObjectBatch`.
- `entities/errors/go_wrapper.go` (`enterrors.GoWrapper`) — any new goroutine (e.g. streaming server handler) must use this. Bare `go` statements will fail `tools/linter_go_routines.sh`.
- `FileReplicationService` basic-auth interceptors (`adapters/handlers/rest/clusterapi/grpc/server.go:107-173`) — new RPCs on the same service inherit auth automatically.
- Logrus `.Error(err)` convention (not `WithError`).

## Testing strategy

The end-to-end correctness guarantee — "no writes lost during movement" — is only meaningful as an acceptance test. The now-uncommented `TestReplicaMovementTenantParallelWrites` is the authoritative check.

**Decoupled deterministic unit tests (worth writing):**
- `changelog.Entry` encode/decode roundtrip, CRC mismatch, zero-payload delete, payload-too-short error.
- `ChangeLog` concurrent append: spawn N goroutines each calling `AppendPut`; assert LSNs are monotonic and gap-free when read back in order.
- `ChangeLog.Finalize`: after Finalize, `AppendPut` returns `ErrLogFinalized`; tailers observe the final LSN.
- `ChangeLogSet` activate/deactivate under concurrent writer: race test with an active movement, assert no deadlock and all entries land in the right log.

**Not worth fabricating as unit tests** (per standing project preference — decoupled deterministic versions are infeasible or would lie):
- "Source receives 10k parallel writes during HYDRATING, target ends up with all 10k" — this is `TestReplicaMovementTenantParallelWrites`.
- "RAFT propagation window is correctly drained post-finalize" — e2e only.

## Verification

1. `make grpc` regenerates the proto artefacts cleanly; generated files compile.
2. `go test -race -count 1 ./cluster/replication/changelog/...` passes.
3. `go test -race -count 1 ./adapters/repos/db/...` passes (the tee changes must not regress existing write-path tests).
4. `go test -tags integrationTest -count 1 -race ./adapters/repos/db/...` passes for replication-related integration tests.
5. `go test -count 1 -race -timeout 15m ./test/acceptance/replication/replica_replication/fast/...` passes, including the uncommented `TestReplicaMovementTenantParallelWrites`. Run at least 20 iterations in CI to confirm it is no longer flaky.
6. `golangci-lint run ./...` and `./tools/linter_go_routines.sh` pass.
7. Smoke: a manual 3-node cluster via `make local`, run a MOVE on a tenant while a client is inserting, verify target has exact same object count as source post-READY.

## Explicit non-goals (V1)

- Source-restart survival of an in-flight movement. Restart aborts; operator reissues.
- Rollback path / feature flag. Old hashtree catchup for movements is removed entirely.
- Log size safety valve (iterative flush + incremental file copy per RFC "safety valve" section). Can be added in a follow-up; V1 assumes HYDRATING completes in reasonable time for the shard size.
- Persistent LSN across shard lifetime — the LSN namespace is per-op, starts at 0, and dies with the op.

## Points to flag before implementation begins

- **Write-quiesce latch blast radius.** The per-shard quiesce latch is new infra on `Shard`. Its only user is FinalizeChangeLog, but its existence must integrate with `isReadOnly` / `asyncReplicationRWMux` patterns already in the write path. Worth a close look during code review to ensure it doesn't deadlock against `backupLock`, `asyncReplicationRWMux`, or `docIdLock`.
- **Batch writes: one LSN per object or one per batch?** One per object, consistent with the tee location (inside `putObjectLSM`, called per object even for batches). This means batches of 1000 produce 1000 log entries; size is proportional to write volume, not batch count.
- **`asyncReplicationRWMux` interaction.** The tee runs inside `RLock`. Starting/stopping async replication (not our path) takes the write lock. Our own StartChangeCapture must not take that lock — it operates on the separate `changeLogs` field. Verify during implementation that there is no accidental ordering dependency.
