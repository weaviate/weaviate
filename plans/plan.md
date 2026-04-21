# Deterministic Replica Movement via Change Capture Log

## Context

Replica movement today catches up writes that land on the source during HYDRATING via hashtree-based async replication with a fixed timing window (`asyncReplicationUpperTimeBound = now+5s` in FINALIZING; `asyncReplicationMinimumWait = 100s` in DEHYDRATING). This is probabilistic: writes that slip past these windows are caught only by the next ~30s hashbeat cycle, and the core correctness test `TestReplicaMovementTenantParallelWrites` is commented out as flaky (`TODO fix flake and uncomment`). RFC is at `plans/rfc.md`.

We replace this for **movements only** with a temporary, append-only change capture log per in-flight op. The source stamps every post-snapshot write with a monotonic LSN. The target tails the log over gRPC and replays in LSN order. Every write is accounted for by exactly one mechanism (file snapshot, log, or direct dual-write on committed target). Non-movement async replication (hashtree) is untouched.

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

"Replaying change-log entries in LSN order up to N on the target produces a state equivalent to what the source had after committing the N-th write."

Per-UUID commit order must match per-UUID LSN order. Global LSN monotonicity is maintained, but cross-UUID total order is not load-bearing (objects are independent; no read-modify-write across UUIDs in the object store).

This is guaranteed by:
1. LSN allocation happens **after** `upsertObjectDataLSM` / `bucket.Delete{With}` succeeds, **inside** `docIdLock[poolId]`. Per-UUID serialization is enforced by `docIdLock`; LSN within a UUID follows commit order.
2. A single `ChangeLog.mu` serializes all appends to one log file, so LSN assignment and file write are atomic with respect to other tee calls.

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
  [N   payload bytes]            // VObjectV2 bytes; empty deletes have deleted=true + no object
  [4B  CRC32 of frame]
  ```
- In-memory `offsetIndex []int64` maps `lsn-snapshotLSN-1 → file offset` so tailers can seek to `fromLSN` without scanning.
- `bufio.Writer` for append throughput; `fd.Sync()` only on `Deactivate` (V1 does not need crash-durability — see decisions).
- Cleanup on Deactivate: close fd, `os.Remove(path)`, drop in-memory state.

## Log entry payload: reuse VObject V2

Do not invent a new object-on-wire format. Use `VObject.MarshalBinaryV2()` (`usecases/objects/replication.go`). Payload carries UUID, `LastUpdateTimeUnixMilli`, `Deleted`, `Vector`/`Vectors`/`MultiVectors`, and a serialized `models.Object`. The target unmarshals into `VObject` and feeds to `OverwriteObjectsFromChangeLog`.

Trade-off: the tee site has a serialized `storobj.Object` (`objBinary` at `shard_write_put.go:273`) and lives objects in memory. Re-wrapping as VObject costs one marshal per write. Accepted for uniformity with the replay path. If profiling shows it hot, we can emit raw storobj bytes and reconstruct VObject on the target — not a V1 concern.

## Tee points (single Shard helper, called from 5 sites)

Add on `Shard`:

```go
// changeLogs holds the set of active change-log sinks for in-flight movements.
// Loaded under atomic.Pointer to avoid adding a mutex on the write hot path.
changeLogs atomic.Pointer[changelog.Set]

func (s *Shard) appendChangeLogPut(obj *storobj.Object, idBytes []byte, objBinary []byte) error
func (s *Shard) appendChangeLogDelete(idBytes []byte, updateTimeMillis int64) error
```

Both return early (nil) when `changeLogs.Load() == nil`, which is the common case. When active, they iterate all logs in the set and append.

Tee call sites (all under `docIdLock[poolId]`, inside existing `asyncReplicationRWMux.RLock`):

| Site | File:line | After |
|---|---|---|
| PUT (covers batch + merge non-mutable) | `adapters/repos/db/shard_write_put.go:280` | `upsertObjectDataLSM` success; skip when `status.skipUpsert` is true |
| MERGE mutable | `adapters/repos/db/shard_write_merge.go` (inside `mutableMergeObjectLSM` bucket put) | bucket put success |
| DELETE single | `adapters/repos/db/shard_write_delete.go:77` | `bucket.Delete` / `bucket.DeleteWith` success; skip when `existing == nil` |
| DELETE batch | `adapters/repos/db/shard_read.go` (inside `batchDeleteObject`) | bucket delete success |

Batch PUTs funnel through `putObjectLSM` via `objectsBatcher`, so one tee covers them. Note: `shard_write_merge.go` has two paths (`mergeObjectInStorage` → `putObjectLSM` is covered by the PUT tee; `mutableMergeObjectLSM` writes through a different path and needs its own tee).

Skip cases are correct no-ops:
- `status.skipUpsert == true`: verified via `compareObjsForInsertStatus` (`shard_write_put.go:545-573`) — this flag is set only when geo props, legacy vector, target vectors, multi-vectors, additional props, AND regular properties are all byte-identical to the stored object. No bucket write, no inverted index update, no hashtree update happens on the source. Source state is unchanged; target (which has the matching state via the file snapshot or a prior LSN replay) is also unchanged. No log entry needed.
- **Important:** only skip on `skipUpsert == true`. The `docIDPreserved` branch (`preserve=true, skip=false`, when additional props OR regular props differ while vectors are equal) DOES rewrite the object bucket and update the inverted index — the tee must fire in that case. Check specifically `if !status.skipUpsert { appendChangeLogPut(...) }`, not `if compareObjsForInsertStatus returned anything`.
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
| `StartChangeCapture(index, shard, op_id)` | Source receives it from the target-side consumer before file listing. Under `backupLock.Lock(shard)`: flush memtable, create log file + counter (snapshotLSN = 0), register it in `shard.changeLogs`. Returns `{op_id, snapshot_lsn}`. |
| `ListFiles` | Unchanged — `FlushMemtables` is idempotent. Log already active. |
| `GetChangeLog(index, shard, op_id, from_lsn) → stream ChangeLogStreamEntry` | Target tails. Server blocks on `cond.Wait` when no new entries; on each Append the producer wakes the streamer. Closes when finalized AND `lastSentLSN >= finalLSN`. |
| `FinalizeChangeLog(index, shard, op_id) → {final_lsn}` | Acquires a **per-shard write-quiesce latch** (new), takes `cl.mu`, sets `cl.finalized = true`, snapshots `final_lsn = cl.lsn`, broadcasts. Writes attempting to append after finalize return `ErrLogFinalized`; the quiesce latch returns `storagestate.ErrStatusReadOnly`-style error to clients, who retry against the cluster (which by now has the target in the replica set via RAFT). Expected duration: milliseconds. |
| `StopChangeCapture(index, shard, op_id)` | Close fd, remove file, deregister from `shard.changeLogs`, release the quiesce latch. |

Activation is *driven by the target-side consumer in `processHydratingOp`* immediately before `CopyReplicaFiles`. Concretely: the consumer calls a new copier method `StartChangeCapture` which wraps the RPC to the source. Then `CopyReplicaFiles` proceeds unchanged; the file snapshot is captured with the log already active, so `snapshotLSN = 0` and every post-listing write lands in the log.

### Per-op concurrency

`shard.changeLogs` is an `atomic.Pointer[changelog.Set]` where `Set` wraps a `sync.Map` keyed by `op_id`. The tee iterates the set. Appending to N logs under an op is `O(N)` file writes; in practice `N <= 2` (COPY-to-two-targets). Activation atomically CAS-updates the set pointer with a new set containing the added op. Deactivation does the inverse.

### V1 restart behavior

On source restart, all `changeLogs` are gone from memory (not reloaded). Any inflight movement ops transition to failed — the consumer will re-issue from REGISTERED on the next tick. This matches today's behavior where replica movement is not checkpoint-resumable.

## Replay path on target

New function in `adapters/repos/db/replication.go`, adjacent to `OverwriteObjects`:

```go
// OverwriteObjectsFromChangeLog applies change-log entries to the local shard
// with pure last-write-wins semantics by LastUpdateTimeUnixMilli. Unlike
// OverwriteObjects it does NOT emit conflicts on StaleUpdateTime mismatch;
// the source-of-truth invariant is the LSN ordering on the source, and the
// target silently skips any entry for which the local object is already
// newer (the dual-write window can produce this legitimately).
func (idx *Index) OverwriteObjectsFromChangeLog(
    ctx context.Context,
    shard string,
    updates []*objects.VObject,
) error
```

Semantics per entry:
- If `u.Deleted` and local update time > `u.LastUpdateTimeUnixMilli`: skip.
- Else if `u.Deleted`: `DeleteObject(ctx, u.ID, time.UnixMilli(u.LastUpdateTimeUnixMilli))`.
- Else if local update time > `u.LastUpdateTimeUnixMilli`: skip.
- Else: `PutObjectBatch(ctx, []*storobj.Object{storobj.FromObject(...)})`.

No `RepairResponse` return — disk errors bubble up and abort the movement.

## State machine integration

### `processHydratingOp` (new prefix)

Before `CopyReplicaFiles`: call `replicaCopier.StartChangeCapture(ctx, srcNode, collection, shard, opID)`. Then proceed unchanged. `snapshotLSN` is implicit (0) and tracked by the tailer.

### `processFinalizingOp` — full replacement of lines 628–666

```go
// Open tailer against the already-active log on the source.
tailer, err := c.replicaCopier.TailChangeLog(ctx, src, coll, shard, op.Op.ID, 0)
if err != nil { return "", err }

// Replay until target is within closeEnoughThreshold of the source's current LSN.
if err := c.replicaCopier.ReplayUntilCaughtUp(ctx, tailer, closeEnoughThreshold); err != nil {
    return "", err
}

// Commit target to the sharding state via RAFT. After this, incoming writes
// dual-write to the target directly.
if !replicaExists {
    if _, err := c.leaderClient.ReplicationAddReplicaToShard(ctx, ...); err != nil { ... }
}

// Freeze the log: brief write-block on source, return deterministic finalLSN.
finalLSN, err := c.replicaCopier.FinalizeChangeLog(ctx, src, coll, shard, op.Op.ID)
if err != nil { return "", err }

// Drain through finalLSN.
if err := c.replicaCopier.ReplayUntilLSN(ctx, tailer, finalLSN); err != nil { return "", err }

switch op.Op.TransferType {
case api.COPY:
    if err := c.replicaCopier.StopChangeCapture(ctx, src, coll, shard, op.Op.ID); err != nil { ... }
    if err := c.sync(ctx, op); err != nil { return "", err }
    return api.READY, nil
case api.MOVE:
    // Do NOT stop the log. In-flight writes can still hit the source until
    // DeleteReplicaFromShard applies. DEHYDRATING will activate a fresh log
    // window (see below) and finalize again.
    return api.DEHYDRATING, nil
}
```

`closeEnoughThreshold` starts at 100 entries (a tunable constant); this reduces how long FinalizeChangeLog must block writes.

### `processDehydratingOp` — full replacement of lines 697–736

```go
// Source is still receiving writes because it's still in the sharding state.
// The log from FINALIZING was un-finalized only for MOVE; reuse it.
tailer, err := c.replicaCopier.TailChangeLog(ctx, src, coll, shard, op.Op.ID, lastAppliedLSN)
if err != nil { return "", err }

// Remove source from sharding state. After RAFT propagates, source stops
// receiving writes.
if _, err := c.leaderClient.DeleteReplicaFromShard(ctx, ...); err != nil { ... }

// Finalize (brief write-block on source; nothing should be landing anyway).
finalLSN, err := c.replicaCopier.FinalizeChangeLog(ctx, src, coll, shard, op.Op.ID)
if err != nil { return "", err }

// Drain.
if err := c.replicaCopier.ReplayUntilLSN(ctx, tailer, finalLSN); err != nil { return "", err }

// Tear down.
if err := c.replicaCopier.StopChangeCapture(ctx, src, coll, shard, op.Op.ID); err != nil { ... }

if err := c.sync(ctx, op); err != nil { return "", err }
return api.READY, nil
```

Note the COPY-vs-MOVE asymmetry: FINALIZING-COPY finalizes before the dual-write window because target is already write-visible via RAFT; FINALIZING-MOVE defers finalize so DEHYDRATING can catch in-flight writes that arrive during RAFT propagation.

### What is removed

- `startAsyncReplication` and `waitForAsyncReplication` calls inside `processFinalizingOp` and `processDehydratingOp`. The helper functions remain (they are used by the non-movement async replication path via other call sites in the shard package).
- `asyncReplicationUpperTimeBoundUnixMillis` and the `asyncReplicationMinimumWait` fetch in those two functions.

## Concrete files

### New

- `cluster/replication/changelog/log.go` — `ChangeLog` struct, `Set`, `Activate`, `AppendPut`, `AppendDelete`, `Finalize`, `Deactivate`, `Tail`, `offsetIndex`.
- `cluster/replication/changelog/entry.go` — `Entry`, `Encode`, `DecodeFrame`, CRC helpers. Uses `usecases/byteops`.
- `cluster/replication/changelog/log_test.go` — encode/decode roundtrip, CRC mismatch, torn frame at tail, concurrent append monotonicity, finalize-then-append-errors, tailer cond wake-up.
- `cluster/replication/copier/copier_changelog.go` — client wrappers: `StartChangeCapture`, `TailChangeLog`, `ReplayUntilCaughtUp`, `ReplayUntilLSN`, `FinalizeChangeLog`, `StopChangeCapture`. Delegates decoding to `changelog` package; delegates replay to `dbWrapper.OverwriteObjectsFromChangeLog`.

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
- `usecases/objects.VObject` + `MarshalBinaryV2` — log payload.
- `storobj.Object.MarshalBinary` — already called at the PUT tee site; `objBinary` is in scope.
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

- **`closeEnoughThreshold` default** = 100 entries. Purely a guess for how close we get before dual-write kicks in. Tunable; we should profile.
- **Write-quiesce latch blast radius.** The per-shard quiesce latch is new infra on `Shard`. Its only user is FinalizeChangeLog, but its existence must integrate with `isReadOnly` / `asyncReplicationRWMux` patterns already in the write path. Worth a close look during code review to ensure it doesn't deadlock against `backupLock`, `asyncReplicationRWMux`, or `docIdLock`.
- **Batch writes: one LSN per object or one per batch?** One per object, consistent with the tee location (inside `putObjectLSM`, called per object even for batches). This means batches of 1000 produce 1000 log entries; size is proportional to write volume, not batch count.
- **`asyncReplicationRWMux` interaction.** The tee runs inside `RLock`. Starting/stopping async replication (not our path) takes the write lock. Our own StartChangeCapture must not take that lock — it operates on the separate `changeLogs` field. Verify during implementation that there is no accidental ordering dependency.
