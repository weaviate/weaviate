# Phased Delivery: Deterministic Replica Movement via Change Capture Log

## Context

The end-to-end design lives at `plans/plan.md` and the RFC at `plans/rfc.md`. This file splits that single-blob implementation into five merge-ordered PRs so each reviewer sees a bounded, testable slice. PR N merges into PR N-1's branch; the final PR merges to `main`.

**Design decisions (do not revisit here — see `plans/plan.md`):**

- Full replacement of hashtree/timing catchup inside the three movement state handlers; no feature flag.
- V1 aborts in-flight ops on source restart.
- `FinalizeChangeLog` does a brief (~ms) per-shard write quiesce.
- Per-op logs — `changeLogs atomic.Pointer[changelog.Set]` on `Shard`.
- Write-path tees land in Phase 2 alongside the shard scaffolding, exercised by integration tests that activate the log directly. The tee is a single atomic pointer load that returns nil outside of in-flight movements, so the hot-path impact is negligible and reviewers can verify correctness in isolation.

**Why this split:**

1. Phase 1 is a pure package — reviewable without domain context.
2. Phase 2 lands the full shard-side write-path surface (activation methods, quiesce latch, tees). Production writes are unaffected because nothing activates a log yet; tees are only reached from tests that call `ActivateChangeLog` directly.
3. Phases 3-4 prepare the cross-node replay mechanism in layers (replay function → gRPC wrapping). Each merge leaves `main` compilable and shipped behaviour identical.
4. Phase 5 is the behavioural cutover: the consumer switches to the new catchup, the acceptance test is uncommented, first production activation happens.

## Dependency DAG

```
P1 changelog pkg ──┐
                   │
P2 shard + tees ───┼──► P4 gRPC + copier client ──► P5 consumer + test
                   │
P3 target replay ──┘
```

P2 and P3 are independent after P1 and can merge in either order. P4 directly depends on P1, P2, and P3. P5 directly depends on P4 only (and transitively on everything upstream).

---

## Phase 1 — Changelog package

**Scope:** a self-contained package under `cluster/replication/changelog/`. No imports from `adapters/`, `cluster/replication/consumer.go`, or any other Weaviate internals except `usecases/byteops` and `entities/errors/go_wrapper.go`.

**New files:**

- `cluster/replication/changelog/entry.go` — `Entry`, frame encode/decode, CRC32 helpers. Uses `usecases/byteops` for allocation-free encoding; no `binary.Read`.
- `cluster/replication/changelog/log.go` — `ChangeLog` (single producer, buffered file append, `offsetIndex []int64`, `sync.Cond` for tailers, `Finalize`, `Deactivate`), `Set` (per-shard map keyed by op-id, CAS swap).
- `cluster/replication/changelog/log_test.go`:
  - Frame encode/decode roundtrip including zero-payload delete.
  - CRC mismatch on read returns error.
  - Torn frame at tail (truncated last entry) returns clean EOF.
  - Concurrent `AppendPut` from N goroutines: LSNs monotonic and gap-free.
  - `Finalize` → subsequent `AppendPut` returns `ErrLogFinalized`; tailer observes final LSN then EOF.
  - Tailer blocked on cond wakes on append; wakes on finalize.
  - `Set` activate/deactivate races with writer: no deadlock, entries land in right log.

**No production code imports this package yet.**

**Merge gate:**
- `go test -race -count 1 ./cluster/replication/changelog/...` passes.
- `golangci-lint run ./cluster/replication/changelog/...` passes.
- `./tools/linter_go_routines.sh` passes (any goroutine in tests uses `enterrors.GoWrapper`).

---

## Phase 2 — Shard scaffolding + write-path tees

**Scope:** give `Shard` the ability to own, activate, finalize, and stop change-capture logs, AND wire the tees on the write hot path. After this phase, the write path conditionally appends to active logs, but nothing in production activates a log, so production behaviour is unchanged. Tees are exercised only by tests that call `ActivateChangeLog` directly.

**Modified files:**

- `adapters/repos/db/shard.go`:
  - Add `changeLogs atomic.Pointer[changelog.Set]` in the atomics block around line 272.
  - Add per-shard write-quiesce latch (new `sync.RWMutex` or equivalent). Its only user in V1 is `FinalizeChangeLog`.
  - Add methods: `ActivateChangeLog(opID) (*changelog.ChangeLog, error)`, `FinalizeChangeLog(opID) (finalLSN, error)`, `StopChangeCapture(opID) error`. `ActivateChangeLog` returns the `*ChangeLog` pointer for the Phase 4 gRPC handler's per-op state; the wire shape of `StartChangeCapture` remains `{op_id, snapshot_lsn}` — the pointer is internal-only and never crosses the gRPC boundary. See plan.md §"`Shard.ActivateChangeLog` return shape".
  - Add helpers `AppendChangeLogPut(idBytes []byte, updateTimeMillis int64, objBinary []byte)` and `AppendChangeLogDelete(idBytes []byte, updateTimeMillis int64)` — **no `error` return**. The tee is contractually infallible from the caller's perspective (user writes must never fail because of tee issues); retry and deactivate are handled inline per the tee-error semantics below. The PUT helper accepts the already-marshalled `objBinary` slice each call site computes before its bucket write — the payload format is raw `storobj.Object` bytes, so the helper reuses the slice verbatim with zero marshal overhead on the tee (see plan.md §"Log entry payload" for the storobj-vs-VObject trade-off). Both helpers return immediately when `changeLogs.Load() == nil` — the common production case.
  - Add orphan cleanup on activate: `ActivateChangeLog` sweeps any `.log` files in the shard's changelog directory whose op-id basename is not currently registered, then opens the new op's file. This bounds disk usage on long-lived HOT shards that cycle through repeated failed COPY ops without restarting — without introducing a new goroutine or a TTL. See plan.md §"Orphan cleanup".
- `adapters/repos/db/shard_write_put.go` — tee inside `putObjectLSM` after `upsertObjectDataLSM` success and **before** `mayUpsertObjectHashTree`, under `docIdLock[poolId]`, `asyncReplicationRWMux.RLock`, and the new `quiesceMux.RLock`. Gate: `if !status.skipUpsert { s.AppendChangeLogPut(...) }` (the early-return at the top of the wrapped function already handles this). The `docIDPreserved` branch (preserve=true, skip=false) still rewrites the bucket and MUST tee.
- `adapters/repos/db/shard_write_delete.go` — tee inside `DeleteObject` after `bucket.Delete` / `bucket.DeleteWith` success and before `mayDeleteObjectHashTree`; skip when `existing == nil` (already early-returns).
- `adapters/repos/db/shard_write_merge.go` — **two tees**: one inside `mergeObjectInStorage` after its own `upsertObjectDataLSM` and before `mayUpsertObjectHashTree`, and one inside `mutableMergeObjectLSM` after its own `upsertObjectDataLSM` and before `mayUpsertObjectHashTree`. Both merge paths call `upsertObjectDataLSM` directly and do NOT funnel through `putObjectLSM`; each needs an explicit tee.
- `adapters/repos/db/shard_read.go` — tee inside `batchDeleteObject` after bucket delete success and before `mayDeleteObjectHashTree`.

All five tees fire **before** the hashtree update (`mayUpsertObjectHashTree` / `mayDeleteObjectHashTree`). The object bucket is the single source of truth for replica-movement catchup; the hashtree is a derived structure used by the non-movement async-replication path (which remains untouched by this phase). Placing the tee before hashtree guarantees that any write committed to the bucket is also logged, independent of hashtree outcomes.

**Tee-error semantics.** The tee never fails the user write. On error:

- **I/O errors on append** (anything other than `ErrLogFinalized` / `ErrLogDeactivated`) are treated as transient and retried up to 3 total attempts with short backoff (1 ms, 5 ms). All retries happen under the locks the write path already holds, so the backoff cap stays small.
- The payload is pre-marshalled at the call site (the same `objBinary` used for the bucket write), so there is no tee-time marshal error to handle — any marshal failure already returned to the user before the tee ran.
- On retry exhaustion: log at ERROR, call `changelog.Unregister` + `log.Deactivate()` inline for the affected op. The target's tailer observes `ErrLogDeactivated` next call, which the Phase 5 consumer translates into an aborted movement op. Other ops in the `Set` are unaffected.

This preserves "Phase 2 leaves production unchanged" even after Phase 5 activates logs in production — user-visible writes are never failed by tee issues.

**New tests (in `adapters/repos/db/`):**

- Integration test that instantiates a shard, calls `ActivateChangeLog`, drives real PUT/DELETE/MERGE/batch-delete operations through the public shard API, calls `FinalizeChangeLog`, reads entries back via the changelog package. Asserts LSN monotonicity, payload integrity, and that the `skipUpsert` and `existing==nil` paths produce no log entries.
- Quiesce latch lock-ordering test: assert no deadlock when held concurrently with `asyncReplicationRWMux.RLock`, `backupLock`, and `docIdLock[poolId]`.
- Micro-benchmark (or at least regression assertion) on the write path with `changeLogs.Load() == nil` to confirm the tee adds no measurable cost in the common case.

**Merge gate:**
- `go test -race -count 1 ./adapters/repos/db/...` passes — existing write-path tests must not regress.
- `go test -tags integrationTest -race -count 1 ./adapters/repos/db/...` passes.
- Code-review focus: (a) tee placement is strictly after bucket success and inside the correct locks; (b) skip branches (`skipUpsert`, `existing==nil`) are correctly identified; (c) quiesce latch blast radius vs existing `isReadOnly` / `asyncReplicationRWMux` / `backupLock` / `docIdLock` conventions.

---

## Phase 3 — Target-side replay function

**Scope:** pure DB-layer function that applies a batch of decoded change-log entries to a local shard with last-write-wins-by-timestamp semantics. Independent of Phase 2 (does not touch `Shard.changeLogs`) and of Phase 4 (no gRPC).

**Modified files:**

- `adapters/repos/db/replication.go`:
  - Add `Index.OverwriteObjectsFromChangeLog(ctx, shard, updates []ChangeLogReplayEntry) error` next to `OverwriteObjects`, where `ChangeLogReplayEntry` carries `{ID, LastUpdateTimeUnixMilli, IsDelete, Payload}` and `Payload` is raw `storobj.Object` bytes (empty for deletes). See plan.md §"Replay path on target".
  - Semantics per entry: skip if local is newer; on delete call `DeleteObject(id, ts)`; on put call `storobj.FromBinary(payload)` then `PutObjectBatch([storobj])`. Decode errors abort the movement. No `RepairResponse`; disk errors bubble up.

**Tests (unit, adjacent to `replication_test.go` if one exists):**

- Local-newer-than-update skip (put and delete variants).
- Successful overwrite of older local.
- Successful delete (tombstone).
- Disk error propagates.

**Merge gate:**
- `go test -race -count 1 ./adapters/repos/db/...` passes.
- Can land before or after Phase 2; both are needed before Phase 4.

---

## Phase 4 — gRPC surface + copier client

**Scope:** wire activation, tailing, finalize, and teardown over the wire. After this phase, the change-capture system is fully functional but nothing in `cluster/replication/consumer.go` invokes it — the replica-movement state machine is still running the old hashtree path.

**Modified files:**

- `adapters/handlers/rest/clusterapi/grpc/protocol/file_replication.proto`:
  - RPCs: `StartChangeCapture`, `GetChangeLog` (server streaming), `FinalizeChangeLog`, `StopChangeCapture`.
  - Messages: `StartChangeCaptureRequest/Response`, `GetChangeLogRequest`, `ChangeLogStreamEntry`, `FinalizeChangeLogRequest/Response`, `StopChangeCaptureRequest/Response`.
- `adapters/handlers/rest/clusterapi/grpc/generated/protocol/*.pb.go` — regenerated by `make grpc`, not hand-edited.
- `adapters/handlers/rest/clusterapi/grpc/file_replication_service.go`:
  - Four handlers. `GetChangeLog` follows the `GetFile` server-streaming pattern at line 130+. Any new goroutine uses `enterrors.GoWrapper`.
  - Auth is inherited from the existing basic-auth interceptor at `adapters/handlers/rest/clusterapi/grpc/server.go:107-173` — no changes there.
- `adapters/repos/db/replication.go`:
  - Add `IncomingStartChangeCapture`, `IncomingGetChangeLog` (adapter to server stream), `IncomingFinalizeChangeLog`, `IncomingStopChangeCapture`. These delegate to the Phase 2 `Shard` methods.

**New files:**

- `cluster/replication/copier/copier_changelog.go`:
  - Client wrappers `StartChangeCapture`, `TailChangeLog`, `ReplayUntilCaughtUp(threshold)`, `ReplayUntilLSN(targetLSN)`, `FinalizeChangeLog`, `StopChangeCapture`.
  - Replay wrappers consume `ChangeLogStreamEntry`s, decode via `changelog` pkg, batch, and call `Index.OverwriteObjectsFromChangeLog` (Phase 3).

**Tests:**

- In-process gRPC test (two Shards in one test, real gRPC dial): start on source, append via direct `Shard.AppendChangeLogPut` calls, tail from target, finalize, stop. Assert target applied all entries and local state converges.
- Server-stream cancellation mid-tail cleans up.

**Merge gate:**
- `make grpc` regenerates cleanly; `git diff generated/` has no hand edits.
- `go test -race -count 1 ./cluster/replication/copier/... ./adapters/handlers/rest/clusterapi/grpc/... ./adapters/repos/db/...` passes.
- `golangci-lint run ./...` and `./tools/linter_go_routines.sh` pass.

---

## Phase 5 — Consumer cutover + uncomment test

**Scope:** the behavioural change. Tees are already live from Phase 2 but never activated in production; this phase flips on activation by rewriting the three movement state handlers and uncommenting the acceptance test.

**Modified files:**

- `cluster/replication/consumer.go`:
  - `processHydratingOp`: call `replicaCopier.StartChangeCapture(...)` before `CopyReplicaFiles`. Rest unchanged.
  - `processFinalizingOp` (lines 628-666): tail → replay until caught up → add replica via RAFT (COPY) → finalize → drain to finalLSN → COPY stops log, MOVE keeps log and returns DEHYDRATING.
  - `processDehydratingOp` (lines 697-736): tail from `lastAppliedLSN` → delete replica via RAFT → finalize → drain → stop → sync → READY.
  - Remove `startAsyncReplication` / `waitForAsyncReplication` calls from these three functions. Helpers stay (used by non-movement async replication callers).
  - Remove `asyncReplicationUpperTimeBoundUnixMillis` and `asyncReplicationMinimumWait` fetches from these three functions.
  - **Cleanup on error paths (required):** every error return out of `processHydratingOp`, `processFinalizingOp`, and `processDehydratingOp` that happens after `StartChangeCapture` succeeded must call `replicaCopier.StopChangeCapture(...)` before returning — a `defer` at the top of each handler is the cleanest pattern. This is the primary mechanism that keeps the source's `changelog/` directory from accumulating orphan `.log` files on a long-lived HOT shard. Phase 2 adds a safety-net sweep inside `ActivateChangeLog`, and `NewShard`'s restart-time sweep remains, but the consumer-side `defer` is what handles the expected case where a movement errors out mid-flight. See plan.md §"Orphan cleanup".
- `test/acceptance/replication/replica_replication/fast/replica_replication_test.go`:
  - Uncomment `TestReplicaMovementTenantParallelWrites` (line 443).
  - Remove `REPLICA_MOVEMENT_MINIMUM_ASYNC_WAIT=10s` env setup — no longer meaningful for movement catchup.

**Merge gate:**
- `go test -race -count 1 ./adapters/repos/db/... ./cluster/replication/...` passes.
- `go test -tags integrationTest -race -count 1 ./adapters/repos/db/...` passes.
- `go test -count 1 -race -timeout 15m ./test/acceptance/replication/replica_replication/fast/...` passes **20 consecutive runs** including `TestReplicaMovementTenantParallelWrites`. This is the flake-retirement gate for the PR.
- `golangci-lint run ./...` and `./tools/linter_go_routines.sh` pass.
- Smoke: manual 3-node cluster via `make local`, MOVE a tenant during active writes, verify target object count matches source.

---

## Rollback posture

No feature flag — the decision is made. Each phase in isolation is revertable:

- **Phases 1-4 revert individually** with no runtime impact (dormant code).
- **Phase 5 revert** restores the old hashtree catchup (we are not deleting `startAsyncReplication` / `waitForAsyncReplication`, only the calls from the three movement handlers). `TestReplicaMovementTenantParallelWrites` would need to be re-commented. This is the only phase with behavioural revert cost. Phase 2 tees stay in `main` after a Phase 5 revert — harmless, since no production caller activates a log.

## Points to flag during review

- `closeEnoughThreshold = 100 entries` in Phase 5 is a guess; profile and adjust.
- The quiesce latch introduced in Phase 2 must survive Phase 5 consumer review without deadlocking against `backupLock`, `asyncReplicationRWMux`, `docIdLock`. Lock-ordering notes belong in the Phase 2 PR description.
- Batch writes produce one LSN per object, not one per batch — consistent with the tee location inside `putObjectLSM`.
- Phase 4 could be further split into "gRPC server handlers" vs "copier client wrappers" if PR size becomes unwieldy during implementation. Not pre-committing to the extra split now.
