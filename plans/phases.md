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
  - Add methods: `ActivateChangeLog(opID)`, `FinalizeChangeLog(opID) (finalLSN, error)`, `StopChangeCapture(opID)`. Shape matches the RPC handlers that will wrap them in Phase 4.
  - Add helpers `AppendChangeLogPut(obj, idBytes, objBinary)` and `AppendChangeLogDelete(idBytes, updateTimeMillis)` that early-return nil when `changeLogs.Load() == nil` — the common production case.
- `adapters/repos/db/shard_write_put.go` — tee at line 281 after `upsertObjectDataLSM`, still under `docIdLock[poolId]` and `asyncReplicationRWMux.RLock`. Gate: `if !status.skipUpsert { s.AppendChangeLogPut(...) }`. The `docIDPreserved` branch (preserve=true, skip=false) still rewrites the bucket and MUST tee.
- `adapters/repos/db/shard_write_delete.go` — tee at line 78 after `bucket.Delete` / `bucket.DeleteWith`; skip when `existing == nil`.
- `adapters/repos/db/shard_write_merge.go` — tee inside `mutableMergeObjectLSM` after bucket put. (`mergeObjectInStorage` funnels through `putObjectLSM` → covered by the PUT tee.)
- `adapters/repos/db/shard_read.go` — tee inside `batchDeleteObject` after bucket delete.

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

**Scope:** pure DB-layer function that applies a batch of `VObject`s to a local shard with last-write-wins-by-timestamp semantics. Independent of Phase 2 (does not touch `Shard.changeLogs`) and of Phase 4 (no gRPC).

**Modified files:**

- `adapters/repos/db/replication.go`:
  - Add `Index.OverwriteObjectsFromChangeLog(ctx, shard, updates []*objects.VObject) error` next to `OverwriteObjects`.
  - Semantics per entry: skip if local is newer; otherwise `DeleteObject(id, ts)` or `PutObjectBatch([storobj])`. No `RepairResponse`; disk errors bubble up.

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
