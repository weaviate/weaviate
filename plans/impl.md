# Schema Mutation Guard During Replica Movement — Implementation Plan

## Context

Replica movement (used for non-HA→HA scale-out via COPY) snapshots a source shard's
files and catches the target up via a change-capture log (CCL). The CCL only captures
**object-level** writes. **Schema-level structural changes** — enabling compression,
adding a named vector, disabling a property index, freezing/unfreezing a tenant, or the
automatic dynamic flat→HNSW upgrade — change the *on-disk structure* of the source shard
in ways the snapshot+CCL cannot reconcile. The canonical failure: enabling PQ/BQ/SQ/RQ
mid-COPY compresses the source while the target keeps a pre-compression copy, leaving two
replicas serving structurally different indexes with no path to converge.

This change installs a **bidirectional guard**:

- **Forward:** reject dangerous schema mutations on a collection/shard that has an active
  replica movement (clear, actionable error; operator retries after it completes).
- **Reverse:** make a movement *wait* (not fail) while a structural op is already in flight
  on the source shard, gated on the source node where that runtime state is observable.
- **Defer (one non-user case):** the dynamic flat→HNSW auto-upgrade has no user to reject,
  so it is deferred while a movement is active and re-tried on the next scheduler tick.

### Design decisions (confirmed with stakeholder)

1. **User-initiated dangerous ops → REJECT** with a clear error. No reconciliation
   machinery exists to support "accept-and-defer" (see discrepancy #3), and building it
   would open a window where schema config and on-disk state disagree. Rejecting matches
   the existing reindex `MutationGuard` precedent and is deterministic.
2. **UpdateClass → precise diff.** Block only when a change is *structurally dangerous*
   (compression newly enabled, or a new named vector added). Allow safe in-memory changes
   (`ef`/`efMin`/`efMax`/`flatSearchCutoff`, BM25 `k1`/`b`, stopwords, replication factor,
   autoTenant toggles).
3. **Reverse guard → source-node gate in `HaltForTransfer`**, using a *non-counting*
   "shard busy" signal so a slow structural op does not exhaust the op's error budget.
4. **Wiring → parallel explicit `replicationFSM` checks**, node-independent, alongside the
   existing reindex `MutationGuard` (not folded into it).

---

## RFC ↔ code discrepancies found (and how the plan resolves them)

1. **`HasOngoingReplication` is worse than "checks the leader's node ID."**
   `schema.updateTenants` passes `s.nodeID` — the **local applying node's** ID
   (`cluster/schema/schema.go:562`) — into `meta_class.go:507`. RAFT applies on *every*
   node, so the `continue` that skips HOT→COLD fires only on the source/target node and not
   elsewhere → the tenant's status **diverges across the cluster's FSM** (a determinism
   violation), not merely "fails to fire." **Fix:** a node-independent check keyed on
   `opsByCollectionAndShard` (RAFT-replicated, identical on every node).

2. **The reverse guard cannot live in `ValidateReplicationReplicateShard`.** Both call sites
   (`cluster/raft_replication_apply_endpoints.go:134`, `cluster/replication/manager.go:125`)
   run at RAFT level against a `SchemaReader` (replicated state). Compression's
   `StatusReadOnly` and the dynamic-upgrade lock live **only in memory on the source node**
   and are invisible to a deterministic FSM apply. **Fix:** gate on the source node inside
   `HaltForTransfer` (`adapters/repos/db/shard_backup.go:45`), reached via
   `IncomingPauseFileActivity` (`adapters/repos/db/replication.go:510`) — extending the
   existing `refuseIfReindexInFlight` pattern.

3. **No reconciliation mechanism exists** (rules out cheap deferral of user ops). Shard load
   does not compress to match schema; `hnsw.ShouldCompressFromConfig()`
   (`adapters/repos/db/vector/hnsw/config_update.go:893`) is **dead code**;
   `updateIndexAddMissingProperties` (`adapters/repos/db/migrator.go:469`) reconciles only
   missing *property* buckets, with no vector/compression analog; and `schemaOnly=true`
   applies skip the DB-side compression and **never reconcile it later** (latent bug — see
   Part 5).

4. **`statusReasonVectorIndexUpdate` is not what compression actually sets.** The RFC says
   compression sets reason `statusReasonVectorIndexUpdate`, but `UpdateVectorIndexConfigs`
   sets a *formatted* string `"UpdateVectorIndexConfigs: <vecs>"`
   (`adapters/repos/db/shard.go:578`). **Fix:** detect compression-in-flight via an explicit
   atomic flag (Part 4), not reason-string matching.

5. **`UpdateClass` is currently unguarded entirely.** The reindex `MutationGuard` covers
   `DeleteClass`/`UpdateProperty`/`Update`+`DeleteTenants` but **not** `UpdateClass` — which
   is exactly where compression-enable and new-named-vector land. We add a brand-new guard
   there.

6. **`DeleteClass`/`DeleteTenants` are intentionally NOT guarded** for movement. They are
   not in the RFC's blocked list and already cancel/clean up movements via
   `DeleteReplicationsByCollection` / `DeleteReplicationsByTenants`
   (`cluster/schema/manager.go:41-42`).

---

## Part 1 — Fix the FSM-divergence bug + add node-independent movement queries

**File:** `cluster/replication/shard_replication_fsm.go` (add methods next to existing
`GetOpsForCollection`/`GetOpsForCollectionAndShard`, ~lines 203-244)

```go
// HasActiveReplicationForShard reports whether any non-terminal replication op
// exists for collection/shard, regardless of which node is source or target.
// Deterministic across the cluster: reads only RAFT-replicated FSM state, so
// every node reaches the same answer (cf. the buggy node-keyed HasOngoingReplication).
func (s *ShardReplicationFSM) HasActiveReplicationForShard(collection, shard string) bool {
    s.opsLock.RLock()
    defer s.opsLock.RUnlock()
    for _, op := range s.opsByCollectionAndShard[collection][shard] {
        if st, ok := s.statusById[op.ID]; ok && st.ShouldConsumeOps() {
            return true
        }
    }
    return false
}

// HasActiveReplicationForCollection — as above but for any shard of the collection.
// Used to gate class-wide mutations (vector config changes) that touch every shard.
func (s *ShardReplicationFSM) HasActiveReplicationForCollection(collection string) bool {
    s.opsLock.RLock()
    defer s.opsLock.RUnlock()
    for _, op := range s.opsByCollection[collection] {
        if st, ok := s.statusById[op.ID]; ok && st.ShouldConsumeOps() {
            return true
        }
    }
    return false
}
```

`ShouldConsumeOps()` (`shard_replication_fsm.go:267`) already defines "non-terminal", and an
op blocked/waiting in `HYDRATING` (Part 4) stays non-terminal — so it correctly continues to
read as active here.

**Replace the buggy call** at `cluster/schema/meta_class.go:507`:
```go
// before: replicationFSM.HasOngoingReplication(m.Class.Class, requestTenant.Name, nodeID)
// after:  replicationFSM.HasActiveReplicationForShard(m.Class.Class, requestTenant.Name)
```

- Add `HasActiveReplicationForShard`/`HasActiveReplicationForCollection` to the
  `replicationFSM` interface in `cluster/schema/manager.go:38-44` **and** to the
  `replicationFSM` interface used by `meta_class.go`.
- Remove `HasOngoingReplication` from the interface + `shard_replication_apply.go:475-481`
  (and helpers `hasOngoingSourceReplication`/`hasOngoingTargetReplication` at 442-473) **iff**
  no other callers remain — do not keep a vestigial node-keyed method.
- Regenerate mocks (`make mocks`) for the schema-package `replicationFSM` mock.

---

## Part 2 — Forward guard: block dangerous schema mutations during movement

All checks use the node-independent Part-1 methods. All reject with `ErrBadRequest`-wrapped,
actionable messages, e.g.:
`"<op> blocked: replica movement in progress for collection %q (shard %q); retry after it completes"`.

### 2a. UpdateClass — block ALL on-disk vector restructures (implemented)
**File:** `cluster/schema/manager.go`, inside the `update` closure right after
`u, err := s.parser.ParseClassUpdate(&meta.Class, req.Class)`. The closure already returns
`ErrBadRequest`-wrapped errors (e.g. the replication-factor check), so returning here is the
established, RAFT-deterministic pattern; both old (`meta.Class`) and new (`u`) are in scope.
Helpers live in new file `cluster/schema/movement_guard.go`.

```go
if s.replicationFSM != nil && s.replicationFSM.HasActiveReplicationForCollection(meta.Class.Class) {
    if reason := dangerousVectorConfigChange(&meta.Class, u); reason != "" {
        return fmt.Errorf("%w: %w: %s on collection %q; retry after it completes",
            ErrBadRequest, ErrReplicaMovementInProgress, reason, meta.Class.Class)
    }
}
```

`ParseClassUpdate` returns `*models.Class` (there is no `parsedUpdate` type). Per the
stakeholder decision, detection blocks **every** on-disk restructure, not just enable+add:
```go
func dangerousVectorConfigChange(old, u *models.Class) string
```
Per vector (legacy `VectorIndexConfig` + each `VectorConfig` entry), `vecDelta` flags:
- **named vector add/remove:** key present on one side only.
- **index-type / distance change:** via the `schemaConfig.VectorIndexConfig` interface methods
  `IndexType()` / `DistanceName()` (hnsw/flat/dynamic all implement them).
- **compression toggled:** `compressionEnabled(old) != compressionEnabled(new)`.
- **compression re-parametrized:** `compressionParamsChanged(old, new)` — structural quant params
  only (PQ segments/centroids/encoder, SQ trainingLimit, RQ bits); query-only knobs
  (RescoreLimit, Cache, ef, flatSearchCutoff) are ignored.

`compressionEnabled`/`compressionParamsChanged` type-switch over `hnswent.UserConfig`,
`flatent.UserConfig`, and `dynamicent.UserConfig` (recursing into `HnswUC`/`FlatUC`) — compression
is not hnsw-only, so the impl.md's hnsw-only sketch was widened.

### 2b. UpdateProperty — disabling a property index
**File:** `cluster/schema/manager.go:520-550`, alongside the existing reindex guard
(lines 532-536). Disabling an index deletes LSM buckets via
`updatePropertyBuckets`→`removeBucket`→`os.RemoveAll` (`shard_init_properties.go:123,451`).

Detection needs old vs new index flags. Read the current property via
`s.schema.ReadOnlyClass(cmd.Class)` → `findProp(cls, req.Property.Name)`, then:
```go
if s.replicationFSM != nil && !req.FromInFlightMigration &&
    s.replicationFSM.HasActiveReplicationForCollection(cmd.Class) {
    if cls, _ := s.schema.ReadOnlyClass(cmd.Class); cls != nil {
        if old := findProp(cls, req.Property.Name); old != nil && disablesAnyIndex(old, req.Property) {
            return fmt.Errorf("%w: %w: property %q index removal blocked on collection %q; retry after it completes",
                ErrBadRequest, ErrReplicaMovementInProgress, req.Property.Name, cmd.Class)
        }
    }
}
```
`disablesAnyIndex` (in `movement_guard.go`) compares
`inverted.HasFilterableIndex/HasSearchableIndex/HasRangeableIndex` old vs new; dangerous when any
goes `true → false`. (Adding indexes/properties stays safe — target reconciles via
`updateIndexAddMissingProperties`.) Respect `FromInFlightMigration` for parity with the reindex
guard. Nested-property index disable removes no buckets (`updatePropertyBuckets` does not recurse
into `NestedProperties`), so guarding the top-level property is sufficient.

### 2c. UpdateTenants — freeze / unfreeze / HOT→COLD
**File:** `cluster/schema/meta_class.go:449-565`, per-tenant loop. Generalize the (now fixed)
COLD check at 507 into a single guard covering all three dangerous transitions, emitting a
**partial error** instead of a silent `continue` — consistent with the FREEZING/UNFREEZING
handling at 476-505 and the "reject with clear error" decision.

The freeze vars (`existedSharedFrozen`/`requestedToFrozen`) move up above the guard so the
combined condition and the existing freeze/unfreeze dispatch both reuse them. Per the
stakeholder decision, the rejection uses a **new dedicated sentinel** `ErrReplicaMovementInProgress`
(not `ErrTenantTransitionalState` — the tenant is HOT, not mid-freeze):
```go
toCold   := requestTenant.Status == models.TenantActivityStatusCOLD // shuts shard down
toFrozen := requestedToFrozen && !existedSharedFrozen               // freeze offloads shard
unfreeze := existedSharedFrozen && !requestedToFrozen               // GetPartitions reassigns nodes
if (toCold || toFrozen || unfreeze) &&
    replicationFSM.HasActiveReplicationForShard(m.Class.Class, requestTenant.Name) {
    partialErrs = append(partialErrs, fmt.Errorf(
        "%w: tenant %q status change to %s blocked; retry after movement completes",
        ErrReplicaMovementInProgress, requestTenant.Name, requestTenant.Status))
    continue
}
```
Transitions *toward available* (→HOT/ACTIVE) remain unguarded (safe). Placed after the
FREEZING/UNFREEZING transitional checks and before the freeze/unfreeze dispatch. `oldTenant` is
never FREEZING/UNFREEZING here (those `continue` earlier), so `existedSharedFrozen` means FROZEN.

---

## Part 3 — Defer the dynamic flat→HNSW auto-upgrade during movement (implemented)

The dynamic flat→HNSW upgrade fires automatically with no RAFT coordination and deletes the flat
bucket dir / creates the HNSW commit log (`dynamic/index.go` `doUpgrade`, bucket removal at ~597).
Its **only** trigger is the async-index scheduler → `VectorIndexQueue.BeforeSchedule` →
`checkCompressionSettings` → `ci.Upgrade` (`vector_index_queue.go:283-313`); the queue registers
with the scheduler only when async indexing is enabled, so there is no sync-mode path to guard.

**Guard** (`adapters/repos/db/vector_index_queue.go`): inside the
`if ci.AlreadyIndexed() > uint64(shouldUpgradeAt)` block, **before** `scheduler.PauseQueue` +
`ci.Upgrade`, so the FSM is consulted only when an upgrade is imminent and the deferral returns
before touching the scheduler:
```go
if iq.shard.AnyActiveMovement() {
    return false // defer: keep indexing into flat; re-checked next tick once movement clears
}
```
This same `ci.Upgrade` site also drives async-enabled HNSW *compression* (`hnsw.ShouldUpgrade()`
returns true for enabled PQ/SQ/RQ), so the guard intentionally defers both the dynamic upgrade and
async-on compression while a movement is active — covering the reverse-ordering TOCTOU's
not-yet-started case (see Part 4 watch-out).

**Reuse the FSM the DB already holds (no new injection).** `DB` already has
`replicationFSM types.ReplicationFSMReader`, set by `SetReplicationFSM(...)` at
`configure_api.go:787`; the db package already imports `cluster/replication`, so there is no
import-cycle barrier (the original closure-injection idea was redundant and was dropped). The only
gap was that `types.ReplicationFSMReader` exposed just the two `FilterOneShardReplicas*` methods —
add `HasActiveReplicationForShard(collection, shard string) bool` to it (the concrete
`*ShardReplicationFSM` already implements it from Part 1; `cluster/router` only consumes the
interface; regenerate `mock_replication_fsm_reader.go`).

One method, `Shard.AnyActiveMovement()` (in `shard.go`):
`if s == nil || s.index == nil || s.index.db == nil || s.index.db.replicationFSM == nil { return
false }; return s.index.db.replicationFSM.HasActiveReplicationForShard(s.index.Config.ClassName,
s.name)`. The shard supplies its own identity, so the queue call site is just
`iq.shard.AnyActiveMovement()`. The **`replicationFSM == nil` guard is required**: the field is nil
in the startup window (`db.New` at `configure_api.go:477` runs before `SetReplicationFSM` at `:773`,
since `ClusterService` is created at `:703`), so a scheduler tick hitting the upgrade threshold then
would otherwise call a method on a nil interface and panic the background goroutine. The FSM is
read live through `db` (process-global, late-bound) and **not** cached per-shard — per-shard copies
would go stale vs `SetReplicationFSM`. No `configure_api.go` wiring is added (the FSM is already
wired there); there is no `DB`-level wrapper (the queue's sole call doesn't warrant one).

**Tests** (`adapters/repos/db/vector_index_queue_movement_test.go`, no build tag, white-box so a
partial `&Shard{index,name}` literal suffices despite `shard` being a concrete `*Shard`): via the
public `BeforeSchedule()`, an imminent upgrade is deferred (skip=false, no `Upgrade`) during a
movement — asserting the shard queried with its own collection+name — and the FSM is **not**
consulted below threshold. Plus a regression test that `Shard.AnyActiveMovement()` doesn't panic on
a nil FSM. The movement-clear→actually-upgrades path needs a live scheduler+DiskQueue and is left to
e2e.

---

## Part 4 — Reverse guard: movement waits for in-flight structural op (source-node)

### 4a. Detect structural ops in flight (explicit flags, not reason strings — discrepancy #4)

Track each window where the **on-disk work actually runs**, not where the RAFT apply returns.
Compression outlives `UpdateVectorIndexConfigs` (`shard.go:599-605` spawns the wg-waiter then
returns), and in async-indexing-**on** mode the compression does not run in
`UpdateVectorIndexConfigs` at all: `UpdateUserConfig` fires its callback immediately
(`hnsw/config_update.go:131-134`) and the real compression runs later in the queue. So a flag set
around `UpdateVectorIndexConfigs`'s waitgroup would cover async-off but **miss async-on entirely** —
do **not** put it there.

Both compression paths — async-off `UpdateUserConfig`→`h.Upgrade` (`config_update.go:143`) and
async-on queue `ci.Upgrade`→`h.Upgrade` (`vector_index_queue.go:302`) — funnel through the single
goroutine `hnsw.compressThenCallback` (`hnsw/config_update.go:176`). Track there, per index:

- **HNSW compression:** add `hnsw.compressing atomic.Bool`; set `true` at the top of
  `compressThenCallback` and `false` via `defer`. This spans the real compression window in **both**
  async modes. `UpgradeInProgress()` on HNSW returns `h.compressing.Load()` — it is **not** a
  constant `false`.
- **Dynamic upgrade:** add `dynamic.upgrading atomic.Bool`, set `true`/`false` around the
  `doUpgrade` goroutine in `dynamic.Upgrade` (`dynamic/index.go:502-516`). `UpgradeInProgress()` on
  dynamic returns `dynamic.upgrading.Load() || (dynamic.upgraded.Load() && inner.UpgradeInProgress())`
  so it also reports an in-flight *compression* of the inner HNSW after the flat→HNSW upgrade.
- **Flat:** `UpgradeInProgress()` returns false (flat does not restructure via this path).
- Expose `UpgradeInProgress() bool` on the `upgradableIndexer` interface (all three implement it).

This replaces the earlier `Shard.vectorIndexUpdating` flag: tracking at the common
`compressThenCallback`/`doUpgrade` sites covers every trigger (async-on, async-off, dynamic) with no
RAFT-apply-vs-async-window gap, so no shard-level flag is needed. (Discovered while verifying the
Part 2a↔Part 4 compression-then-movement TOCTOU: Part 2a only blocks the forward ordering, Part 3
defers the async-on not-yet-started case, and Part 4 here must catch already-running compression in
*both* async modes — hence the flag belongs at `compressThenCallback`, not `UpdateVectorIndexConfigs`.)

New shard helper:
```go
// structuralVectorOpInFlight reports an in-progress compression or dynamic upgrade whose
// on-disk effects would make a file snapshot structurally inconsistent.
func (s *Shard) structuralVectorOpInFlight() (busy bool, reason string) {
    _ = s.ForEachVectorIndex(func(name string, vi VectorIndex) error {
        if u, ok := vi.(upgradableIndexer); ok && u.UpgradeInProgress() {
            busy = true
            reason = fmt.Sprintf("structural vector op (compression or flat→HNSW upgrade) in progress on vector %q", name)
        }
        return nil
    })
    return busy, reason
}
```

### 4b. Gate the transfer
**File:** `adapters/repos/db/shard_backup.go:37-48`, immediately after `refuseIfReindexInFlight`:
```go
if !offloading {
    if blockedErr := s.index.refuseIfReindexInFlight(s.name); blockedErr != nil {
        return blockedErr
    }
    if busy, reason := s.structuralVectorOpInFlight(); busy {
        return fmt.Errorf("%w: shard %q: %s; transfer deferred until it completes",
            ErrShardBusyStructuralOp, s.name, reason)
    }
}
```
This also gates the backup path (`offloading=false`), which is desirable — snapshotting a
shard mid-compression is equally unsafe for backups.

### 4c. Non-counting "shard busy" signal (so the op WAITS, never auto-cancels)
Today every refusal calls `ReplicationRegisterError` and at `MaxErrors = 50`
(`shard_replication_op_state.go:26`) the op is **auto-cancelled** (`manager.go:141-150`).
A slow compression/upgrade would burn the budget and cancel the movement.

- Define sentinel `ErrShardBusyStructuralOp` (and treat the existing
  `ErrBackupBlockedByInFlightReindex` the same way — this also fixes that latent risk).
- **Cross the gRPC boundary:** map these to a distinct gRPC code (e.g.
  `codes.FailedPrecondition`) in the file-replication gRPC server
  (`adapters/handlers/rest/clusterapi/grpc/file_replication_service.go:44-62`), since
  `errors.Is` identity is lost over the wire.
- **Consumer:** in `processStateAndTransition`
  (`cluster/replication/consumer.go:412-450`), when the handler error is a "shard busy" signal
  (gRPC code check via `isShardBusyError(err)`), **skip** `ReplicationRegisterError`
  (line 436) and return so the op stays in `HYDRATING` and is re-polled — without consuming
  the error budget. Surface a benign, non-counting "waiting: source shard busy" note in op
  status for operator visibility.

---

## Part 5 — Latent bug discovered (must pin per CLAUDE.md "no bug is out of scope")

`schemaOnly=true` applies skip the DB-side compression (`cluster/schema/manager.go:864`,
`store_apply.go:101-108`) and **nothing reconciles it later**: if a node defers/loses
compression work and later replays the `UpdateClass` with `schemaOnly=true` (RAFT catch-up),
the on-disk index stays uncompressed forever while schema says compressed. This is
pre-existing and *not* worsened by this change (we add no new deferral), but it is a real
data-correctness gap.

**Action:** add a failing (red) regression test pinning it and surface explicitly to the
stakeholder; decide whether to fix here (wire up `ShouldCompressFromConfig` + a vector analog
of `updateIndexAddMissingProperties` to reconcile on load) or track separately. Do **not**
leave it unpinned.

---

## Files to modify (summary)

| Area | File | Change |
|---|---|---|
| FSM queries | `cluster/replication/shard_replication_fsm.go` | add `HasActiveReplicationFor{Shard,Collection}` |
| FSM cleanup | `cluster/replication/shard_replication_apply.go` | remove vestigial `HasOngoingReplication` (+helpers) if unused |
| Interfaces/mocks | `cluster/schema/manager.go` (`replicationFSM` iface), meta_class iface, generated mocks | add new methods; `make mocks` |
| UpdateClass guard | `cluster/schema/manager.go:347-423` + new `dangerousVectorConfigChange` helper | block compression-enable / new named vector |
| UpdateProperty guard | `cluster/schema/manager.go:520-550` + `disablesAnyIndex` helper | block index disable |
| UpdateTenants guard | `cluster/schema/meta_class.go:449-565` | fix node-id bug, block freeze/unfreeze/COLD as partial error |
| Dynamic upgrade defer | `adapters/repos/db/vector_index_queue.go` (`checkCompressionSettings` guard) | defer upgrade/compression when movement active |
| Movement query | `Shard.AnyActiveMovement()` in `shard.go` (nil-guards incl. `replicationFSM`); `+HasActiveReplicationForShard` on `cluster/replication/types.ReplicationFSMReader` (+ regen mock) | reuse the DB's existing `replicationFSM` — no new injection, no `DB` wrapper, no `configure_api.go` wiring |
| Compression flag | `adapters/repos/db/vector/hnsw/config_update.go:176` (`compressThenCallback`) + `upgradableIndexer` iface | `hnsw.compressing atomic.Bool`; `UpgradeInProgress()` returns it (spans async-on + async-off) |
| Upgrade flag | `adapters/repos/db/vector/dynamic/index.go:502-516` + `upgradableIndexer` iface | `dynamic.upgrading` flag; `UpgradeInProgress()` = upgrading OR inner-HNSW compressing |
| Reverse gate | `adapters/repos/db/shard_backup.go:37-48` + `structuralVectorOpInFlight` | refuse transfer when busy |
| Sentinel + gRPC | new `ErrShardBusyStructuralOp`; `.../grpc/file_replication_service.go` | distinct gRPC code |
| Non-counting retry | `cluster/replication/consumer.go:412-450` + `isShardBusyError` | don't count busy refusals toward MaxErrors |

---

## Testing & verification

**Unit (table-driven, prefer per-package):**
- `shard_replication_fsm` — `HasActiveReplicationFor{Shard,Collection}` across states
  (REGISTERED/HYDRATING/…/READY/CANCELLED, `ShouldDelete` variants).
- **Regression for the divergence bug** — `meta_class` `UpdateTenants` HOT→COLD with an active
  movement is blocked **identically regardless of which node applies** (parametrize the
  applying `nodeID` over {source, target, uninvolved, leader}). Fails before Part 1, passes
  after.
- `dangerousVectorConfigChange` — compression `false→true` (PQ/BQ/SQ/RQ) blocked; new named
  vector blocked; `ef`/`efMin`/BM25/stopwords/RF/autoTenant allowed; compression already-on
  (no change) allowed.
- `disablesAnyIndex` — filterable/searchable/rangeable `true→false` blocked; enabling/adding
  allowed.
- `UpdateTenants` — freeze, unfreeze, HOT→COLD blocked during movement (partial error);
  →HOT/ACTIVE allowed.
- `BeforeSchedule` — upgrade deferred when movement active; proceeds when not (and on a later
  tick after movement clears).
- `HaltForTransfer` — refuses with `ErrShardBusyStructuralOp` when any index reports
  `UpgradeInProgress()` (HNSW compressing — async-on **and** async-off — or dynamic upgrading);
  passes otherwise. Add a case per async mode so the async-on (queue-driven) compression window is
  not silently uncovered.
- **Regression for auto-cancel** — N>50 consecutive busy refusals keep the op non-terminal and
  do **not** trigger `CancelReplication` (Part 4c).
- **Red test** for Part 5 (schemaOnly replay leaves index uncompressed vs schema).

**Integration / e2e (testcontainers; per-package; pre-build image per CLAUDE.md):**
- Start movement, attempt enable-compression / disable-index / freeze → rejected with the
  movement message; same op succeeds once movement reaches READY.
- Enable compression first (long-running), then start a movement → movement waits in
  HYDRATING (visible in `GET /replication/<uuid>` status, error count not climbing toward 50),
  then completes after compression finishes; both replicas converge (identical query results).
- Dynamic index near threshold + active movement → no upgrade until movement completes;
  source/target file lists stay consistent.

**Linters (required at end):** `golangci-lint run ./...` and `./tools/linter_go_routines.sh`.

## Least-certain items to validate during implementation
- Exact `parsedUpdate`/`ParseClassUpdate` return type and the cleanest `compressionEnabled`
  type-assertion target (confirm `hnswent.UserConfig` exposure of `*.Enabled`).
- Whether `meta_class.go`'s `replicationFSM` interface is the same declaration as
  `manager.go`'s (one vs two edits) and the full mock-regeneration surface.
- Best injection point for `shardHasActiveMovement()` from the queue (shard back-ref vs
  injected callback) without import cycles between `adapters/repos/db` and `cluster`.
- `ForEachVectorIndex` exists (`shard_accessors.go`: `func (s *Shard) ForEachVectorIndex(f
  func(targetVector string, index VectorIndex) error) error`, iterates named + legacy under
  `vectorIndexMu.RLock`) — use it directly in `structuralVectorOpInFlight`.
