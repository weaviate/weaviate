# Two-phase RAFT-coordinated swap (PREP barrier)

**Status:** proposal — pre-implementation, awaiting review.
**Branch:** `prep-swap-barrier` (off `runtime-reindex`).
**Target:** independent follow-up PR into `main`, lands after weaviate/weaviate#11326.
**Tracks:** weaviate/0-weaviate-issues#225 (Etienne directive at
22:30Z 2026-05-17). Supersedes the per-shard tokenization overlay's
implicit "eventual cross-replica consistency" tradeoff.
**Breaking change:** yes — explicitly authorized for the pre-v1.38
window. No backward-compatibility mechanisms.

## 1. Problem statement (verified)

Today's `OnGroupCompleted` runs PREP → OVERLAY → ATOMIC SWAP linearly
on each node, on each node's own scheduler-tick timeline. The
only RAFT coordination point is `RecordPostCompletionAck`, which
fires *after* each node's local swap completes. As a result:

- Different nodes' `OnGroupCompleted` fire at slightly different
  wall-clock times (scheduler-tick jitter + per-node PREP variance).
- Each node's PREP duration is proportional to the migrated bucket
  size. At billion-scale, PREP can take **minutes** (FlushAndSwitch +
  PrependSegmentsFromBucket + ShutdownBucket against a multi-GB
  bucket is disk-I/O-bound).
- During that minutes-long stagger, different replicas of the same
  shard hold different bucket states. Single-shard queries land on
  one replica or another via the per-query consistency level; the
  user observes mixed-tokenization (or mixed-bucket-presence) results
  for the duration of the stagger window.
- The per-shard tokenization overlay (from #216) only closes the
  per-node bucket-vs-schema window. It does **not** address the
  cross-replica stagger because every node's overlay is set
  per-node and only when *that* node has swapped.

The 3×3 test setup (3 shards, 3 nodes, RF=3) on which the system was
validated hides this failure mode: every node holds a full replica of
every shard, so every query can be served entirely from the local
node and the cross-node stagger is invisible to client-side
consistency. As soon as any sharding placement requires cross-node
query traversal (e.g. 5 shards on 3 nodes with RF=1; sharded MT;
billion-scale clusters where shards must be distributed), the
stagger surfaces.

Per Etienne: "minutes are NEVER acceptable, which we all agreed on
on #216."

## 2. Target architecture

A two-phase RAFT-coordinated swap. The cluster-wide synchronization
point moves from *after* the swap (today's `RecordPostCompletionAck`)
to *between* PREP and SWAP — so SWAP happens on every node only
after every node has finished PREP.

### Phase split

```
                   ┌─────────────────────────────────────────┐
                   │ STARTED — units running reindex iter.   │
                   └────────────────────┬────────────────────┘
                                        │  Every UnitStatus terminal
                                        │  (AllUnitsTerminal === true)
                                        │  Manager FSM transition (RAFT)
                                        ▼
                   ┌─────────────────────────────────────────┐
                   │ PREPARING — per-node PREP running        │
                   │ (parallel, minutes possible)             │
                   └────────────────────┬────────────────────┘
                                        │  Per node: PREP done
                                        │  ⇒ RecordPrepCompleteAck (RAFT)
                                        │
                                        │  Manager FSM: all expected
                                        │  PrepAcks landed (success)
                                        │  ⇒ transition (RAFT)
                                        ▼
                   ┌─────────────────────────────────────────┐
                   │ SWAPPING — barrier lifted; per-node swap │
                   │ (atomic, microseconds; per-node ack)     │
                   └────────────────────┬────────────────────┘
                                        │  Per node: SWAP done
                                        │  ⇒ RecordPostCompletionAck (RAFT)
                                        │
                                        │  Manager FSM: all expected
                                        │  SwapAcks landed (success)
                                        │  ⇒ MarkTaskFinalized (RAFT)
                                        ▼
                   ┌─────────────────────────────────────────┐
                   │ FINISHED — schema flip (OnTaskCompleted) │
                   └─────────────────────────────────────────┘
```

### Cross-replica consistency invariant

For semantic migrations:

- During **PREPARING**: every node still serves OLD bucket + OLD
  schema. PREP work is double-write callbacks (writing to ingest
  bucket alongside live main) + reindex bucket flush + prepend into
  ingest. Live queries unaffected.
- During **SWAPPING**: each node's per-node swap moment is bounded
  by RAFT propagation latency (10s of ms typical, sub-second
  worst-case) — NOT by per-node PREP duration. The cross-replica
  stagger is reduced from "max-PREP-duration" to
  "RAFT-commit-propagation + scheduler-tick latency", which Etienne
  explicitly accepted.
- After **FINISHED**: cluster-wide schema flip via existing
  `OnTaskCompleted` path. All nodes converge on NEW bucket + NEW
  schema; per-shard tokenization overlay clears.

The per-shard tokenization overlay is **still needed** to close the
per-node window between local SwapBucketPointer commit and
cluster-wide schema flip commit (the same gap #216 originally fixed,
now narrowed to the RAFT-propagation window between
`MarkTaskFinalized` and `OnTaskCompleted`'s schema flip — still
non-zero, still single-node).

### Failure handling

Per phase, what fails and what happens:

- **PREP fails on any node** → that node's `RecordPrepCompleteAck`
  carries `Success=false`. Manager FSM transitions task to `FAILED`
  immediately (matches today's `RecordPostCompletionAck` failure
  semantics). No node proceeds to SWAP. The schema flip is skipped.
  Local cleanup happens via the existing
  `CleanStalePartialReindexState` path on next submit or restart.
- **SWAP fails on any node** → that node's `RecordPostCompletionAck`
  carries `Success=false`. Manager FSM transitions task to `FAILED`.
  Schema flip skipped. **However:** other nodes may have already
  swapped successfully — those replicas now have NEW bucket while
  the schema flag stays at OLD. The per-shard tokenization overlay
  on those nodes keeps queries correct on the swapped replicas; the
  un-swapped nodes serve OLD. **This is unchanged from today's
  behavior** — the SWAP step itself is per-node and not coordinated
  even in the new design (only PREP-completion is coordinated). The
  recovery path is: operator inspects the failed task, decides
  whether to re-submit (which triggers a full new task lifecycle)
  or to manually intervene.

The above means the new design **shrinks but does not eliminate**
cross-replica disagreement. The remaining window is bounded by:

- RAFT-propagation of `MarkSwappingTransition` to all nodes
- Scheduler-tick latency on each node observing SWAPPING
- The atomic-swap subwindow itself (microseconds per #216 budget)
- The post-swap RAFT ack roundtrip

Total: typically <1s, bounded sub-second worst-case under healthy
RAFT. That matches Etienne's "10s of ms, seconds to minutes is not"
acceptance criterion.

## 3. FSM changes — `cluster/distributedtask/`

### 3.1 New `TaskStatus` values

```go
const (
    TaskStatusStarted   TaskStatus = "STARTED"
    TaskStatusPreparing TaskStatus = "PREPARING"  // NEW
    TaskStatusSwapping  TaskStatus = "SWAPPING"   // NEW
    TaskStatusFinished  TaskStatus = "FINISHED"
    TaskStatusCancelled TaskStatus = "CANCELLED"
    TaskStatusFailed    TaskStatus = "FAILED"
)
```

`TaskStatusFinalizing` **removed**. (Breaking change — authorized.
Format-only migrations never used it meaningfully; semantic ones
transit PREPARING → SWAPPING instead.) Existing callers that test
`task.Status == TaskStatusFinalizing` need to either:

- Test `IsActive(task.Status)` for "not terminal yet"
- Test `task.Status == TaskStatusSwapping` for the previous "swap
  is in flight" semantic
- Test `IsCoordinationPhase(task.Status)` (helper) for
  "PREPARING or SWAPPING" (any post-units, pre-finished phase)

### 3.2 New RAFT command + FSM apply

`RecordDistributedTaskPrepCompleteAckRequest` — same shape as
`RecordDistributedTaskPostCompletionAckRequest`:

```go
type RecordDistributedTaskPrepCompleteAckRequest struct {
    Namespace          string
    Id                 string
    Version            uint64
    NodeId             string
    Success            bool
    Error              string
    AckedAtUnixMillis  int64
}
```

`Manager.RecordPrepCompleteAck(c *api.ApplyRequest) error` — mirror
of the existing `RecordPostCompletionAck`:

- Idempotent at the per-(task, node) layer.
- Silent on terminal tasks (drop late acks).
- `Success=false` flips PREPARING → FAILED immediately.
- `Success=true` accumulates; when all expected acks landed,
  transition PREPARING → SWAPPING.

Stored on `Task` as a new field `PrepCompletionAcks
map[string]PostCompletionAck` (reuse the type — same shape).

### 3.3 FSM transition rules

| From | To | Trigger | New? |
|---|---|---|---|
| (none) | STARTED | AddTask | existing |
| STARTED | PREPARING | `AllUnitsTerminal && noUnitFailed` (semantic migrations only) | NEW — was STARTED → FINALIZING |
| STARTED | SWAPPING | `AllUnitsTerminal && noUnitFailed` (format-only migrations) | NEW — format-only skips PREP-barrier |
| STARTED | FAILED | `AnyUnitFailed` (during STARTED) | existing |
| PREPARING | SWAPPING | All expected `PrepCompleteAcks` landed with `Success=true` | NEW |
| PREPARING | FAILED | Any `PrepCompleteAck` with `Success=false` | NEW |
| SWAPPING | FINISHED | `MarkTaskFinalized` once all `PostCompletionAcks` landed with `Success=true` | adapted |
| SWAPPING | FAILED | Any `PostCompletionAck` with `Success=false` | existing |
| any active | CANCELLED | external cancel | existing |

Helper predicates:

```go
func (t TaskStatus) IsTerminal() bool {
    return t == TaskStatusFinished || t == TaskStatusFailed || t == TaskStatusCancelled
}
func (t TaskStatus) IsCoordinationPhase() bool {
    return t == TaskStatusPreparing || t == TaskStatusSwapping
}
```

### 3.4 Format-only vs semantic migration branching

Format-only migrations (`enable-rangeable`, `repair-*`,
`roaring-set refresh`) do not need a barrier because:

- Each shard runs its full lifecycle (PREP + SWAP) inside its own
  `RunOnShard` independently.
- There is no cluster-wide schema flip to coordinate.
- Cross-replica disagreement during format-only migrations is
  bounded by the per-shard local swap time, not by per-node PREP
  duration — replicas of the same shard live on the same processing
  pipeline.

For format-only, FSM transitions skip PREPARING:
`STARTED → SWAPPING → FINISHED` (or `STARTED → FINISHED` if we
collapse SWAPPING for format-only — TBD in implementation,
documented preference is to keep SWAPPING uniformly for status
observability even if format-only nodes pass through it in zero
time).

`Provider` interface needs a way to declare "this task is
semantic" so the FSM knows which transition to use. Add to
`UnitAwareProvider`:

```go
type UnitAwareProvider interface {
    Provider
    OnGroupCompleted(task *Task, groupID string, localGroupUnitIDs []string) error
    OnSwapRequested(task *Task, groupID string, localGroupUnitIDs []string) error  // NEW
    OnTaskCompleted(task *Task)
    NeedsPrepBarrier(task *Task) bool  // NEW — returns true for semantic migrations
}
```

`NeedsPrepBarrier` is read by the Manager's FSM at the
`AllUnitsTerminal` transition to decide STARTED → PREPARING vs
STARTED → SWAPPING. The Manager doesn't directly call the provider
(FSM-determinism requires the predicate to be pure-on-payload) — so
in practice `NeedsPrepBarrier` is implemented by parsing the task
payload and asking "is this a semantic migration?", same shape as
the existing `IsSemanticMigration(payload.MigrationType)` predicate
on `db.ReindexProvider`. The interface lets non-reindex providers
opt in to the barrier shape too.

## 4. Scheduler changes — `cluster/distributedtask/scheduler.go`

Today's per-tick loop (around line 510-655):

1. For each task:
   1. Per-group: if `AllGroupUnitsTerminal || postStarted`, fire
      `OnGroupCompleted`. Capture per-group error.
   2. Per-node: if all local groups fired, `RecordPostCompletionAck`.
   3. Per-task: if past STARTED, fire `OnTaskCompleted`.
   4. If FINALIZING + all acks landed, `MarkTaskFinalized`.

New per-tick loop:

1. For each task:
   1. **PREP phase** — if status is PREPARING and per-group hasn't
      fired locally, fire `OnGroupCompleted` (does PREP only).
      Capture per-group error.
   2. **PREP ack** — if all local groups' PREP fired, emit
      `RecordPrepCompleteAck`. Aggregate success across local
      groups. If failure, ack with `Success=false`.
   3. **SWAP phase** — if status is SWAPPING and per-group hasn't
      fired-for-swap locally, fire `OnSwapRequested` (does OVERLAY
      + ATOMIC SWAP + post-swap tidy). Capture per-group error.
   4. **SWAP ack** — if all local groups' SWAP fired, emit
      `RecordPostCompletionAck` (existing).
   5. **MarkFinalized** — if SWAPPING + all SwapAcks landed,
      `MarkTaskFinalized` → FINISHED.
   6. **OnTaskCompleted** — fire on past-STARTED (existing).

Per-(task, node) tracking maps add a parallel `prepCallbackFired`
and `swapCallbackFired` (renaming the existing
`groupCallbackFired` accordingly), plus `prepAckEmitted` and
keeping `postCompletionAckEmitted` (which becomes the "swap ack
emitted" mark).

Context-cancellation handling (rolling-restart) for each phase
follows the existing pattern: on `context.Canceled`, drop the
"fired" mark, skip the ack, let recovery re-fire on next tick.

## 5. Provider changes — `adapters/repos/db/reindex_provider.go`

### 5.1 Split `OnGroupCompleted`

Today's `OnGroupCompleted` is a sequential PREP → OVERLAY → SWAP →
tidy block. Split:

```go
// OnGroupCompleted (under the new design) runs ONLY PREP +
// per-task RunPrepareOnShard. No overlay, no swap.
func (p *ReindexProvider) OnGroupCompleted(task *Task, groupID string, localIDs []string) error {
    // ... unchanged identification of local units / tasks ...
    for _, reindexTask := range unitTasks {
        if err := reindexTask.RunPrepareOnShard(ctx, shard); err != nil { ... }
    }
    return nil
}

// OnSwapRequested (NEW) fires after the cluster-wide PREP barrier
// lifts. Sets the overlay, does the atomic swap.
func (p *ReindexProvider) OnSwapRequested(task *Task, groupID string, localIDs []string) error {
    // ... unchanged identification ...
    overlayWasSet := maybeSetTokenizationOverlayPreSwap(shard, payload)
    for _, reindexTask := range unitTasks {
        if err := reindexTask.RunSwapOnShard(ctx, shard); err != nil { ... }
    }
    maybeClearTokenizationOverlayOnAllFailed(shard, payload, overlayWasSet, anySwapped)
    return nil
}

// NeedsPrepBarrier reports whether this task uses the PREP barrier.
// Today: equivalent to IsSemanticMigration(payload.MigrationType).
func (p *ReindexProvider) NeedsPrepBarrier(task *Task) bool {
    payload, err := p.loadPayload(task)
    if err != nil { return false }  // unparseable → skip barrier; let FSM-fail path handle
    return IsSemanticMigration(payload.MigrationType)
}
```

The rehydrate-on-restart branch in current `OnGroupCompleted` (when
the cache is empty post-restart) needs to split too: PREP-rehydrate
in the new `OnGroupCompleted`, SWAP-rehydrate would just be the
existing in-cache instances since the cache survives a single tick
(both fire in the same scheduler instance).

### 5.2 Recovery handling

`reindex_recovery.go::DiscoverInFlightReindexTasks` already
classifies migrations by sentinel-file presence (`started.mig`,
`reindexed.mig`, `tidied.mig`). Add a new classification: if
`prepended.mig + merged.mig` are present but `swapped.mig` is not
AND the cluster task is at `PREPARING` or `SWAPPING`, this is a
post-restart resume in the new barrier window.

Recovery actions:
- Task at PREPARING: re-fire local PREP (idempotent — `RunPrepareOnShard` on already-merged tasks short-circuits). Re-emit PrepAck.
- Task at SWAPPING: re-fire local SWAP. Re-emit PostCompletionAck.

This matches the existing recovery model — both phases are
individually idempotent.

## 6. Crash safety analysis

The new design adds two FSM states (PREPARING, SWAPPING) and one
new ack (`RecordPrepCompleteAck`). Crash boundaries:

| Crash point | What's persisted | Recovery |
|---|---|---|
| During STARTED (units running) | `started.mig`, per-unit progress in RAFT | Existing — scheduler re-fires StartTask |
| Between AllUnitsTerminal and STARTED→PREPARING RAFT commit | RAFT-replicated unit state | Existing — next tick sees AllUnitsTerminal, retries transition |
| During PREPARING (PREP running locally) | per-task sentinels (`prepended.mig`, `merged.mig`) | Local recovery: `RunPrepareOnShard` is idempotent — re-fires from sentinel state |
| Between local PREP done and PrepAck commit | per-task sentinels written | Next tick: re-emit PrepAck (idempotent) |
| Between PrepAck commit and PREPARING→SWAPPING transition | RAFT-replicated ack | Existing-equivalent: next tick observes all-acks-landed condition, retries transition |
| During SWAPPING (local SWAP running) | per-prop sentinels (`swapped.mig`, `tidied.mig`) | Local recovery: `RunSwapOnShard` is idempotent — re-fires from sentinel state |
| Between local SWAP done and SwapAck commit | per-prop sentinels written | Next tick: re-emit SwapAck (idempotent) |
| Between SwapAck commit and SWAPPING→FINISHED transition | RAFT-replicated ack | Existing — next tick triggers MarkTaskFinalized |
| Between FINISHED and OnTaskCompleted (schema flip) | RAFT-replicated status | Existing — OnTaskCompleted is RAFT-idempotent |
| Between schema flip and overlay clear | RAFT-committed schema | Existing — defensive overlay self-clear in `Shard.TokenizationFor` |

No new sentinel files needed. The existing per-task and per-prop
`.migrations/<dir>/*.mig` sentinels cover both PREP and SWAP phases
because the existing code already wrote them around the boundaries
that we're now using as the per-phase boundaries.

## 7. Status reporting impact

### 7.1 GET /v1/schema/{class}/indexes — per-property status

User-visible status enum stays at
`["ready", "indexing", "pending", "failed", "cancelled"]` —
client-tooling-stable. Mapping from new TaskStatus values:

| TaskStatus | IndexStatus.Status |
|---|---|
| STARTED (no progress yet) | `pending` |
| STARTED (with progress) | `indexing` |
| PREPARING | `indexing` (with progress 0.95 floor — visible "still working") |
| SWAPPING | `indexing` (with progress 0.99 floor — visible "almost done") |
| FINISHED | `ready` |
| FAILED | `failed` |
| CANCELLED | `cancelled` |

The "finalize-window override" in `mergeReindexStatus` (which
mapped a brief stale FINISHED → `indexing@100%` to bridge the
schema-flip-lag gap, with a 10s bound) becomes simpler: the
PREPARING and SWAPPING states now legitimately render as
`indexing`, so the brief gap that motivated the override no longer
exists.

### 7.2 DTM API — full status surface

Operators inspecting raw DTM state via the cluster API see the new
states. New `TaskStatus.String()` returns the literal values
("PREPARING", "SWAPPING"). Any UI/operator dashboards consuming the
DTM API need updating to render the new states — within Weaviate
itself, the only such consumer is `mergeReindexStatus` (handled
above) plus the optional `GET /v1/cluster/distributed-tasks`
endpoint if it exists (verify).

## 8. Breaking changes inventory

Explicit list of what breaks with no backward-compatibility shim:

1. **`TaskStatusFinalizing` constant removed.** Any external
   consumer comparing against `"FINALIZING"` (string-side) or
   `TaskStatusFinalizing` (Go-side) breaks. Internal callers
   (`reindex_conflict.go`, etc.) update to test against
   `TaskStatusPreparing || TaskStatusSwapping` — the
   `IsCoordinationPhase` helper centralizes this.
2. **`Provider.OnGroupCompleted` semantic change.** Now PREP-only
   for providers that opt into the barrier; previously was the
   full PREP+OVERLAY+SWAP. Format-only providers' behavior is
   preserved (they pass through SWAPPING with the swap inlined).
3. **New `UnitAwareProvider.OnSwapRequested` callback required.**
   Providers that opt into the barrier (return `true` from
   `NeedsPrepBarrier`) must implement.
4. **New `UnitAwareProvider.NeedsPrepBarrier` required.** Default
   implementation (returns `false`) preserves today's no-barrier
   behavior.
5. **`RecordDistributedTaskPrepCompleteAckRequest` RAFT message
   added.** Mixed-version cluster (some nodes pre-v1.38, some on
   v1.38) will fail because the older nodes don't know this RAFT
   command type. Acceptance: v1.38 ships as a hard cluster-wide
   minimum version for clusters running runtime-reindex. Operator
   procedure: upgrade ALL nodes before submitting any runtime
   reindex task.

## 9. Test plan

### 9.1 Unit tests

- `cluster/distributedtask/manager_test.go`:
  - `TestRecordPrepCompleteAck_FirstAckWins_Idempotent`
  - `TestRecordPrepCompleteAck_FailureFlipsToFailed`
  - `TestRecordPrepCompleteAck_AllAcksLanded_TransitionsToSwapping`
  - `TestRecordPrepCompleteAck_Silent_On_TerminalTask`
  - `TestFSMTransitions_FormatOnly_SkipsPreparing`
  - `TestFSMTransitions_Semantic_GoesThroughPreparing`
- `cluster/distributedtask/scheduler_multinode_test.go`:
  - Stagger test: 3 nodes, simulate 1s/3s/5s PREP durations, verify
    no node fires SWAP until all three PREP acks landed.

### 9.2 Acceptance tests

- `test/acceptance/reindex_multinode/prep_barrier_test.go` (new):
  - 5 shards / 3 nodes / RF=1 (per Etienne's explicit recipe).
    Submit `change-tokenization`. Query the affected property at
    high frequency throughout the migration. Verify: no
    cross-replica result divergence beyond the per-RAFT-propagation
    window (sub-second).
  - Same setup, kill the slow-PREP node mid-PREP. Verify task
    FAILS cleanly. Verify no other replica swapped (the load-bearing
    invariant — barrier holds).

### 9.3 Crash-safety tests

- Existing `test/acceptance/reindex_multinode/restart_matrix_test.go`
  + `finalizing_crash_test.go` need updates: the FINALIZING-window
  crash scenarios now have to cover PREPARING-window AND
  SWAPPING-window separately. Each per-phase crash recipe needs a
  cell.

### 9.4 Regression / pin tests

- Existing `test/acceptance/reindex_multinode/round_trip_test.go`
  (the #10675 pin) — must stay green; new design doesn't change
  the bucket-pointer-flip mechanism, only adds the coordination
  before it.
- `change_tokenization_*_test.go` family — same.

## 10. Implementation order

Each step independently `go build ./...` clean:

1. **FSM types** — add `TaskStatusPreparing`, `TaskStatusSwapping`,
   helper predicates. Remove `TaskStatusFinalizing`. All internal
   callers update to new predicates. Tests: `manager_test.go`
   transitions, `types_test.go` predicates.
2. **RAFT command + Manager apply** —
   `RecordDistributedTaskPrepCompleteAckRequest`, `Manager.RecordPrepCompleteAck`,
   `Task.PrepCompletionAcks` field. Tests: idempotency, terminal-task
   silence, failure-flip.
3. **Provider interface** — add `OnSwapRequested`, `NeedsPrepBarrier`
   to `UnitAwareProvider`. All call sites (mock providers, test
   doubles) update. Tests: scheduler_test.go provider-shape coverage.
4. **Scheduler tick split** — new per-tick flow with PREP-phase /
   SWAP-phase gating on TaskStatus. Update `scheduler_multinode_test.go`
   to exercise the staggered-PREP barrier.
5. **`ReindexProvider` split** — `OnGroupCompleted` becomes PREP-only;
   `OnSwapRequested` carries overlay + swap. `NeedsPrepBarrier`
   delegates to `IsSemanticMigration`. Provider-level tests:
   `reindex_provider_on_group_completed_test.go` updates +
   new `reindex_provider_on_swap_requested_test.go`.
6. **REST status mapping** — `mergeReindexStatus` updated for new
   states. `handlers_indexes_*_test.go` coverage updates. Remove
   the finalize-window override (no longer needed; PREPARING +
   SWAPPING legitimately render as `indexing`).
7. **Recovery** — `DiscoverInFlightReindexTasks` adds the new
   PREPARING/SWAPPING classifications. Tests:
   `reindex_provider_recovery_test.go`.
8. **Acceptance tests** — new `prep_barrier_test.go` in
   `reindex_multinode`. Existing tests updated for new TaskStatus
   values (mostly assertion-text changes).
9. **Documentation** —
   `docs/runtime-reindex.md` updated lifecycle diagram + new
   "PREP barrier" section. PR body covers breaking-changes,
   migration path, and the QA-verified cross-replica test case.

## 11. Open questions for review

- **Collapse SWAPPING for format-only?** Pro: keeps the state
  machine smaller for the common case. Con: less uniform status
  surface; format-only and semantic look different. Current
  preference: keep SWAPPING for both for observability uniformity
  (format-only nodes pass through it in microseconds).
- **`NeedsPrepBarrier` on the FSM side or the Provider side?**
  Today's design has FSM-determinism so the Manager doesn't call
  back into the Provider. `NeedsPrepBarrier` here is on the
  Provider — meaning the Manager has to be able to determine the
  needs-barrier-or-not from the task payload alone. Acceptable
  because we already have `IsSemanticMigration(payload.MigrationType)`
  on the reindex side. But: if a non-reindex namespace wants the
  barrier later, it needs to expose the same predicate. Could be
  cleaner to put a `NeedsBarrier bool` field directly on the Task
  payload at AddTask time, but that pushes the decision to the
  caller. **Preference:** task-payload field, set by the
  `ReindexProvider`'s task-creation path. Simpler FSM, cleaner
  separation. Will revisit during implementation.
- **What about the per-shard tokenization overlay — still
  needed?** Yes (see §2 "Cross-replica consistency invariant" —
  still closes the per-node window between local SwapBucketPointer
  and cluster-wide schema flip). But the gap it covers is smaller
  in the new design; the overlay's lifetime is bounded by the
  RAFT-propagation latency from `MarkTaskFinalized` to the local
  `OnTaskCompleted` tick, which is sub-second.

## 12. Out of scope

- Vector indexes (HNSW / flat / dynamic) — separate subsystem,
  unaffected by this design.
- Backup interaction with reindex — already broken on
  `runtime-reindex`, tracked at weaviate/0-weaviate-issues#215.
- The remaining #227 gaps (rebuild-searchable-from-objects;
  `algorithm` field) — orthogonal, separate PR.

---

*Status as of design draft: ready for review. Implementation
gated on this doc landing + QA Claude's explorative-test
confirmation of the bug repro on 5/3/RF=1 setup. Will start
implementation immediately on either signal arriving.*
