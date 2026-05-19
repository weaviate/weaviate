# Runtime property reindex

Change inverted-index configuration on a live class — without restarting
the cluster, without losing writes that arrive during the rebuild, and
without dropping query availability on the affected property.

This document is the entry-point for anyone reviewing or extending the
feature. It covers the full surface area: REST API, architecture layer
by layer, the migration-strategy catalogue, crash-safety contract,
concurrency model, multi-tenancy, tokenization overlay, the
deferred-finalize / per-migration generation design that closes the
load-bearing #10675 family of data-loss bugs, and the test map.

The package-level godocs of the touched packages are the per-symbol
source of truth; this doc is the navigable overview that ties them
together. If a section here disagrees with a godoc in source, the
godoc wins — and that's a bug in this doc.

## 1. Overview

A runtime reindex rebuilds one or more inverted-index buckets on a
property in place while the class stays open for reads and writes. The
typical journeys it unlocks:

- Change a text property's tokenization (`word` → `trigram`, etc.)
  with no downtime — both the searchable and filterable buckets are
  rewritten on every replica and the schema's `tokenization` field is
  flipped only after every shard has committed its swap.
- Add a missing inverted index after the fact: `enable-filterable`,
  `enable-searchable`, `enable-rangeable`.
- Repair a bucket suspected of corruption: `repair-filterable`,
  `repair-searchable` (which is also the Map → Blockmax format
  upgrade), `repair-rangeable`.
- Cancel an in-flight migration; the cluster cleans up the partial
  state and the property is back to its pre-submit on-disk shape.

The whole feature is built on top of three substrates:

- The **LSM bucket-swap primitives** ([`adapters/repos/db/lsmkv/store.go`](../adapters/repos/db/lsmkv/store.go)
  `SwapBucketPointer` + `FinalizeBucketSwap`) — the atomic in-memory
  pointer flip and its deferred on-disk counterpart.
- The **Distributed Task Manager (DTM)** ([`cluster/distributedtask/`](../cluster/distributedtask/))
  — RAFT-backed task state, per-unit assignment, group barriers,
  per-node `PREPARING` and `SWAPPING` coordination states, two-phase
  PrepComplete + PostCompletion ack barriers (§6.3).
- The **schema FSM mutation guard** — rejects external mutations on
  classes / properties / tenants while a reindex is in flight, so the
  bucket↔schema invariant cannot be broken by a concurrent
  `DeleteClass` or `UpdateProperty`.

The reindex feature itself is the orchestration that ties these
together into one user-visible verb on `PUT
/v1/schema/{class}/indexes/{property}`.

## 2. REST surface

### `PUT /v1/schema/{class}/indexes/{property}`

Submit a migration. Body shape selects which one:

| Body | Migration type | Notes |
|---|---|---|
| `{"searchable":{"enabled":true,"tokenization":"word"}}` | `enable-searchable` | Creates a Blockmax bucket, flips `IndexSearchable=true` + `Tokenization` on completion. Requires `text`/`text[]`. |
| `{"filterable":{"enabled":true}}` | `enable-filterable` | Creates a RoaringSet bucket, flips `IndexFilterable=true`. |
| `{"rangeable":{"enabled":true}}` | `enable-rangeable` | Creates a RoaringSetRange bucket, flips `IndexRangeFilters=true`. Numeric types only (`int`, `number`, `date`). |
| `{"searchable":{"tokenization":"trigram"}}` | `change-tokenization` | Rewrites BOTH searchable and filterable buckets when both exist. |
| `{"filterable":{"tokenization":"word"}}` | `change-tokenization-filterable` | Filterable-only retokenize variant. Use when the property has no searchable index. |
| `{"searchable":{"rebuild":true}}` | `repair-searchable` | Rebuild the searchable bucket. Also serves as the Map → Blockmax upgrade — `OnMigrationComplete` flips the class-level `UsingBlockMaxWAND` flag once every searchable property has been rebuilt. |
| `{"filterable":{"rebuild":true}}` | `repair-filterable` | RoaringSet refresh. |
| `{"rangeable":{"rebuild":true}}` | `repair-rangeable` | RoaringSetRange rebuild. |
| `{"<type>":{"cancel":true}}` | (cancel verb) | Cancels the in-flight task on `(class, property, indexType)`. 404 if no STARTED task matches. |

Query parameters:

- `?tenants=t1,t2` — scope to named tenants on a multi-tenant class.
  Required only when the operator wants a subset. Rejected on
  single-tenant classes. Rejected on semantic migrations
  (`change-tokenization*`) because the cluster-wide schema flip cannot
  be sub-scoped — all tenants must migrate together.

Response shapes:

- `202 Accepted` with the new task ID. Poll `GET /indexes` (or DTM API)
  to observe progress.
- `400 Bad Request` — validation failure with a structured next-step
  hint (e.g. "property X has no searchable index; use
  `{filterable:{tokenization:...}}` to retokenize the filterable bucket").
- `404 Not Found` — class or property doesn't exist; cancel target
  doesn't exist.
- `409 Conflict` — an in-flight task already touches this property.
  The error names the offending task ID and migration type.
- `429 / 503` — per-collection in-flight cap reached (default 32) or
  cluster-service unavailable.

### `DELETE /v1/schema/{class}/properties/{property}/index/{indexName}`

Drop a configured inverted index. `indexName` is one of `filterable`,
`searchable`, `rangeFilters`. Flips the corresponding schema flag to
false and scrubs all migration sentinels + sidecar buckets so a
subsequent re-enable starts from a clean slate. Subject to the same
MutationGuard as `UpdateProperty` — rejected while a reindex on this
property is in flight.

### `GET /v1/schema/{class}/indexes`

Per-property, per-index-type snapshot:

```json
{
  "collection": "Article",
  "properties": [{
    "name": "body",
    "dataType": "text",
    "indexes": [
      { "type": "filterable", "status": "ready", "tokenization": "word" },
      { "type": "searchable", "status": "indexing", "progress": 0.42,
        "tokenization": "word", "targetTokenization": "trigram",
        "algorithm": "blockmax" }
    ]
  }]
}
```

Status values: `ready`, `pending`, `indexing`, `failed`, `cancelled`.
The status is synthesized in `mergeReindexStatus` from a snapshot of
the active DTM task list crossed with the live schema flags. A short
**finalize-window override** lets a FINISHED-but-schema-not-yet-flipped
entry render as `indexing@100%` for up to 10s (the bound, see
`finalizeWindowMax`); without that override the UI would briefly show
"None" between task FINISHED and the schema-flag flip, which was the
user-visible face of weaviate/weaviate#10675.

Read-access is gated on `READ` of `CollectionsMetadata`; `PUT`/`DELETE`
require the stronger `UPDATE` on `Collections`.

## 3. End-to-end lifecycle

The diagram below tracks state across **two independent status
surfaces** — keep the distinction in mind reading top-to-bottom:

- **`UnitStatus`** (`PENDING → IN_PROGRESS → COMPLETED`/`FAILED`) —
  per-unit. One unit = one shard × one node. `COMPLETED` means *this
  single replica* has finished its piece of the reindex work; the
  per-shard swap and cluster-wide schema flip are still ahead.
- **`TaskStatus`** — per-task aggregate. The transition sequence
  depends on whether the task opts into the PREP barrier
  (`NeedsPreparationBarrier`, set automatically for semantic migrations by
  the submit handler; full mechanics in §6.3):
  - **Semantic migrations** (`change-tokenization`,
    `enable-searchable`, `enable-filterable`):
    `STARTED → PREPARING → SWAPPING → FINISHED`.
    `PREPARING` and `SWAPPING` are both reached only after every
    unit across the cluster is at terminal status. The FSM gates
    `PREPARING → SWAPPING` on every node's `PreparationCompleteAck`
    landing successfully, and gates `SWAPPING → FINISHED` on
    every node's `PostCompletionAck` landing successfully.
  - **Format-only migrations** (`enable-rangeable`, `repair-*`,
    `roaring-set refresh`): `STARTED → SWAPPING → FINISHED`.
    `PREPARING` is skipped because there is no cross-replica
    state alignment to bound — each shard's `RunOnShard`
    completes the full lifecycle locally and there is no
    cluster-wide schema flip.
  - Either path may terminate at `FAILED` (any per-node ack
    `Success=false`) or `CANCELLED` (operator-initiated).
    `FINISHED` is the correct signal for "fully done".

These are different Go types in the source —
[`cluster/distributedtask/types.go`](../cluster/distributedtask/types.go)
defines `UnitStatus` and `TaskStatus` distinctly — so the
COMPLETED-before-PREPARING ordering in the diagram is not a sequence
on the same field; it's the terminal value of the per-unit field
preceding a transition on the per-task field. Annotations
(`← UnitStatus` / `← TaskStatus`) mark which surface each box lives on.

```
                ┌────────────────────────────────────────────────────────┐
                │             PUT /v1/schema/{class}/indexes/{p}         │
                └──────────────────────────┬─────────────────────────────┘
                                           │
        ┌──────────────────────────────────▼─────────────────────────────┐
        │  REST handler (adapters/handlers/rest/handlers_indexes.go)    │
        │   • Per-(class, prop) ReindexSubmitLock                       │
        │   • Validate body, classify migration type, dispatch          │
        │   • checkReindexConflict (read-side mirror of FSM check)      │
        │   • Pre-submit CleanStalePartialReindexState                  │
        │   • AddDistributedTask(namespace="reindex", payload, units)   │
        └──────────────────────────────────┬─────────────────────────────┘
                                           │  RAFT-replicated AddTask
        ┌──────────────────────────────────▼─────────────────────────────┐
        │  cluster/distributedtask FSM (Manager)              ← TaskSt. │
        │   • ConflictDetector.CheckConflict (FSM-deterministic)        │
        │   • Append task to FSM state at STARTED                       │
        └──────────────────────────────────┬─────────────────────────────┘
                                           │  Scheduler tick on each node
        ┌──────────────────────────────────▼─────────────────────────────┐
        │  ReindexProvider.StartTask (per node, per task)     ← UnitSt. │
        │   • processOneUnit per local unit, in a bounded worker pool   │
        │     (per unit: PENDING → IN_PROGRESS → COMPLETED on success)  │
        │   • Build ShardReindexTaskGeneric per (strategy, unit)        │
        │   • persistRecoveryRecord (payload.mig)                       │
        │   • RunReindexOnlyOnShard — iterate objects, write to         │
        │     __reindex_<N>/ bucket; install double-write callbacks     │
        │   • markReindexed → UnitStatus = COMPLETED  ← per-Unit status │
        └──────────────────────────────────┬─────────────────────────────┘
                              All units terminal across the cluster
                                           │
                          (semantic only — format-only skips to SWAPPING)
                                           │
                                  ┌────────▼────────┐
                                  │ PREPARING       │ ← TaskStatus
                                  └────────┬────────┘    (semantic only;
                                           │             format-only skips)
                                           │  scheduler fires per-node
        ┌──────────────────────────────────▼─────────────────────────────┐
        │  Provider.OnGroupCompleted (per-node, semantic only, barrier) │
        │   PHASE A — PREP per local shard, idempotent at merged.mig:   │
        │    1. PREP (background): FlushAndSwitch + Prepend             │
        │      (disk-I/O proportional to bucket size — minutes at       │
        │       billion-scale; this is what the barrier closes)         │
        │   RecordPreparationCompleteAck(success bool) — RAFT (per node)       │
        └──────────────────────────────────┬─────────────────────────────┘
                  Every node's PreparationCompleteAck landed (success on all)
                                           │   FSM gates the transition
                                  ┌────────▼────────┐
                                  │ SWAPPING        │ ← TaskStatus
                                  └────────┬────────┘   (the barrier
                                           │            bounded cross-node
                                           │            swap window is
                                           │            now ≈ tens of ms)
                                           │  scheduler fires per-node
        ┌──────────────────────────────────▼─────────────────────────────┐
        │  Provider.OnSwapRequested (per-node, semantic only, barrier)  │
        │   PHASE B — OVERLAY + SWAP per local shard:                   │
        │    2. OVERLAY SET: per-shard tokenization resolver            │
        │    3. ATOMIC SWAP: SwapBucketPointer per prop (microseconds)  │
        │    + post-atomic inline tidy (Shutdown + rename old → backup) │
        │    + OnMigrationComplete (per-strategy hook)                  │
        │   RecordPostCompletionAck(success bool) — RAFT (per node)     │
        │                                                                │
        │  (Format-only path: Provider.OnGroupCompleted runs the       │
        │   inline PREP+OVERLAY+SWAP body in a single callback; no      │
        │   PreparationCompleteAck barrier; SWAPPING fires directly from       │
        │   AllUnitsTerminal.)                                          │
        └──────────────────────────────────┬─────────────────────────────┘
              Every node's PostCompletionAck landed (success on all)
                                           │
        ┌──────────────────────────────────▼─────────────────────────────┐
        │  Provider.OnTaskCompleted (per-node, semantic only)           │
        │   flipSemanticMigrationSchema — RAFT UpdatePropertyInternal   │
        │   (idempotent: every node fires, first commit wins)           │
        │   ClearTokenizationOverlay on every local shard               │
        └──────────────────────────────────┬─────────────────────────────┘
                                           │  scheduler marks finalized
                                  ┌────────▼────────┐
                                  │ FINISHED        │ ← TaskStatus
                                  └─────────────────┘   (operator-visible
                                           │            "fully done" signal)
                                           │  next process startup
        ┌──────────────────────────────────▼─────────────────────────────┐
        │  FinalizeCompletedMigrations (pre-LSM init)                   │
        │   For each (prop, indexType) with tidied.mig:                 │
        │     rename property_p_<idx>__<ingestSuffix>_<N>/              │
        │       → property_p_<idx>/                                     │
        │     remove backup dirs, lower-gen sidecars, tracker dir       │
        └────────────────────────────────────────────────────────────────┘
```

Format-only migrations (`enable-rangeable`, `repair-*`,
`roaring-set refresh`) skip the OnGroupCompleted barrier — each shard
runs the full lifecycle inside its own `RunOnShard` and there is no
cluster-wide schema flip. The flow is otherwise identical.

### What goes through RAFT vs. what is local-only

The diagram is easy to misread as "PREPARING and SWAPPING are
cluster-wide synchronization moments; every node atomically swaps in
lock-step". The truth is more nuanced — the FSM transitions ARE
cluster-wide RAFT commits, but each node's local PREP and SWAP work
runs on its own timeline. The barrier (`PREPARING → SWAPPING`) bounds
the cross-node window where local timelines diverge.

What goes through RAFT (cluster-wide commits):

- `AddTask` — task created at STARTED.
- `RecordUnitProgress` / `RecordUnitCompletion` — per unit, on the
  node that owns the unit.
- The transition `STARTED → PREPARING` (semantic) or
  `STARTED → SWAPPING` (format-only) — happens once the cluster-wide
  `AllUnitsTerminal` predicate becomes true on the Manager's FSM
  state. Routing depends on `Task.NeedsPreparationBarrier`.
- `RecordPreparationCompleteAck` — one per node, with `Success=bool`.
  Semantic-migration path only — fires after each node's local PREP
  body (`OnGroupCompleted`) returns.
- The transition `PREPARING → SWAPPING` — committed inside
  `RecordPreparationCompleteAck`'s apply once every expected ack has landed
  with `Success=true`.
- `RecordPostCompletionAck` — one per node, with `Success=bool`.
  Fires after each node's local SWAP body (`OnSwapRequested` for
  barrier tasks, `OnGroupCompleted` for non-barrier tasks) returns.
- The transition `SWAPPING → FINISHED` — committed by
  `MarkTaskFinalized` once every expected PostCompletionAck has
  landed successfully.
- `UpdateProperty` from `OnTaskCompleted` — the cluster-wide schema
  flip (semantic migrations only).

What does NOT go through RAFT (local-only):

- **PREP** (FlushAndSwitch, Prepend, Shutdown the reindex bucket).
- **OVERLAY SET** (install per-shard tokenization resolver).
- **ATOMIC SWAP** (`SwapBucketPointer` per prop, in microseconds).
- **Post-atomic inline tidy** (Shutdown old main, rename old → backup).
- **`OnMigrationComplete`** per-strategy hook (in-memory shard-local
  state mutations).

All five of those are pure in-process operations on each node's local
LSM store. There is no single instant where every node atomically
swaps. Different nodes fire `OnGroupCompleted` at slightly different
times (scheduler-tick jitter, typically sub-second to a few seconds).
Each node runs its own three-phase dance on its own local timeline.

### What "atomic" actually means

Two different scopes of atomicity carry weight here:

- **Per-node atomic** — inside `RunSwapOnShard`'s Phase 2a, the per-
  prop `SwapBucketPointer` tight loop holds the mixed-state subwindow
  ("some props swapped, others not") to microseconds. A query landing
  on this node during this subwindow that hits a not-yet-swapped prop
  would tokenize input with the new value against an old-tokenized
  bucket; the loop staying microseconds ensures the probability is
  negligible. This is the meaning of "ATOMIC SWAP" in the diagram.
- **Cluster-wide convergence (NOT instantaneous synchronization)** —
  the system guarantees that either every node converges to the new
  bucket+schema state, or the migration fails cleanly and every node
  stays at the old state. It does NOT guarantee that nodes flip at
  the same wall-clock instant.

The cluster-wide convergence guarantee is enforced by two independent
mechanisms:

1. **Per-shard tokenization overlay (the per-node bridge).** Between
   the local `SwapBucketPointer` (bucket is now NEW) and the eventual
   cluster-wide schema flip (schema flag is now NEW), this node's
   queries need to tokenize input matching the new bucket content.
   The overlay installs the new tokenization at the per-shard query
   path so the per-node window between "swap committed locally" and
   "schema flip committed cluster-wide" is correct on this node's
   reads. The overlay is cleared from `OnTaskCompleted` after the
   schema flip lands. See §10.
2. **Two-phase ack barrier (the cross-node handshake).** For semantic
   migrations each node submits `RecordPreparationCompleteAck(Success=bool)`
   after its local PREP returns, then `RecordPostCompletionAck(Success=bool)`
   after its local SWAP returns. The FSM gates `PREPARING → SWAPPING`
   on every node's PreparationCompleteAck (success on all), then gates
   `SWAPPING → FINISHED` on every node's PostCompletionAck (success
   on all). Any `Success=false` on either ack flips the task to
   `FAILED`, which makes `OnTaskCompleted` skip the cluster-wide
   schema flip. So the schema never moves to NEW unless every node
   has successfully prepared AND swapped. Format-only migrations skip
   the PreparationCompleteAck barrier (no cross-replica tokenization
   alignment to bound) and gate `SWAPPING → FINISHED` on
   PostCompletionAck only. See §6.3.

### Why PREP runs inside PREPARING (not earlier)

Two reasons it can't move earlier into the STARTED phase:

- **PREP requires the reindex bucket to exist on disk with complete
  segments.** `PrependSegmentsFromBucket(reindex → ingest)` needs the
  reindex bucket fully populated. The reindex bucket is filled by
  `RunReindexOnlyOnShard` — which is the work that drives
  `UnitStatus → COMPLETED`. So PREP can't start before the unit hits
  COMPLETED on this node.
- **Running PREP at per-unit completion time (still during STARTED on
  the cluster level) would mean each node starts heavy disk I/O the
  moment its OWN units finish, while other nodes are still
  reindexing.** That doubles the cluster's I/O profile during the
  long phase. Concentrating PREP inside the per-node
  `OnGroupCompleted` (PHASE A) keeps the resource curve well-defined:
  STARTED = "everyone reindexing", PREPARING = "everyone running PREP
  locally then acking", SWAPPING = "everyone running atomic SWAP
  locally then acking".

The role `PREPARING` plays is twofold: (a) signaling that every unit
is terminal, so every node has the right on-disk state to start its
local PREP; and (b) gating the cross-replica SWAP barrier — only
after every node has acked `PrepComplete` does any node proceed to
PHASE B (`OnSwapRequested`). This is what bounds the cross-replica
stagger window to RAFT propagation latency instead of per-node PREP
duration variance. The synchronize-or-fail-cleanly invariant lives
in the ack barriers that follow.

## 4. Architecture by layer

### 4.1 REST layer — `adapters/handlers/rest/handlers_indexes.go` + `handlers_reindex.go`

Validation, dispatch, response shaping. Two structural details worth
calling out:

**Per-`(collection, property)` submit lock.** Held across class read,
validation, conflict check, and the RAFT `AddDistributedTask` call.
Without this, a parallel `DELETE /properties/{p}/index/{name}` could
win the lock between the PUT's `ReadOnlyClass` snapshot and its
RAFT task-add, leaving the PUT validating against a schema that no
longer matches the on-disk bucket. The lock is process-wide; the
multi-node version of the same race is closed by the FSM-side
`MutationGuard` (see §4.3). Stored on `state.ReindexSubmitLocks` so the
PUT handler and the DELETE-property-index handler share the same
entries. See [`adapters/handlers/rest/state/reindex_submit_locks.go`](../adapters/handlers/rest/state/reindex_submit_locks.go).

**Pre-submit `CleanStalePartialReindexState` sweep.** Defense in depth
against the CANCEL→retry silent-failure family (same shape as
DELETE→re-enable): if a previous cancelled run left stale
`.migrations/<dir>/started.mig` + sidecar dirs on disk, the new task
would resume against them, finish in <1s with a no-op, flip the schema
flag, and report success against an empty bucket. The cancel handler
already runs this synchronously, but the wait can time out or the node
can crash mid-cancel. Submit-time cleanup catches that gap. Critically,
for `change-tokenization` the sweep wipes BOTH `searchable` AND
`filterable` migration dirs — cleaning only one of them was the root
cause of a Sev 1 (see `indexTypesFromMigrationType`'s godoc).

### 4.2 Distributed Task Manager — `cluster/distributedtask/`

The substrate. Package doc at [`cluster/distributedtask/doc.go`](../cluster/distributedtask/doc.go)
documents the four "journey" shapes; reindex uses **Journey 3** for
semantic migrations and **Journey 2** for format-only ones, plus
**Journey 4** for per-tenant grouped MT.

Key types & contracts:

- **`Manager`** — the RAFT FSM. Owns task state, applies `AddTask`,
  `RecordUnitProgress`, `RecordUnitCompletion`, `RecordPreparationCompleteAck`,
  `RecordPostCompletionAck`, `MarkTaskFinalized`. Every mutation is
  FSM-deterministic.
- **`Scheduler`** — per-node loop. Polls Manager for current task list,
  starts/stops local work via Provider, fires `OnGroupCompleted`
  (PHASE A: PREP for barrier tasks, PREP+SWAP for non-barrier),
  fires `OnSwapRequested` (PHASE B: SWAP for barrier tasks),
  `OnTaskCompleted` (cluster-wide schema flip), submits
  `MarkTaskFinalized` when the local callbacks succeed.
- **`Provider` / `UnitAwareProvider`** — the extension point.
  `ReindexProvider` implements the latter and provides both
  `OnGroupCompleted` and `OnSwapRequested` for barrier-aware
  semantic migrations.
- **`ConflictDetector`** — pluggable interface implemented per
  namespace (here, by `ReindexProvider.CheckConflict`). Called under
  Manager's lock from the AddTask apply path, BEFORE the task is
  appended. FSM-deterministic.
- **`SchemaMutationDetector`** — the other half of the conflict
  surface: gates external schema mutations
  (`UpdateProperty`/`DeleteClass`/`DeleteTenants`/`UpdateTenants`)
  while a reindex is in flight. Implementation:
  `ReindexProvider.CheckPropertyUpdate` / `CheckClassMutation` /
  `CheckTenantMutation`. Uses `TaskStatus.IsActive()` so PREPARING,
  SWAPPING, and STARTED all count as "in flight" for mutation gating.
- **`TaskStatusPreparing` and `TaskStatusSwapping`** — the post-units,
  pre-FINISHED coordination states that split per-node PREP from
  per-node SWAP with a cluster-wide PreparationCompleteAck barrier in between.
  Semantic migrations transit STARTED → PREPARING → SWAPPING →
  FINISHED; format-only migrations skip PREPARING and transit STARTED
  → SWAPPING → FINISHED. The task is NOT safe to act on from the API
  surface in either coordination state; callers polling for "fully
  done" must wait for `FINISHED`. Format-only journeys pass through
  SWAPPING in essentially zero time.
  These states close the schema-flip-lag race where a task would
  otherwise be FINISHED at the FSM layer before every node's
  post-completion callback had committed its bucket-pointer flip, and
  the two-phase split additionally bounds the cross-replica stagger
  window to RAFT propagation latency (tens of ms) instead of per-node
  PREP duration variance (which scales with bucket size and reaches
  minutes at billion-scale).
- **Two-phase ack barrier** — `RecordPreparationCompleteAck` (semantic only)
  + `RecordPostCompletionAck` (all paths). Every node's scheduler
  records its phase outcome (success or error) on the task before the
  cluster is allowed to advance: PREPARING → SWAPPING is gated on
  every PreparationCompleteAck landing with Success=true; SWAPPING →
  FINISHED is gated on every PostCompletionAck landing with
  Success=true. A `Success=false` on EITHER ack flips the task to
  `FAILED`, which makes `OnTaskCompleted` skip the cluster-wide
  schema flip — the load-bearing invariant that prevents a per-node
  swap failure from leaving the schema pointing at not-yet-swapped
  buckets. Acks idempotent and rehydrate over restart.
- **Permanent-rejection sentinels** —
  `ErrTaskNotRunning`/`ErrTaskDoesNotExist`/`ErrUnitAlreadyTerminal`/
  `ErrUnitWrongNode`/`ErrTaskNotInFinalizingState`, all matched by
  the umbrella `ErrPermanentRejection`. Encoded over gRPC with a
  stable `codes.FailedPrecondition` + `[dtm-perm/<id>] ...` message
  marker so the sentinel identity survives wire transit and gets
  re-attached on the receiving side by `RehydratePermanentRejection`.
  Mixed-version friendly (pre-sentinel peers return plain-text
  phrasing; the classifier substring-matches as a fallback). See
  [`cluster/distributedtask/errors.go`](../cluster/distributedtask/errors.go).

### 4.3 Schema FSM — `cluster/schema/` + `cluster/proto/api/`

Three changes here serve the reindex feature:

**`BucketGeneration` counter on `Property`.** Bumped on every reindex
that touches a property. Lets unrelated machinery (notably backup) tell
"this is the same bucket I saw before" apart from "this property has
been rebuilt since I last looked."

**`UpdateProperty` fieldmask.** `applyPerPropertySchemaUpdate` passes a
`fields []string` mask all the way down to the FSM apply path
(`MergePropsMasked`). Two strategies running in parallel on the same
property — each touching different fields — no longer clobber each
other on RAFT apply: the FSM merges only the listed fields onto the
live class state. An empty mask falls back to "replace every field"
semantics for callers that don't care.

**`FromInFlightMigration` flag.** Routed via
`UpdatePropertyInternalFromMigration`. Bypasses the MutationGuard
described next: migration-driven schema flips are exactly the kind of
"in-flight" mutation the guard would otherwise reject, so they need an
explicit opt-out signal that's set only by the provider's
`OnTaskCompleted` path.

**`MutationGuard` (cross-FSM).** The schema FSM consults
`distributedtask.SchemaMutationDetector` on every `UpdateProperty`,
`DeleteClass`, `UpdateTenants(FROZEN)`, `DeleteTenants` apply.
`ReindexProvider`'s implementation rejects any mutation overlapping an
in-flight reindex task's properties on the (`STARTED`, `PREPARING`,
or `SWAPPING`; admitted via `TaskStatus.IsActive()`)
same collection. The motivating failure mode is documented verbatim on
`CheckPropertyUpdate`'s godoc: a `change-tokenization` migration spawns
separate per-shard sub-tasks for searchable and filterable; a DELETE
arrives mid-flight; `cleanStaleMigrationDirs` wipes the searchable
sub-task's working dir; the searchable sub-unit FAILs; the filterable
sub-unit commits its local swap; per-shard ack barrier sees mixed acks;
task FAILED; `flipSemanticMigrationSchema` skipped; schema stays at OLD
tokenization while the filterable bucket on disk holds NEW-tokenized
data. **Bucket↔schema inversion.** Same family as the ack-barrier
failure mode but triggered by an external schema mutation instead of
a crash.

The guard is intentionally blanket: any overlap rejects. Migration-
driven flips bypass via `FromInFlightMigration=true` so the migration's
own scheduled completion flip still works. Class-wide
(`CheckClassMutation`) is stricter — any reindex on the class
(regardless of property) blocks DeleteClass.

### 4.4 Reindex orchestration — `adapters/repos/db/reindex_*` & `inverted_reindex_*`

**`ReindexProvider`** ([`reindex_provider.go`](../adapters/repos/db/reindex_provider.go))
— the DTM `UnitAwareProvider` implementation. Lifecycle hooks:

- `StartTask` → per-node bootstrap. Unmarshals the payload, identifies
  this node's local units, hands them to `processUnits`. Bounded
  concurrency via `ConcurrencyLimiter`.
- `processOneUnit` → per-(unit, shard) bootstrap. Constructs the
  strategy instance(s) at the right per-node generation
  (`nextMigrationGeneration`), writes the recovery payload, runs the
  reindex iteration via `ShardReindexTaskGeneric`. For semantic
  migrations it stops at `markReindexed` (barrier); for format-only
  it runs the full lifecycle including the swap.
- `OnGroupCompleted` (semantic only) → the swap phase, per local
  shard. Three-phase: PREP → OVERLAY SET → ATOMIC SWAP. See §6.
- `OnTaskCompleted` (semantic only) → `flipSemanticMigrationSchema`
  via RAFT, then `ClearTokenizationOverlay` on every local shard.
- `CheckConflict` / `CheckPropertyUpdate` / `CheckClassMutation` /
  `CheckTenantMutation` — see §4.3 & §7.

**`ShardReindexTaskGeneric`** ([`inverted_reindex_task_generic.go`](../adapters/repos/db/inverted_reindex_task_generic.go))
— the strategy-parameterized lifecycle. State machine, merge / swap /
tidy, object iteration, progress tracking, sentinel writes. The
file-level godoc documents the three-phase contract (PREP / ATOMIC /
DEFERRED) that every code path must preserve.

**`MigrationStrategy`** ([`inverted_reindex_strategy.go`](../adapters/repos/db/inverted_reindex_strategy.go))
— the per-migration extension surface. Each strategy supplies bucket
naming, the per-key transform, source/target/backup LSM strategies,
the Add/Delete double-write callbacks, the optional `AnalyzerOverlay`
(see §8), the `PreReindexHook`, and the `OnMigrationComplete` hook
(see §4.5 phase contract).

**`reindex_conflict.go`** — `CheckConflict` (FSM-deterministic),
`CheckPropertyUpdate`, `CheckClassMutation`, `CheckTenantMutation`,
plus the predicates `ReindexPropsOverlap`, `TouchesSearchable`,
`TouchesFilterable`, `TypesConflictReason`. The exhaustive switches in
the `Touches*` predicates intentionally panic on unknown migration
types so a new `ReindexMigrationType` cannot silently be treated as
"doesn't touch X" — it surfaces on the first request.

**`reindex_recovery.go`** — `DiscoverInFlightReindexTasks`,
`buildRecoveryTasks`, `NewShardReindexerV3FromRecovered`,
`SeedReindexProviderFromRecovery`. Called from `MakeAppState` BEFORE
`DB.WaitForStartup`, so reconstructed `ShardReindexTaskGeneric`
instances are registered before any post-restart write can reach the
shard. See §6 crash safety.

**`reindex_cancel_cleanup.go`** —
`CleanStalePartialReindexState(collection, prop, indexType)`. Called
from the cancel handler (after `WaitForLocalTaskDrain`) and from the
submit handler (defense in depth). Per-shard; per-shard failures don't
stop iteration so a stuck shard can't permanently wedge a
`(collection, prop, indexType)` tuple.

**`inverted_reindex_finalize.go`** — startup-time deferred dir rename
(see §9), `nextMigrationGeneration`, `maxMigrationGeneration`,
`completedMigrationGens`, `parseMigrationDirName`. The finalize
algorithm handles every shape defensively: tidied / merged-but-not-
tidied / lower-gen sidecars / in-flight gens left alone for
`DiscoverInFlightReindexTasks` to pick up.

### 4.5 Strategy implementations — `inverted_reindex_strategy_*.go`

Seven strategy implementations, one file each:

| Strategy | Type | Source bucket | Target bucket | OnMigrationComplete |
|---|---|---|---|---|
| `MapToBlockmaxStrategy` | `repair-searchable` | `searchable` (MapCollection) | `searchable` (Inverted/Blockmax) | Per-prop: bump `BucketGeneration`; class-level: flip `UsingBlockMaxWAND` once every searchable prop is on Blockmax. |
| `RoaringSetRefreshStrategy` | `repair-filterable` | `filterable` (RoaringSet) | `filterable` (RoaringSet) | No-op (format unchanged). |
| `FilterableToRangeableStrategy` | `enable-rangeable` / `repair-rangeable` | objects → builds RoaringSetRange | `rangeFilters` (RoaringSetRange) | Per-shard `setRangeableLocallyReady` so this shard's queries observe ready=true at the same moment as the RAFT flip; per-prop `IndexRangeFilters=true` via `UpdatePropertyInternalFromMigration`. Format-only. |
| `EnableFilterableStrategy` | `enable-filterable` | objects → builds RoaringSet | `filterable` (RoaringSet) | No-op; cluster-wide `IndexFilterable=true` flips from `OnTaskCompleted` to avoid the first-shard-flips-wins-the-cluster race. |
| `EnableSearchableStrategy` | `enable-searchable` | objects → builds Blockmax | `searchable` (Blockmax) | No-op; cluster-wide flip from `OnTaskCompleted`. |
| `SearchableRetokenizeStrategy` | `change-tokenization` (searchable half) | `searchable` | `searchable` (new tokenization) | No-op; `Tokenization` flip from `OnTaskCompleted`. |
| `FilterableRetokenizeStrategy` | `change-tokenization` (filterable half) + `change-tokenization-filterable` | `filterable` | `filterable` (new tokenization) | No-op; same flip path. |

`change-tokenization` spawns TWO strategy instances per unit
(`SearchableRetokenizeStrategy` + `FilterableRetokenizeStrategy`) so
the searchable + filterable buckets retokenize in lock-step, with
their per-shard swaps inside the same tokenization-overlay window.
Per-shard cleanup (`indexTypesFromMigrationType`) must wipe BOTH
tracker dirs — see §4.1.

These instances are constructed and parameterized by
`ShardReindexTaskGeneric` (the generic V3 task lifecycle), so all
strategies share a single state machine, sentinel writer, callback
manager, and progress tracker.

The strategy interface itself documents the per-method contract; see
[`inverted_reindex_strategy.go`](../adapters/repos/db/inverted_reindex_strategy.go).
Of particular note is `OnMigrationComplete`'s phase contract — it
fires in Phase 2c, AFTER the per-prop `SwapBucketPointer` tight loop
and AFTER the inline `oldMain.Shutdown` + `oldMain → backup` rename
loop, but still INSIDE the per-shard tokenization-overlay window for
migrations that use one. The godoc enumerates what's allowed and
forbidden in that position and is the authoritative spec for adding
a new strategy.

**Semantic vs format-only.** `IsSemanticMigration` is the predicate:
`change-tokenization`, `change-tokenization-filterable`,
`enable-filterable`, `enable-searchable` are semantic — every shard
must reindex before any shard swaps (Journey 3 barrier), and the
schema flip happens cluster-wide from `OnTaskCompleted`. The rest are
format-only — each shard runs the full lifecycle independently
(Journey 2), with no cluster-wide schema dependency.

**`enable-rangeable` is intentionally format-only.** Range queries'
correctness during the migration is gated by the per-shard
`rangeableLocalReady` flag — falling back to the filterable bucket
walk on shards that haven't completed locally is slow but correct.
The barrier dance would be over-engineering for a journey that has a
correct (if slow) per-shard fallback.

### 4.6 LSM primitives — `adapters/repos/db/lsmkv/store.go`

Two primitives carry the load:

**`Store.SwapBucketPointer(targetName, sourceName)`.** Atomic in-memory
pointer flip — all future `Store.Bucket(targetName)` calls return the
bucket currently registered as `sourceName`. The source name is
removed from the map; the source bucket's on-disk path is released
from `GlobalBucketRegistry`. The caller is responsible for shutting
down the returned old bucket, persisting any crash-safety markers
around this call, and finalizing directory renames at a later point
(typically next restart).

The registry release is what makes back-to-back migrations in the same
process work: a second migration's ingest bucket can claim the same
canonical path after the on-disk dir has been cleaned by
`cleanStaleSidecarDirs`. Without the release, the second cycle aborts
at `OnAfterLsmInit` with "bucket already registered".

**`Store.FinalizeBucketSwap(canonicalDir, currentDir, backupDir)`.**
The deferred-finalize counterpart: flush memtable, remove backup dir,
`os.Rename(currentDir → canonicalDir)`, rewrite `bucket.dir` +
`bucket.disk.dir` + every segment's in-memory `.path`, create a fresh
active memtable. **MUST only be called during startup, before the
bucket serves any queries.** Calling on a live bucket creates a race
where step 3 (`os.Rename`) and step 4 (`updateBucketDir`) form a
non-atomic window; a concurrent compaction reading `bucket.disk.dir`
sees the old path and post-rename writes fail with `ENOENT`. See §9
for the full history.

The atomic-phase contract in the orchestrator file enforces this
rule: a unit test fails if `SwapBucketPointer` is preceded by any
disk-I/O or compaction-wait op inside Phase 2 (`testHookPostPropSwap`
+ wall-clock budget assertion).

## 5. Migration strategies — quick map

```
                               semantic?     analyzer overlay?
change-tokenization              ✓                ✓ (tokenization)
change-tokenization-filterable   ✓                ✓ (tokenization)
enable-filterable                ✓                ✓ (ForceFilterable)
enable-searchable                ✓                ✓ (ForceSearchable + tokenization)
enable-rangeable                                  
repair-filterable                                 
repair-searchable                                 
repair-rangeable                                  
```

The four semantic migrations need both the cluster-wide barrier and
the per-shard analyzer overlay; the four format-only migrations need
neither.

## 6. Crash safety

### 6.1 Sentinel files

Every per-shard migration owns a tracker dir under
`<lsm>/.migrations/<strategy-prefix>_<propname-suffix>_<gen>/`. Phase
transitions write fsync'd sentinel files:

| Sentinel | Set when |
|---|---|
| `started.mig` | Reindex iteration started (first run). |
| `reindexed.mig` | Iteration terminal: every object processed into the `__reindex_<N>` bucket. |
| `prepended.mig` | `__reindex_<N>` segments prepended into the `__ingest_<N>` bucket; reindex bucket shut down. |
| `merged.mig` | All per-prop prepends complete; ingest bucket holds the complete dataset. |
| `swapped.mig` | Per-prop `SwapBucketPointer` committed. |
| `tidied.mig` | All per-prop swaps complete; old main shut down + renamed to backup. |
| `payload.mig` | JSON dump of the typed `ReindexTaskPayload` + task descriptor. Written by `persistRecoveryRecord` before the first iteration. Source of truth for `DiscoverInFlightReindexTasks`. |
| `progress.mig` | Per-iteration progress checkpoint. |
| `properties.mig` | List of properties this task targets on this shard. |

Per-prop variants exist for `swapped.mig` (one per property) so a
crash mid Phase 2 can resume from the last successfully-swapped prop.

### 6.2 Recovery — startup `DB` init

Sequence in `MakeAppState`:

1. `DiscoverInFlightReindexTasks(rootPath)` walks every shard's
   `.migrations/` dir. For each tracker dir with
   `started.mig + reindexed.mig` present and `tidied.mig` absent,
   loads the persisted `payload.mig` and reconstructs a per-shard
   `ShardReindexTaskGeneric` at the correct generation. The narrow
   window — "terminal but not yet tidied" — is exactly the recovery
   gap the design exists to close.
2. `NewShardReindexerV3FromRecovered` wires the recovered tasks into
   a stripped-down recovery-only `ShardReindexerV3` that fires
   `OnAfterLsmInit` per shard load — re-installing the double-write
   callbacks BEFORE any post-restart write can reach the shard.
   Without this, writes that arrive between shard init and the swap
   that completes a deferred reindex go only to the old main bucket
   and are lost when the swap replaces it with the ingest bucket.
3. `SeedReindexProviderFromRecovery` pre-populates the provider's
   per-descriptor task cache so `OnGroupCompleted` reuses the same
   instances rather than creating fresh ones and calling
   `OnAfterLsmInit` a second time (which would attempt to load
   already-loaded ingest buckets).
4. `FinalizeCompletedMigrations` runs per shard before LSM init
   reloads any buckets. For each `(prop, indexType)`, finds the
   highest tidied (or merged-but-not-tidied) generation, promotes
   its ingest dir to the canonical name, deletes lower-gen
   sidecars, deletes the tracker dir. See §9 for the multi-gen
   algorithm.

### 6.3 The two-phase ack barrier (PREPARING + SWAPPING)

The post-completion barrier is split into two phases.

**Semantic migrations (NeedsPreparationBarrier=true):**

1. `OnGroupCompleted` (PHASE A) runs PREP per local shard. Returns a
   non-nil error iff any task in the group failed to merge. The
   scheduler emits `RecordPreparationCompleteAck(Success=bool)` per node.
2. FSM transitions `PREPARING → SWAPPING` only when every expected
   PreparationCompleteAck has landed with `Success=true`. The transition is
   committed inside the FSM apply path (atomic) so no node can
   advance to SWAPPING before every node has finished PREP.
3. `OnSwapRequested` (PHASE B) runs OVERLAY+SWAP per local shard.
   Returns a non-nil error iff any task's RunSwapOnShard failed. The
   scheduler emits `RecordPostCompletionAck(Success=bool)` per node.
4. FSM transitions `SWAPPING → FINISHED` only when every expected
   PostCompletionAck has landed with `Success=true`.
5. `OnTaskCompleted` fires → cluster-wide schema flip commits.

**Format-only migrations (NeedsPreparationBarrier=false):** PHASE A is
skipped; the FSM goes `STARTED → SWAPPING` directly. `OnGroupCompleted`
runs the inline PREP+OVERLAY+SWAP body and the scheduler emits
`RecordPostCompletionAck`. SWAPPING → FINISHED is gated on the
PostCompletionAck barrier only.

**Failure handling (both paths):**

- Any `Success=false` on EITHER ack → task flips to `FAILED`
  immediately → `OnTaskCompleted` early-returns, schema flip is
  SKIPPED.
- Per-node swap failure can no longer let the cluster-wide schema flip
  propagate while one replica's bucket pointer never moved.
- On node restart, the recovery path re-fires the appropriate phase
  callback (PHASE A for PREPARING, PHASE B for SWAPPING) via the
  rehydrate branch; if the per-shard work succeeds the second time,
  the ack lands and the task completes normally.

**Cross-replica window:** The time during which different nodes'
buckets are in mixed-tokenization state (some swapped, some not) is
bounded by RAFT propagation latency between PHASE B firing on the
fastest-node and PHASE B firing on the slowest-node — tens of
milliseconds regardless of PREP duration. Decoupling SWAP from PREP
across nodes is the reason this window stays bounded even when PREP
runs minutes per shard at billion-scale.

**Why two phases instead of one** — the empirical anchor: at 1M-object
scale on a 3-node × RF=1 × 5-shard cluster, QA observed a ~1-second
total in-flight window with a 248 ms within-window mixed-state
captured between two probe samples on adjacent shards (see
weaviate/0-weaviate-issues#225). At that scale the per-node PREP
duration variance dominates: with no barrier, the cross-replica
mixed-state window scales with PREP duration (minutes at
billion-scale); with the barrier, it scales with RAFT propagation
(milliseconds). The cost is one extra cluster-wide RAFT roundtrip per
semantic migration. Without this anchor, future maintainers reading
the FSM table see "two RAFT-coordinated phases" and may propose
collapsing it back to one — the bug it closes is not visually
obvious from the code alone.

Acks idempotent (first ack per `(task, node)` wins); rehydrate over
restart (the scheduler re-fires on the next tick); silent on already-
terminal tasks (a late-arriving ack from a slow follower must not
produce a noisy apply failure).

### 6.4 The three-phase swap

`OnGroupCompleted` runs a strict prep / atomic / defer split per local
shard. The contract is enumerated verbatim at the top of
[`inverted_reindex_task_generic.go`](../adapters/repos/db/inverted_reindex_task_generic.go);
in short:

- **Phase 1 — PREP (background, NOT inside the overlay window):**
  `FlushAndSwitch` reindex bucket, `Shutdown` it, `PrependSegmentsFromBucket`
  per property, `removeReindexBucketsDirs`, sentinel writes. Disk-I/O-
  heavy. Schema = OLD, bucket = OLD throughout — safe with live queries.
- **Phase 2a — ATOMIC SWAP (microseconds, inside overlay window):**
  per prop, `Store.SwapBucketPointer(mainName, ingestName)` followed
  by `markSwappedProp`. Tight loop. Bounds the per-shard
  "mixed-state" subwindow (some props swapped, others not) to a few
  microseconds total — queries during this subwindow that hit
  not-yet-swapped props would tokenize input with the new value
  against an old-tokenized bucket. Must stay microseconds.
- **Phase 2b — POST-ATOMIC INLINE TIDY (slow but correctness-safe):**
  `oldMainBucket.Shutdown(ctx)` + `os.Rename(oldMainDir, backupDir)`
  per property, then `markSwapped` + `markTidied`. After every prop
  has flipped in 2a, the mixed-state subwindow is closed; queries
  during 2b see all-new buckets with the overlay still active. The
  `oldMain.Shutdown` is REQUIRED inline (not deferred) because
  `Bucket.Shutdown` is the only call that removes the bucket's path
  from `GlobalBucketRegistry`; deferring leaks the path entry
  process-wide.
- **Phase 2c — POST-ATOMIC INLINE FINALIZE:**
  `OnMigrationComplete` + `trimOlderGenerationsLocked`. Outside the
  mixed-state subwindow. Strategy-specific hook for in-memory
  shard-local query-path state mutations (e.g. `setRangeableLocallyReady`)
  or for non-semantic RAFT calls.
- **Phase 3 — DEFERRED LIVE-BUCKET RENAME (next process startup):**
  `FinalizeCompletedMigrations` runs the ingest → canonical dir
  rename before LSM init reloads any bucket. See §9.

The PREP / OVERLAY-SET / ATOMIC-SWAP ordering is what closes the
SWAPPING-window misalignment. Setting the overlay before prep would
expose the very gap it closes (NEW-tokenized analyzer input against
OLD-tokenized bucket content for hundreds of ms). Setting it between
prep and atomic swap means the window is bounded by the in-memory
pointer flip (microseconds).

Under the two-phase barrier (semantic migrations), PREP and OVERLAY
SET + ATOMIC SWAP fire from two different scheduler callbacks
(`OnGroupCompleted` for PREP, `OnSwapRequested` for OVERLAY+SWAP) with
the cluster-wide `RecordPreparationCompleteAck` barrier in between. The
phase ordering on a single node is preserved; the barrier additionally
bounds the cross-node skew between fastest-node SWAP and slowest-node
SWAP to RAFT propagation latency instead of per-node PREP duration
variance.

## 7. Concurrency model

**Per-`(class, property)` REST submit lock.** Closes the same-process
PUT/DELETE race. See §4.1.

**Cluster-wide FSM conflict check** (`ConflictDetector.CheckConflict`).
Called under `Manager.mu` from the RAFT-apply AddTask path BEFORE the
new task is appended to FSM-stored state. FSM-deterministic: every
node applies the same RAFT log entry, sees the same `existingTasks`
snapshot, runs the same predicate, reaches the same accept/reject
decision. Survives leader re-election.

**Conflict rule:** any two reindex migrations on overlapping properties
of the same collection conflict, regardless of which bucket type they
primarily write to. Earlier versions allowed parallel migrations as
long as they wrote to different bucket types; that turned out to be a
real Sev 1, because the completing migration's `OnMigrationComplete`
fires an `UpdateProperty` whose `MergeProps` preserved the still-false
sibling flag (the other migration hadn't flipped its flag yet), and
the FSM apply path then ran `cleanStaleMigrationDirs` for every index
whose flag was now false — wiping the in-flight migration's working
directory. The closure happens at submit time: reject any new task
whose property set overlaps an in-flight task's, so the caller gets a
clean conflict error and can serialize. `PREPARING` and `SWAPPING`
both count as in-flight (via `TaskStatus.IsActive()`).

**Cluster-wide FSM schema-mutation check**
(`SchemaMutationDetector`). See §4.3. Blanket reject any external
mutation overlapping an in-flight task's properties on the same
collection. Migration-driven flips bypass via `FromInFlightMigration`.

**Per-collection in-flight cap: 32** (`maxConcurrentReindexPerCollection`).
Bumped from the original DTM default of 4 specifically for this
feature — wide schemas where every property needs a semantic
migration must complete in operator-tractable time, and serializing
them at 4 made multi-hour migrations into multi-week migrations. 32
was chosen empirically as the point where LSM compaction throughput
saturates on a single shard's disk for the typical migration mix; the
REST handler returns 503 once the cap is reached.

**Per-collection worker pool** in `processUnits`. Bounded by the
`concurrency` function passed to the provider (typically a
`runtime.DynamicValue`), so operators can throttle on overloaded
clusters.

## 8. Multi-tenancy

`?tenants=t1,t2` scopes the task to the named tenants on a multi-
tenant class. The handler validates:

- `tenants` on a single-tenant class → 400.
- No `tenants` on a multi-tenant class → defaults to all tenants for
  format-only migrations; rejected as ambiguous for semantic
  migrations (semantic must apply cluster-wide, no sub-scoping).
- `tenants` with a semantic migration → 400 ("all tenants must be
  targeted").
- Tenant in `OFFLOADED` / `FROZEN` → 400 with the named tenant.

Per-tenant unit groups (Journey 4 from the DTM doc): one
`UnitSpec.GroupID` per tenant, so `OnGroupCompleted` fires per-tenant
as each tenant's replicas all finish. Tenant A starts serving new
data immediately even while tenant B is still reindexing.

`FROZEN` tenants are rejected by the MutationGuard. A reindex on a
tenant that transitions to FROZEN mid-flight fails the affected unit;
the post-completion ack barrier propagates that failure to the task.
Operator can resume by un-freezing the tenant (transition back to HOT)
and re-submitting.

## 9. Per-migration generation + deferred live-bucket rename

The original design problem this section answers:

> The natural finalize step after a successful in-memory swap is to
> rename the new bucket's on-disk dir from `…__<ingestSuffix>_<N>/` to
> canonical `property_<p>_<index>/`. That rename is not safe at
> runtime (segments mid-flight hold path snapshots; renaming under
> them produces `ENOENT` on the next write).

The solution has two parts: defer the rename to next startup, and
give every migration a per-node generation suffix so back-to-back
migrations on the same property don't collide.

### 9.1 Why the rename is deferred

After a successful `runtimeSwap` on each node:

- `bucketsByName[mainName]` → the ingest bucket instance.
- That bucket's `dir` still points at
  `property_<p>_<index>__<ingestSuffix>_<N>/`; no filesystem rename.
- The old main has been `Shutdown` and its dir renamed to
  `…__<backupSuffix>_<N>/`.

`Store.FinalizeBucketSwap` would rename ingest → canonical, but as its
godoc spells out, **it MUST only be called during startup, before the
bucket serves any queries**. Calling on a live bucket creates a
non-atomic window between `os.Rename` (step 3) and
`updateBucketDir` (step 4) where a concurrent compaction reading
`bucket.disk.dir` sees the old path; the next write fails with
`ENOENT` (`rename ...l0.s5.db ...deleteme: no such file` is the
production-observed shape).

Pausing compactions doesn't fix it cleanly: reads in flight also hold
paths from the consistent-view-of-segments snapshot, and any defer-
callback that touches `bucket.disk.dir` mid-update would crash or
scribble onto the wrong path.

The cost-benefit is wrong anyway: the rename is purely a tidiness
step. The bucket already serves correct data from the ingest-named
dir; the rename only matters across a process restart, so deferring
to next-startup `FinalizeCompletedMigrations` (when no buckets are
loaded) is correct and zero-risk.

### 9.2 Why per-migration generation suffixes

Once the rename is deferred, a second problem appears: back-to-back
migrations on the same property. After `T_N` tidied, `bucketsByName[mainName]`
points at the gen-N ingest bucket and that bucket's dir is
`…__<ingestSuffix>_<N>/`. The next migration `T_(N+1)` would, without
a generation suffix, try to create a new ingest bucket at the same
path the live main is currently serving from → `GlobalBucketRegistry`
collision or two `*Bucket` structs pointing at the same physical
directory.

Each migration takes its own gen-suffixed sidecars:

```
T_(N+1) on prop=text:
  reindex dir : property_text_searchable__retokenize_reindex_<N+1>
  ingest dir  : property_text_searchable__retokenize_ingest_<N+1>
  backup dir  : property_text_searchable__retokenize_backup_<N+1>
  tracker dir : .migrations/searchable_retokenize_text_<N+1>
```

No path collision with the gen-N state still on disk. `T_(N+1)`'s
`runtimeSwap` replaces the gen-N pointer with the gen-N+1 one; the
old gen-N bucket is shut down and renamed to its gen-N+1 backup.

### 9.3 Generation is per-node, not in the RAFT payload

Each node computes its own gen by scanning its own disk. The RAFT
payload does NOT carry the gen. Different nodes may use different
gens for the same RAFT task — and that's correct: gen is purely a
per-node implementation detail of the deferred-finalize. A node that
restarted between `T_N` and `T_(N+1)` will have promoted gen N to
canonical at startup, so on that node `T_(N+1)` picks gen 1; a node
that didn't restart picks gen N+1. The cluster-wide logical state
still converges via the regular swap-then-flip pipeline.

### 9.4 Trim at end of swap keeps depth bounded

After `T_N` tidies, the per-shard `runtimeSwap` does an in-process
trim that deletes every older gen's sidecar dirs (reindex / ingest /
backup) and tracker dir. The invariant: at any time, on disk for any
`(prop, indexType)`, there is **at most one tidied generation plus at
most one in-flight generation** (the trim runs at end of swap, so two
tidied gens can only coexist if the trim crashed between `markTidied`
and its `os.RemoveAll` loop).

### 9.5 `FinalizeCompletedMigrations` handles every shape

Per namespace (strategy-prefix + props-suffix):

1. Find the highest gen `T` with `tidied.mig`.
2. Find the highest gen `M` with `merged.mig` (regardless of tidied).
3. `effective = max(T, M)`.
4. If `effective` exists:
   - `effective == T`: standard path. Rename `…_<ingestSuffix>_<T>/` →
     `property_<prop>_<index>/`, remove `…_<backupSuffix>_<T>/`.
   - `effective == M > T`: recovery path. The in-process swap on this
     node crashed AFTER `markMerged` but BEFORE `markTidied`. The
     ingest dir at gen M holds the target-tokenization data the
     schema expects (the cluster-wide schema flip in
     `flipSemanticMigrationSchema` likely already committed via RAFT,
     since the DTM task was FINISHED before this node died). Write
     `swapped.mig` + `tidied.mig` sentinels and promote the same way.
     **CRITICAL:** otherwise this node serves the old data under the
     new schema → divergence vs other replicas → #10675-shape bug.
5. Remove every dir on disk with gen < effective.
6. Remove the tracker dir for `effective`.
7. Leave gens > effective alone (in-flight; `DiscoverInFlightReindexTasks`
   handles them).

### 9.6 Hard rules

- **Do not** call `Store.FinalizeBucketSwap` at runtime. Single
  biggest landmine in the reindex code path; every previous attempt
  to "clean up the cosmetic naming" has produced a Sev 1.
- **Do not** rename the live main bucket's physical directory while
  loaded, by any means. The mmap'd segments survive `os.RemoveAll`
  (POSIX unlink-while-open) and can serve reads from cached pages,
  but new segment writes will land in a missing dir and silently
  lose data. weaviate/weaviate#10675 is exactly this failure mode.
- **Do not** put the gen in the RAFT payload. The whole point of the
  deferred-finalize design is that each node's on-disk state is its
  own — forcing cluster-wide agreement on a per-node implementation
  detail would re-introduce the collisions the per-node gen was
  created to avoid.

## 10. Per-shard tokenization overlay

Defined in [`adapters/repos/db/inverted/tokenization.go`](../adapters/repos/db/inverted/tokenization.go).

The problem: on each replica, the bucket pointer flips to NEW-tokenized
data at the end of `RunSwapOnShard` (Phase 2a), but the cluster-wide
schema flip (`OnTaskCompleted`'s RAFT commit) doesn't propagate to the
local FSM for tens to hundreds of milliseconds afterward. Queries that
arrive in that window tokenize their input against the still-OLD
schema value and search a NEW-tokenized bucket — wrong results.

The overlay closes this. `Shard.SetTokenizationOverlay(prop, newTok)`
installs a per-shard map; every query path that needs a property's
tokenization calls `inverted.ResolveTokenization(shard.tokenizationResolver, propName, prop.Tokenization)`
which consults the overlay before falling back to the schema value.

Lifecycle:

1. **Set** — `maybeSetTokenizationOverlayPreSwap` runs in Phase 2 of
   `OnGroupCompleted`, between PREP and ATOMIC SWAP. Only for
   tokenization-changing migrations (`change-tokenization`,
   `change-tokenization-filterable`, `enable-searchable`).
2. **Cover** — the entire Phase 2 (atomic swap + post-atomic tidy +
   `OnMigrationComplete`) runs with the overlay active. Queries see
   NEW-tokenized analyzer input against NEW-tokenized bucket content.
3. **Clear** — `OnTaskCompleted` clears the overlay on every local
   shard AFTER `flipSemanticMigrationSchema`'s RAFT commit succeeds.
   The live schema's `prop.Tokenization` is now NEW, so subsequent
   queries hit the right answer via the regular schema-lookup path.
4. **Self-clear backstop** — `Shard.TokenizationFor` has a defensive
   self-clear: if the live schema's tokenization for a prop now
   matches the overlay value, the entry is removed. Catches the case
   where the explicit clear in (3) was skipped (schema-flip failure)
   — the next query touching the prop after the schema eventually
   catches up will lazily clean up.

For migrations that DON'T change tokenization (the
`AnalyzerOverlay`-driven `enable-filterable` etc., or format-only
`enable-rangeable` / `repair-*`), no tokenization overlay is needed
— the per-shard local-ready flag and the `OnMigrationComplete`
hook handle the gap.

## 11. The analyzer overlay

A different but related mechanism. Defined per-strategy via
`MigrationStrategy.AnalyzerOverlay(props) → map[string]inverted.PropertyOverlay`.

Applied by the inverted analyzer during the backfill scan. Used by
"from-scratch" strategies (`enable-filterable` / `enable-searchable` /
`enable-rangeable`) that build a brand-new inverted bucket while the
corresponding schema flag is still false in the RAFT-stored schema.
Without this override the analyzer would skip the targeted property
(see `HasAnyInvertedIndex` in `inverted/objects.go`) and the new
bucket would come out empty.

Strategies that don't need an overlay (the live schema flag is already
true for the targeted properties — `retokenize`, `map → blockmax`,
`roaring-set refresh`) embed `noAnalyzerOverlay` to get the nil-return
default.

Different from the tokenization overlay: the analyzer overlay is read
by the backfill iterator (write-side); the tokenization overlay is
read by the query analyzer (read-side). They operate on different
phases of different concerns and don't share state.

## 12. Cancel + DELETE-property-index

**Cancel** (`{"<type>":{"cancel":true}}`):

1. Find the STARTED task targeting `(collection, prop, indexType)`.
2. RAFT `CancelDistributedTask`.
3. Wait for the local reindex goroutine to drain
   (`WaitForLocalTaskDrain`, 10s timeout). Bounded so a stuck
   goroutine doesn't turn the HTTP request into a hang.
4. `CleanStalePartialReindexState` — wipe sidecars + migration dir
   so the next submit starts from a clean slate.
5. 202 with the cancelled task ID.

If the drain times out, return 202 anyway — the next submit's
defense-in-depth cleanup will pick up the work. If the node crashes
mid-cancel, the on-disk state survives; the next submit's pre-cleanup
catches that gap.

**DELETE `/properties/{p}/index/{name}`:**

1. Acquire the same `ReindexSubmitLocks` entry as PUT (closes the
   race described in §4.1).
2. Schema FSM applies the `UpdateProperty` flipping the flag to
   false. The MutationGuard rejects if a reindex is in flight on this
   property (`FromInFlightMigration=false` on this path).
3. `cleanStaleMigrationDirs` wipes the canonical bucket dir + any
   sidecar dirs for the dropped index type, plus the tracker dir.
4. Subsequent re-enable starts from clean state.

## 13. Out of scope (broken; tracked follow-up)

Backups and migrations across runtime-reindex state are intentionally
left broken on this branch and will not be fixed in the v1.38 Preview
merge. The fixes live on `backup-runtime-reindex-fixes` and will
land as a follow-up PR. Tracking: weaviate/0-weaviate-issues#215.

Operators should not rely on backup/restore or schema migration
interacting cleanly with an in-flight or recently-completed reindex
while running v1.38 Preview.

## 14. Files of interest

**REST**

- [`adapters/handlers/rest/handlers_indexes.go`](../adapters/handlers/rest/handlers_indexes.go) — GET / PUT / DELETE.
- [`adapters/handlers/rest/handlers_reindex.go`](../adapters/handlers/rest/handlers_reindex.go) — validation helpers, status synthesis.
- [`adapters/handlers/rest/state/reindex_submit_locks.go`](../adapters/handlers/rest/state/reindex_submit_locks.go) — per-(class, prop) submit lock.
- [`entities/models/index_*.go`](../entities/models/) — swagger DTOs (`IndexStatus`, `IndexUpdateRequest`, `PropertyIndexStatus`).
- [`openapi-specs/`](../openapi-specs/) — source of truth for the DTOs.

**Orchestration**

- [`adapters/repos/db/reindex_provider.go`](../adapters/repos/db/reindex_provider.go) — DTM provider, three-phase swap, `flipSemanticMigrationSchema`.
- [`adapters/repos/db/reindex_provider_payload.go`](../adapters/repos/db/reindex_provider_payload.go) — `ReindexTaskPayload`, migration type constants.
- [`adapters/repos/db/reindex_conflict.go`](../adapters/repos/db/reindex_conflict.go) — `CheckConflict`, `CheckPropertyUpdate`, `CheckClassMutation`, `CheckTenantMutation`, `Touches*` predicates.
- [`adapters/repos/db/reindex_recovery.go`](../adapters/repos/db/reindex_recovery.go) — `DiscoverInFlightReindexTasks`, `buildRecoveryTasks`, recovery-only `ShardReindexerV3`.
- [`adapters/repos/db/reindex_cancel_cleanup.go`](../adapters/repos/db/reindex_cancel_cleanup.go) — `CleanStalePartialReindexState`.

**Strategy + finalize**

- [`adapters/repos/db/inverted_reindex_strategy.go`](../adapters/repos/db/inverted_reindex_strategy.go) — `MigrationStrategy` interface, `applyPerPropertySchemaUpdate`, `reindexTaskConfig`.
- [`adapters/repos/db/inverted_reindex_strategy_*.go`](../adapters/repos/db/) — one per strategy.
- [`adapters/repos/db/inverted_reindex_strategy_dir_names.go`](../adapters/repos/db/inverted_reindex_strategy_dir_names.go) — `genSuffix`, `parseMigrationDirName`, strategy dir prefix constants.
- [`adapters/repos/db/inverted_reindex_task_generic.go`](../adapters/repos/db/inverted_reindex_task_generic.go) — `ShardReindexTaskGeneric`, the **phase-contract godoc** at the top of the file is the authoritative spec.
- [`adapters/repos/db/inverted_reindex_finalize.go`](../adapters/repos/db/inverted_reindex_finalize.go) — `FinalizeCompletedMigrations`, `nextMigrationGeneration`, `maxMigrationGeneration`, `completedMigrationGens`.

**LSM primitives**

- [`adapters/repos/db/lsmkv/store.go`](../adapters/repos/db/lsmkv/store.go) — `SwapBucketPointer`, `FinalizeBucketSwap`, `updateBucketDir`. The two function godocs document the in-memory-vs-disk split.
- [`adapters/repos/db/lsmkv/segment_group.go`](../adapters/repos/db/lsmkv/segment_group.go) — `PrependSegmentsFromBucket`.

**Inverted analyzer / overlay**

- [`adapters/repos/db/inverted/tokenization.go`](../adapters/repos/db/inverted/tokenization.go) — `TokenizationResolver`, `ResolveTokenization`.
- [`adapters/repos/db/shard.go`](../adapters/repos/db/shard.go) — `SetTokenizationOverlay`, `ClearTokenizationOverlay`, `TokenizationFor` (with the self-clear backstop).

**DTM**

- [`cluster/distributedtask/doc.go`](../cluster/distributedtask/doc.go) — package-level architecture + the four "journey" shapes.
- [`cluster/distributedtask/types.go`](../cluster/distributedtask/types.go) — `Task`, `Unit`, `UnitSpec`, `TaskStatusPreparing`, `TaskStatusSwapping`, `NeedsPreparationBarrier`, `TaskStatus.IsActive()` / `IsCoordinationPhase()` helpers.
- [`cluster/distributedtask/manager.go`](../cluster/distributedtask/manager.go) — FSM. `RecordPostCompletionAck`, `MarkTaskFinalized` godocs are essential reading.
- [`cluster/distributedtask/scheduler.go`](../cluster/distributedtask/scheduler.go) — per-node loop, callback dispatch.
- [`cluster/distributedtask/errors.go`](../cluster/distributedtask/errors.go) — permanent-rejection sentinels + gRPC wire encoding.

**Schema FSM**

- `cluster/proto/api/` — `PropertyField*` constants used by the fieldmask, `FromInFlightMigration` flag.
- `cluster/schema/meta_class.go` — `MergePropsMasked` fieldmask apply path.
- `usecases/schema/` — `UpdatePropertyInternal`, `UpdatePropertyInternalFromMigration`.

## 15. Tests of interest

Tests are layered: unit close to the symbol; per-package integration
(build tag `integrationTest`) where multi-component interaction
matters; acceptance under [`test/acceptance/reindex_*`](../test/acceptance/)
with the modern testcontainer style.

**Acceptance — single node** ([`test/acceptance/reindex_singlenode/`](../test/acceptance/reindex_singlenode/)):

- `happy_path` per migration type (one file each:
  `enable_filterable_test`, `enable_searchable_test`,
  `enable_rangeable_test`, `change_tokenization_test`,
  `change_tokenization_filterable_test`, `blockmax_test`,
  `roaring_set_test`).
- `delete_then_reenable_test` / `delete_reenable_multicycle_test` /
  `delete_reenable_indexing_bleed_test` / `delete_reenable_shortcircuit_test`
  — the #10675 family + the `mergeReindexStatus` finalize-window
  override bound test.
- `change_tok_delete_journeys_test` — the cross-strategy clobber +
  `cleanStaleMigrationDirs` family.
- `cancel_test` / `cancel_then_retry_test` — cancel + the
  defense-in-depth cleanup.
- `torn_resume_test` / `restart_during_swap_test` — crash recovery in
  every phase boundary.
- `property_state_migration_matrix_test` — exhaustive matrix
  (~510 cells × 6 data types × 15 body shapes).
- `scope_assertion_test` — blast-radius bounds: a migration on one
  property never mutates buckets on any other property.
- `api_validation_test` — REST contract.
- `repair_rangeable_test` / `finished_race_test`.

**Acceptance — multi-node** ([`test/acceptance/reindex_multinode/`](../test/acceptance/reindex_multinode/)):

- `happy_path_test` — 3-node baseline.
- `finalizing_window_test` — per-shard tokenization overlay coverage.
- `finalizing_crash_test` — ack barrier rehydrate over restart.
- `restart_matrix_test` — rolling restart × migration type.
- `restart_test` — full restart at every sentinel boundary.
- `post_restart_test` — recovery + finalize after planned restart.
- `round_trip_test` — the original word → field → word pin for #10675.
- `round_trip_adjacent_test` — adjacent journeys (multi-round,
  different tokenizations, filterable-only, searchable-only,
  enable-then-change, MT, concurrent-different-props).
- `in_flight_rangeable_test` — query correctness during enable-
  rangeable mid-flight (relies on `rangeableLocalReady`).
- `migration_journeys_test` — full lifecycle coverage of every
  semantic migration.
- `concurrent_migrations_test` — parallel non-conflicting submits.

**Acceptance — concurrency** ([`test/acceptance/reindex_concurrent/`](../test/acceptance/reindex_concurrent/)):

- `concurrent_test` — same-collection concurrent submits.
- `parallel_conflict_matrix_test` — every (migrationA × migrationB)
  pair on overlapping vs disjoint properties.
- `parallel_same_property_test` — race-condition coverage on the
  per-(class, prop) submit lock + FSM conflict check.

**Acceptance — multi-tenant** ([`test/acceptance/reindex_mt/`](../test/acceptance/reindex_mt/)):

- `reindex_mt_test` — `?tenants=` filtering, per-tenant repair,
  FROZEN-tenant resume, per-tenant `OnGroupCompleted` barrier.

**Distributed task framework** ([`test/acceptance/distributed_tasks/`](../test/acceptance/distributed_tasks/)):

- `unit_tracking_test` — end-to-end unit creation under reindex
  provider on 3-node cluster.

**Shared helpers** — [`test/acceptance/helpers/reindex/helpers.go`](../test/acceptance/helpers/reindex/helpers.go).
HTTP-level helpers (`SubmitIndexUpdate`, `AwaitReindexFinished`,
`GetIndexes`, `AwaitReindexViaIndexes`, `BoolPtr`, `IdsMatchUnordered`)
with `WithTenants` / `WithTimeout` functional options. Plus the
fixture API: `SetupClass`, `SetupClassWithConfig`, `ImportObjects`,
`WithEnv` — consolidated from prior duplicated copies across the four
test packages.

**Unit tests of interest**

- `mergeReindexStatus` — `handlers_indexes_edge_test.go` /
  `handlers_indexes_gaps_test.go`.
- `checkReindexConflict` / `ReindexPropsOverlap` /
  `TypesConflictReason` / `Touches*` — `reindex_conflict_test.go`.
- `failUnit` and recovery — `reindex_provider_failunit_test.go`,
  `reindex_provider_recovery_test.go`,
  `reindex_provider_repair_guidance_test.go`.
- `parseMigrationDirName`, `nextMigrationGeneration`, multi-gen
  `FinalizeCompletedMigrations` paths —
  `inverted_reindex_finalize_test.go`.
- `OnGroupCompleted` cache + rehydrate —
  `reindex_provider_on_group_completed_test.go`.
- Tokenization overlay set/clear/self-clear —
  `reindex_provider_tokenization_overlay_test.go`,
  `shard_tokenization_overlay_test.go`.
- Shard CoW callbacks — `shard_callbacks_test.go`.
- DTM finalizing + ack barrier — `cluster/distributedtask/manager_test.go`,
  `scheduler_multinode_test.go`, `errors_test.go`.
- LSM swap primitives — `lsmkv/store_bucket_swap_test.go`.
- Atomic-phase regression guard — `inverted_reindex_task_generic_test.go`
  (the `testHookPostPropSwap` wall-clock budget assertion).

## 16. Deferred simplifications

[`docs/proposals/deferred_reindex_simplifications.md`](./proposals/deferred_reindex_simplifications.md)
catalogues three refactors that the scout pass identified as
worthwhile but that were deliberately not applied autonomously,
because each touches either a crash-safety path or the hottest write
hook. Re-evaluated for the v1.38 Preview merge and kept deferred.
Source of truth for follow-up work.
