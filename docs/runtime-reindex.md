# Runtime property reindex

Change inverted-index configuration on a live class — without restarting
the cluster, without losing writes that arrive during the rebuild, and
without dropping query availability on the affected property.

This document is the entry-point for anyone reviewing or extending the
feature. It covers the full surface area: REST API, architecture layer
by layer, the migration-strategy catalogue, crash-safety contract,
concurrency model, multi-tenancy, property overlay, the
deferred-finalize / per-migration generation design that closes the
load-bearing #10675 family of data-loss bugs, and the test map.

The package-level godocs of the touched packages are the per-symbol
source of truth; this doc is the navigable overview that ties them
together. If a section here disagrees with a godoc in source, the
godoc wins — and that's a bug in this doc.

> **How migration state is stored, as of this build.** One JSON record per
> (shard, migration) at `<shard>/lsm/.migrations/records/` holds it, in five
> states — Iterating, Iterated, Merged, Swapped, Promoted — and
> reconciliation at shard load makes every load-time decision. The record
> names each directory the migration touches, so the swap removes the
> directory it displaces at the recorded handle rather than renaming it to a
> derived backup name. `inverted_reindex_record.go` and
> `inverted_reindex_reconcile.go` are the source of truth.
>
> Two sections describe the retired representation end to end and should be
> read as history only: §9.4 and §9.5. Elsewhere the
> stale parts are individual mentions of a marker file (`started.mig`,
> `reindexed.mig`, `prepended.mig`, `merged.mig`, `swapped.mig`, `tidied.mig`,
> `properties.mig`), of `FinalizeCompletedMigrations`, of
> `completedMigrationGens`, of a rename-to-backup, or of a "tidied" state. The
> API, the strategy catalogue, the concurrency model, multi-tenancy and the
> tokenization overlay are unaffected and still accurate.

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
- Migrate a searchable index from WAND (Map) to BlockMax (Inverted):
  `change-algorithm`.
- Repair a bucket suspected of corruption: `repair-filterable`,
  `rebuild-searchable` (rebuild an existing BlockMax bucket in place),
  `repair-rangeable`.
- Upgrade a searchable index from the deprecated WAND format to
  BlockMax: `change-algorithm`.
- Cancel a migration until every tenant's swap is decided (`SWAPPING`).
  Tenants already decided keep their swap, which can leave the collection
  partly migrated (§8, §12).

The whole feature is built on top of three substrates:

- The **LSM bucket-swap primitives** ([`adapters/repos/db/lsmkv/store.go`](../adapters/repos/db/lsmkv/store.go)
  `SwapBucketPointer` + `FinalizeBucketSwap`) — the atomic in-memory
  pointer flip and its deferred on-disk counterpart.
- The **Distributed Task Manager (DTM)** ([`cluster/distributedtask/`](../cluster/distributedtask/))
  — RAFT-backed task state, per-unit assignment, group barriers,
  per-node `PREPARING` and `SWAPPING` coordination states, two-phase
  PreparationComplete + PostCompletion ack barriers (§6.3).
- The **schema FSM mutation guard** — rejects external mutations on
  classes / properties / tenants while a reindex is in flight, so the
  bucket↔schema invariant cannot be broken by a concurrent
  `DeleteClass` or `UpdateProperty`.

The reindex feature itself is the orchestration that ties these
together into a small REST surface rooted at
`/v1/schema/{className}/properties/{propertyName}/index/{indexType}`.

## 2. REST surface

> **Runtime reindex is off by default.** Set
> `RUNTIME_REINDEX_ENABLED=true` to enable it. While it is off, submitting
> a migration returns `400 Bad Request` with
> `runtime reindex is disabled; enable with RUNTIME_REINDEX_ENABLED=true`.
> The cancel verb and `GET .../indexes` keep working, so a task that was
> already running stays observable and cancellable (§12). Everything below describes behavior with
> the flag on, except recovery at shard load (§6.2), which re-arms or
> stamps an in-flight migration whatever the flag says.

A replica movement requested while a migration runs on its collection is
refused with 409. One that reaches a shard still being cleaned up after a
cancelled or failed migration waits for the cleanup to finish.

### `PUT /v1/schema/{className}/properties/{propertyName}/index/{indexType}`

Declaratively upsert one inverted index. `{indexType}` is one of
`filterable`, `searchable`, `rangeFilters` (`rangeable`, the former Preview
name, is accepted as a write-only alias; GET returns `rangeFilters`).

The body (`IndexUpsertRequest`) carries only configuration, never a verb:
`tokenization` (searchable / filterable) and `algorithm` (searchable
only). `rangeFilters` takes no fields. An empty body `{}` is valid and
means "ensure the index exists with its current/default config". At most
one of tokenization / algorithm may change per call. The server diffs the
body against current state and picks the migration:

| Path + body | Effect | Migration type |
|---|---|---|
| `.../index/searchable` `{"tokenization":"word"}`, no searchable index yet | Creates a Blockmax bucket, flips `IndexSearchable=true` + `Tokenization`. `text`/`text[]` only; `tokenization` required. | `enable-searchable` |
| `.../index/searchable` `{"tokenization":"trigram"}`, index exists | Rewrites BOTH the searchable and filterable buckets when both exist. | `change-tokenization` |
| `.../index/searchable` `{"algorithm":"blockmax"}`, index on WAND | Map → Blockmax upgrade. `flipSemanticMigrationSchema`, from `OnTaskCompleted`, flips the class-level `UsingBlockMaxWAND` once every searchable property is blockmax. `wand` is rejected (deprecated); this is the only WAND → Blockmax path. | `change-algorithm` |
| `.../index/filterable` `{}`, no filterable index yet | Creates a RoaringSet bucket, flips `IndexFilterable=true`. A supplied tokenization must equal the property's current one. | `enable-filterable` |
| `.../index/filterable` `{"tokenization":"word"}`, index exists, no searchable index | Filterable-only retokenize. Rejected (400) when a searchable index exists, since both indexes share one tokenization; use `.../index/searchable` instead. | `change-tokenization-filterable` |
| `.../index/rangeFilters` `{}`, no range index yet | Creates a RoaringSetRange bucket, flips `IndexRangeFilters=true`. Numeric types only (`int`, `number`, `date`). | `enable-rangeable` |

A PUT whose desired config already matches current state AND has no reindex
task in flight submits no task and returns `200` with `{"status":"NO_OP"}`.
While a partial run stands on the property, such a PUT that names a
tokenization answers `409` instead (§8).
A PUT that converges on a task still in flight also submits no task, but the
config is not in place yet, so it returns `202` with that task's `taskId`.

`202` with `{"status":"STARTED", "taskId": ...}` means a task is running for
the requested configuration and `taskId` is the handle to poll. It does not
assert that this call created the task: a request that converges on an
already-running migration joins it and receives that task's ID.

### `POST /v1/schema/{className}/properties/{propertyName}/index/{indexType}/rebuild`

Rebuild the index from stored objects with unchanged configuration
(repair / format refresh). No request body. A WAND searchable index is
rejected — migrate to Blockmax first via `PUT {"algorithm":"blockmax"}`.

| Index type | Effect | Migration type |
|---|---|---|
| `searchable` (already Blockmax) | Rebuild the Blockmax bucket. | `rebuild-searchable` |
| `filterable` | RoaringSet refresh. | `repair-filterable` |
| `rangeFilters` | RoaringSetRange rebuild. | `repair-rangeable` |

### `POST /v1/schema/{className}/properties/{propertyName}/index/{indexType}/cancel`

Cancel the in-flight task on `(class, property, indexType)`. No request
body. Idempotent: `202` + `Status: CANCELLED` (with `taskId`) when a
`STARTED` or `PREPARING` task is cancelled; tenants whose swap is already
decided (§6.3) keep it, every other tenant stops. `202` + `Status: NO_OP`
(no `taskId`) when nothing matches (already finished, never submitted, or
already cancelled). `409` for a `SWAPPING` task (it finishes on its own)
and for a status this build does not recognize. See §12. Never `404` for
"nothing to cancel".

Query parameters (PUT and rebuild):

- `?tenants=t1,t2` — scope to named tenants on a multi-tenant class.
  Rejected on single-tenant classes. Rebuild allows it for every index
  type; PUT rejects it outright, because every migration PUT can submit
  is semantic (`IsSemanticMigration`) and the cluster-wide schema flip
  cannot be sub-scoped — all tenants must migrate together.

Cancel takes no query parameters; a `tenants` value on the URL is silently
ignored. Cancel targets the task itself, which already carries the tenant
set of the rebuild that submitted it, so `POST .../cancel` after
`rebuild?tenants=t1,t2` stops only that work, leaving other tenants untouched.

Response shapes (PUT / rebuild / cancel):

- `200 OK` — PUT only: desired config already in place AND no task in
  flight, so no task submitted (`{"status":"NO_OP"}`).
- `202 Accepted` — a task is running for the requested configuration (PUT /
  rebuild, `status: STARTED`, `taskId` naming that task), or cancel
  processed; body is an `IndexUpdateResponse` (`taskId` + `status`).
  `STARTED` asserts that the task is running, not that this call created it:
  a PUT converging on an already-running migration joins it and receives that
  task's ID. Cancel answers `CANCELLED` + `taskId` when it cancelled a
  cancellable task, or `NO_OP` (no `taskId`) when nothing matched; it is
  idempotent and never returns 404 for "no task to cancel".
- `400 Bad Request` — validation failure with an actionable hint (e.g.
  "property X has no searchable index; PUT
  `/v1/schema/{className}/properties/X/index/searchable` with a
  tokenization to add one first").
- `404 Not Found` — class or property doesn't exist.
- `409 Conflict` — four distinct meanings. On PUT / rebuild, an in-flight
  task already touches this property; the error names the offending task
  ID unless another submit won the race. On PUT / rebuild, a replica
  movement is in flight on the collection. On PUT / rebuild, a partial run
  stands on the property (§8); the error names the resubmit that finishes it.
  On cancel, either arm can refuse. The pre-flight refuses a status this
  build does not recognize and names the task ID and that status. The
  apply refuses a target that reached such a status or a terminal one
  after that read, and names no status, since it cannot tell which. Neither refusal cancelled anything, but the
  apply refusal cannot tell you what did: a concurrent cancel may have won
  the race, so re-read the index status to see where it landed —
  `CANCELLED` included. A pre-flight refusal leaves the task in flight,
  and a status this build does not recognize keeps reading as `indexing`
  here until it reaches a terminal state on the nodes that do (§12).
- `422 Unprocessable Entity` — `{indexType}` outside the enum.
- `429 / 503` — per-collection in-flight cap reached (default 32), or the
  cluster service is unavailable / an in-flight task's payload cannot be
  parsed so conflict-freedom cannot be proven / a local worker of an earlier
  task on this property did not exit within 10 s (typically right after a
  cancel; retry shortly).

### `DELETE /v1/schema/{className}/properties/{propertyName}/index/{indexType}`

Drop a configured inverted index. `{indexType}` is one of `filterable`,
`searchable`, `rangeFilters` (`rangeable` accepted as an alias). Flips the
corresponding schema flag to false, drops the bucket dir, and removes the
sidecar buckets, so a subsequent re-enable starts from a clean slate. Subject to the same MutationGuard as
`UpdateProperty` — rejected while a reindex on this property is in flight,
or while a partial run stands on it (the answer names the resubmit; §8).

### `GET /v1/schema/{className}/indexes`

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

Status values: `ready`, `pending`, `indexing`, `failed`, `cancelled`,
`partial` (a stopped run left some tenants migrated; `migratedTenants` and
`totalTenants` count them). The counts, and the tenant counts in a partial
run's refusals, go only to a caller who may read the collection's tenants;
a count of tenants is tenant metadata.
The status is synthesized in `mergeReindexStatus` from the DTM task
list crossed with the schema flags. Both values are read from the
node serving the request, and the task list is read first, so the
schema is never the older of the two: a class read at a later applied
index carries every flag flip the tasks it is compared against have
already committed. A FINISHED task surfaces nothing of its own — the
schema flag alone decides whether an entry is emitted.

#### The apply edge

`PUT .../index/{indexType}` returns `202` as soon as the **leader** applied
the add-task entry (`Raft.Execute`). On a follower the call does not wait for
that node's own apply; a leader-served PUT has already applied locally when
the `202` returns. Cancel works the same way: it decides on the leader-routed
task list and applies through `Raft.Execute`.

A GET against a lagging follower right after that `202` finds no task and
answers from the flags alone, in one of two shapes:

- `enable-*`, flag still off: the entry is dropped. The `indexes` array reads
  `null` only when every applicable type on the property is off; with
  `indexFilterable` still on you get a non-null array that is merely missing
  one entry.
- `change-tokenization` / `change-algorithm`, flag already on at submit time:
  the entry emits as `ready` with the pre-migration tokenization, reporting
  the property done and unchanged.

For a client pinned to one node the window is bounded by that node's
replication lag, and the entry does not flap. It is left open deliberately:
closing it means blocking a mutation on a local apply, which turns a `202`
into a latency-bound call that can hang on a partitioned follower, to remove
a transient polling artifact.

A client that round-robins across nodes does see it flap. Nodes that have
applied the add-task entry and nodes that have not answer differently, so the
pill alternates between `indexing` and no entry at all, and `algorithm`
between `blockmax` and `wand` for a property whose stamp only one of them has.

This endpoint is node-local while `GET /v1/tasks` is leader-routed, so a UI
polling both against a lagging follower can render the task FINISHED and the
index pill `indexing` at the same moment.

### RBAC

Authorization is symmetric across every index write verb: `PUT` (upsert),
`POST .../rebuild`, `POST .../cancel`, and `DELETE` all require `UPDATE` on
`Collections`, which resolves to **both** `update_collections` (metadata) and
`update_data`. A per-property index write rewrites indexed data, so metadata
permission alone is insufficient — a caller with only `update_collections`
gets `403`.

`GET .../indexes` requires `READ` on `CollectionsMetadata`, i.e.
`read_collections`.

| Verb | Permissions |
| --- | --- |
| `GET .../indexes` | `read_collections` |
| `PUT .../index/{indexType}` | `update_collections` + `update_data` |
| `POST .../index/{indexType}/rebuild` | `update_collections` + `update_data` |
| `POST .../index/{indexType}/cancel` | `update_collections` + `update_data` |
| `DELETE .../index/{indexType}` | `update_collections` + `update_data` |

Mutations authorize before any conflict/in-flight pre-flight, so an
unprivileged caller never learns whether a reindex is running (no `404`/`409`
leak). The REST handler and the manager (`DeleteClassPropertyIndex`) enforce
the same permission.

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
    `change-tokenization-filterable`, `enable-filterable`,
    `enable-searchable`, `enable-rangeable`,
    `change-algorithm`):
    `STARTED → PREPARING → SWAPPING → FINISHED`.
    `PREPARING` is reached once every unit is terminal. A group (one
    per tenant) prepares as soon as its own units are COMPLETED, even
    while the task is STARTED, and its swap is decided once every node
    owning one of its units acked PREP success. `PREPARING → SWAPPING`
    happens once every group is decided, and `SWAPPING → FINISHED`
    once no replica owes a swap (§6.3).
  - **Format-only migrations** (`repair-filterable`,
    `repair-rangeable`, `rebuild-searchable`):
    `STARTED → SWAPPING → FINISHED`.
    `PREPARING` is skipped because there is no cross-replica
    state alignment to bound — each shard's `RunOnShard`
    completes the full lifecycle locally and there is no
    cluster-wide schema flip.
  - Either path may terminate at `FAILED` (a failed PREP ack, or a
    failed swap ack while `SWAPPING`) or `CANCELLED` (operator-initiated).
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
                │         PUT .../properties/{prop}/index/{type}         │
                └──────────────────────────┬─────────────────────────────┘
                                           │
        ┌──────────────────────────────────▼─────────────────────────────┐
        │  REST handler (adapters/handlers/rest/handlers_indexes.go)    │
        │   • Per-(class, prop) ReindexSubmitLock                       │
        │   • Validate body, classify migration type, dispatch          │
        │   • checkReindexConflict (read-side mirror of FSM check)      │
        │   • Pre-submit DB.NewStalePartialReindexSweep                 │
        │   • AddDistributedTaskWith[Groups]Barrier("reindex", payload) │
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
        │   • Semantic: RunReindexOnlyOnShard — iterate objects, write  │
        │     to __reindex_<N>/ bucket; install double-write callbacks; │
        │     stop at markReindexed (the swap waits for the barrier)    │
        │   • Format-only: RunOnShard — the same iteration, then prep   │
        │     and swap, all locally                                     │
        │   • UnitStatus = COMPLETED  ← per-Unit status                 │
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
        │   RecordPreparationCompleteAck(group, success) — per group+node│
        └──────────────────────────────────┬─────────────────────────────┘
                  Every group decided (each group's owners acked success)
                                           │   FSM gates the transition
                                  ┌────────▼────────┐
                                  │ SWAPPING        │ ← TaskStatus
                                  └────────┬────────┘   (replicas may differ
                                           │            for a window that
                                           │            grows with in-flight
                                           │            PREP and swaps)
                                           │  scheduler fires per-node
        ┌──────────────────────────────────▼─────────────────────────────┐
        │  Provider.OnSwapRequested (per-node, semantic only, barrier)  │
        │   PHASE B — OVERLAY + SWAP per local shard:                   │
        │    2. OVERLAY SET: per-shard tokenization resolver            │
        │    3. ATOMIC SWAP: SwapBucketPointer per prop (microseconds)  │
        │    + post-atomic inline tidy (Shutdown + rename old → backup) │
        │    + OnMigrationComplete (per-strategy hook)                  │
        │   RecordPostCompletionAck(group, success) — pays the debt     │
        │                                                                │
        │  (Format-only path: neither callback does swap work —        │
        │   RunOnShard already finished PREP+SWAP on the node during    │
        │   StartTask. No PreparationCompleteAck barrier; SWAPPING      │
        │   fires directly from AllUnitsTerminal.)                      │
        └──────────────────────────────────┬─────────────────────────────┘
              No replica owes a swap (format-only: every node acked)
                                           │
        ┌──────────────────────────────────▼─────────────────────────────┐
        │  Provider.OnTaskCompleted (per-node, semantic only)           │
        │   flipSemanticMigrationSchema — RAFT UpdatePropertyInternal   │
        │   (idempotent: every node fires, first commit wins)           │
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
        │     remove backup dirs, lower-gen sidecars                    │
        └────────────────────────────────────────────────────────────────┘
```

Format-only migrations (`repair-filterable`,
`repair-rangeable`, `rebuild-searchable`) skip the OnGroupCompleted
barrier. Each shard runs the full lifecycle inside its own `RunOnShard`,
which sequences `RunReindexOnlyOnShard` → `RunPrepareOnShard` →
`RunSwapOnShard` against the sentinels on disk, and there is no
cluster-wide schema flip. Because every phase dispatches on persisted
state, a task relaunched after a restart resumes from wherever the
previous run stopped rather than reporting the unit COMPLETED with the
swap still outstanding. The flow is otherwise identical.

### What goes through RAFT vs. what is local-only

The diagram is easy to misread as "PREPARING and SWAPPING are
cluster-wide synchronization moments; every node atomically swaps in
lock-step". The truth is more nuanced — the FSM transitions ARE
cluster-wide RAFT commits, but each node's local PREP and SWAP work
runs on its own timeline. The barrier (`PREPARING → SWAPPING`) bounds
the cross-node window where local timelines diverge.

What goes through RAFT (cluster-wide commits):

- `AddTask` — task created at STARTED.
- `UpdateUnitProgress` / `RecordUnitCompletion` — per unit, on the
  node that owns the unit.
- The transition `STARTED → PREPARING` (semantic) or
  `STARTED → SWAPPING` (format-only) — happens once the cluster-wide
  `AllUnitsTerminal` predicate becomes true on the Manager's FSM
  state. Routing depends on `Task.NeedsPreparationBarrier`.
- `RecordPreparationCompleteAck` — one per group and node, with
  `Success=bool`. Semantic-migration path only — fires after the node's
  PREP for that group (`OnGroupCompleted`) returns, in STARTED or PREPARING.
- The transition `PREPARING → SWAPPING` — committed inside
  `RecordPreparationCompleteAck`'s apply once every group is decided.
- `RecordPostCompletionAck` — one per group and node for barrier tasks
  (after `OnSwapRequested`; a success pays that replica's swap debt),
  one per node for non-barrier tasks (after `OnGroupCompleted`).
- The transition `SWAPPING → FINISHED` — committed by
  `MarkTaskFinalized` once no swap is owed (barrier) or every node
  acked success (non-barrier).
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

1. **Per-shard property overlay (the per-node bridge).** Between the
   local `SwapBucketPointer` (bucket is now NEW) and the eventual
   cluster-wide schema flip (schema flag is now NEW), this node's
   analyzer would still describe the property the pre-migration way.
   The overlay installs the target view at the per-shard query and
   write paths, so reads in that window tokenize against the content
   the bucket now holds, and writes in it reach the migrated index.
   No task status clears it: only the schema-update sweep that removes a
   disabled index's bucket retires an entry's fields, and otherwise the
   entry lives until the shard unloads. See §10.
2. **Two-phase ack barrier (the cross-node handshake).** See §6.3. A
   failed PREP ack, or a failed swap ack while `SWAPPING`, fails the task,
   so `OnTaskCompleted` skips the cluster-wide schema flip.

## 4. Architecture by layer

### 4.1 REST layer — `adapters/handlers/rest/handlers_indexes.go` + `handlers_reindex.go`

Validation, dispatch, response shaping. Two structural details worth
calling out:

**Per-`(collection, property)` submit lock.** Held across class read,
validation, conflict check, and the RAFT `AddDistributedTaskWithBarrier`
(or `AddDistributedTaskWithGroupsBarrier` for a multi-tenant semantic
migration) call.
Without this, a parallel `DELETE /properties/{p}/index/{name}` could
win the lock between the PUT's `ReadOnlyClass` snapshot and its
RAFT task-add, leaving the PUT validating against a schema that no
longer matches the on-disk bucket. The lock is process-wide; the
multi-node version of the same race is closed by the FSM-side
`MutationGuard` (see §4.3). Stored on `state.ReindexSubmitLocks` so the
PUT handler and the DELETE-property-index handler share the same
entries. See [`adapters/handlers/rest/state/reindex_submit_locks.go`](../adapters/handlers/rest/state/reindex_submit_locks.go).

**Pre-submit `DB.NewStalePartialReindexSweep` sweep.** Defense in depth
against the CANCEL→retry silent-failure family (same shape as
DELETE→re-enable): if a previous cancelled run left a stale migration
record + sidecar dirs on disk, the new task
would resume against them, finish in <1s with a no-op, flip the schema
flag, and report success against an empty bucket. The cancel handler
already runs this synchronously, but the wait can time out or the node
can crash mid-cancel. Submit-time cleanup catches that gap. Critically,
for `change-tokenization` the sweep wipes BOTH `searchable` AND
`filterable` migration dirs — cleaning only one of them was the root
cause of a Sev 1.

### 4.2 Distributed Task Manager — `cluster/distributedtask/`

The substrate. Package doc at [`cluster/distributedtask/doc.go`](../cluster/distributedtask/doc.go)
documents the four "journey" shapes; reindex uses **Journey 3** for
semantic migrations and **Journey 2** for format-only ones, plus
**Journey 4** for per-tenant grouped MT.

Key types & contracts:

- **`Manager`** — the RAFT FSM. Owns task state, applies `AddTask`,
  `UpdateUnitProgress`, `RecordUnitCompletion`, `RecordPreparationCompleteAck`,
  `RecordPostCompletionAck`, `MarkTaskFinalized`, `CancelTask`,
  `CleanUpTask`. Every mutation is FSM-deterministic.
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
  namespace (here, by `ReindexDetector.CheckConflict`). Called under
  Manager's lock from the AddTask apply path, BEFORE the task is
  appended. FSM-deterministic.
- **`SchemaMutationDetector`** — the other half of the conflict
  surface: gates external schema mutations
  (`UpdateProperty`/`DeleteClass`/`DeleteTenants`/`UpdateTenants`)
  while a reindex is in flight. Implementation:
  `ReindexDetector.CheckPropertyUpdate` / `CheckClassMutation` /
  `CheckTenantMutation`. Uses `TaskStatus.IsActive()`, so every
  non-terminal status counts as "in flight" for mutation gating (see
  **Task status classification** below).
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
  post-completion callback had committed its bucket-pointer flip. For what
  a tenant's replicas serve meanwhile, see "Cross-replica window" in §6.3.
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

#### Task status classification

`TaskStatus` is classified by five predicates in
[`cluster/distributedtask/types.go`](../cluster/distributedtask/types.go):

| Predicate | True for |
|---|---|
| `IsTerminal()` | `FINISHED`, `FAILED`, `CANCELLED` |
| `IsActive()` | everything else (defined as the exact negation of `IsTerminal()`) |
| `IsCompleted()` | `SWAPPING`, `FINISHED` — every unit succeeded |
| `IsRecognized()` | any status this build declares |
| `IsCancellable()` | `STARTED`, `PREPARING` (in `SWAPPING` nothing is left to cancel) |

`IsCancellable()` is a literal comparison rather than a classification,
which is what makes it different from the other four. `Manager.CancelTask`
is an FSM apply, so every binary that will ever replay the entry has to
reach the same verdict, including one that cannot name the status.
Classifying would let a node that has never heard of a status cancel a
migration a newer node is still coordinating, and follower apply errors
are discarded, so the divergence would be silent.

Because `IsActive()` is the negation of `IsTerminal()` rather than a list
of known in-flight statuses, **a status this build does not recognize
counts as in flight**. That is the case an older node hits when a newer
release introduces a status and the cluster is mid rolling-upgrade.
`IsRecognized()` deliberately has no `default` case, so the `exhaustive`
linter fails until a newly added status is classified here.

The default is fail-closed on purpose: reading an unknown status as
"done" would admit a second migration onto a property a newer node is
still migrating, and would let the TTL sweep delete live state. It costs
availability instead. For as long as such a task is in the node's state,
the node:

- rejects schema mutations overlapping the task's properties, and rejects
  new reindex submits that overlap them (`CheckPropertyUpdate`,
  `CheckClassMutation`, `CheckTenantMutation`, `CheckConflict`);
- refuses backups on the shards the task lists
  (`db.NewShardReindexActivityLookup` feeding
  `Index.refuseIfReindexInFlight` through a `newReindexGate` gate);
- reports the property's index as `indexing` on `GET .../indexes` rather
  than `ready` or `pending`, since the per-unit progress does not prove
  that no shard has started;
- refuses a cancel with 409, at the REST pre-flight and again at
  `Manager.CancelTask`. `IsCancellable()` names only the in-flight
  statuses this build declares, so an unrecognized status is not
  cancellable. The mutation-refusal messages
  say so directly: for this case they drop the usual "cancel it" advice
  and tell the operator the task has to reach a terminal state on the
  nodes that do recognize the status (`MutationRemedy` in
  [`reindex_conflict.go`](../adapters/repos/db/reindex_conflict.go)).

The backup gate routes through `db.IsLiveReindexTaskStatus`, so the rule
for a new status is answered in one place.

TTL cleanup is the one exit. `Manager.CleanUpTask` refuses a task that is
both active **and** recognized, so an unrecognized-status task is deleted
once its `completedTaskTTL` has elapsed. That is sound because the
`Scheduler` TTL sweep is the only proposer of `CLEAN_UP` (pinned by
`TestStructuralInvariant_TTLSweepIsTheOnlyCleanUpProposer`) and it filters
on `IsActive()` against the list it read from the leader. So a `CLEAN_UP`
naming a status this build cannot classify can only come from a node that
does recognize it and reads it as terminal, which is exactly when deleting
the local copy is right. Without the exit the entry is unreachable
forever: no transition can advance it (`MarkTaskFinalized` refuses every
status but `FINISHED` and `SWAPPING`), no later sweep on this node
proposes it, and it keeps blocking schema mutations and backups on its
collection through the local map.

`Scheduler.warnOnUnrecognizedStatuses` names every task in an
unrecognized status on one aggregated warn line, sampled at three lines
an hour. The condition holds until an operator acts on it, so the log is
the diagnostic and not the signal: the per-namespace gauge
`weaviate_distributed_tasks_unrecognized_status` is written on every
tick, which is what makes the state alertable. The line does not offer a
cancel — the FSM refuses one for exactly this task set.

The cost of the two directions is asymmetric, which is what governs how
new statuses get introduced. Adding a new **terminal** status is only
safe once every version in the supported upgrade and rollback range
recognizes it: a node that does not reads a finished task as in flight,
dropping schema mutations the rest of the cluster has already committed.
That window normally closes at `completedTaskTTL` — a node that does
recognize the status proposes the clean-up, the entry replicates, and
the unrecognized arm of `CleanUpTask` above deletes the old node's copy.
Three cases have no such bound: a **full rollback**, where no node
recognizes the status so nothing ever proposes a clean-up; a node
that misses the clean-up entry and is caught up by a snapshot instead,
because `Manager.Restore` merges into the existing task map rather than
replacing it (weaviate/0-weaviate-issues#245); and a
node whose `completedTaskTTL` is longer than the proposer's, because
`CleanUpTask` re-checks the age against that node's own clock and TTL
inside the apply, and an apply that refuses is logged and dropped rather
than retried.
Adding a new **non-terminal** status is cheaper,
because the fail-closed reading is the correct one, but it is not free:
such a node still refuses backups on the collection and reports the index
as `indexing`.

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
`UpdatePropertyInternalFromMigration` and set only by the provider's
`OnTaskCompleted` path. The MutationGuard described next would refuse a
migration's own schema flip, because its run is still in flight. The
flag routes the flip to `CheckMigrationPropertyUpdate` instead, which
admits it only while a run on that property is `SWAPPING`, so a flip
arriving after its run ended is refused.

**`MutationGuard` (cross-FSM).** The schema FSM consults
`distributedtask.SchemaMutationDetector` on every `UpdateProperty`,
`DeleteClass`, `UpdateTenants(FROZEN)`, `DeleteTenants` apply.
`ReindexDetector`'s implementation rejects any mutation overlapping the
properties of an in-flight reindex task on the same collection. In flight
means any non-terminal status, admitted via `TaskStatus.IsActive()` (see
§4.2). The motivating failure mode is summarized on
`CheckPropertyUpdate`'s godoc: a `change-tokenization` migration spawns
separate per-shard sub-tasks for searchable and filterable; a DELETE
arrives mid-flight; the stale-sidecar sweep wipes the searchable
sub-task's working copy; the searchable sub-unit FAILs; the filterable
sub-unit commits its local swap; per-shard ack barrier sees mixed acks;
task FAILED; `flipSemanticMigrationSchema` skipped; schema stays at OLD
tokenization while the filterable bucket on disk holds NEW-tokenized
data. **Bucket↔schema inversion.** Same family as the ack-barrier
failure mode but triggered by an external schema mutation instead of
a crash.

The guard is intentionally blanket: any overlap rejects. Migration-
driven flips (`FromInFlightMigration=true`) are checked by
`CheckMigrationPropertyUpdate` instead, which admits them only while a
run on the property is `SWAPPING`. Class-wide
(`CheckClassMutation`) is stricter — any reindex on the class
(regardless of property) blocks DeleteClass.

### 4.4 Reindex orchestration — `adapters/repos/db/reindex_*` & `inverted_reindex_*`

**`ReindexProvider`** ([`reindex_provider.go`](../adapters/repos/db/reindex_provider.go))
— the DTM `UnitAwareProvider` implementation. Lifecycle hooks:

- `StartTask` → per-node bootstrap. Unmarshals the payload, identifies
  this node's local units, hands them to `processUnits`. Bounded
  concurrency via `ConcurrencyLimiter`.
- `processOneUnit` → per-(unit, shard) bootstrap. Constructs the
  strategy instance(s) at the submission's task version, writes the
  recovery payload, runs the reindex iteration via
  `ShardReindexTaskGeneric`. For semantic migrations it stops at
  `markReindexed` (barrier); for format-only it runs the full
  lifecycle including the swap.
- `OnGroupCompleted` (semantic only) → the swap phase, per local
  shard. Three-phase: PREP → OVERLAY SET → ATOMIC SWAP. See §6.
- `OnTaskCompleted` (semantic only) → `flipSemanticMigrationSchema`
  via RAFT. It does not touch the property overlay: an entry describes
  this shard's buckets, and no task status changes those (§10).
- The RAFT-apply checks are not provider hooks. They live on
  `ReindexDetector` in `reindex_conflict.go`, below. See §4.3 & §7.

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

**`reindex_conflict.go`** — `ReindexDetector`: `CheckConflict` (FSM-deterministic),
`CheckPropertyUpdate`, `CheckClassMutation`, `CheckTenantMutation`,
plus the predicates `ReindexPropsOverlap`, `TouchesSearchable`,
`TouchesFilterable`, `TypesConflictReason`. The exhaustive switches in
the `Touches*` predicates intentionally panic on unknown migration
types so a new `ReindexMigrationType` cannot silently be treated as
"doesn't touch X" — it surfaces on the first request.

**`reindex_recovery.go`** — `MigrationRecoveryReindexer`
(`RecoverMigrations`, `AbandonUnarmed`) and `buildRecoveryTasks`.
`MakeAppState` sets it as the DB's `ShardReindexerV3` before any shard
loads, so each shard rebuilds its in-flight tasks and re-arms their
double-write mirrors before it takes a write. See §6 crash safety.

**`reindex_cancel_cleanup.go`** — `DB.NewStalePartialReindexSweep(tenants)`
returns a `func(ctx, collection, prop, indexType) error` that fans out to
`Shard.CleanStalePartialReindexState` per shard. Called from the cancel
handler (after `SealLocalTaskDrain`), from the submit handler
(defense in depth). Once a task reaches FAILED or CANCELLED,
`autoCleanupAfterTerminal` does not sweep: on every node it has the
reconciler discard the task's records, and their directories, on the shards
the index map holds. Per-shard failures don't stop
iteration so a stuck shard can't permanently wedge a
`(collection, prop, indexType)` tuple.
One sweep serves a whole call so an unloaded shard's directory listing is
read once across the index types a single migration touches; a loaded
shard's sweep always reads the filesystem directly and never acts on the
cached snapshot.

**`inverted_reindex_finalize.go`** — startup-time deferred dir rename
(see §9), `completedMigrationGens`
(`parseMigrationDirName` lives in
`inverted_reindex_strategy_dir_names.go`). The finalize algorithm
handles every shape defensively: tidied / merged-but-not-
tidied / lower-gen sidecars / in-flight gens left alone for
recovery at shard load (§6.2) to pick up.

### 4.5 Strategy implementations — `inverted_reindex_strategy_*.go`

Eight strategy implementations, one file each:

| Strategy | Type | Source bucket | Target bucket | OnMigrationComplete |
|---|---|---|---|---|
| `MapToBlockmaxStrategy` | `change-algorithm` | `searchable` (MapCollection) | `searchable` (Inverted/Blockmax) | No-op; the class-level `UsingBlockMaxWAND` flip is cluster-wide from `OnTaskCompleted`. |
| `RebuildSearchableStrategy` | `rebuild-searchable` | `searchable` (Inverted/Blockmax) | `searchable` (Inverted/Blockmax) | No-op; the property was already searchable + BlockMax, so no schema flag moves. Format-only. |
| `RoaringSetRefreshStrategy` | `repair-filterable` | `filterable` (RoaringSet) | `filterable` (RoaringSet) | No-op (format unchanged). |
| `FilterableToRangeableStrategy` | `enable-rangeable` / `repair-rangeable` | objects → builds RoaringSetRange | `rangeFilters` (RoaringSetRange) | Per-shard `setRangeableLocallyReady` only. `enable-rangeable` is semantic, so cluster-wide `IndexRangeFilters=true` flips from `OnTaskCompleted`; `repair-rangeable` arrives with the flag already true. |
| `EnableFilterableStrategy` | `enable-filterable` | objects → builds RoaringSet | `filterable` (RoaringSet) | No-op; cluster-wide `IndexFilterable=true` flips from `OnTaskCompleted` to avoid the first-shard-flips-wins-the-cluster race. |
| `EnableSearchableStrategy` | `enable-searchable` | objects → builds Blockmax | `searchable` (Blockmax) | No-op; cluster-wide flip from `OnTaskCompleted`. |
| `SearchableRetokenizeStrategy` | `change-tokenization` (searchable half) | `searchable` | `searchable` (new tokenization) | No-op; `Tokenization` flip from `OnTaskCompleted`. |
| `FilterableRetokenizeStrategy` | `change-tokenization` (filterable half) + `change-tokenization-filterable` | `filterable` | `filterable` (new tokenization) | No-op; same flip path. |

`change-tokenization` spawns TWO strategy instances per unit
(`SearchableRetokenizeStrategy` + `FilterableRetokenizeStrategy`) so
the searchable + filterable buckets retokenize in lock-step, with
their per-shard swaps inside the same property-overlay window.
Per-shard cleanup (`indexTypesFromMigrationType`) must wipe BOTH
halves' sidecars — see §4.1.

These instances are constructed and parameterized by
`ShardReindexTaskGeneric` (the generic V3 task lifecycle), so all
strategies share a single state machine, sentinel writer, callback
manager, and progress tracker.

The strategy interface itself documents the per-method contract; see
[`inverted_reindex_strategy.go`](../adapters/repos/db/inverted_reindex_strategy.go).
Of particular note is `OnMigrationComplete`'s phase contract — it
fires in Phase 2c, AFTER the per-prop `SwapBucketPointer` tight loop
and AFTER the inline `oldMain.Shutdown` + `oldMain → backup` rename
loop, but still INSIDE the per-shard property-overlay window on a
semantic migration.

**Semantic vs format-only.** `IsSemanticMigration` is the predicate:
`change-tokenization`, `change-tokenization-filterable`,
`enable-filterable`, `enable-searchable`, `enable-rangeable`, and
`change-algorithm` are semantic. Every shard must reindex before any
shard swaps (Journey 3 barrier), and the schema flip happens
cluster-wide from `OnTaskCompleted`. The rest are format-only: each
shard runs the full lifecycle independently (Journey 2), with no
cluster-wide schema dependency.

**Why `enable-rangeable` is semantic.** It turns an index on, so its
flag is the cluster's statement that every shard can serve range
queries. Flipping it per-shard made that statement from the first
shard to swap, and a task that then stopped left the flag on over
shards holding an empty bucket
([0-weaviate-issues#464](https://github.com/weaviate/0-weaviate-issues/issues/464)).
`repair-rangeable` stays format-only: its flag is already true at
submit, so it has no cutover to coordinate.

### 4.6 LSM primitives — `adapters/repos/db/lsmkv/store.go`

Two primitives carry the load:

**`Store.SwapBucketPointer(ctx, targetName, sourceName)`.** Atomic in-memory
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

**`Store.FinalizeBucketSwap(ctx, bucketName, canonicalDir, currentDir, backupDir)`.**
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
disk-I/O or compaction-wait op inside Phase 2
(`ShardReindexTaskGeneric.processOneSwapPropFn` + wall-clock budget
assertion).

## 5. Migration strategies — quick map

```
                               semantic?   property        analyzer
                                           overlay (§10)   overlay (§11)
change-tokenization              ✓              ✓
change-tokenization-filterable   ✓              ✓
enable-filterable                ✓              ✓               ✓
enable-searchable                ✓              ✓               ✓
change-algorithm                 ✓
enable-rangeable                 ✓              ✓               ✓
repair-rangeable                                                ✓
repair-filterable
rebuild-searchable
```

The six semantic migrations take the cluster-wide barrier and the
cluster-wide schema flip; the three format-only ones take neither. The
analyzer overlay lets a from-scratch build see a property whose schema
flag is still false (`MigrationStrategy.AnalyzerOverlay`). The property
overlay is that same value again, now installed on the shard at the
local bucket flip, plus the target tokenization for the two retokenize
types (which no strategy carries). `change-algorithm` takes neither: it
only swaps the searchable bucket strategy.

So a unit gets a property overlay only when two things hold
(`propertyOverlayViews`): the migration is semantic, and it is a
tokenization change or its strategy's value is non-empty.
`change-algorithm` fails the second, `repair-rangeable` the first.

## 6. Crash safety

### 6.1 The migration record

Every per-shard migration owns one record file under
`<lsm>/.migrations/records/`, named by its task version, strategy code and
unit. It is the only state file a migration writes, and every phase
transition rewrites it atomically:

| Part | Holds |
|---|---|
| `state` | `iterating`, `iterated`, `merged`, `swapped` or `promoted`. |
| `subject.key` | Task version (the generation), strategy code and unit. |
| `subject` | Task ID, migration type, collection, bucket strategy, target and original tokenization, and each property's staged, canonical and sidecar directory. |
| `checkpoint` | The last processed key, while iterating. |
| `flip` | The swapped properties and the directories they displaced. |

### 6.2 Recovery — at shard load

Recovery runs as each shard loads, not at boot, so a shard that never
loads (a cold tenant) costs nothing. `MakeAppState` builds the reindex
provider and the `MigrationRecoveryReindexer` before any shard loads.
Then, per shard load:

1. `Shard.finalizeLegacyMigrations` runs before any record is read and
   before any bucket opens, because it renames bucket directories. It
   finishes the migrations a previous version left finished, as named by
   their tracker files. See §9 for the multi-gen algorithm.
2. The shard loads its migration records once. A records directory that
   cannot be read is reported by the load reconciler, and nothing is
   recovered.
3. `RecoverMigrations` reads those records. Raft may not be open yet, so
   the record is its only source. For each record not yet promoted, it
   rebuilds a per-shard `ShardReindexTaskGeneric`: the generation is the
   record's task version, and a change-tokenization half is chosen by
   the record's strategy code. A record it
   cannot build a task from is stamped `Unmirrored`, unless another record
   on the shard is unreadable, which freezes the store and the load
   reconciler instead. New tasks seed the
   provider's per-descriptor task cache, so `OnGroupCompleted` reuses
   the armed instances instead of calling `OnAfterLsmInit` a second
   time (which would attempt to load already-loaded ingest buckets). A
   reload of the same shard gets back the instances the provider holds,
   which is their only owner. A load after the task ended still arms its
   migration from the record but caches nothing, so an ended task's
   instances are not kept; the provider keeps one descriptor per ended
   task until restart.
4. The load reconciler runs over the same records. It runs after the
   stamp, because it must not promote stale staged data over the live
   bucket.
5. Once the shard's buckets are open, `Shard.armRecoveredMigrations`
   calls `OnAfterLsmInit` on each recovered task, re-installing the
   double-write callbacks before any post-restart write can reach the
   shard. Without this, writes that arrive between shard init and the
   swap that completes a deferred reindex go only to the old main bucket
   and are lost when the swap replaces it with the ingest bucket. The arm
   does not use the request's context, so a client that gives up on a
   lazy load cannot fail it. The outcomes:
   - **The mirror arms.** The migration continues.
   - **The mirror fails to arm and the stamp lands.** Only possible while
     the migration's swap is not committed (below). The shard loads
     writable. `AbandonUnarmed` stamps the record `Unmirrored` and drops
     the unit's tasks from the provider's cache. `RunPrepareOnShard`
     refuses an `Unmirrored` record, and the swap phase swaps no task of a
     unit that has one. A semantic migration fails at this node's prepare
     ack, before any replica or sibling half swaps; a format-only one
     fails its unit, and other replicas may already have swapped. Either
     way it has to be resubmitted.
   - **The mirror fails to arm and no stamp can be written.** This covers a
     failing disk, an unreadable record on the shard (the store then
     refuses every write), and a migration whose swap is committed. The
     shard loads `READONLY` with its own status reason: it serves reads
     and takes no writes, so the staged copy stays current.
     - A swap is committed (`migrationSwapCommitted`) once the other
       replicas made it or will make it. This node must then not give it
       up: a stamp is permanent, so it would leave this replica on the old
       index for good. That holds when the local record is past the flip,
       when RAFT decided the group's swap, when this node acked its
       prepare on a task that is still active, or when the node cannot
       read its task list yet (eager loads at boot; setting the shard
       READY later retries the arm with the list readable).
     - The held migration's own iteration unit (while it is still
       iterating) or prepare, and another migration's unit or prepare on
       the shard, wait and run again on a later tick. A swap
       that a `SWAPPING` task owes on a held shard fails the task instead,
       because such a task cannot be cancelled: the swap stays owed, the
       node makes it once the shard is set READY, and a resubmit then
       lands the schema change (§8). If the shard also holds an
       unreadable record, a unit fails its migration, as on any shard; a
       prepare fails only when its migration has no task cached on the
       shard, and otherwise waits too.
     - The shard list (`GET /v1/schema/<class>/shards`) shows only
       READONLY, with no reason, the same as under disk pressure. The
       node's `reindex_held_shards` gauge counts its held shards, with no
       labels; it drops when a hold is lifted or the shard shuts down or
       is dropped.
     - The owning node logs one Error line per hold, when it starts,
       naming the shard, the cause and the way out. A waiting step's
       retry line is at Debug and names the hold; with Debug on it
       repeats once per held shard and waiting migration on every tick
       and wake. A task whose unit waits is relaunched on every tick and
       wake. A task's first "started distributed task execution" line is
       always logged; the line for each relaunch is sampled on a budget
       of its own: at most 5 a node per 5 ticks. A unit, prepare or swap that waits, including one of a
       migration submitted after the hold began, logs a Warn naming its
       task, unit and shard. These lines share one budget with the line
       about a teardown holding a unit: at most 10 a minute per node.
     - A refused write names the hold's status reason; a refused lift
       names the cause, or that a recovery retry started.
     - Of the status writers, only a manual status update starts a lift, and a restart retries too (below). A vector-index config
       change is refused while the shard is READONLY, and disk-pressure
       recovery lifts only its own reason.
     - A lift never waits for the disk: it starts a retry of the arm in the
       background and is refused at once. If the mirror arms, the retry makes the
       shard writable once disk and memory pressure clears, and the migration continues; if only the stamp lands,
       the migration must be
       resubmitted; otherwise any bucket the retry opened is set back to
       READONLY, the owning node logs the cause at Error level, and the
       next lift starts another retry. Every refused
       lift shows up on each replica where the shard is held as an `apply
       command` Error and in `weaviate_cluster_store_fsm_apply_failures_total`.
       Here that does not mean the cluster state diverged.
     - The way out depends on what stopped the arm, which the hold log and
       the refusal name. A migration whose swap is not committed leaves
       the hold once what blocks its stamp is cleared: the stamp lands,
       and it must be resubmitted. One whose swap is committed, or that
       is already swapped in, has no such exit: its mirror has to arm, and
       until then the hold covers the whole shard, so no other migration
       on it is stamped either. The waiting tasks keep blocking deleting
       the collection, deleting or deactivating its tenants, and updating
       the properties they cover.

       | What stopped the arm | Way out |
       |---|---|
       | The disk fails reads or writes | Once it works, set the shard READY |
       | A staged bucket cannot open because a segment file is damaged | Repair that file in place, then set the shard READY or restart the node. Never put back an older copy: compaction reuses segment file names, so an older copy silently lacks entries a later compaction merged in. If the migration's swap is not committed, and no migration with a committed swap or already swapped in is held on the same shard, you can instead make the records dir writable (or repair the unreadable record and restart) and set READY: the migration then fails and must be resubmitted. The refused lift says when a swap is committed ("has a committed swap"); then see the next row |
       | A migration whose swap is committed but not yet made on this node, and whose damaged segment cannot be repaired | **There is currently no supported way to make this shard writable again without losing data.** We say this plainly because the remaining options are serious, and a fix that rebuilds the staged copy from this shard's own objects is planned. Nothing is lost while the shard stays READONLY: it keeps serving reads, and the other replicas keep serving the tenant. Take the time to choose, and contact Weaviate support before any step that deletes data. This node's swap step fails the task, so it ends FAILED and partial. So does every other `SWAPPING` migration that owes a swap on this shard; cancel any other migration on the collection that is still `STARTED` or `PREPARING`. The swap stays owed: once the segment is repaired and the shard set READY, the node makes it on its own, and a resubmit then lands the schema change. Once the task has ended, in the order we recommend: **1. Recover from a healthy replica.** With a replication factor above 1, the other replicas hold the migrated data. It may be possible to rebuild this replica from one of them with the replication API: copy the tenant to a node that does not hold it (`POST /v1/replication/replicate`, type `COPY`), then remove the held replica with a scale plan (`POST /v1/replication/scale`, `removeNodes`), after making its records dir writable. Removing a held replica has not been verified, so try it on a test cluster first. **2. Restore from a backup.** If no healthy replica exists, for example with a replication factor of 1, restore the collection from a backup. This replaces the whole collection: every write since the backup is lost, on every tenant. **3. Delete the tenant or the collection, only as a last resort.** This permanently deletes that data on every replica, not only on this node. Use it only when neither a replica nor a backup can recover the data, and make the records dir writable first, or the delete leaves the record behind. **Do not delete the staged bucket directories** to get past the hold: READY then succeeds, but this replica never makes its swap and silently returns different results from its peers. |
       | A migration already swapped in whose segment cannot be repaired, while its promotion is blocked: by a second fault (an unwritable records dir or another unreadable record), or because this node loaded before applying the schema change the migration makes | No supported exit. While its task still runs, it blocks deleting the collection, deleting or deactivating its tenants, and updating the properties it covers, and so does any other migration whose unit or prepare waits on this shard; a SWAPPING task that owes a swap on this shard fails, and a failed step on any node can move a SWAPPING task to FAILED, which lifts its block but not the hold. Once the task has FINISHED (a swapped-in record outlives its task until the shard's next load), only such another migration blocks those changes (cancel it first if it is STARTED or PREPARING), and this shard stays READONLY. The replica copy below is refused while any task on the collection is still running; cancel one that is STARTED or PREPARING first. Clearing what blocks the promotion and restarting makes the shard fail to load. Once the task has FINISHED, look for a healthy copy first. With a replication factor above 1, it may be possible to rebuild the tenant from a healthy replica with the replication API: copy it to a node that does not hold it (`POST /v1/replication/replicate`, type `COPY`), then remove the broken replica with a scale plan (`POST /v1/replication/scale`, `removeNodes`), after making its records dir writable. Removing a held replica has not been verified, so confirm on a test cluster before relying on it. With a replication factor of 1 there is no healthy copy: restoring a backup replaces the whole collection (delete it, then restore it), and every write since the backup is lost on every tenant. Deleting the tenant also ends the hold, but it permanently deletes that tenant's data on every replica; use it only when no copy or backup can recover the tenant, and make the records dir writable first, or the delete leaves the record behind. Deactivating the tenant is not a way out: while the promotion is still blocked, reactivating holds the shard again; once nothing blocks it, reactivating makes the shard fail to load, as a restart does. |
     - Reads are served while the retry runs, and a lift meanwhile is
       refused. The owning node runs a lift inside its schema apply, which
       therefore never waits on the disk. The shard's shutdown waits for a
       running retry: a migration is stamped only because its arm failed,
       never because the shard shut down. The migration's steps wait until
       the shard is writable. A cancel or
       failure that ends the migration while the retry runs waits up to
       10 s for it; past that, cleanup on this node is skipped with a
       Warn, and the next reconciliation pass (within a minute of the
       retry ending) disarms the mirror and removes the staged data. A
       lift that meets such a
       cleanup leaves that migration held and is refused; set the status
       again once the cleanup ends.
     - On a multi-node cluster the status PUT can answer 200 while the
       owning node refused the lift. Confirm whether the shard is still
       READONLY with `GET /v1/schema/<class>/shards`, and read the cause in
       the owning node's log.
     - A restart retries too. An unreadable record stays unreadable until
       it is repaired and the node restarted.

### 6.3 The two-phase ack barrier (PREPARING + SWAPPING)

The post-completion barrier is split into two phases.

**Semantic migrations (NeedsPreparationBarrier=true):**

1. `OnGroupCompleted` (PHASE A) runs PREP per local shard of a group
   (one group per tenant) as soon as that group's units are done. The
   scheduler emits `RecordPreparationCompleteAck(group, Success=bool)`.
2. Once every node owning a unit of the group acked success, the FSM
   records the group's swap as decided (`Task.SwapDecidedGroups`). From
   then on every replica of the group owes its swap (`Task.SwapsOwed`),
   even if the task later fails or is cancelled, and the task is kept
   until every replica paid. `PREPARING → SWAPPING` happens once every
   group is decided.
3. `OnSwapRequested` (PHASE B) runs OVERLAY+SWAP per local shard of a
   group this node owes, in any task status, and retries a failed swap
   every tick. The scheduler emits `RecordPostCompletionAck(group,
   Success=bool)`; a success pays that debt, and a failure in SWAPPING
   fails the task.
4. `OnTaskCompleted` fires → cluster-wide schema flip commits. The
   scheduler runs this phase (`runCompletedCallbackPhase`) before it
   proposes finalization.
5. FSM transitions `SWAPPING → FINISHED` only when no swap is owed
   AND this node's `OnTaskCompleted` has fired — `runFinalizePhase`
   skips any task whose `completedCallbackFired` is unset.

**Format-only migrations (NeedsPreparationBarrier=false):** PHASE A is
skipped; the FSM goes `STARTED → SWAPPING` directly. PREP and SWAP have
already run locally inside `RunOnShard` during `StartTask`, so
`OnGroupCompleted` is a no-op and the scheduler emits
`RecordPostCompletionAck`. SWAPPING → FINISHED is gated on the
PostCompletionAck barrier only.

**Failure handling (both paths):**

- A failed PREP ack, or a failed swap ack while `SWAPPING` → task flips to `FAILED`
  immediately → `OnTaskCompleted` early-returns, schema flip is
  SKIPPED. Groups already decided still swap, which leaves the collection
  `partial` until the same migration is submitted again (§8).
- Per-node swap failure can no longer let the cluster-wide schema flip
  propagate while one replica's bucket pointer never moved.
- On node restart, the recovery path re-fires the appropriate phase
  callback (PHASE A for a group not yet decided, PHASE B for every group
  this node owes, in any status) via the
  rehydrate branch; if the per-shard work succeeds the second time,
  the ack lands and the task completes normally.

**Cross-replica window:** The replicas of one tenant serve different BM25
and filter results from the first replica's swap to the last; no data is
lost. No fixed bound holds: a node runs its decided swaps one at a time,
a swap can wait behind PREP work on that node, and a failing swap is
retried only on a later tick. Different tenants are not coupled: each
swaps when its own replicas are ready.

**Why two phases instead of one** — the empirical anchor: at 1M-object
scale on a 3-node × RF=1 × 5-shard cluster, QA observed a ~1-second
total in-flight window with a 248 ms within-window mixed-state
captured between two probe samples on adjacent shards (see
weaviate/0-weaviate-issues#225). At that scale the per-node PREP
duration variance dominates: with no barrier, the cross-replica
mixed-state window scales with PREP duration (minutes at
billion-scale); with the barrier, a decided swap no longer waits for
the slowest node's PREP of its own shard, though it can still wait behind
other preparation work in flight on a node. The cost is one extra
cluster-wide RAFT roundtrip per semantic migration. Without this
anchor, future maintainers reading the FSM table see "two RAFT-coordinated phases" and may propose
collapsing it back to one — the bug it closes is not visually
obvious from the code alone.

PREP acks are idempotent (first ack per `(task, group, node)` wins); a swap
ack pays its replica's debt in any status, and a failed one fails the task while
`SWAPPING` and is ignored after; rehydrate over
restart (the scheduler re-fires on the next tick); silent on already-
terminal tasks (a late-arriving ack from a slow follower must not
produce a noisy apply failure).

### 6.4 The three-phase swap

`OnGroupCompleted` runs a strict prep / atomic / defer split per local
shard. The contract is summarized at the top of
[`inverted_reindex_task_generic.go`](../adapters/repos/db/inverted_reindex_task_generic.go);
in short:

- **Phase 1 — PREP (background, NOT inside the overlay window):**
  `FlushAndSwitch` reindex bucket, `Shutdown` it, `PrependSegmentsFromBucket`
  per property, `removeReindexBucketsDirs`, sentinel writes. Disk-I/O-
  heavy. Schema = OLD, bucket = OLD throughout — safe with live queries.
- **Phase 2a — ATOMIC SWAP (microseconds, inside overlay window):**
  per prop, `Store.SwapBucketPointer(ctx, mainName, ingestName)` followed
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
phase ordering on a single node is preserved; for the cross-node skew see
"Cross-replica window" in §6.3.

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
the FSM apply path then ran a stale-state sweep for every index
whose flag was now false — wiping the in-flight migration's working
directory. The closure happens at submit time: reject any new task
whose property set overlaps an in-flight task's, so the caller gets a
clean conflict error and can serialize. Every non-terminal status counts
as in-flight (via `TaskStatus.IsActive()`), `PREPARING` and `SWAPPING`
included; see §4.2.

**Cluster-wide FSM schema-mutation check**
(`SchemaMutationDetector`). See §4.3. Blanket reject any external
mutation overlapping an in-flight task's properties on the same
collection. Migration-driven flips (`FromInFlightMigration`) are checked
by `CheckMigrationPropertyUpdate`, admitted only while a run on the
property is `SWAPPING`.

**Per-collection in-flight cap: 32** (`maxConcurrentReindexPerCollection`).
Bumped from the original DTM default of 4 specifically for this
feature — wide schemas where every property needs a semantic
migration must complete in operator-tractable time, and serializing
them at 4 made multi-hour migrations into multi-week migrations. 32
was chosen empirically as the point where LSM compaction throughput
saturates on a single shard's disk for the typical migration mix; the
REST handler returns 429 once the cap is reached
(`reindexCapExceededResponder`; on this verb 503 instead means an
in-flight task carries a payload this build cannot decode).

**Per-collection worker pool** in `processUnits`. Bounded by the
`concurrency` function passed to the provider (typically a
`runtime.DynamicValue`), so operators can throttle on overloaded
clusters.

## 8. Multi-tenancy

`?tenants=t1,t2` scopes the task to the named tenants on a multi-
tenant class. The handler validates:

- `tenants` on a single-tenant class → 400.
- No `tenants` on a multi-tenant class → defaults to all tenants, for
  semantic and format-only migrations alike. A multi-tenant semantic
  migration takes the per-tenant group barrier
  (`AddDistributedTaskWithGroupsBarrier`); see below.
- `tenants` with a semantic migration → 400 ("all tenants must be
  targeted").
- Any targeted tenant that is not HOT → 400, with a fixed message that
  names no tenant.

Per-tenant unit groups (Journey 4 from the DTM doc): one
`UnitSpec.GroupID` per tenant, so `OnGroupCompleted` fires per-tenant
as each tenant's replicas all finish, and a tenant swaps once its own
replicas have prepared (§6.3).

For a semantic migration, a swapped tenant's shards carry the migrated
tokenization and index flags in their overlay (§10) until the
collection-wide schema flip lands after the last tenant. After a
tokenization change the tenant answers queries with the new index at
once; an index a migration enables answers only once the migration
finishes.

A run that fails or is cancelled leaves the decided tenants migrated and
the rest not. `GET .../indexes` then reads `partial`, with
`migratedTenants` / `totalTenants` counted over every run of that
migration since the last finished flip. Submit the same migration again
until it reads `ready`, also before changing the binary (§14). The
resubmit skips the tenants every replica already swapped; when that is
all of them, it reruns every tenant to land the flip. A single-tenant
collection whose swap was decided reads `partial` too, and its resubmit
reruns every shard. Migrating back, or on to another target, is not
supported yet.

While a partial run stands on a property, `ReindexPartialRunConflict`
refuses everything but that resubmit, and names it: a `PUT` that would
start any other migration, a no-op `PUT` that names a tokenization and a
rebuild (`409`), and an index `DELETE` (`422`).

Status after cancelling a tenant-scoped task is reported at the
collection level: `GET /v1/schema/{className}/indexes` renders
`cancelled` for the property as a whole. The task is the unit of
identity, so there is no per-tenant index-status surface.

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
give every migration a generation suffix so back-to-back migrations
on the same property don't collide.

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
```

No path collision with the gen-N state still on disk. `T_(N+1)`'s
`runtimeSwap` replaces the gen-N pointer with its own; the old gen-N
bucket is shut down and renamed to the later migration's backup dir.
`N` and `N+1` here label two successive submissions, whose generations
rise but need not be consecutive (§9.3).

### 9.3 The generation is the submission's task version

The generation is `TaskDescriptor.Version` — the version RAFT assigns
the submission, once, cluster-wide. Every node derives the same
directory names, and every later submission gets a strictly higher
number, so two migrations can never derive one name whatever each
node's disk happens to hold.

The generation used to be a per-node counter, `max(existing on disk) +
1`. That made the name's correctness depend on the listing being
complete, and the listing never covered the bucket working copies in
the shard's LSM root. A cleanup that failed to remove its working
copies therefore handed the next migration a number already taken, and
that migration opened its working copy on top of the previous one's
files.

The number is a RAFT log index, so it is large and non-contiguous.
Nothing reads it as small or dense: `parseMigrationDirName` accepts any
integer ≥ 1, and every consumer compares generations rather than
counting them.

### 9.4 Trim at end of swap keeps depth bounded

After `T_N` tidies, the per-shard `runtimeSwap` does an in-process
trim that deletes every older gen's sidecar dirs (reindex / ingest /
backup). The invariant: at any time, on disk for any
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
6. Leave gens > effective alone (in-flight; recovery at shard load
   handles them, §6.2).

### 9.6 Hard rules

- **Do not** call `Store.FinalizeBucketSwap` at runtime. Single
  biggest landmine in the reindex code path; every previous attempt
  to "clean up the cosmetic naming" has produced a Sev 1.
- **Do not** rename the live main bucket's physical directory while
  loaded, by any means. The mmap'd segments survive `os.RemoveAll`
  (POSIX unlink-while-open) and can serve reads from cached pages,
  but new segment writes will land in a missing dir and silently
  lose data. weaviate/weaviate#10675 is exactly this failure mode.
- **Do not** derive the gen from a disk scan again. The number has to
  be settled before any directory is inspected; a scan that misses the
  bucket working copies would hand out a number already taken (§9.3).

## 10. Per-shard property overlay

The overlay value is [`inverted.PropertyOverlay`](../adapters/repos/db/inverted/analyzer.go);
the query-side resolver is in [`adapters/repos/db/inverted/tokenization.go`](../adapters/repos/db/inverted/tokenization.go).

The problem: on each replica, the bucket pointer flips to the migrated
content at the end of `RunSwapOnShard` (Phase 2a), but the cluster-wide
schema flip (`OnTaskCompleted`'s RAFT commit) doesn't propagate to the
local FSM for tens to hundreds of milliseconds afterward. In that window
the analyzer still describes the property the pre-migration way, which
breaks two things:

- **Reads**, on a change-tokenization migration. Query input is
  tokenized against the OLD schema value and looked up in a
  NEW-tokenized bucket — wrong results.
- **Writes**, on a migration that enables an index type. The
  double-write mirror comes down when `runtimeSwap` returns and the
  ordinary write path gates on a schema flag that is still false, so a
  write accepted here is acked, durable in the objects bucket, and
  indexed nowhere. Nothing rescans afterwards, so it stays missing
  (weaviate/etienne-claude-issues#449).

With per-tenant swaps (§6.3) the window lasts until the slowest tenant
finishes, and forever after a failure. So every shard load re-installs
the overlay from its promoted migration records: each one whose flip is
decided while its task is listed and not FINISHED (or the task list is
not readable yet), limited to what the live schema does not show. The
load also opens the bucket of an index a migration turned on, and the
schema's index-off sweeps leave that bucket alone.

The overlay closes both. `Shard.SetPropertyOverlay(prop, overlay)`
installs a per-shard map of `inverted.PropertyOverlay` values, each
saying which index flags to treat as on and which tokenization to use.
The write path reads the whole value: `Shard.writePathAnalyzerOverlay`
hands it to the analyzer's `WithSchemaOverlay`. The query path reads the
tokenization half only, via
`inverted.ResolveTokenization(b.tokResolver, prop.Name, prop.Tokenization)`;
the searcher's `tokResolver` is `Shard.TokenizationFor`, attached via
`WithTokenizationResolver` wherever the shard builds a `Searcher` or
`BM25Searcher`.

An entry is a fact about this shard's buckets, never about a task and
never about the schema. It claims only what is physically true here: a
forced flag while that bucket exists, a tokenization while the
searchable bucket's content uses it. Lifecycle:

1. **Set at swap** — `maybeWirePerPropOverlaySet` runs in Phase 2 of
   `OnGroupCompleted`, between PREP and ATOMIC SWAP, installing a
   per-prop `swapPropAtomic` hook on each task (plus an `onPropSwapped`
   fallback the recovery/resume path still uses). The overlay for a prop
   is then SET inside the swap's Phase 2a tight loop, ATOMICALLY with
   that prop's bucket-pointer flip — one critical section via
   `Shard.SwapBucketAndSetOverlay` — not once-for-the-whole-shard up
   front (which opens a disk-I/O-sized `overlay=NEW`, `bucket=OLD`
   window across `RunSwapOnShard`'s preamble). The value is the one the
   task's strategy already hands the backfill scan (§11) plus, for the
   two retokenize types, the payload's target tokenization — the one
   part no strategy carries. A second swap on the same property unions
   its facts into the entry already there (`mergePropertyOverlay`); a
   newer tokenization replaces an older one. A strategy handing an empty
   value installs nothing, which is why `change-algorithm` gets no entry
   despite being semantic (§5).
2. **Reduced on read** — the whole of Phase 2 (atomic swap +
   post-atomic tidy + `OnMigrationComplete`) and everything after it
   runs with the entry active. Queries see analyzer input matching the
   bucket content; writes reach the migrated bucket. Each write reduces
   the entry against this node's schema (`PropertyOverlay.BeyondLiveSchema`
   in `Shard.writePathAnalyzerOverlay`), so once the cluster-wide flip
   lands, the parts it made real drop out. Queries read the stored
   tokenization as is; it matches the schema once the flip of the
   migration that stored it lands. It is not re-derived when the schema
   moves, because it never described the schema.
3. **Dropped with the bucket** — `Shard.updatePropertyBuckets` retires
   the fields describing an index type's bucket immediately before
   removing that bucket (`Shard.retirePropertyOverlay`). This is the
   only retirement correctness requires: an entry that outlived its
   bucket would aim the next write at a bucket the shard no longer has,
   turning a silently-unindexed write into a failed one. No task status
   and no schema status is consulted. Otherwise entries live until the
   shard unloads.

A property the overlay forced on has no property-length and no
null-state bucket — shard init skips creating those for a property with
no inverted index. `inverted.Property.OverlayForcedOnly` marks such a
property so both write legs skip those two buckets; without it every
write to the collection would fail on the missing bucket.

Range queries stay in their pre-migration state for the whole window.
`HasRangeableIndex` is false on every shard until the flip, which is
exactly the pre-migration state, so queries fall back to the
filterable bucket walk cluster-wide. A shard serving from its new
bucket while its siblings have not swapped would reintroduce the
first-shard-wins inconsistency the cluster-wide flip removes. Slower
for the length of the window, not wrong.

`repair-*` and `rebuild-*` need neither overlay: their flags are
already true, so the ordinary write path covers the property
throughout.

## 11. The analyzer overlay

A different but related mechanism. Defined per-strategy via
`MigrationStrategy.AnalyzerOverlay(props) → map[string]inverted.PropertyOverlay`.

Applied by the inverted analyzer during the backfill scan. Used by
"from-scratch" strategies (`EnableFilterableStrategy`,
`EnableSearchableStrategy`, `FilterableToRangeableStrategy` for both
`enable-rangeable` and `repair-rangeable`) that build a brand-new
inverted bucket while the corresponding schema flag is still false in
the RAFT-stored schema. Without this override the analyzer would skip
the targeted property (see `HasAnyInvertedIndex` in
`inverted/objects.go`) and the new bucket would come out empty.

Strategies that don't need an overlay embed `noAnalyzerOverlay` to get
the nil-return default: both retokenize strategies,
`MapToBlockmaxStrategy`, and `RoaringSetRefreshStrategy`. The live
schema flag is already true for the properties they target.
`RebuildSearchableStrategy` returns nil directly, for a different
reason: a rebuild must not change tokenization, which is a separate
verb.

Related to the property overlay (§10) by value, not by lifetime. The
analyzer overlay is a pure function of the strategy, read by the
backfill iterator while the scan runs. A semantic migration's swap hook
takes that same value and installs it on the shard, where it lives until
the schema-update sweep removes the index it describes or the shard
unloads, and is read by the ordinary write and query paths. One is the
source, the other is a copy that outlives it.

## 12. Cancel + DELETE-property-index

**Cancel** (`POST .../index/{indexType}/cancel`):

1. Find the non-terminal task targeting `(collection, prop, indexType)`.
   If none matches (already finished, never submitted, or already
   cancelled), return 202 with `Status: NO_OP` and no `taskId`. The
   verb is idempotent: caller's `(collection, property)` was already
   verified to exist by the outer handler, so "nothing to cancel" is
   surfaced as a no-op rather than overloading 404 with two distinct
   meanings.
2. If that task is `SWAPPING`, or in a status this build does not
   recognize (§4.2), return 409 and stop. A `SWAPPING` task finishes on
   its own; for an unrecognized status this build cannot tell whether the
   task has swapped anywhere, and the mutation-refusal guards agree
   (`MutationRemedy`). Every other in-flight status cancels
   (`TaskStatus.IsCancellable`): tenants whose swap is decided (§6.3)
   still swap, every other tenant stops. The
   REST pre-flight and `Manager.CancelTask` apply the same predicate, so a
   cancel that loses the race to a status transition is refused at the FSM
   too, and `cancelApplyFailureResponder` renders that refusal as the same
   409 the pre-flight would have.
3. RAFT `CancelDistributedTask`.
4. Wait for the local reindex goroutine to drain
   (`SealLocalTaskDrain`, 10s timeout). Bounded so a stuck
   goroutine doesn't turn the HTTP request into a hang.
5. `DB.NewStalePartialReindexSweep(tenants)` — wipe sidecars
   so the next submit starts from a clean slate. Runs for every index
   type the migration touches, not just the one named in the URL:
   `change-tokenization` spawns a searchable and a filterable strategy
   under one task, so cleaning only one leaves the sibling orphaned.
   Sidecars a committed migration record still names are preserved: a
   property that got that far is finished by resubmitting the same
   migration (a rebuild answers `409` and names the resubmit).
6. 202 with `Status: CANCELLED` + the cancelled task ID.

If the drain times out, return 202 anyway. The next submit waits up to
10 s for that worker and returns 503 while it still runs, so the client
retries. If the node crashes
mid-cancel, the on-disk state survives; the next submit's pre-cleanup
catches that gap.

**DELETE `/properties/{propertyName}/index/{indexType}`:**

1. Acquire the same `ReindexSubmitLocks` entry as PUT (closes the
   race described in §4.1).
2. Schema FSM applies the `UpdateProperty` flipping the flag to
   false. The MutationGuard rejects if a reindex is in flight on this
   property (`FromInFlightMigration=false` on this path).
3. `removeBucket` drops the canonical bucket dir, and `cleanStaleSidecarDirs`
   removes the leftover `__reindex` / `__ingest` / `__backup` dirs.
4. Subsequent re-enable starts from clean state.

## 13. Known limitation: schema migration

Schema migration across runtime-reindex state does not work, and the
v1.38 Preview merge does not fix it. The fixes live on
`backup-runtime-reindex-fixes` and land as a follow-up PR. Tracking:
weaviate/0-weaviate-issues#215.

Backups are covered: `Index.refuseIfReindexInFlight`
([`reindex_inflight.go`](../adapters/repos/db/reindex_inflight.go))
refuses a backup on any shard DTM reports a live reindex on, and
[`test/acceptance/reindex_backup/`](../test/acceptance/reindex_backup/)
covers it. Operators should not rely on schema migration interacting
cleanly with an in-flight or recently-completed reindex while running
v1.38 Preview.

## 14. Upgrade / operations: `DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS`

This knob sets how long FINISHED distributed tasks (completed reindex
tasks included) linger in the RAFT task list before garbage collection.
`0` means immediate GC on the next scheduler tick. It is now accepted
because the per-property blockmax truth is durable in the schema stamp,
so no reader depends on FINISHED reindex tasks lingering.

### Upgrade assumptions

Runtime reindex is a preview feature and comes without warranty. Upgrading
to this version assumes two things:

1. **No property was moved to BlockMax on its own on v1.38** (a
   `{"algorithm":"blockmax"}` PUT, or enabling a searchable index, on a
   collection whose `usingBlockMaxWAND` flag is off). v1.38 built BlockMax
   buckets without recording it on the property. This version reads only
   the `searchableBlockmax` record (written since v1.39.0) and the
   collection flag, so it treats such a property as WAND: the indexes
   endpoint reports `wand`, `{"algorithm":"blockmax"}` starts a migration
   instead of answering `NO_OP`, a searchable rebuild is refused, and a
   tokenization change rebuilds the index in WAND format. No objects are
   lost, but BM25 scores change wherever an object repeats a word in the
   property (WAND counts it once, BlockMax every time).
2. **No migration is running or stopped partway.** Let every running one
   finish, and resubmit every one that reads `partial` until it reads
   `ready`.

As far as we know, no customer ran runtime reindex on v1.38, and the
customers on v1.39 ran only tokenization changes and enable-rangeable,
which never build BlockMax buckets with the flag off.

**Status of an old run after upgrading from v1.39.15.** A node that
replays such a run from its RAFT log, instead of restoring it from a
snapshot, can label it `failed` or `cancelled` on the indexes endpoint
while the other nodes report it finished, and keeps the files of an index
the run enabled if that index was deleted since. Queries, data and the
schema are not affected. The label clears with the completed-task TTL (a
`failed` one only once a node that reports the run as finished leads the
cluster) or when a newer migration of the same property replaces the run.

**Downgrade:** the stamp is an additive `searchableBlockmax` property field;
an older binary ignores it and derives blockmax from the class flag / task
list as before, so a downgrade does not corrupt data. The residual is only
that a partial-class property seeded solely by the stamp (its FINISHED task
already GC'd) resolves as WAND on the older binary until a re-migration.

**Migration records, both directions.** Drain and promote every reindex, and
submit every migration that reads `partial` again until it reads `ready`,
before you change the binary, up or down. Neither direction is enforced.

This build emits no load-time signal for an unpromoted marker-era migration.

- **"Drain and promote before you change the binary" is not executable for a
  multi-tenant collection.** The old build's only promoter runs per shard load,
  so a tenant that has not been activated since its swap never promotes, and
  never emits the old build's warning either. There is no operation that drains
  them all.
- **The preserve set comes from the records alone.** A marker-era migration
  directory under `.migrations/` is never read, so it neither preserves nor
  withholds anything.

Downgrading is the mirror of it. A migration this build flipped and has not yet
promoted keeps its live data under the staged name, and the record in
`<shard>/lsm/.migrations/records/` is the only thing that says so.

The staged directory itself survives an ordinary downgrade — the older
release's sidecar sweep runs on an index DELETE, not on load. But that sweep
derives its preserve set from the `tidied.mig` / `merged.mig` markers, which
this build never writes, so **disabling the index to fix the empty results is
what destroys the data**. Upgrade again instead and let reconciliation promote.

**Rolling back below this version after a reindex run.** Older releases
read neither per-tenant preparation nor a cancel during PREPARING as this
build does. An older node that replays a reindex run from its RAFT log can
keep the run PREPARING for good, refusing every schema change on the
collection, or run its swap and schema flip again. Before rolling back:

1. Resubmit every migration that reads `partial`, in any collection on
   any node, until it reads `ready`. An older release cannot tell which
   tenants a stopped run migrated: at a migrated tenant's next load it
   deletes an index the run turned on and reads a re-tokenized index with
   the old tokenization.
2. Make every node snapshot past its last reindex run. Restart it on this
   version with `RAFT_SNAPSHOT_THRESHOLD=1 RAFT_SNAPSHOT_INTERVAL=5`. Its
   `raft node constructed` line logs `last_store_applied_index_on_start`.
   Wait until that line's `last_snapshot_index`, or the `index` of a later
   `snapshot complete up to` line, is at least that value (not
   `raft_last_index`), then roll back with both variables removed.

## 15. Files of interest

**REST**

- [`adapters/handlers/rest/handlers_indexes.go`](../adapters/handlers/rest/handlers_indexes.go) — GET / PUT / DELETE.
- [`adapters/handlers/rest/handlers_reindex.go`](../adapters/handlers/rest/handlers_reindex.go) — validation helpers, status synthesis.
- [`adapters/handlers/rest/state/reindex_submit_locks.go`](../adapters/handlers/rest/state/reindex_submit_locks.go) — per-(class, prop) submit lock.
- [`entities/models/index_*.go`](../entities/models/) — swagger DTOs (`IndexStatus`, `IndexUpdateRequest`, `PropertyIndexStatus`).
- [`openapi-specs/`](../openapi-specs/) — source of truth for the DTOs.

**Orchestration**

- [`adapters/repos/db/reindex_provider.go`](../adapters/repos/db/reindex_provider.go) — DTM provider, three-phase swap, `flipSemanticMigrationSchema`.
- [`adapters/repos/db/reindex_provider_payload.go`](../adapters/repos/db/reindex_provider_payload.go) — `ReindexTaskPayload`, migration type constants.
- [`adapters/repos/db/reindex_conflict.go`](../adapters/repos/db/reindex_conflict.go) — `CheckConflict`, `CheckPropertyUpdate`, `CheckClassMutation`, `CheckTenantMutation`, `Touches*` predicates, `MutationRemedy`.
- [`adapters/repos/db/reindex_activity_lookup.go`](../adapters/repos/db/reindex_activity_lookup.go) — `NewShardReindexActivityLookup`, the backup gate's snapshot.
- [`adapters/repos/db/reindex_recovery.go`](../adapters/repos/db/reindex_recovery.go) — `MigrationRecoveryReindexer`, the shard-load recovery `ShardReindexerV3`, and `buildRecoveryTasks`.
- [`adapters/repos/db/reindex_cancel_cleanup.go`](../adapters/repos/db/reindex_cancel_cleanup.go) — `DB.NewStalePartialReindexSweep`.
- [`adapters/repos/db/reindex_inflight.go`](../adapters/repos/db/reindex_inflight.go) — `Index.refuseIfReindexInFlight` and `newReindexGate`, the backup gate.

**Strategy + finalize**

- [`adapters/repos/db/inverted_reindex_strategy.go`](../adapters/repos/db/inverted_reindex_strategy.go) — `MigrationStrategy` interface, `applyPerPropertySchemaUpdate`, `reindexTaskConfig`.
- [`adapters/repos/db/inverted_reindex_strategy_*.go`](../adapters/repos/db/) — one per strategy.
- [`adapters/repos/db/inverted_reindex_strategy_dir_names.go`](../adapters/repos/db/inverted_reindex_strategy_dir_names.go) — `genSuffix`, `parseMigrationDirName`.
- [`adapters/repos/db/inverted_reindex_task_generic.go`](../adapters/repos/db/inverted_reindex_task_generic.go) — `ShardReindexTaskGeneric`, the **phase-contract godoc** at the top of the file is the authoritative spec.
- [`adapters/repos/db/inverted_reindex_record.go`](../adapters/repos/db/inverted_reindex_record.go) — `MigrationRecord` and its five variants, `MigrationStrategyCode`.
- [`adapters/repos/db/inverted_reindex_reconcile.go`](../adapters/repos/db/inverted_reindex_reconcile.go) — `migrationReconciler`, the load-time owner of every state transition.

**LSM primitives**

- [`adapters/repos/db/lsmkv/store.go`](../adapters/repos/db/lsmkv/store.go) — `SwapBucketPointer`, `FinalizeBucketSwap`, `updateBucketDir`. The two function godocs document the in-memory-vs-disk split.
- [`adapters/repos/db/lsmkv/segment_group_prepend.go`](../adapters/repos/db/lsmkv/segment_group_prepend.go) — `SegmentGroup.PrependSegmentsFromBucket`; the wrapper the reindex path calls, `Bucket.PrependSegmentsFromBucket`, is in [`bucket.go`](../adapters/repos/db/lsmkv/bucket.go).

**Inverted analyzer / overlay**

- [`adapters/repos/db/inverted/tokenization.go`](../adapters/repos/db/inverted/tokenization.go) — `TokenizationResolver`, `ResolveTokenization`.
- [`adapters/repos/db/inverted/analyzer.go`](../adapters/repos/db/inverted/analyzer.go) — `PropertyOverlay`, `BeyondLiveSchema`, `Property.OverlayForcedOnly`.
- [`adapters/repos/db/shard.go`](../adapters/repos/db/shard.go) — `SetPropertyOverlay`, `SwapBucketAndSetOverlay`, `retirePropertyOverlay`, `SnapshotPropertyOverlay`, `TokenizationFor`.
- [`adapters/repos/db/shard_write_inverted.go`](../adapters/repos/db/shard_write_inverted.go) — `writePathAnalyzerOverlay`, the write path's read of the overlay.

**DTM**

- [`cluster/distributedtask/doc.go`](../cluster/distributedtask/doc.go) — package-level architecture + the four "journey" shapes.
- [`cluster/distributedtask/types.go`](../cluster/distributedtask/types.go) — `Task`, `Unit`, `UnitSpec`, `TaskStatusPreparing`, `TaskStatusSwapping`, `NeedsPreparationBarrier`, the `TaskStatus.IsTerminal()` / `IsActive()` / `IsCompleted()` / `IsRecognized()` classification helpers, and `IsCancellable()` (§4.2).
- [`cluster/distributedtask/manager.go`](../cluster/distributedtask/manager.go) — FSM. `RecordPostCompletionAck`, `MarkTaskFinalized` godocs are essential reading.
- [`cluster/distributedtask/scheduler.go`](../cluster/distributedtask/scheduler.go) — per-node loop, callback dispatch.
- [`cluster/distributedtask/errors.go`](../cluster/distributedtask/errors.go) — permanent-rejection sentinels + gRPC wire encoding.

**Schema FSM**

- `cluster/proto/api/` — `PropertyField*` constants used by the fieldmask, `FromInFlightMigration` flag.
- `cluster/schema/meta_class.go` — `MergePropsMasked` fieldmask apply path.
- `usecases/schema/` — `UpdatePropertyInternal`, `UpdatePropertyInternalFromMigration`.

## 16. Tests of interest

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
  — the #10675 family.
- `change_tok_delete_journeys_test` — the cross-strategy clobber +
  stale-state sweep family.
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
- `finalizing_window_test` — per-shard property overlay coverage.
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

**Acceptance — backup gate** ([`test/acceptance/reindex_backup/`](../test/acceptance/reindex_backup/)):

- `suite_test` — backup refused while a reindex is live on the shard.

**Acceptance — rangeable** ([`test/acceptance/reindex_rangeable/`](../test/acceptance/reindex_rangeable/)):

- `concurrent_writes_test` — writes landing during an
  `enable-rangeable` build. Smoke coverage, as is
  `reindex_multinode/enable_rangeable_concurrent_updates_test`: this
  storm ends before the reindex does and the multinode one re-PATCHes
  every object after the flip, so neither pins §10's uncovered write
  window.

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
- `parseMigrationDirName` — `inverted_reindex_finalize_test.go`.
- `OnGroupCompleted` cache + rehydrate —
  `reindex_provider_on_group_completed_test.go`.
- Property overlay set / retire, and the per-migration-type
  wiring — `property_overlay_lifecycle_test.go`,
  `reindex_provider_tokenization_overlay_test.go`,
  `shard_tokenization_overlay_test.go`.
- The overlay's effect on the analyzer, and what
  `BeyondLiveSchema` leaves for it to do —
  `inverted/objects_test.go` (`TestIndexInverted`).
- A write landing in the swap window of each migration that enables an
  index type — `inverted_reindex_semantic_swap_window_test.go`.
- Shard CoW callbacks — `shard_callbacks_test.go`.
- DTM finalizing + ack barrier — `cluster/distributedtask/manager_test.go`,
  `scheduler_multinode_test.go`, `errors_test.go`.
- LSM swap primitives — `lsmkv/store_bucket_swap_test.go`.
- Atomic-phase regression guard — `inverted_reindex_task_generic_test.go`
  (the `processOneSwapPropFn` wall-clock budget assertion).

## 17. Deferred simplifications

[`docs/proposals/deferred_reindex_simplifications.md`](./proposals/deferred_reindex_simplifications.md)
catalogues two refactors that the scout pass identified as
worthwhile but that were deliberately not applied autonomously,
because each touches either a crash-safety path or the hottest write
hook. Re-evaluated for the v1.38 Preview merge and kept deferred.
Source of truth for follow-up work.
