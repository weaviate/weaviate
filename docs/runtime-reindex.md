# Runtime property reindex

Change inverted-index configuration on a live class — without restarting
the cluster, without losing writes that arrive during the rebuild, and
without dropping query availability on the affected property.

This document is the entry-point for anyone reviewing or extending the
feature. It covers the full surface area: REST API, architecture layer
by layer, the migration-strategy catalogue, how a shard decides which
index folder it serves (generations and views), crash safety,
concurrency, multi-tenancy, upgrade rules and the test map.

The package-level godocs of the touched packages are the per-symbol
source of truth; this doc is the navigable overview that ties them
together. If a section here disagrees with a godoc in source, the
godoc wins — and that's a bug in this doc.

> **How serving works, in one paragraph.** Every reindex run builds its
> new index into a folder named for the run's *generation*. What a shard
> serves is a pure function of cluster state: the class (its settings
> plus a *live generation* per property and index type) and the
> collection's reindex tasks (each task's generation and the tenants it
> has *switched*). Each shard caches that answer as a *view*; one refresh
> loop per node keeps the views current. No file on disk decides what is
> served, and no node-local record is kept per migration. §6 has the
> rule, §9 the folders.

## 1. Overview

A runtime reindex rebuilds one or more inverted-index buckets on a
property while the class stays open for reads and writes. The typical
journeys it unlocks:

- Change a text property's tokenization (`word` → `trigram`, etc.)
  with no downtime — the searchable and filterable indexes are both
  rebuilt into a new generation, each tenant switches to it once its
  build is done, and the class's `tokenization` field and live
  generation are written when every tenant has switched.
- Add a missing inverted index after the fact: `enable-filterable`,
  `enable-searchable`, `enable-rangeable`.
- Migrate a searchable index from WAND (Map) to BlockMax (Inverted):
  `change-algorithm`.
- Repair a bucket suspected of corruption: `repair-filterable`,
  `rebuild-searchable` (rebuild an existing BlockMax bucket),
  `repair-rangeable`.
- Cancel a migration until every tenant's switch is decided (`SWAPPING`).
  Tenants already switched keep their new index, which can leave the
  collection partly migrated (§8, §11).

All nine migration types are generation runs: they build a new
generation of the index and switch tenant by tenant. The whole feature
is built on three substrates:

- The **per-shard view over generation folders**
  ([`adapters/repos/db/reindex_shard_view.go`](../adapters/repos/db/reindex_shard_view.go),
  [`reindex_cluster_view.go`](../adapters/repos/db/reindex_cluster_view.go)):
  each shard computes from cluster state which folder every property
  index serves, which builds are staged for writes, and which settings
  (tokenization, rangeable readiness) apply. Reads and writes take their
  buckets from one view (§6).
- The **Distributed Task Manager (DTM)** ([`cluster/distributedtask/`](../cluster/distributedtask/))
  — RAFT-backed task state, per-unit assignment, group barriers,
  `PREPARING` and `SWAPPING` coordination states, the task rules that
  give a run its generation and compact it when it ends (§4.2).
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
> already running stays observable and cancellable (§11). Serving does
> not depend on the flag: a shard serves the generations cluster state
> names whether or not the flag is on.

A replica movement requested while a migration runs on its collection is
refused with 409.

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
| `.../index/searchable` `{"tokenization":"word"}`, no searchable index yet | Builds a Blockmax index, then turns `IndexSearchable` on and sets `Tokenization`. `text`/`text[]` only; `tokenization` required. | `enable-searchable` |
| `.../index/searchable` `{"tokenization":"trigram"}`, index exists | Rebuilds BOTH the searchable and filterable indexes when both exist. | `change-tokenization` |
| `.../index/searchable` `{"algorithm":"blockmax"}`, index on WAND | Map → Blockmax upgrade. The flip stamps the property as Blockmax and sets the class-level `UsingBlockMaxWAND` once every searchable property is stamped. `wand` is rejected (deprecated); this is the only WAND → Blockmax path. | `change-algorithm` |
| `.../index/filterable` `{}`, no filterable index yet | Builds a RoaringSet index, then turns `IndexFilterable` on. A supplied tokenization must equal the property's current one. | `enable-filterable` |
| `.../index/filterable` `{"tokenization":"word"}`, index exists, no searchable index | Filterable-only retokenize. Rejected (400) when a searchable index exists, since both indexes share one tokenization; use `.../index/searchable` instead. | `change-tokenization-filterable` |
| `.../index/rangeFilters` `{}`, no range index yet | Builds a RoaringSetRange index, then turns `IndexRangeFilters` on. Numeric types only (`int`, `number`, `date`). | `enable-rangeable` |

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
| `searchable` (already Blockmax) | Rebuild the Blockmax index. | `rebuild-searchable` |
| `filterable` | RoaringSet refresh. | `repair-filterable` |
| `rangeFilters` | RoaringSetRange rebuild. | `repair-rangeable` |

### `POST /v1/schema/{className}/properties/{propertyName}/index/{indexType}/cancel`

Cancel the in-flight task on `(class, property, indexType)`. No request
body. Idempotent: `202` + `Status: CANCELLED` (with `taskId`) when a
`STARTED` or `PREPARING` task is cancelled; tenants whose switch is
already decided (§6.3) keep it, every other tenant stops and drops its
build. `202` + `Status: NO_OP` (no `taskId`) when nothing matches
(already finished, never submitted, or already cancelled). `409` for a
`SWAPPING` task (every tenant is already decided and the task is
finishing) and for a status this build does not recognize. See §11.
Never `404` for "nothing to cancel".

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
  tokenization to add one first"). Also a submit that names a tenant that
  is not `HOT`, or on a collection with no shards (§4.2).
- `404 Not Found` — class or property doesn't exist.
- `409 Conflict` — five distinct meanings. On PUT / rebuild, an in-flight
  task already touches this property; the error names the offending task
  ID unless another submit won the race. On PUT / rebuild, a replica
  movement is in flight on the collection. On PUT / rebuild, a partial run
  stands on the property (§8); the error names the resubmit that finishes it.
  On PUT / rebuild, the collection was deleted between the request and the
  apply. On cancel, either arm can refuse. The pre-flight refuses a status this
  build does not recognize and names the task ID and that status. The
  apply refuses a target that reached such a status or a terminal one
  after that read, and names no status, since it cannot tell which. Neither refusal cancelled anything, but the
  apply refusal cannot tell you what did: a concurrent cancel may have won
  the race, so re-read the index status to see where it landed —
  `CANCELLED` included. A pre-flight refusal leaves the task in flight,
  and a status this build does not recognize keeps reading as `indexing`
  here until it reaches a terminal state on the nodes that do (§11).
- `422 Unprocessable Entity` — `{indexType}` outside the enum.
- `429 / 503` — per-collection in-flight cap reached (default 32), or the
  cluster service is unavailable / an in-flight task's payload cannot be
  parsed so conflict-freedom cannot be proven.

### `DELETE /v1/schema/{className}/properties/{propertyName}/index/{indexType}`

Drop a configured inverted index. `{indexType}` is one of `filterable`,
`searchable`, `rangeFilters` (`rangeable` accepted as an alias). Flips the
corresponding schema flag to false. A property served only through its plain
folder loses the folder at once; a property a generation serves loses its
folders when the refresh loop retires them, after the last reader releases
them (§6.2). The live generation is raised past every run still serving the
index, so a re-enable builds a new generation instead of finding old data
(§6.5). Subject to the same MutationGuard as `UpdateProperty` — rejected
while a reindex on this property is in flight, or while a partial run stands
on it, unless the DELETE turns off an index type that run builds (§8).

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
list crossed with the schema flags. When several runs touch one index, the
one reported is picked by status priority, then task version, then start
time. A compacted run reports progress from its summary. Both values are read from the
node serving the request, and the task list is read first, so the
schema is never the older of the two: a class read at a later applied
index carries every flag flip the tasks it is compared against have
already committed. A FINISHED task surfaces nothing of its own — the
schema flag alone decides whether an entry is emitted.

This endpoint is the completion signal. A successful full run's task is
removed when its flip lands, so once the index reads `ready` with no
`taskId` the class serves the new generation. A tenant-scoped successful
run stays as a compact `FINISHED` record, because it holds switched
tenants (§4.2).

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
polling both against a lagging follower can render the two out of step.

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
  single replica* has finished building its new index folder. The
  tenant still serves the old index until its switch is decided.
- **`TaskStatus`** — per-task aggregate:
  `STARTED → PREPARING → SWAPPING → FINISHED`, and `FAILED` (a failed
  unit, a failed preparation ack, or a failed flip) or `CANCELLED`
  (operator-initiated) from the non-final states.
  - `STARTED`: units are building. A group (one per tenant; one group
    `""` covering every shard on a single-tenant collection) is ready once
    all its units are `COMPLETED`, even while the task is still `STARTED`.
  - `PREPARING`: reached once every unit is terminal. Each node owning a
    unit of a group acks the group (`RecordPreparationCompleteAck`); once
    every owner acked success the group's switch is **decided** and the
    group joins the task's switched set (`Task.SwapDecidedGroups`). §6.3.
  - `SWAPPING`: every group is decided. A full run now flips the class
    (below); a tenant-scoped run has nothing to flip.
  - `FINISHED`: the flip landed (full run) or every group is decided
    (scoped run).

  All nine migration types take this path, `repair-*` and
  `rebuild-searchable` included: they build a new generation and switch
  per tenant like the rest. `FINISHED` is not the signal that the class
  serves the new generation for a full run, because its task is removed
  at the flip; the index status (§2) is.

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
        │  REST handler (adapters/handlers/rest/handlers_indexes*.go)   │
        │   • Per-(class, prop) ReindexSubmitLock                       │
        │   • Validate body, classify migration type, dispatch          │
        │   • checkReindexConflict (read-side mirror of FSM check)      │
        │   • AddDistributedTaskWithGroupsBarrier("reindex", payload)   │
        │     (no unit list: the apply builds the units)                │
        └──────────────────────────────────┬─────────────────────────────┘
                                           │  RAFT-replicated AddTask
        ┌──────────────────────────────────▼─────────────────────────────┐
        │  cluster/distributedtask FSM (Manager)              ← TaskSt. │
        │   • ConflictDetector.CheckConflict (FSM-deterministic)        │
        │   • TaskRules.Admit: generation, takeover or refusal          │
        │   • TaskRules.Units: one unit per shard replica, from the     │
        │     sharding state at this log index                          │
        │   • Task stored at STARTED (a takeover with nothing to build  │
        │     goes straight to SWAPPING, or FINISHED when scoped)       │
        └──────────────────────────────────┬─────────────────────────────┘
                 task-state notice to each node's refresh loop
        ┌──────────────────────────────────▼─────────────────────────────┐
        │  Refresh loop (per node) → each loaded shard's view           │
        │   • opens the task's build folder, emptied while the unit is  │
        │     PENDING; the folder is staged: written by live writes,    │
        │     never read                                                │
        └──────────────────────────────────┬─────────────────────────────┘
                                           │  Scheduler tick on each node
        ┌──────────────────────────────────▼─────────────────────────────┐
        │  ReindexProvider.StartTask (per node, per task)     ← UnitSt. │
        │   • one build per local unit, in a bounded worker pool        │
        │   • wait for staging → record unit start → wait until this    │
        │     node applied it → scan every object into a scratch bucket │
        │     and place its segments under the build                    │
        │   • UnitStatus = COMPLETED  ← per-Unit status                 │
        └──────────────────────────────────┬─────────────────────────────┘
                              All units terminal across the cluster
                                  ┌────────▼────────┐
                                  │ PREPARING       │ ← TaskStatus
                                  └────────┬────────┘
                       Each group: every owning node acked → decided
                       (the tenant now serves the new generation)
                                  ┌────────▼────────┐
                                  │ SWAPPING        │ ← TaskStatus
                                  └────────┬────────┘
        ┌──────────────────────────────────▼─────────────────────────────┐
        │  Provider.OnTaskCompleted (full runs only)                    │
        │   proposes one TYPE_REINDEX_FLIP per property: Live = G and   │
        │   the target settings (idempotent: every node proposes it)    │
        └──────────────────────────────────┬─────────────────────────────┘
                                  ┌────────▼────────┐
                                  │ FINISHED        │ ← TaskStatus
                                  └────────┬────────┘
                       end rules run in the same apply: a finished
                       full run deletes itself and the runs it covers
```

A failed or cancelled run keeps its decided tenants: they continue to
serve the generation the run built. The run stays in the task list with
its switched set until a resubmit takes it over, a newer run covers it, the
index is turned off, or its tenants and collection are deleted (§8).

### What goes through RAFT vs. what is local-only

What goes through RAFT (cluster-wide commits):

- `AddTask` — task created at STARTED, with its generation and units.
- `UpdateUnitProgress` / `RecordUnitCompletion` — per unit, on the
  node that owns the unit. The first records that the unit started.
- The transition `STARTED → PREPARING` — once every unit is terminal.
- `RecordPreparationCompleteAck` — one per group and node, with
  `Success=bool`.
- The transition `PREPARING → SWAPPING` — committed inside
  `RecordPreparationCompleteAck`'s apply once every group is decided.
- `RecordPostCompletionAck` — a no-op for tasks with the preparation
  barrier, kept so old logs replay.
- `TYPE_REINDEX_FLIP` — the class-level flip of a full run.
- The transition `SWAPPING → FINISHED` — committed by
  `MarkTaskFinalized` once this node's `OnTaskCompleted` succeeded.

What does NOT go through RAFT (local-only):

- Opening, staging and retiring a shard's index folders (the refresh loop).
- The build itself: the object scan, the scratch bucket, placing its
  segments under the build folder.
- Computing a view. Every node computes its own view from the state it
  has applied.

There is no single instant at which every node switches a tenant. A
tenant's replicas each learn that the group is decided when they apply
the entry that decided it, and each publishes its new view when its
refresh loop reaches the notice. Reads in that window can be answered
from either generation, each complete and internally consistent (§6.3).

## 4. Architecture by layer

### 4.1 REST layer — `adapters/handlers/rest/handlers_indexes.go` + `handlers_reindex.go`

Validation, dispatch, response shaping. One structural detail worth
calling out:

**Per-`(collection, property)` submit lock.** Held across class read,
validation, conflict check, and the RAFT `AddDistributedTaskWithGroupsBarrier`
call. Without this, a parallel `DELETE /properties/{p}/index/{name}` could
win the lock between the PUT's `ReadOnlyClass` snapshot and its
RAFT task-add, leaving the PUT validating against a schema that no
longer matches the index. The lock is process-wide; the
multi-node version of the same race is closed by the FSM-side
`MutationGuard` (see §4.3). Stored on `state.ReindexSubmitLocks` so the
PUT handler and the DELETE-property-index handler share the same
entries. See [`adapters/handlers/rest/state/reindex_submit_locks.go`](../adapters/handlers/rest/state/reindex_submit_locks.go).

The submit lists no tenants and no replicas: the entry carries the
payload only, and every node builds the units when it applies the entry
(§4.2). Neither submit nor cancel sweeps any node's disk.

### 4.2 Distributed Task Manager — `cluster/distributedtask/`

The substrate. Package doc at [`cluster/distributedtask/doc.go`](../cluster/distributedtask/doc.go)
documents the four "journey" shapes; reindex uses the per-tenant grouped
**Journey 4** with the preparation barrier for every run.

Key types & contracts:

- **`Manager`** — the RAFT FSM. Owns task state, applies `AddTask`,
  `UpdateUnitProgress`, `RecordUnitCompletion`, `RecordPreparationCompleteAck`,
  `MarkTaskFinalized`, `MarkTaskFailed`, `CancelTask`, `CleanUpTask`.
  Every mutation is FSM-deterministic. It also fires a state listener
  after every add, takeover, decision, end and removal (the refresh
  loop's source of notices), and answers `ShardTaskStates`, which reports
  per task what it means for one shard on one node.
- **`TaskRules`** — the namespace's rules inside the apply
  ([`group_states.go`](../cluster/distributedtask/group_states.go)),
  implemented for reindex by `db.ReindexTaskRules`
  ([`reindex_task_rules.go`](../adapters/repos/db/reindex_task_rules.go)):
  - `Admit` runs in the `AddTask` apply before the task is stored. It
    gives the run a generation, one more than the highest generation the
    class's live state or any task on the same properties records. A run
    **takes over** the newest failed or cancelled run on the same
    properties and target that still has switched tenants: it inherits
    that run's generation and switched tenants, builds no unit for them,
    and the old task is deleted. A finished run is never taken over. A
    run of another target is refused with `ErrTaskPartlyMigrated` (409)
    while a failed or cancelled run on the same properties still has
    switched tenants, because waiting for it or cancelling it cannot
    help. Only payloads marked `ViewServed` are shaped, so a log written
    by v1.39.x replays unchanged.
  - `Units` builds the run's units from the collection's sharding state
    at the entry's log index: shards sorted by name, then replicas in
    their stored order, unit ID `shard__node`. The order never follows a
    map, so every replica stores the same task. A named tenant that does
    not exist is refused (`ErrTaskTenantsMissing`, 400), as is any shard
    in scope that is not `HOT` (`ErrTaskTenantsNotHot`, 400, count
    first) and a collection with no shards (`ErrTaskHasNoUnits`, 400).
    A collection deleted meanwhile answers `ErrTaskCollectionGone`
    (409). Because the apply reads the sharding state at its own log
    position, no tenant can appear or change between listing and apply.
  - `Ended` runs once per task that became terminal, in the same apply,
    and `BornSwitched` marks the tenants created during a full run.
    See "End rules" below.
  - `IndexOff` computes the live generation that turns an index off
    (§6.5).
- **`Scheduler`** — per-node loop. Polls Manager for current task list,
  starts/stops local work via Provider, fires `OnGroupCompleted` (a no-op
  for reindex), `OnTaskCompleted` (the flip) and submits
  `MarkTaskFinalized` when the local callback succeeds.
- **`Provider` / `UnitAwareProvider`** — the extension point.
  `ReindexProvider` implements it.
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
  **Task status classification** below). A failed or cancelled run with
  switched tenants also refuses property updates other than its own
  index-off (§8).
- **`TaskStatusPreparing` and `TaskStatusSwapping`** — the post-units,
  pre-FINISHED coordination states. Callers polling for "fully done"
  poll the index status (§2), not the task.
- **Preparation barrier** — `RecordPreparationCompleteAck`. Each node
  acks each group it owns a unit of; the group's switch is decided once
  every owner acked success, and PREPARING → SWAPPING is gated on every
  group being decided. A failed ack flips the task to `FAILED`.
  `RecordPostCompletionAck` is a no-op for barrier tasks (old logs
  replay it).
- **Permanent-rejection sentinels** —
  `ErrTaskNotRunning`/`ErrTaskDoesNotExist`/`ErrUnitAlreadyTerminal`/
  `ErrUnitWrongNode`/`ErrTaskNotInFinalizingState`, plus the submit
  refusals above, all matched by the umbrella `ErrPermanentRejection`.
  Encoded over gRPC with a stable `codes.FailedPrecondition` +
  `[dtm-perm/<id>] ...` message marker so the sentinel identity survives
  wire transit and gets re-attached on the receiving side by
  `RehydratePermanentRejection`. Mixed-version friendly (pre-sentinel
  peers return plain-text phrasing; the classifier substring-matches as a
  fallback). See
  [`cluster/distributedtask/errors.go`](../cluster/distributedtask/errors.go).

#### End rules and compaction

Every apply that can end a task (unit completion, preparation acks,
finalize, fail, cancel) and every restore runs the namespace's end rules
once per task that became terminal, before the lock is released
(`ReindexTaskRules.Ended`). For a run with a generation:

- A run that ended with switched tenants takes them from older ended runs
  on the same properties whose index types it covers. An older run left
  with none is deleted.
- A finished **full** run deletes itself and every ended run on its
  properties that it covers, with or without a generation, because its
  flip already made the class serve its generation.
- Any other ended run is compacted in place: its units become a summary
  (`UnitSummary`: group count, counts per unit status, up to 20 errors),
  and its switched tenants stay. `Task.GroupTotal` counts switched groups
  without units (born, taken over) and answers from the summary once
  compacted.

Runs without a generation are left as they are. Deleting a tenant removes
it from a task's switched set even when the task holds no unit for it.

#### Retention

`ShouldRetainCompletedTask` keeps a run with a generation that is
`FAILED` or `CANCELLED`, or `FINISHED` with switched tenants. Everything
else, including every run without a generation, follows
`DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS` (§13). A kept run leaves
through the rules above, a takeover, an index-off, or the deletion of its
tenants or collection; there is no age limit on it.

#### Cutover check

`MarkTaskFailed` records `FINISHED` instead of `FAILED` when
`ReindexDetector.CutoverLanded` finds the flip already applied: the
class's live generations hold the run's generation for every index type
it builds. A missing entry is not landed. A run without a generation is
reported landed, because it never serves.

#### Task status classification

`TaskStatus` is classified by five predicates in
[`cluster/distributedtask/types.go`](../cluster/distributedtask/types.go):

| Predicate | True for |
|---|---|
| `IsTerminal()` | `FINISHED`, `FAILED`, `CANCELLED` |
| `IsActive()` | everything else (defined as the exact negation of `IsTerminal()`) |
| `IsCompleted()` | `SWAPPING`, `FINISHED` — every unit succeeded |
| `IsRecognized()` | any status this build declares |
| `IsCancellable()` | `STARTED`, `PREPARING` (in `SWAPPING` every tenant is decided and nothing is left to cancel) |

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
on `IsActive()` against this node's own task copy, where a terminal status
never changes once applied. So a `CLEAN_UP`
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
tick at which this node knows a leader, which is what makes the state alertable. The line does not offer a
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

**Live generations.** The class carries, outside its public schema, a
live generation per property and index type. A property index serves
`max(Live, generation of every task on it that switched the tenant)`
(§6.1). Only two applies write it: `TYPE_REINDEX_FLIP` and the index-off
update (§6.5).

**`TYPE_REINDEX_FLIP`** (RAFT apply type 7;
`ReindexFlipRequest{Prop, Gen, Tokenization, IndexTypes, Enable, Blockmax}`).
Proposed by `ReindexProvider.OnTaskCompleted` once every group of a full
run is decided, one entry per property. The apply
(`applyReindexFlip`) sets `Live = Gen` for the built index types and the
target settings: the tokenization, the index flags of an enable run, the
property's blockmax stamp, and the class-level `UsingBlockMaxWAND` once
every searchable property is stamped. It needs no shard work, because
every tenant already serves the generation. An entry whose generation is
not above the current one changes nothing, so every node can propose it
and a replay is harmless. A tenant-scoped run never flips: no class
value can name only some tenants.

**`UpdateProperty` fieldmask.** `applyPerPropertySchemaUpdate` passes a
`fields []string` mask all the way down to the FSM apply path
(`MergePropsMasked`). Two strategies running in parallel on the same
property — each touching different fields — no longer clobber each
other on RAFT apply: the FSM merges only the listed fields onto the
live class state. An empty mask falls back to "replace every field"
semantics for callers that don't care.

**`MutationGuard` (cross-FSM).** The schema FSM consults
`distributedtask.SchemaMutationDetector` on every `UpdateProperty`,
`DeleteClass`, `UpdateTenants(FROZEN)`, `DeleteTenants` apply.
`ReindexDetector`'s implementation rejects any mutation overlapping the
properties of an in-flight reindex task on the same collection. In flight
means any non-terminal status, admitted via `TaskStatus.IsActive()` (see
§4.2). The guard also names, from the update's field mask, the index type
an update turns off (`indexTurnedOff`), so a partial run's own index can
be turned off while every other update stays refused (§8). The motivating
failure mode is summarized on `CheckPropertyUpdate`'s godoc: a
`change-tokenization` migration builds the searchable and filterable
indexes of one property; a DELETE arrives mid-flight and removes one of
them; the run then serves one index at the new tokenization and the other
at the old one. **Bucket↔schema inversion.** Same family as the
ack-barrier failure mode but triggered by an external schema mutation
instead of a crash.

The guard is intentionally blanket: any overlap rejects. Class-wide
(`CheckClassMutation`) is stricter — any reindex on the class
(regardless of property) blocks DeleteClass.

The `UPDATE_PROPERTY` apply still reads `FromInFlightMigration` and
`MigrationRun`, and still routes such an update to
`CheckMigrationPropertyUpdate`, so a log written by v1.39.x replays. This
build proposes no such update; its own flip is `TYPE_REINDEX_FLIP`.

### 4.4 Reindex orchestration — `adapters/repos/db/reindex_*` & `inverted_reindex_*`

**`ReindexProvider`** ([`reindex_provider.go`](../adapters/repos/db/reindex_provider.go),
[`reindex_provider_generation.go`](../adapters/repos/db/reindex_provider_generation.go))
— the DTM `UnitAwareProvider` implementation. Lifecycle hooks:

- `StartTask` → per-node bootstrap. Refuses a task without a generation
  (boot makes that unreachable, §13). Identifies this node's local units
  and runs each one's build (`startGenerationRun`) in a bounded worker
  pool (`ConcurrencyLimiter`).
- Per unit (`runGenerationUnit`): wait until the refresh loop stages the
  build folder, record the unit's start, wait until this node applied it,
  then `Shard.ReindexBuild` per property. Only then does the unit report
  `COMPLETED`. A completion that cannot be reported is logged through a
  sampler; an interrupted unit logs nothing and is started again by the
  next task relaunch.
- `OnGroupCompleted` does nothing: each unit reported done only once its
  build was durable.
- `OnTaskCompleted` → `completeGenerationRun`: for a full run in
  `SWAPPING`, one `TYPE_REINDEX_FLIP` per property through the wired
  `ReindexFlipper`.
- The RAFT-apply checks are not provider hooks. They live on
  `ReindexDetector` in `reindex_conflict.go`, below. See §4.3 & §7.

**`MigrationStrategy`** ([`inverted_reindex_strategy.go`](../adapters/repos/db/inverted_reindex_strategy.go))
— the per-migration extension surface. Each strategy supplies bucket
naming, the per-key transform, source/target LSM strategies, the
Add/Delete double-write callbacks, and the optional `AnalyzerOverlay`
(§10). A staged build calls the strategy's double-write callbacks, given
the staged bucket, for every write on a view that stages it.

**`reindex_conflict.go`** — `ReindexDetector`: `CheckConflict` (FSM-deterministic),
`CheckPropertyUpdate`, `CheckClassMutation`, `CheckTenantMutation`,
plus the predicates `ReindexPropsOverlap`, `TouchesSearchable`,
`TouchesFilterable`, `TypesConflictReason`. The exhaustive switches in
the `Touches*` predicates intentionally panic on unknown migration
types so a new `ReindexMigrationType` cannot silently be treated as
"doesn't touch X" — it surfaces on the first request.

**`reindex_retainer.go`** — the retention rule, the cutover check
(`CutoverLanded`) and the partial-run helpers (`ReindexRuns`,
`ReindexPartialRun`, `ReindexPartialRunConflict`) that the status, the
conflict messages and the submit refusals share.

**`reindex_upgrade_check.go`** ([`cluster/`](../cluster/reindex_upgrade_check.go))
and **`reindex_legacy_records.go`** — the boot check and the load-time
conversion of pre-generation state (§13).

**`inverted_reindex_legacy_finalize.go`** — the frozen legacy step for
the v1.39.0-2 marker layout (tracker files such as `tidied.mig`). At
shard load, before any bucket opens, it finishes the migrations those
releases left finished, and any error fails the load. While the class has
that index off, it keeps the plain folder and the tracker, and the load
sweep spares the folder. It is never extended.

### 4.5 Strategy implementations — `inverted_reindex_strategy_*.go`

Eight strategy implementations, one file each:

| Strategy | Type | Source | Target |
|---|---|---|---|
| `MapToBlockmaxStrategy` | `change-algorithm` | `searchable` (MapCollection) | `searchable` (Inverted/Blockmax) |
| `RebuildSearchableStrategy` | `rebuild-searchable` | `searchable` (Inverted/Blockmax) | `searchable` (Inverted/Blockmax) |
| `RoaringSetRefreshStrategy` | `repair-filterable` | `filterable` (RoaringSet) | `filterable` (RoaringSet) |
| `FilterableToRangeableStrategy` | `enable-rangeable` / `repair-rangeable` | objects → builds RoaringSetRange | `rangeFilters` (RoaringSetRange) |
| `EnableFilterableStrategy` | `enable-filterable` | objects → builds RoaringSet | `filterable` (RoaringSet) |
| `EnableSearchableStrategy` | `enable-searchable` | objects → builds Blockmax | `searchable` (Blockmax) |
| `SearchableRetokenizeStrategy` | `change-tokenization` (searchable half) | `searchable` | `searchable` (new tokenization) |
| `FilterableRetokenizeStrategy` | `change-tokenization` (filterable half) + `change-tokenization-filterable` | `filterable` | `filterable` (new tokenization) |

A strategy owns no swap, record or completion hook: the view switches
the tenant, and the flip writes the class settings. A strategy
contributes to a staged build its target bucket kind, its per-object
transform, its double-write callbacks and its analyzer overlay.

`change-tokenization` builds TWO strategy instances per property
(`SearchableRetokenizeStrategy` + `FilterableRetokenizeStrategy`) so the
searchable and filterable indexes switch in the same view, never one
without the other.

The strategy interface itself documents the per-method contract; see
[`inverted_reindex_strategy.go`](../adapters/repos/db/inverted_reindex_strategy.go).

**Semantic vs format-only.** `IsSemanticMigration` is the predicate:
`change-tokenization`, `change-tokenization-filterable`,
`enable-filterable`, `enable-searchable`, `enable-rangeable`, and
`change-algorithm` are semantic: they change what the class declares, so
a full run flips the class settings at its end. The rest
(`repair-filterable`, `repair-rangeable`, `rebuild-searchable`) change
no setting; their flip raises the live generation and writes no flag.
Both kinds build a generation and switch per tenant.

**Why `enable-rangeable` is semantic.** It turns an index on, so its
flag is the cluster's statement that every shard can serve range
queries. Writing it before every tenant switched would make that
statement from the first tenant to finish, and a task that then stopped
left the flag on over shards holding an empty index
([0-weaviate-issues#464](https://github.com/weaviate/0-weaviate-issues/issues/464)).
The view closes this: it answers `RangeableReady` from the generation it
serves, and the flag is written only by the flip.

### 4.6 Bucket lookup and view primitives

**`lsmkv.BucketLookup`** ([`adapters/repos/db/lsmkv/`](../adapters/repos/db/lsmkv/))
is the one interface through which every property-bucket lookup goes. The
store implements it for properties no view governs; `shardView`
implements it for the rest. `AcquireBucketForRead` returns a bucket and a
release function.

**`shardView`** (§6.2) holds a reference count. A reader acquires the
view once (`Shard.acquireView`) and takes every bucket, the tokenization
and the rangeable-ready flag from it. A publish replaces the shard's
current view in one atomic store and never waits for a reader. A retire
deletes a replaced folder only after the replaced view and every older
one are released.

There is no bucket-pointer swap and no directory rename. Folders are
opened under their generation's name and never renamed while loaded.

## 5. Migration strategies — quick map

```
                               semantic?   analyzer overlay (§10)
change-tokenization              ✓
change-tokenization-filterable   ✓
enable-filterable                ✓              ✓
enable-searchable                ✓              ✓
change-algorithm                 ✓
enable-rangeable                 ✓              ✓
repair-rangeable                                ✓
repair-filterable
rebuild-searchable
```

All nine take the preparation barrier and the per-tenant switch. The six
semantic ones also write their target settings at the flip of a full run.
The analyzer overlay lets a from-scratch build see a property whose
schema flag is still false (`MigrationStrategy.AnalyzerOverlay`), and
lets a staged write analyze the same way.

While a run builds, the view analyzes a staged write with the settings
the view serves once the build switches: the view's settings with the
build's target applied (the winning task's target, else the class). That
is the same settings the build folder is opened with and the main write
is analyzed with (`analyzeForBuild`, `analyzeObjectInView`).

## 6. Serving: generations, views and crash safety

### 6.1 The serving rule

For tenant T, property P and index type I, the served generation is

```
max( Live(P, I),  generation of every task on (P, I) that has T switched )
```

A task's switched tenants are the groups in its `SwapDecidedGroups`. A
group joins that set in one of three ways: its swap is decided by the
barrier (`PREPARING`), the tenant is created during a full run (it holds
no data to build, so it is **born switched**), or a takeover inherits it
from the run it replaced. Settings (tokenization, index on, blockmax)
come from the winning task's target, else from the class. An index that
is off serves nothing.

While a *running* task has an unswitched unit for T on this shard, the
shard also **stages** that task's generation: it opens the build folder,
every write goes into it as well, and nothing reads it.

Generation 0 is the plain folder `property_<name>[_searchable|_rangeable]`
and is always what the store opened. A run's generation is one more than
the highest generation the class or any task on the same properties
records, so two runs never share a folder name (§9).

Every function above reads cluster state only. Two nodes that applied the
same entries compute the same answer, and a restart computes it again
from scratch: nothing on disk is consulted to decide it.

### 6.2 The shard view, load and refresh loop

**Load.** Before any property bucket opens, a shard reads its tasks, then
the live generations, then the class, and computes its view
(`Shard.loadReindexView`). It opens exactly the folders the view names:
the served ones and the staged builds. A load **refuses** to serve a
folder above generation 0 that is missing while the objects bucket holds
objects, so a lost folder is never replaced by an empty index. Generation
0 is never refused.

**Load sweep.** The load deletes every folder of a class property's
index that the view does not name: plain folders, generation folders,
`.build` scratch folders and the pre-generation sidecar folders. It keeps
the folders of other properties and every staged build. A folder it
cannot delete is reported on one line per shard and is never opened.
This is how a crash between a switch and its cleanup, or an index turned
off on an unloaded tenant, is repaired.

**Refresh loop.** One loop per node (`reindexRefresh`,
[`reindex_refresher.go`](../adapters/repos/db/reindex_refresher.go))
takes notices and republishes each *loaded* shard's view; it never loads
a cold shard, because a shard's load computes its own view. A notice
comes from the task-state listener (wired in `configure_api.go`), from
the class apply (`Index.updateProperty`), from a reload after a snapshot
install (`Migrator.UpdateIndex`), and from an index's registration
(`DB.registerIndex`, one notice for all its shards). A shard also queues
one refresh for itself once it is visible as loaded, which repairs a
change its load read too early.

A refresh opens the folders the new view needs and publishes it with one
atomic store. It never waits for a reader. A folder still being retired
leaves the shard "not ready", and a failed or not-ready shard is retried
from a min-heap with backoff from 50 ms to 5 s. A missing served folder
fails the refresh the same way as at load and is never created empty.

**Retire.** A folder the new view no longer names is shut down and
deleted off the loop, once the replaced view and every older one are
released. Shard shutdown and drop wait for pending retires. A folder that
fails to delete is logged once and is never opened again, because no view
names it and the next load sweeps it.

**Operability.**

- Gauge `reindex_view_refresh_pending`: shards on this node whose refresh
  failed or was not ready and waits for a retry.
- At most one warning per collection per minute names the first failing
  shard, its error, how many more are pending, and the tasks waiting on
  its staging.
- A unit waiting for its build folder fails once opening that folder kept
  failing for `REINDEX_STAGING_FAILURE_TIMEOUT` (default 30 minutes, a
  guess, not a measurement). A folder still being retired never counts,
  and any other refresh outcome restarts the clock.

**The view holds only folders it opened.** Generation 0 is the store's
plain folder for every property, governed or not, and store tools such
as the startup reindexers replace it. A *served* entry therefore exists
only for "off" (nil) or generation 1 or higher; a missing entry means the
store's plain folder. A property the view does not govern (no live
generation, no switched task, no staged build) goes to the store as it
always did.

### 6.3 Reads, writes and the switch

**Reads.** Each shard query (`ObjectSearch`, `ObjectVectorSearch`,
`ObjectList`, `Aggregate`, `FindUUIDs`) acquires one view at its start and
takes every property bucket, the tokenization, the rangeable-ready flag
and the fallback-to-searchable flag from it (`inverted.IndexView`). A
switch published mid-query cannot hand the query a bucket from one
generation and a tokenization from another; the view's reference holds
every bucket the query reads. A range filter uses a rangeable index
exactly when the view says it is ready.

**Writes.** Each write acquires one view and takes its analysis, its main
buckets and its staged buckets from it. The previous and the new version
of the object go through every staged build's own strategy callbacks
(`writeStaged`), so a write on the old view lands in both folders. A
write that loaded a view *without* the build holds the scan back
(`ReindexBuild` waits until no such write still runs), so no write is
lost between staging and the scan.

**The switch.** When the barrier decides a group, the task's switched
set grows. Each replica of the tenant applies that entry and then
publishes a view that serves the new generation when its refresh loop
reaches the notice. There is no per-replica swap step that can fail or be
owed: the switch is a function of state every replica has.

**Cross-replica window.** The replicas of one tenant switch at slightly
different times (scheduler and refresh jitter). In that window a query
is answered from the old or the new generation, each complete and
consistent on its own; no data is lost. No fixed bound holds: a refresh
that finds its folder still being retired is retried with backoff.
Different tenants are not coupled.

**Why the barrier exists.** The empirical anchor: at 1M-object scale on
a 3-node × RF=1 × 5-shard cluster, QA observed a ~1-second total
in-flight window with a 248 ms within-window mixed state between two
probe samples on adjacent shards (weaviate/0-weaviate-issues#225). The
barrier keeps the decision cluster-wide and per tenant: no replica
serves the new generation until every replica of that tenant has built
it. Without this anchor, a reader of the FSM table sees "two
RAFT-coordinated phases" and may propose collapsing them.

### 6.4 The build

A unit's build is `Shard.ReindexBuild` ([`reindex_shard_view.go`](../adapters/repos/db/reindex_shard_view.go)).
Its order is fixed, because a crash can interrupt it anywhere:

1. **Stage.** The refresh loop opens the build folder
   (`property_<name>.gen<G>…`), **emptied while this replica's unit is
   `PENDING` in its own log**. It is staged: live writes reach it.
2. **Record the start.** The unit proposes its start and waits until this
   node applied it. From then on a refresh keeps the folder as it is.
3. **Wait for old writers.** The scan starts only when no write that
   loaded a view without the build still runs.
4. **Scan.** Every object is scanned into a **scratch bucket**
   (`….build` folder) and the scratch's segments are placed *under* the
   build's, so live writes and tombstones win over scanned entries. The
   scan has no time filter, because the coordinator stamps update times
   on its own clock and a local cut-off could drop an object written
   before staging. Every exit deletes the scratch. The build holds a view
   reference until it returns, so a retire of its folder waits for it.
5. **Rangeable in-memory structure.** A rangeable build gets its
   in-memory structure after its last prepend, since a prepend refuses a
   bucket that has it.
6. **Report done.** `RecordUnitCompletion`. A completion refused with
   `ErrUnitAlreadyTerminal` counts as success: an earlier attempt's
   completion landed.

**A build folder belongs to its unit, not to one attempt.** After a crash,
a unit reopens its folder as it is and prepends one more scan layer; a
replica empties the folder only while its unit is `PENDING` in its own
log. A unit relaunched in the same process after its completion reply was
lost finds its builds already scanned and proposes the completion again.
A late completion from a run that was cancelled meanwhile is refused by
the FSM.

**A cancelled run.** The view stops naming the build once the task is
terminal, so the refresh loop retires its folder after the last reader
releases it. A tenant that had not switched keeps serving generation 0;
a switched tenant keeps its generation.

**Crash points.** The test matrix (§15) crashes a node while the unit
builds, in the middle of the prepend (the copied segment left as `.tmp`
files), after the unit is done and before the decision, after the
decision with the flip missing from the node's log, and after the flip.
Each recovers by computing its view again and reopening the same folder.

### 6.5 Turning an index off, and snapshot install

**Index off.** `DELETE .../index/{indexType}` sends an `UPDATE_PROPERTY`
update that the apply recognizes as turning one index type off
(`indexTurnedOff`, from its field mask). The apply also raises
`Live(P, I)` to the highest generation of any run with switched tenants on
that index (`TaskRules.IndexOff`, `Manager.TurnIndexOff`) and removes
each ended run once `Live`, counting this off, covers every index type it
builds; a run of two index types stays until both are off. A re-enable
then builds a new generation. The refresh retires the folder of the index
turned off, after readers release it, whether or not a view governs the
property. The REST
handler sets `servedByRun` when a run serves the index to switched
tenants, so an index the class already has off still reaches RAFT; a
failed task listing sets it too, and the apply decides.

**Snapshot install.** A snapshot install changes tasks and live
generations without the applies that send notices. `Migrator.UpdateIndex`
ends with a refresh notice for its class, so every loaded shard
republishes its view. `shardHasProperty` asks the shard's view for the
filterable bucket, so a reload does not re-add a property served past
generation 0.

**Startup reindexers.** The startup reindexers work on generation 0
through the store, as before. `REINDEX_INDEXES_AT_STARTUP` **skips** the
plain folder of an index a run serves past generation 0, and logs one
sampled warning per shard and property pointing to a runtime repair or
rebuild (`ShardLike.servedIndexName`); it does not act on a generation
folder. The missing-text-filterable task reads the searchable folder by
its served name and skips a filterable a run already built.

## 7. Concurrency model

**Per-`(class, property)` REST submit lock.** Closes the same-process
PUT/DELETE race. See §4.1.

**Cluster-wide FSM conflict check** (`ConflictDetector.CheckConflict`).
Called under `Manager.mu` from the RAFT-apply AddTask path BEFORE the
new task is appended to FSM-stored state. FSM-deterministic: every
node applies the same RAFT log entry, sees the same `existingTasks`
snapshot, runs the same predicate, reaches the same accept/reject
decision. Survives leader re-election.

**Submit and movement ordering.** A submit and a replica movement on one
collection are applied one after the other (`taskMovementLocks`, held
until the apply returns), so a movement is checked against a task that
has already applied, and the reverse.

**Conflict rule:** any two reindex migrations on overlapping properties
of the same collection conflict, regardless of which bucket type they
primarily write to. Two runs on one property would each write the
property's schema flags and settings at their own flip, and each flip
must see the other's result. The closure happens at submit time: reject
any new task whose property set overlaps an in-flight task's, so the
caller gets a clean conflict error and can serialize. Every non-terminal status counts
as in-flight (via `TaskStatus.IsActive()`), `PREPARING` and `SWAPPING`
included; see §4.2.

**Cluster-wide FSM schema-mutation check**
(`SchemaMutationDetector`). See §4.3. Blanket reject any external
mutation overlapping an in-flight task's properties on the same
collection.

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

**Worker pool** in `startGenerationRun`. Bounded by the
`concurrency` function passed to the provider (typically a
`runtime.DynamicValue`), so operators can throttle on overloaded
clusters.

**Views.** A view is immutable once published; publishing is one atomic
store and never waits for a reader. A reader holds a reference count; a
retire waits for the replaced view and every older one. A refresh never
holds a lock a reader needs.

## 8. Multi-tenancy, partial runs and takeover

`?tenants=t1,t2` scopes the task to the named tenants on a multi-
tenant class. The handler validates:

- `tenants` on a single-tenant class → 400.
- No `tenants` on a multi-tenant class → defaults to all tenants, for
  semantic and format-only migrations alike. Each tenant is its own group
  in the preparation barrier; a single-tenant collection has one group
  `""` covering every shard.
- `tenants` with a semantic migration → 400 ("all tenants must be
  targeted").
- Any targeted tenant that is not HOT → 400, with a message that gives
  the count and names no tenant.

A tenant switches once its own replicas have built the generation and
acked (§6.3). After a tokenization change the tenant answers queries with
the new index at once; an index a migration enables is read by the tenant
at once as well, but the class flag that declares it (and the class
tokenization) is written at the flip, after the last tenant.

**Tenants created during a run.** A tenant created while a full run is
running starts switched, because it holds no data to build. It stays
switched after a cancel or a failure.

**A run that fails or is cancelled** leaves the decided tenants migrated
and the rest not. `GET .../indexes` then reads `partial`, with
`migratedTenants` / `totalTenants` counted over the newest partial run
that has a generation. A single-tenant collection whose group was decided
reads `partial` too.

### Resubmit, takeover and the exits from a kept run

While a partial run stands on a property, `ReindexPartialRunConflict`
refuses everything but the exits below, and names the resubmit: a `PUT`
that would start any other migration (`409`), a no-op `PUT` that names a
tokenization and a rebuild (`409`), and an index `DELETE` (`422`).

A kept `FAILED` or `CANCELLED` run does not expire, so an operator needs
a way out. The exits, in the order to try them:

1. **Resubmit the same target (takeover).** The new run takes over the
   partial run's generation and switched tenants, builds only what is
   left, and finishes the migration. The old task is deleted. When
   nothing is left to build, the apply finishes the takeover itself: a
   full run goes straight to `SWAPPING` and its flip lands as usual; a
   tenant-scoped run is marked `FINISHED` with the submit time as its end
   time and is compacted at once.
2. **Then the new target.** Once the migration reads `ready`, a run of
   another target is admitted. A different target submitted *before* that
   is refused with `409` ("a failed or cancelled run of another target on
   this property still has switched tenants; resubmit that target and let
   it finish first").
3. **Turn the index off** (for an enable run, or any run whose index type
   you can drop). `DELETE .../index/{indexType}` is admitted while the
   run stands when it turns off an index type that run builds: it raises
   the live generation past the run, so no tenant serves anything. The
   ended run is removed once every index type it builds is off: a
   change-tokenization run needs both its searchable and its filterable
   index turned off (§6.5).
4. **Delete the tenant or the collection.** A deleted tenant leaves the
   run's switched set; deleting the collection removes its tasks.

Migrating back, or on to another target, without finishing the partial
run first is not supported.

Status after cancelling a tenant-scoped task is reported at the
collection level: `GET /v1/schema/{className}/indexes` renders
`cancelled` for the property as a whole. The task is the unit of
identity, so there is no per-tenant index-status surface.

`FROZEN` tenants are rejected by the MutationGuard. A reindex on a
tenant that transitions to FROZEN mid-flight fails the affected unit;
the barrier propagates that failure to the task. Operator can resume by
un-freezing the tenant (transition back to HOT) and re-submitting. A
frozen or offloaded tenant carries its generation folders with the shard
folder.

## 9. Generation folders and the cleanup rule

A property's index folders are named by generation:

```
property_<name>[_searchable|_rangeable]            generation 0, the plain name
property_<name>.gen<G>[_searchable|_rangeable]     generation G >= 1
property_<name>.gen<G>[_searchable|_rangeable].build   scratch of an unfinished scan
```

A property name never holds a `.`, so no two triples of property,
generation and index type share a folder, and the property name of a
folder ends at its first `.`.

**The generation is allocated by the apply.** `ReindexTaskRules.Admit`
assigns it at the `AddTask` entry: one more than the highest generation
the class's live state or any task on the same properties records. It is
a function of replicated state, so every node derives the same folder
names, and no directory scan decides a number. A takeover keeps the taken
run's generation.

**What exists on disk.** For each (property, index type) a shard has at
most: the served folder; the staged build folders of running tasks with
an unswitched unit here; and folders still waiting for a reader to
release them. Everything else is deleted:

- A folder a refresh replaced is retired after the replaced view and every
  older one are released (§6.2).
- A folder no view names at shard load is deleted by the load sweep.
- A kept `FAILED` or `CANCELLED` run's switched tenants keep serving its
  generation, so its folder stays for those tenants until a newer run
  takes their generation, the index is turned off, or the tenant is
  deleted.
- A finished full run's flip makes the class serve its generation, and the
  generation-0 folder (and any older generation) is retired.

**Hard rules.**

- Do not rename a served folder while it is loaded. The mmap'd segments
  survive an unlink, but new segment writes land in a missing directory
  and silently lose data (weaviate/weaviate#10675 is exactly this failure
  mode). Generation folders are opened under their final name and never
  renamed.
- Do not derive a generation from a disk scan. The number is settled by
  the apply before any directory is inspected.
- Do not decide what is served from a file. The view is a function of
  cluster state; a file that disagrees with it is deleted at the next
  load.

## 10. The analyzer overlay

Defined per-strategy via
`MigrationStrategy.AnalyzerOverlay(props) → map[string]inverted.PropertyOverlay`
([`inverted/analyzer.go`](../adapters/repos/db/inverted/analyzer.go)).

A staged build asks its strategy for the overlay when the build is
opened (`stagedStrategy`) and attaches it to the analyzer
(`WithSchemaOverlay`) for the scan and for every staged write
(`analyzeForBuild`). Used by "from-scratch" strategies
(`EnableFilterableStrategy`, `EnableSearchableStrategy`,
`FilterableToRangeableStrategy` for both `enable-rangeable` and
`repair-rangeable`) that build a brand-new inverted index while the
corresponding schema flag is still false in the RAFT-stored schema.
Without this override the analyzer would skip the targeted property (see
`HasAnyInvertedIndex` in `inverted/objects.go`) and the new index would
come out empty.

Strategies that don't need an overlay embed `noAnalyzerOverlay` to get
the nil-return default: both retokenize strategies,
`MapToBlockmaxStrategy`, and `RoaringSetRefreshStrategy`. The live
schema flag is already true for the properties they target.
`RebuildSearchableStrategy` returns nil directly, for a different
reason: a rebuild must not change tokenization, which is a separate
verb.

The overlay is a pure function of the strategy and lives only as long as
the staged build. What the *served* index uses is not an overlay: the
view's settings (tokenization, rangeable readiness) are the winning run's
target while its switched tenants serve it, and the class's after the flip.
`inverted.Property.OverlayForcedOnly` marks a property the build forced on
that has no property-length or null-state bucket yet, so both write legs
skip those two buckets; without it every write to the collection would
fail on the missing bucket.

## 11. Cancel + DELETE-property-index

**Cancel** (`POST .../index/{indexType}/cancel`):

1. Find the non-terminal task targeting `(collection, prop, indexType)`.
   If none matches (already finished, never submitted, or already
   cancelled), return 202 with `Status: NO_OP` and no `taskId`. The
   verb is idempotent: caller's `(collection, property)` was already
   verified to exist by the outer handler, so "nothing to cancel" is
   surfaced as a no-op rather than overloading 404 with two distinct
   meanings.
2. If that task is `SWAPPING`, or in a status this build does not
   recognize (§4.2), return 409 and stop. A `SWAPPING` task has decided
   every tenant and ends once the flip lands; for an unrecognized status
   this build cannot tell what has happened, and the mutation-refusal
   guards agree (`MutationRemedy`). Every other in-flight status cancels
   (`TaskStatus.IsCancellable`): tenants whose switch is decided (§6.3)
   keep it, every other tenant stops. The
   REST pre-flight and `Manager.CancelTask` apply the same predicate, so a
   cancel that loses the race to a status transition is refused at the
   FSM too, and `cancelApplyFailureResponder` renders that refusal as the same
   409 the pre-flight would have.
3. RAFT `CancelDistributedTask`.
4. 202 with `Status: CANCELLED` + the cancelled task ID.

Cancel sweeps no disk and waits for no worker. The task goes terminal in
the apply; each node's refresh loop then retires the build folders of the
unswitched tenants once their readers release them, and a node that was
down computes its view from cluster state when it loads. A cancelled run
with switched tenants stays as a kept run (§8).

**DELETE `/properties/{propertyName}/index/{indexType}`:**

1. Acquire the same `ReindexSubmitLocks` entry as PUT (closes the
   race described in §4.1).
2. Schema FSM applies the `UpdateProperty` flipping the flag to
   false and raising the live generation (§6.5). The MutationGuard rejects
   if a reindex is in flight on this property
   (`FromInFlightMigration=false` on this path), or a partial run stands
   on it that this DELETE does not turn off.
3. The folder is removed: at once for a property no view governs, by the
   refresh loop's retire for a property a view governs.
4. A subsequent re-enable builds a new generation from clean state.

## 12. Known limitation: schema migration, backup and restore

Schema migration across runtime-reindex state does not work, and the
v1.38 Preview merge does not fix it. The fixes live on
`backup-runtime-reindex-fixes` and land as a follow-up PR. Tracking:
weaviate/0-weaviate-issues#215.

Backups are covered: `Index.refuseIfReindexInFlight`
([`reindex_inflight.go`](../adapters/repos/db/reindex_inflight.go))
refuses a backup on any shard DTM reports a live reindex on, and
[`test/acceptance/reindex_backup/`](../test/acceptance/reindex_backup/)
covers it.

A backup also carries each class's **ended reindex tasks** (the compacted
copies, `LocalEndedTasks`) and its **live generations**
(`reindexTasks` and `reindexLive` in the backup descriptor). A failed or
cancelled run with switched tenants therefore survives backup and
restore. The restore entry installs, in order, the class's ended tasks
(`Manager.RestoreCollectionTasks`), then the class with its live
generations, then the index, so a shard that loads inside the entry
already serves its generation. The restore:

- installs only tasks with a generation, of the restored class (also when
  the restore strips its namespace), with the payload's collection
  rewritten to the restored name;
- turns a task captured while running into `FAILED`;
- drops switched tenants the restored class lacks (the group `""` of a
  single-tenant run stays);
- keeps a held task with a higher version;
- runs the end rules on what it installed.

A backup taken before these fields existed restores as generation 0.

## 13. Upgrade / operations

### `DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS`

This knob sets how long terminal distributed tasks without anything to
serve linger in the RAFT task list before garbage collection. `0` means
immediate GC on the next scheduler tick. It applies to tasks without a
generation and to `FINISHED` runs with no switched tenants. It does not
apply to a run with a generation that is `FAILED` or `CANCELLED`, or
`FINISHED` with switched tenants: those are kept until the rules in §4.2
remove them. It is accepted because the per-property blockmax truth is
durable in the schema stamp, so no reader depends on finished reindex
tasks lingering.

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
   `ready`. Finish every reindex before the first node restarts, and submit
   or cancel none until the last node runs this build.

If you moved a property to BlockMax on v1.38 with the collection flag off,
submit `{"algorithm":"blockmax"}` for it again after upgrading.

### The boot check

This version serves only generations, so a run from before them that is
still unfinished cannot be served at all. On the process's first DB load,
`Store.reloadDBFromSchema` runs `refuseUnfinishedLegacyRuns`
([`cluster/reindex_upgrade_check.go`](../cluster/reindex_upgrade_check.go))
over the reindex tasks in cluster state and stops the node at **fatal
level** for a reindex task without a generation that is

- not terminal, or
- `FAILED` or `CANCELLED` with switched tenants, and not covered by a newer
  `FINISHED` run of the same collection that covers its properties, index
  types and tenants.

The message names the task, collection, property and switched-tenant
count, and the way out: roll back to v1.39.17, resubmit the same
migration and let it finish (or cancel it there, only when no tenant has
switched), then upgrade again. A runtime snapshot install and
metadata-only nodes skip the check.

**Limitation.** The check reads cluster state only. **The check does not
detect a partial run from v1.39.0-16 that ended FAILED or CANCELLED, nor
one whose task v1.39.3-14 already deleted.** It sees a partial run only
through `SwapDecidedGroups`, which v1.39.17 added, and v1.39.3-14 deleted
failed tasks after 5 days.
The precondition above and best-effort schema state cover it. Such a
tenant serves what the class names; where v1.39.x had already deleted its
plain folder, that index is empty for that tenant.

A second limitation concerns a v1.39.x `MarkTaskFailed` replayed from the
log after the upgrade: for a task without a generation the cutover check
reports it landed and records `FINISHED`. A v1.39.x run whose flip did not
land is then recorded `FINISHED`, and the boot check does not see it as
partial. A node whose snapshot already held the v1.39.x verdict keeps it,
so replicas can disagree on that old task's status. The task has no
generation and never serves, so only its status is affected.

### Legacy records at load

A shard still holding v1.39.3-17 migration record files
(`<shard>/lsm/.migrations/records/`) converts them before its view loads
(`convertLegacyRecords`, [`reindex_legacy_records.go`](../adapters/repos/db/reindex_legacy_records.go)):

- Every record decodes first; an unreadable one fails the load with
  nothing changed.
- Per plain folder, only the newest record acts. A swapped or merged
  record whose type changes no setting, or whose effect the class shows,
  moves its staged folder to the plain name (remove plain, rename, then
  delete records; safe to repeat). A promoted record never touches the
  plain folder.
- Then every record is deleted, and the load sweep (§6.2) deletes every
  staged and sidecar folder the conversion left.

Fixtures for the thirteen v1.39.17 layouts are under
`adapters/repos/db/testdata/reindex_legacy`. A partial run whose task is
gone serves what the class names (the accepted limitation above).

### Mixed versions and downgrade

- **Old logs and snapshots.** v1.39.x `UPDATE_PROPERTY` flips replay
  unchanged; DTM entries replay with the end rules; a barrier
  post-completion ack is a no-op; `swapsOwed` in an old snapshot is
  ignored.
- **A task entry without unit specs** is applied only by this version or
  later. During a rolling upgrade a node of an earlier release rejects it.
- **Downgrade.** An older binary cannot apply `TYPE_REINDEX_FLIP`, and it
  does not know generation folders: downgrade only before the first run on
  this version. Roll a cluster back below this version after a run only by
  restoring from a backup.
- **Metrics.** `migration_records_wedged_total`,
  `migration_records_not_understood_total`, `reindex_held_shards` and
  `rangeable_inmemory_rebuild_degraded_total` no longer exist, and
  `reindex_view_refresh_pending` is new. Dashboards and alerts on the
  removed names go quiet.

**Status of an old run after upgrading from v1.39.15 or v1.39.16.** A node that
replays such a run from its RAFT log, instead of restoring it from a
snapshot, can label it `failed` or `cancelled` on the indexes endpoint
while the other nodes report it finished. Queries, data and the
schema are not affected. The label clears with the completed-task TTL (a
`failed` one only once a node that reports the run as finished leads the
cluster) or when a newer migration of the same property replaces the run.

**Downgrade of the blockmax stamp:** the stamp is an additive
`searchableBlockmax` property field; an older binary ignores it and
derives blockmax from the class flag / task list as before, so this alone
does not corrupt data. The residual is only that a partial-class property
seeded solely by the stamp (its FINISHED task already GC'd) resolves as
WAND on the older binary until a re-migration.

## 14. Files of interest

**REST**

- [`adapters/handlers/rest/handlers_indexes.go`](../adapters/handlers/rest/handlers_indexes.go) — GET / cancel / status.
- [`adapters/handlers/rest/handlers_indexes_upsert.go`](../adapters/handlers/rest/handlers_indexes_upsert.go) — PUT / rebuild submit, the refusal mapping.
- [`adapters/handlers/rest/handlers_reindex.go`](../adapters/handlers/rest/handlers_reindex.go) — validation helpers, status synthesis.
- [`adapters/handlers/rest/state/reindex_submit_locks.go`](../adapters/handlers/rest/state/reindex_submit_locks.go) — per-(class, prop) submit lock.
- [`entities/models/index_*.go`](../entities/models/) — swagger DTOs (`IndexStatus`, `IndexUpdateRequest`, `PropertyIndexStatus`).
- [`openapi-specs/`](../openapi-specs/) — source of truth for the DTOs.

**Orchestration**

- [`adapters/repos/db/reindex_provider.go`](../adapters/repos/db/reindex_provider.go) — DTM provider, `IsLiveReindexTaskStatus`.
- [`adapters/repos/db/reindex_provider_generation.go`](../adapters/repos/db/reindex_provider_generation.go) — the unit build and the flip proposal.
- [`adapters/repos/db/reindex_provider_payload.go`](../adapters/repos/db/reindex_provider_payload.go) — `ReindexTaskPayload`, migration type constants.
- [`adapters/repos/db/reindex_task_rules.go`](../adapters/repos/db/reindex_task_rules.go) — `ReindexTaskRules`: `Admit`, `Units`, `Ended`, `IndexOff`, `BornSwitched`.
- [`adapters/repos/db/reindex_retainer.go`](../adapters/repos/db/reindex_retainer.go) — retention, `CutoverLanded`, partial-run helpers.
- [`adapters/repos/db/reindex_conflict.go`](../adapters/repos/db/reindex_conflict.go) — `CheckConflict`, `CheckPropertyUpdate`, `CheckClassMutation`, `CheckTenantMutation`, `Touches*` predicates, `MutationRemedy`.
- [`adapters/repos/db/reindex_activity_lookup.go`](../adapters/repos/db/reindex_activity_lookup.go) — `NewShardReindexActivityLookup`, the backup gate's snapshot.
- [`adapters/repos/db/reindex_inflight.go`](../adapters/repos/db/reindex_inflight.go) — `Index.refuseIfReindexInFlight` and `newReindexGate`, the backup gate.
- [`cluster/reindex_upgrade_check.go`](../cluster/reindex_upgrade_check.go) — the boot check.
- [`adapters/repos/db/reindex_legacy_records.go`](../adapters/repos/db/reindex_legacy_records.go) — legacy record conversion.

**Serving**

- [`adapters/repos/db/reindex_cluster_view.go`](../adapters/repos/db/reindex_cluster_view.go) — `computeReindexView`, the serving rule, `reindexBucketName`.
- [`adapters/repos/db/reindex_shard_view.go`](../adapters/repos/db/reindex_shard_view.go) — `shardView`, load, refresh, retire, `ReindexBuild`, staged writes.
- [`adapters/repos/db/reindex_refresher.go`](../adapters/repos/db/reindex_refresher.go) — the node's refresh loop.
- [`adapters/repos/db/helpers/helpers.go`](../adapters/repos/db/helpers/helpers.go) — folder names at a generation.

**Strategy + legacy**

- [`adapters/repos/db/inverted_reindex_strategy.go`](../adapters/repos/db/inverted_reindex_strategy.go) — `MigrationStrategy` interface, `applyPerPropertySchemaUpdate`, `reindexTaskConfig`.
- [`adapters/repos/db/inverted_reindex_strategy_*.go`](../adapters/repos/db/) — one per strategy.
- [`adapters/repos/db/inverted_reindex_legacy_finalize.go`](../adapters/repos/db/inverted_reindex_legacy_finalize.go) — the frozen v1.39.0-2 finalizer.
- [`adapters/repos/db/inverted_reindexer*.go`](../adapters/repos/db/) — the startup reindexers.

**Inverted analyzer**

- [`adapters/repos/db/inverted/analyzer.go`](../adapters/repos/db/inverted/analyzer.go) — `PropertyOverlay`, `WithSchemaOverlay`, `Property.OverlayForcedOnly`.

**DTM**

- [`cluster/distributedtask/doc.go`](../cluster/distributedtask/doc.go) — package-level architecture + the four "journey" shapes.
- [`cluster/distributedtask/types.go`](../cluster/distributedtask/types.go) — `Task`, `Unit`, `UnitSpec`, `TaskStatusPreparing`, `TaskStatusSwapping`, `NeedsPreparationBarrier`, the `TaskStatus.IsTerminal()` / `IsActive()` / `IsCompleted()` / `IsRecognized()` classification helpers, and `IsCancellable()` (§4.2).
- [`cluster/distributedtask/group_states.go`](../cluster/distributedtask/group_states.go) — `TaskRules`, `EndRuling`, `TakeOver`, `TurnIndexOff`.
- [`cluster/distributedtask/compact.go`](../cluster/distributedtask/compact.go) — `LocalEndedTasks`, `UnitSummary`, `GroupTotal`.
- [`cluster/distributedtask/collection_tasks.go`](../cluster/distributedtask/collection_tasks.go) — `RestoreCollectionTasks`.
- [`cluster/distributedtask/manager.go`](../cluster/distributedtask/manager.go) — FSM. `MarkTaskFinalized`, `MarkTaskFailed` godocs are essential reading.
- [`cluster/distributedtask/scheduler.go`](../cluster/distributedtask/scheduler.go) — per-node loop, callback dispatch.
- [`cluster/distributedtask/errors.go`](../cluster/distributedtask/errors.go) — permanent-rejection sentinels + gRPC wire encoding.

**Schema FSM**

- [`cluster/schema/reindex_state.go`](../cluster/schema/reindex_state.go) — `ReindexFlip`, `applyReindexFlip`, `turnIndexOff`, `indexTurnedOff`.
- [`cluster/proto/api/reindex_state_requests.go`](../cluster/proto/api/reindex_state_requests.go) — `ReindexFlipRequest`.
- `cluster/proto/api/` — `PropertyField*` constants used by the fieldmask.
- `cluster/schema/meta_class.go` — `MergePropsMasked` fieldmask apply path.
- `usecases/schema/` — `DeleteClassPropertyIndex` and the property update path.

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
  — the #10675 family.
- `change_tok_delete_journeys_test` — the cross-strategy clobber +
  stale-state sweep family.
- `cancel_test` / `cancel_then_retry_test` — cancel and resubmit.
- `restart_during_swap_test` — restart while a run is switching.
- `property_state_migration_matrix_test` — exhaustive matrix
  (~510 cells × 6 data types × 15 body shapes).
- `scope_assertion_test` — blast-radius bounds: a migration on one
  property never mutates buckets on any other property.
- `api_validation_test` — REST contract.
- `repair_rangeable_test`.

**Acceptance — multi-node** ([`test/acceptance/reindex_multinode/`](../test/acceptance/reindex_multinode/)):

- `happy_path_test` — 3-node baseline.
- `finalizing_window_test` — reads and writes while tenants switch.
- `finalizing_crash_test` — ack barrier rehydrate over restart.
- `cross_replica_barrier_test` — PREPARING and SWAPPING through REST.
- `restart_matrix_test` — rolling restart × migration type.
- `restart_test` — full restart at every boundary.
- `post_restart_test` — recovery after planned restart.
- `round_trip_test` — the original word → field → word pin for #10675.
- `round_trip_adjacent_test` — adjacent journeys (multi-round,
  different tokenizations, filterable-only, searchable-only,
  enable-then-change, MT, concurrent-different-props).
- `in_flight_rangeable_test` — query correctness during enable-
  rangeable mid-flight.
- `migration_journeys_test` — full lifecycle coverage of every
  semantic migration.
- `concurrent_migrations_test` — parallel non-conflicting submits.
- `replica_movement_exclusion_test` / `replica_move_before_finalize_test`
  — replica movement refused during a run.
- `cancel_cleanup_cluster_wide_test` — cancel across nodes.

**Acceptance — concurrency** ([`test/acceptance/reindex_concurrent/`](../test/acceptance/reindex_concurrent/)):

- `concurrent_test` — same-collection concurrent submits.
- `parallel_conflict_matrix_test` — every (migrationA × migrationB)
  pair on overlapping vs disjoint properties.
- `parallel_same_property_test` — race-condition coverage on the
  per-(class, prop) submit lock + FSM conflict check.

**Acceptance — multi-tenant** ([`test/acceptance/reindex_mt/`](../test/acceptance/reindex_mt/)):

- `reindex_mt_test` — `?tenants=` filtering, per-tenant repair,
  FROZEN-tenant resume, per-tenant barrier.
- `tenant_scoped_cancel_test` / `cold_and_unhydrated_cancel_test` —
  cancel of scoped runs and of cold tenants.

**Acceptance — backup gate** ([`test/acceptance/reindex_backup/`](../test/acceptance/reindex_backup/)):

- `suite_test` — backup refused while a reindex is live on the shard.

**Acceptance — rangeable** ([`test/acceptance/reindex_rangeable/`](../test/acceptance/reindex_rangeable/)):

- `concurrent_writes_test` — writes landing during an
  `enable-rangeable` build.

**Distributed task framework** ([`test/acceptance/distributed_tasks/`](../test/acceptance/distributed_tasks/)):

- `unit_tracking_test` — end-to-end unit creation under reindex
  provider on 3-node cluster.

**Shared helpers** — [`test/acceptance/helpers/reindex/helpers.go`](../test/acceptance/helpers/reindex/helpers.go).
HTTP-level helpers (`SubmitIndexUpdate`, `AwaitReindexFinished`,
`GetIndexes`, `AwaitReindexViaIndexes`, `BoolPtr`, `IdsMatchUnordered`)
with `WithTenants` / `WithTimeout` functional options. Plus the
fixture API: `SetupClass`, `SetupClassWithConfig`, `ImportObjects`,
`WithEnv`.

**Unit and integration tests of interest** (`adapters/repos/db`):

- Generation harness — `reindex_generation_harness_test.go` runs the real
  schema and task FSMs, scheduler, provider and DB on every node over an
  in-process log. `reindex_generation_runs_test.go`: each of the nine
  types on 3 nodes at RF 3, takeover, cancel, born-switched tenants,
  deletes and updates during a build, single-tenant runs, the empty
  takeover, index-off, and reload after a snapshot install.
  `reindex_generation_crash_test.go`: crash points, late and re-sent
  completions, crash mid-prepend, a takeover over a dead run's folder.
- The view — `reindex_shard_view_test.go` (load, refresh, retire, index
  off, load sweep, staging limit), `reindex_cluster_view_test.go` (the
  serving rule), `reindex_view_reads_test.go` and
  `reindex_view_writes_test.go` (a query or write on an old view versus a
  new one).
- The rules — `reindex_task_rules_test.go`, `reindex_task_units_test.go`,
  `reindex_conflict_test.go`, `reindex_header_readers_test.go`.
- Upgrade — `reindex_legacy_records_test.go` (the thirteen v1.39.17
  fixtures), `cluster/reindex_upgrade_check_test.go` (the boot check).
- `failUnit` and the provider — `reindex_provider_failunit_test.go`,
  `reindex_provider_test.go`, `reindex_provider_unit_claim_test.go`.
- The backup gate — `reindex_inflight_test.go`,
  `reindex_inflight_kill_switch_test.go`.

**Cluster tests of interest**

- `mergeReindexStatus` — `handlers_indexes_edge_test.go` /
  `handlers_indexes_gaps_test.go`.
- DTM — `cluster/distributedtask/manager_test.go`,
  `group_states_test.go`, `compact_test.go`, `collection_tasks_test.go`,
  `scheduler_multinode_test.go`, `errors_test.go`.
- Flip and restore — `cluster/schema/reindex_state_test.go`,
  `cluster/schema/restore_reindex_test.go`.
