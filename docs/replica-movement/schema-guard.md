# Schema guard during replica movement

Keep a shard's two copies identical while one is being moved to another node. This guard stops schema changes that would secretly make the source and target differ — changes the copy mechanism cannot undo.

## Why this is needed

To scale out (for example, non-HA → HA), Weaviate **copies** a shard to a new node:

1. Copy the shard's files to the target.
2. Catch the target up with a **change log** of writes that arrived during the copy.

The change log only carries **object writes** — add, update, delete of your data. It does **not** carry **structural changes** to the shard's on-disk shape: turning on vector compression, adding a named vector, deleting an index, freezing a tenant.

If a structural change happens **during** a move, the source changes shape but the target keeps the old shape. There is no way to reconcile them afterwards. The two replicas now answer queries differently.

> **Canonical failure:** enable vector compression (PQ/BQ/SQ/RQ) mid-copy. The source compresses its vectors; the target stays uncompressed. One replica serves compressed results, the other uncompressed — with no path to converge.

## The guard, at a glance

The guard works in **both directions**, plus one special case that has no user to say "no" to.

```
   Copy a shard:   source ──── files ────▶ target
                          ──── change log (object writes only) ────▶
                          structural changes are NOT in the log
                                        │
            ┌───────────────────────────┴───────────────────────────┐
            │                        GUARD                           │
            └───────────────────────────┬───────────────────────────┘
                                         │
   ┌─────────────────────┐  ┌────────────────────────┐  ┌──────────────────────┐
   │ FORWARD             │  │ REVERSE                │  │ DEFER                │
   │ schema change while │  │ move starts while a    │  │ automatic flat→HNSW  │
   │ a move is running   │  │ structural op is       │  │ upgrade (no user)    │
   │                     │  │ already running        │  │                      │
   │  → REJECT           │  │  → WAIT, then proceed  │  │  → POSTPONE          │
   │  clear error,       │  │  never silently        │  │  retry next tick     │
   │  retry after move   │  │  cancelled             │  │  after move ends     │
   └─────────────────────┘  └────────────────────────┘  └──────────────────────┘
```

- **Forward** — a dangerous schema change is **rejected** while a move is in progress. The operator gets a clear message and retries once the move finishes. (These are metadata-only operations, so blocking them does not stop reads or writes of your data.)
- **Reverse** — if a structural operation is **already running** on the source when a move is requested, the move **waits** instead of failing, then proceeds once the operation finishes.
- **Defer** — the dynamic `flat → HNSW` index upgrade fires automatically with no user behind it, so it cannot be "rejected." It is **postponed** while a move is active and retried on the next scheduler tick.

Blocking is **scoped to the affected collection** — other collections are untouched.

## What is guarded

| Operation | During a move |
|---|---|
| Enable / change vector compression (PQ/BQ/SQ/RQ) | **Blocked** |
| Add or remove a named vector | **Blocked** |
| Change vector index type or distance | **Blocked** |
| Disable a property index (filterable/searchable/rangeable) | **Blocked** |
| Tenant FREEZE / UNFREEZE / HOT→COLD | **Blocked** |
| Dynamic `flat → HNSW` auto-upgrade | **Deferred** |
| Start a move while compression is running on the source | **Waits** |
| Start a move while a `flat → HNSW` upgrade is running | **Waits** |

## What stays allowed

These make no on-disk structural change, so they are safe during a move:

- Search-time knobs: `ef`, `efMin`, `efMax`, `efFactor`, `flatSearchCutoff`
- BM25 `k1` / `b`, stopwords
- Replication factor, `autoTenantCreation` / `autoTenantActivation`
- Creating a new tenant (a separate shard, unrelated to the moving one)
- Adding a new property (the target reconciles the new empty buckets on load)

## How it works (one level deeper)

- **One source of truth for "is a move active?"** A node-independent check reads only the cluster-replicated movement state, so **every node reaches the same answer.** This replaces an older check that keyed off the RAFT leader's node ID and could disagree across the cluster.
- **Forward guards** sit in the schema apply path (`UpdateClass`, `UpdateProperty`, `UpdateTenants`). They reject only **structurally dangerous** changes; safe in-memory changes pass through.
- **Reverse guard** lives on the **source node**, where the runtime state is actually visible. Two atomic flags — `compressing` (HNSW) and `upgrading` (dynamic) — mark exactly the window where the on-disk work runs. The transfer gate refuses to snapshot the shard while either is set.
- **Waiting, not cancelling.** A move that hits a busy source returns a special **"shard busy"** signal that does **not** count against the movement's error budget. A slow compression therefore makes the move **wait** rather than burning through the retry limit and auto-cancelling itself.

## Related known gap

While building this guard we found a separate, pre-existing bug: a `schemaOnly` replay of an "enable compression" change updates the schema but never compresses the on-disk index, so schema and disk can disagree permanently. It is **not** made worse by this change. It is pinned with a failing test and tracked for a follow-up fix.
