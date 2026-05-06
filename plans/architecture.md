# Architecture: Deterministic Replica Movement via Change Capture Log

This document describes how the change capture log fits into Weaviate's storage and
replication code. It is a companion to `rfc.md` (the design) and `phases.md` (the
delivery plan, ordered by merge).

---

## 1. The big picture

Replica movement copies a shard from a source node to a target node while the
collection stays writable. The system has to answer one question:

> "How do writes that land on the source *during* the move reach the target without
> losing any of them, and without freezing writes?"

Today, the answer is **probabilistic**: we compare Merkle hashtrees and rely on
timing windows of 5 s, 30 s, and 60 s. This proposal makes the answer
**deterministic**: an append-only log records every write that lands on the source
after the file snapshot, with one entry per write.

```
┌────────────────────────────────────────────────────────────────────────┐
│                     Replica movement, bird's-eye view                  │
└────────────────────────────────────────────────────────────────────────┘

  SOURCE NODE                              TARGET NODE
  ┌────────────────────────┐               ┌────────────────────────┐
  │  Shard (source copy)   │               │  Shard (target copy)   │
  │  ┌──────────────────┐  │  file copy    │  ┌──────────────────┐  │
  │  │ LSM segments     │──┼──────────────▶│  │ LSM segments     │  │
  │  │ (snapshotLSN)    │  │               │  │ (snapshotLSN)    │  │
  │  └──────────────────┘  │               │  └──────────────────┘  │
  │                        │               │                        │
  │  Writes ─┬─▶ storage   │               │  Writes replayed       │
  │          │             │   gRPC        │  via OverwriteObjects  │
  │          └─▶ changelog │  stream       │  ▲                     │
  │              (LSN++)   │──────────────▶│  │                     │
  │              append    │               │  │ tailing loop        │
  └────────────────────────┘               └────────────────────────┘
```

The log is a side-channel. The normal write path is unchanged.

---

## 2. Where the change capture log sits in the stack

Weaviate's data path is `DB → Index → Shard → LSM store / vector indexes`. The log
is owned by the Shard, attached via a single atomic pointer. When no movement is in
progress, the pointer is `nil`, so the hot path costs one atomic load and nothing
else.

```
┌────────────────────────────────────────────────────────────────────────┐
│                     Shard (per-tenant storage unit)                    │
│                                                                        │
│   ┌───────────────┐   ┌────────────────┐   ┌───────────────────────┐   │
│   │ LSM store     │   │ Vector index   │   │ Inverted index        │   │
│   │ (objects,     │   │ (HNSW / flat / │   │ (filterable props)    │   │
│   │  properties)  │   │  dynamic)      │   │                       │   │
│   └───────────────┘   └────────────────┘   └───────────────────────┘   │
│          ▲                    ▲                      ▲                 │
│          │                    │                      │                 │
│   ┌──────┴────────────────────┴──────────────────────┴──────────┐      │
│   │                    Shard write methods                      │      │
│   │  PutObject / DeleteObject / MergeObject / BatchDelete ...   │      │
│   └──────────────────────────────┬──────────────────────────────┘      │
│                                  │ after bucket success                │
│                                  ▼                                     │
│                 ┌─────────────────────────────────┐                    │
│                 │ changeLogs  atomic.Pointer[Set] │──┐                 │
│                 └─────────────────────────────────┘  │ nil in steady   │
│                                  │                   │ state → no-op   │
│                                  ▼                   │                 │
│                 ┌─────────────────────────────────┐  │                 │
│                 │ changelog.Set (per-op ChangeLog)│  │                 │
│                 │  op-id → ChangeLog (append,     │  │                 │
│                 │  LSN counter, tailer cond)      │  │                 │
│                 └─────────────────────────────────┘  │                 │
└──────────────────────────────────────────────────────┴─────────────────┘
```

**Key invariants:**

- One `Set` per shard, swapped atomically (CAS). The Set holds one `ChangeLog` per
  in-flight movement.
- The LSN counter is an atomic `uint64`, scoped to a single log (not global).
- The log has one writer: the source Shard. No consensus needed.
- Entries carry full object snapshots (raw `storobj.Object` bytes, reusing the slice
  the write path already produces via `MarshalBinary`), not raw operations. Replay
  is idempotent and last-write-wins by timestamp.

---

## 3. The write-path tee

Phase 2 adds tees to the four write entry points. Each tee runs **after** the LSM
bucket write succeeds, **inside** the same locks that guard the bucket write:

```
     caller (batch, REST, gRPC)
             │
             ▼
   ┌──────────────────────────────────┐
   │ Shard write method               │
   │  - acquire docIdLock[poolId]     │
   │  - acquire asyncReplicationRLock │
   │  - bucket.Put / bucket.Delete    │◀── existing logic, unchanged
   │  - AppendChangeLog{Put,Delete}() │◀── NEW tee, same locks held
   │  - release locks                 │
   └──────────────────────────────────┘
                     │
                     ▼
         ┌───────────────────────────┐
         │ changeLogs.Load()         │
         │  nil? → return (no-op)    │◀── production steady state
         │  non-nil? → for each log  │
         │    in the Set, append     │
         └───────────────────────────┘
```

Tee sites (Phase 2) — **5 total**. Each tee fires after the bucket write succeeds
and *before* the hashtree update. This makes the object bucket the single source of
truth for replica-movement catchup, independent of the hashtree. The hashtree is a
derived structure used by non-movement async replication, which is untouched.

| Entry point | File | Skip condition |
|---|---|---|
| PUT (incl. batch via `putObjectLSM`) | `shard_write_put.go` (`putObjectLSM`) | `status.skipUpsert` |
| MERGE (non-mutable path) | `shard_write_merge.go` (`mergeObjectInStorage`) | `status.skipUpsert` (already filtered upstream) |
| MERGE (mutable path) | `shard_write_merge.go` (`mutableMergeObjectLSM`) | — |
| DELETE (single) | `shard_write_delete.go` (`DeleteObject`) | `existing == nil` |
| DELETE (batch) | `shard_read.go` (`batchDeleteObject`) | `existing == nil` |

Both merge paths call `upsertObjectDataLSM` directly and do **not** go through
`putObjectLSM`, so each needs its own tee. Batch PUTs **do** go through
`putObjectLSM` (via `objectsBatcher`), so one tee at the PUT site covers them and
produces one LSN per object (not per batch). This is intentional: replay
granularity matches source bucket granularity.

---

## 4. Lifecycle: COPY path

```
    ┌──────────┐    ┌───────────┐    ┌────────────┐    ┌───────┐
    │REGISTERED│───▶│ HYDRATING │───▶│ FINALIZING │───▶│ READY │
    └──────────┘    └───────────┘    └────────────┘    └───────┘
                          │                 │
                          │                 │
   source:                ▼                 ▼
    ──────────────────────────────────────────────────────▶ time
    [log active]    [log active]      [log active,
                    writes appended   writes still
                    with LSN]         appended]
                          │                 │
                    snapshotLSN        finalLSN
                    recorded           (after brief
                                       quiesce + finalize)
   target:
    ─────────────────────────────────────────────────────▶
                          │                 │
                    copy files         tail + replay
                    from source        from snapshotLSN
                    (LSM segments)     until caughtUp,
                                       then add replica
                                       via RAFT, finalize,
                                       drain to finalLSN,
                                       stop capture
```

**Every write goes through exactly one mechanism:**

1. LSN ≤ `snapshotLSN` → in the file snapshot (copied via `CopyReplicaFiles`).
2. `snapshotLSN` < LSN ≤ `finalLSN` → in the change capture log, replayed on the target.
3. LSN > `finalLSN` → the target is already a sharding-state replica; writes land via
   the normal replicated write path.

No timing windows. No hashtree diffs.

**Phase 5 ordering**: the FINALIZING handler runs `Snapshot → cap'd drain → RAFT-add`
(and for COPY: → Finalize → uncapped drain → Stop). The cap'd drain catches the target
up to `snap` *before* it joins the sharding state. So a `consistency=ONE` coordinator
read routed to the target after RAFT-add never sees state older than the FINALIZING
boundary.

---

## 5. Lifecycle: MOVE path (adds DEHYDRATING)

```
    ┌──────────┐  ┌───────────┐  ┌────────────┐  ┌─────────────┐  ┌───────┐
    │REGISTERED│─▶│ HYDRATING │─▶│ FINALIZING │─▶│ DEHYDRATING │─▶│ READY │
    └──────────┘  └───────────┘  └────────────┘  └─────────────┘  └───────┘
                                       │                │
                                       │                │
   source:                             ▼                ▼
    ──────────────────────────────────────────────────────────────▶
                                  [log stays       [log stays
                                   active;         active until
                                   target is       source is
                                   added as        removed from
                                   replica         sharding state,
                                   via RAFT]       then final drain]

   target:                             │                │
    ──────────────────────────────────────────────────────────────▶
                                  drain log      tail remaining
                                  up to         log entries
                                  current LSN,  (RAFT propagation
                                  MOVE keeps    delay may produce
                                  log alive     a few more),
                                                finalize + stop
```

The change capture log replaces today's `asyncReplicationMinimumWait` timing
assumption with "drain the log until LSN matches `finalLSN`". The target knows it
has every write because the source reports `finalLSN` explicitly.

---

## 6. Cross-node wire protocol

Phase 4 adds five RPCs to the existing `FileReplicationService` gRPC surface, next
to `GetFile`, under the same basic-auth interceptor. A single op-id names the log
for the entire movement; the source seals it only at the very end.

```
  ┌──────────────────────┐           ┌──────────────────────┐
  │ target (consumer.go) │           │ source (shard)       │
  └──────────┬───────────┘           └──────────┬───────────┘
             │                                  │
             │ StartChangeCapture(shard, opID)  │
             │─────────────────────────────────▶│  activate Set entry
             │◀─────────────────────────────────│  (ack)
             │                                  │
             │ CopyReplicaFiles (existing)      │
             │─────────────────────────────────▶│
             │◀═════════════════════════════════│  file chunks (GetFile stream)
             │                                  │
             │ ── FINALIZING phase boundary ──  │
             │ SnapshotChangeLogLSN(shard, opID)│
             │─────────────────────────────────▶│  brief write-quiesce (~ms),
             │◀─────────────────────────────────│  return current cl.lsn → N
             │                                  │
             │ GetChangeLog(shard, opID,        │
             │   until_lsn=N)                   │  open tailer with cap=N
             │─────────────────────────────────▶│  stream entries 1..N,
             │◀═════════════════════════════════│  emit io.EOF at cap
             │ (target applies via              │  (log STAYS WRITABLE —
             │  OverwriteObjectsFromChangeLog)  │   new entries get LSN > N)
             │                                  │
             │ ReplicationAddReplicaToShard ──► │  (RAFT-add only after
             │   (only after cap'd drain)       │   target is caught up)
             │                                  │
             │ ── DEHYDRATING (MOVE only) ──    │
             │ DeleteReplicaFromShard via RAFT  │
             │ ──────► (RAFT propagation) ──────│
             │                                  │
             │ FinalizeChangeLog(shard, opID)   │
             │─────────────────────────────────▶│  brief write-quiesce,
             │◀─────────────────────────────────│  seal log, return finalLSN
             │                                  │
             │ GetChangeLog(shard, opID,        │
             │   until_lsn=finalLSN)            │  resume tailer to drain tail
             │─────────────────────────────────▶│  stream entries N+1..finalLSN,
             │◀═════════════════════════════════│  emit io.EOF at finalLSN
             │                                  │
             │ StopChangeCapture(shard, opID)   │
             │─────────────────────────────────▶│  remove from Set, close file
             │◀─────────────────────────────────│
```

For COPY, the FINALIZING phase boundary is followed immediately by RAFT-add →
`FinalizeChangeLog` → uncapped drain in the same handler (no DEHYDRATING). The
single-log shape is the same in both cases.

`SnapshotChangeLogLSN` is the consumer's "drain up to here without sealing"
primitive. The cap'd `GetChangeLog` stream EOFs cleanly at N while the log stays
writable, so writes that arrive during RAFT propagation between FINALIZING and
DEHYDRATING keep landing in the same log and get caught by the second
(Finalize-driven) drain. **`until_lsn=0` is the quiet-shard guard, not a
wildcard** — it emits no entries and EOFs immediately. The post-Finalize drain
must use the `finalLSN` returned by `FinalizeChangeLog`.

The streaming `GetChangeLog` follows the existing `GetFile` server-streaming pattern.
Any new goroutine in the handler uses `enterrors.GoWrapper` (enforced by
`tools/linter_go_routines.sh`).

---

## 7. Interaction with existing Weaviate features

### 7.1 LSM store (`adapters/repos/db/lsmkv`)

- The log does **not** write to the LSM. It is an append-only file (or lsmkv bucket
  — see `phases.md` for the exact choice), separate from the shard's own buckets.
- Overlap case: an object is in the file snapshot *and* updated during HYDRATING.
  The snapshot version sits in an older LSM segment; compaction reclaims it
  normally. Standard LSM write amplification, not new overhead.
- Disk usage: log size = `write throughput × HYDRATING duration`. Scales with write
  volume, not shard size.

### 7.2 RAFT / sharding state (`cluster/`)

- The log is a shard-level side-channel. It does **not** touch RAFT. Only the
  `ReplicationAddReplicaToShard` and `DeleteReplicaFromShard` commands flow through
  RAFT at the phase boundaries, exactly as today.
- No leader election, no quorum writes, no membership protocol.

### 7.3 Async replication / hashtree (`cluster/replication`)

- Phase 5 removes the `startAsyncReplication` and `waitForAsyncReplication` calls
  from `processHydratingOp`, `processFinalizingOp`, `processDehydratingOp`. Those
  helpers and the hashtree code stay in the codebase — they are still used by
  **non-movement** async replication (background convergence between replicas that
  are already present).
- `asyncReplicationUpperTimeBoundUnixMillis` and `asyncReplicationMinimumWait` are
  no longer consulted by the movement state machine.

### 7.4 Write path locks

The tee inherits the locks of the enclosing write method:

```
  docIdLock[poolId]       ── per-document ordering (existing)
  asyncReplicationRWMux   ── read-held during writes (existing)
  backupLock              ── read-held during writes (existing)
  isReadOnly              ── boolean guard (existing)
  changeLogs (atomic)     ── single atomic pointer load (NEW)
```

Phase 2 adds a per-shard **write-quiesce latch**. In V1, its only user is
`FinalizeChangeLog`, which holds in-flight writes for the few milliseconds it takes
to record `finalLSN`. Phase 2 includes a lock-ordering test that asserts no
deadlock against the four existing primitives.

### 7.5 Vector indexes (HNSW / flat / dynamic)

- No changes. Object updates on the target take the normal write path through
  `OverwriteObjectsFromChangeLog` → `PutObjectBatch` → index insertion.
- The log carries the object + vector payload; vector index updates on the target
  happen through replay, not the log itself.

### 7.6 Module system (`modules/`)

- No changes. The log captures post-validation, post-vectorization object
  snapshots. Replay skips validation on the target (the source already validated).

---

## 8. What changes

- New package `cluster/replication/changelog` (Phase 1).
- `Shard` gains `changeLogs atomic.Pointer[Set]`, a quiesce latch, three lifecycle
  methods, and tee helpers (Phase 2).
- Five tee sites on the write path (Phase 2).
- New `OverwriteObjectsFromChangeLog` on `Index` (Phase 3).
- Four new gRPC RPCs on `FileReplicationService`, plus copier client wrappers
  (Phase 4).
- The three movement state handlers in `consumer.go` switch from hashtree/timing to
  log-based catchup (Phase 5).
