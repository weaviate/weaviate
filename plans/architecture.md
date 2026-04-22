# Architecture: Deterministic Replica Movement via Change Capture Log

This document describes the architecture of the change capture log proposal and how it
plugs into the existing Weaviate storage and replication stack. It is a companion to
`rfc.md` (the design) and `phases.md` (the merge-ordered delivery plan).

---

## 1. The big picture

Replica movement relocates a shard from a source node to a target node while the
collection remains writable. The question the system must answer is:

> "How do writes that land on the source *during* the move reach the target without
> losing any of them, and without needing to freeze writes?"

Today, that question is answered **probabilistically** by comparing Merkle hashtrees
and a 5 s / 30 s / 60 s set of timing windows. This proposal answers it
**deterministically** with an append-only, per-op log of every write that landed on
the source after the file snapshot was taken.

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

The log is the side-channel. The primary write path is unchanged.

---

## 2. Where the change capture log sits in the stack

Weaviate's data path is `DB → Index → Shard → LSM store / vector indexes`. The log
is owned by the Shard and attached via a single atomic pointer. Outside of an
in-flight movement that pointer is `nil`, so the hot path pays a single atomic load
and nothing else.

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

- One `Set` per shard, swapped via CAS. The Set owns one `ChangeLog` per in-flight op.
- LSN counter is atomic `uint64`, scoped to a single log (not global).
- The log is single-writer (the source Shard). No consensus needed.
- Entries carry full object snapshots (raw `storobj.Object` bytes, reusing
  the slice each write path already produces via `MarshalBinary`), not raw
  ops — replay is idempotent and last-write-wins by timestamp.

---

## 3. The write-path tee

Phase 2 wires the tees into the four write entry points. The tee is always
**after** the LSM bucket write has succeeded, **inside** the same locks that
guard the bucket write:

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

Tee sites (Phase 2) — **5 total**. Every tee fires after the bucket write
succeeds and *before* the hashtree update, so the object bucket remains the
single source of truth for replica-movement catchup independent of hashtree
outcomes. The hashtree is a derived structure used by the non-movement
async-replication path, which remains untouched.

| Entry point | File | Skip condition |
|---|---|---|
| PUT (incl. batch via `putObjectLSM`) | `shard_write_put.go` (`putObjectLSM`) | `status.skipUpsert` |
| MERGE (non-mutable path) | `shard_write_merge.go` (`mergeObjectInStorage`) | `status.skipUpsert` (already filtered upstream) |
| MERGE (mutable path) | `shard_write_merge.go` (`mutableMergeObjectLSM`) | — |
| DELETE (single) | `shard_write_delete.go` (`DeleteObject`) | `existing == nil` |
| DELETE (batch) | `shard_read.go` (`batchDeleteObject`) | `existing == nil` |

Both merge paths call `upsertObjectDataLSM` directly and do NOT funnel through
`putObjectLSM`, so each needs its own explicit tee. Batch PUTs do funnel
through `putObjectLSM` via `objectsBatcher`, so one tee at the PUT site covers
them and produces one LSN per object (not per batch) — intentional, so replay
granularity matches the source bucket granularity.

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

**Every write is accounted for by exactly one mechanism:**

1. Writes with LSN ≤ `snapshotLSN` → in the file snapshot (copied via `CopyReplicaFiles`).
2. Writes with LSN > `snapshotLSN`, ≤ `finalLSN` → in the change capture log, replayed on the target.
3. Writes with LSN > `finalLSN` → target is already a sharding-state replica; they land via the normal replicated write path.

No timing windows. No hashtree diffs.

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

The change capture log replaces the current `asyncReplicationMinimumWait` timing
assumption with "drain the log until LSN matches `finalLSN`." The target knows it
has every write because the source reports the final LSN explicitly.

---

## 6. Cross-node wire protocol

Phase 4 adds four RPCs to the existing `FileReplicationService` gRPC surface, next
to `GetFile`, under the same basic-auth interceptor:

```
  ┌──────────────────────┐           ┌──────────────────────┐
  │ target (consumer.go) │           │ source (shard)       │
  └──────────┬───────────┘           └──────────┬───────────┘
             │                                  │
             │ StartChangeCapture(shard, opID)  │
             │─────────────────────────────────▶│  activate Set entry,
             │◀─────────────────────────────────│  return snapshotLSN
             │                                  │
             │ CopyReplicaFiles (existing)      │
             │─────────────────────────────────▶│
             │◀═════════════════════════════════│  file chunks (GetFile stream)
             │                                  │
             │ GetChangeLog(shard, opID,        │
             │              fromLSN=snapshotLSN)│
             │─────────────────────────────────▶│  open tailer
             │◀═════════════════════════════════│  stream of ChangeLogStreamEntry
             │ (target decodes, batches,        │  (server-streaming, bounded
             │  calls OverwriteObjectsFrom-     │  backlog)
             │  ChangeLog)                      │
             │                                  │
             │ FinalizeChangeLog(shard, opID)   │
             │─────────────────────────────────▶│  brief write-quiesce (~ms),
             │◀─────────────────────────────────│  return finalLSN
             │                                  │
             │ (drain until local LSN == final) │
             │                                  │
             │ StopChangeCapture(shard, opID)   │
             │─────────────────────────────────▶│  remove from Set, close file
             │◀─────────────────────────────────│
```

The streaming `GetChangeLog` follows the existing `GetFile` server-streaming pattern.
Any new goroutine in the handler uses `enterrors.GoWrapper` (enforced by
`tools/linter_go_routines.sh`).

---

## 7. Interaction with existing Weaviate features

### 7.1 LSM store (`adapters/repos/db/lsmkv`)

- The log does **not** write to the LSM. It is an append-only file (or lsmkv bucket —
  see phases.md for exact choice), separate from the shard's own buckets.
- Overlap case: an object is present in the file snapshot *and* updated during
  HYDRATING. The snapshot version sits in an older LSM segment; compaction reclaims
  it normally. This is standard LSM write-amplification, not new overhead.
- Disk accounting: log size = `write throughput × HYDRATING duration`. Scales with
  write volume, not shard size.

### 7.2 RAFT / sharding state (`cluster/`)

- The change capture log is strictly a shard-level side-channel. It does **not**
  interact with RAFT. Only the `ReplicationAddReplicaToShard` /
  `DeleteReplicaFromShard` commands still flow through RAFT at the phase boundaries,
  exactly as they do today.
- No leader election, no quorum writes, no membership protocol.

### 7.3 Async replication / hashtree (`cluster/replication`)

- Phase 5 removes the `startAsyncReplication` / `waitForAsyncReplication` calls from
  `processHydratingOp`, `processFinalizingOp`, `processDehydratingOp`. Those
  helpers and the hashtree infrastructure remain in the codebase — they are used
  by **non-movement** async replication flows (e.g. background convergence between
  already-present replicas).
- `asyncReplicationUpperTimeBoundUnixMillis` and `asyncReplicationMinimumWait` stop
  being consulted by the movement state machine.

### 7.4 Write path locks

The tee inherits the locks of the enclosing write method:

```
  docIdLock[poolId]       ── per-document ordering (existing)
  asyncReplicationRWMux   ── read-held during writes (existing)
  backupLock              ── read-held during writes (existing)
  isReadOnly              ── boolean guard (existing)
  changeLogs (atomic)     ── single atomic pointer load (NEW)
```

Phase 2 adds a per-shard **write-quiesce latch** whose *only* user in V1 is
`FinalizeChangeLog`, to hold all in-flight writes for the ~ms it takes to record
`finalLSN`. Phase 2 includes a lock-ordering test asserting no deadlock against
the four existing primitives.

### 7.5 Vector indexes (HNSW / flat / dynamic)

- No changes. Object updates on the target take the normal write path through
  `OverwriteObjectsFromChangeLog` → `PutObjectBatch` → index insertion.
- The log captures the object + vector payload; vector index updates on the target
  are driven by the replay, not by the log itself.

### 7.6 Module system (`modules/`)

- No changes. The log captures post-validation, post-vectorization object snapshots.
  Replay bypasses validation on the target (source already validated).

---

## 8. What changes, what doesn't

**Changes:**

- New package `cluster/replication/changelog` (Phase 1).
- `Shard` gains `changeLogs atomic.Pointer[Set]`, quiesce latch, three lifecycle
  methods, and tee helpers (Phase 2).
- Five tee sites on the write path (Phase 2).
- New `OverwriteObjectsFromChangeLog` on `Index` (Phase 3).
- Four new gRPC RPCs on `FileReplicationService`, plus copier client wrappers
  (Phase 4).
- `consumer.go`'s three movement state handlers switch from hashtree/timing to
  log-based catchup (Phase 5).

**Does NOT change:**

- REST / GraphQL / gRPC query APIs.
- Existing `CopyReplicaFiles` / `GetFile` streaming.
- Non-movement async replication convergence.
- RAFT commands, sharding state propagation, or membership.
- LSM bucket strategies, vector indexes, inverted index, modules.
- The replica movement state machine's state names or ordering
  (`REGISTERED → HYDRATING → FINALIZING → [DEHYDRATING →] READY`).
