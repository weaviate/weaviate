# RAFT-Based Object Replication: Iterative Implementation Plan

> **Living document** — update status and notes as each phase progresses.
> Companion research: `research.md`

---

## Goal

Replace per-bucket WAL durability with RAFT log durability for RAFT-replicated shards, then expand the FSM to support all object operations needed for production, and finally handle state transfer, auxiliary persistence, and multi-tenancy.

---

## Storage Duplication

RAFT-based object replication introduces storage duplication that does not exist in the current system. This section quantifies the duplication and describes the mitigations built into each phase.

### Current WAL System: No Steady-State Duplication

In the existing LSM storage layer, objects exist in either `.wal` files (in-memory memtable + WAL) or `.db` segment files (after flush), never both in steady state. WAL files are deleted immediately after memtable flush (`memtable_flush.go:200`). There is no meaningful duplication.

### RAFT Introduces Bounded Duplication

After `FSM.Apply()` writes an object to the memtable and it flushes to a segment, the same object exists in both `raft.db` and the `.db` segment until RAFT truncates the log. This is the inherent cost of RAFT-based replication — followers need full object data in log entries to apply writes without a separate data fetch.

The duplication is bounded by `SnapshotThreshold + TrailingLogs` entries. With the shard-tuned defaults from Phase 1d:

| Window                            | Pre-Phase 5                     | Post-Phase 5         |
|-----------------------------------|---------------------------------|----------------------|
| `SnapshotThreshold`               | 1024 entries (~10MB at 10KB/obj)| 1024 entries (~10MB) |
| `TrailingLogs`                    | 4096 entries (~41MB)            | 0 entries (0MB)      |
| **Total steady-state duplication**| **~51MB per shard**             | **~10MB per shard**  |

After RAFT log truncation, entries are truly deleted from BoltDB (freed pages are reused internally). However, the `raft.db` file never shrinks — it stays at peak size, with freed pages reused for future entries.

### WAL Elimination Is Still Beneficial

Eliminating per-bucket WALs removes 15+ redundant WAL writes per object (one per bucket: objects, filterable properties, searchable properties, rangeable properties, etc.). The RAFT log replaces all of these with a single log entry.

### I/O Trade-Off

|                       | WAL (current)                          | RAFT (proposed)                |
|-----------------------|----------------------------------------|--------------------------------|
| **Writes per object** | N buffered writes (one per bucket WAL) | 1 BoltDB transaction           |
| **Fsync behavior**    | Page cache only (no fsync per write)   | BoltDB fsync per transaction   |
| **Durability**        | Data at risk until OS flushes page cache | Durable immediately after fsync |
| **Net effect**        | Fewer total writes, but each is more expensive | Stronger durability guarantees |

Phase 1e (compression) further reduces raft.db size and replication bandwidth by ~2x.

---

## Phase Overview

| Phase | Title                              | Depends On  | Status      |
|-------|------------------------------------|-------------|-------------|
| 1a-c  | WAL-Less LSM Infrastructure        | —           | Not started |
| 1d    | Shard RAFT Snapshot Configuration  | —           | Not started |
| 1e    | RAFT Log Entry Compression         | —           | Not started |
| 2     | Snapshot-Flush Safety              | Phase 1a-c  | Not started |
| 3     | Write Path Integration             | Phase 1a-c  | Not started |
| 4     | Expand FSM Operations              | Phase 3     | Not started |
| 5     | Out-of-Band State Transfer         | Phase 2, 4  | Not started |
| 6     | Auxiliary State & Cleanup          | Phase 2, 3  | Not started |
| 7     | Multi-Tenancy Compatibility        | Phase 3, 5  | Not started |

```
Phase 1a-c ──┬──> Phase 2 ──────────┬──> Phase 5 (revised) ──> Phase 7
Phase 1d ────┘                      │        │
Phase 1e ────┘                      │        └──> reduce TrailingLogs to 0
                                    │
            └──> Phase 3 ──┬──> Phase 4
                            │
                            └──> Phase 6
```

---

## Phase 1: WAL-Less LSM Infrastructure

**Goal:** Build the foundational components that allow LSM buckets to operate without a WAL, gated behind a bucket-level option. No behavioral changes yet — just the building blocks.

### 1a: No-Op Commit Logger

Create a `nopCommitLogger` that satisfies the `memtableCommitLogger` interface (`lsmkv/commitlogger.go:34`) with all methods as no-ops. This follows the precedent set by `adapters/repos/db/vector/hnsw/commit_logger_noop.go`.

**Key files:**
- `adapters/repos/db/lsmkv/commitlogger.go` — add `nopCommitLogger` type

### 1b: Bucket WAL Toggle

Add a `WithWALDisabled()` bucket option that, when set, causes `createNewActiveMemtable()` (`lsmkv/bucket.go:1304`) to use the `nopCommitLogger` instead of a real WAL.

**Key files:**
- `adapters/repos/db/lsmkv/bucket_options.go` — add `WithWALDisabled()` option
- `adapters/repos/db/lsmkv/bucket.go` — use no-op logger in `createNewActiveMemtable()` when `walDisabled`

### 1c: Skip WAL Recovery When Disabled

When `walDisabled` is set, `mayRecoverFromCommitLogs()` should return early — there are no WAL files to recover from.

**Key files:**
- `adapters/repos/db/lsmkv/bucket_recover_from_wal.go` — early-return guard

### Acceptance Criteria (1a-c)
- [ ] `nopCommitLogger` compiles and satisfies `memtableCommitLogger`
- [ ] `WithWALDisabled()` option is available and plumbed through bucket init
- [ ] Bucket created with `WithWALDisabled()` does not create `.wal` files
- [ ] WAL recovery is skipped when WAL is disabled
- [ ] Unit tests for no-op logger and disabled-WAL bucket creation

---

## Phase 1d: Shard RAFT Snapshot Configuration

**Goal:** Tune shard-level RAFT parameters to bound storage duplication, independent of the schema-level RAFT defaults which are designed for small metadata.

### Problem

The shard `StoreConfig` (`cluster/shard/store.go:56-83`) falls back to HashiCorp RAFT defaults that are calibrated for the schema-level RAFT ring (small metadata). Applied to object data, these defaults allow excessive duplication:

| Parameter            | Schema Default                  | Effect at 10KB/object                |
|----------------------|---------------------------------|--------------------------------------|
| `SnapshotThreshold`  | 8192 (`environment.go:1164`)    | ~82MB in raft.db before snapshot     |
| `SnapshotInterval`   | 120s (`environment.go:1156`)    | Up to 2 min before truncation        |
| `TrailingLogs`       | 10240 (`environment.go:1172`)   | ~102MB always retained after snapshot|

Total steady-state duplication: **~184MB per shard** before any tuning.

### Solution

Introduce separate shard-level defaults calibrated to match the aggressiveness of WAL cleanup.

**Design principle:** WAL cleanup happens when the objects bucket memtable reaches `defaultMemTableThreshold = 10MB` (`bucket.go:211`). RAFT snapshot (which triggers `FlushMemtables()` + log truncation in Phase 2) should trigger at equivalent data volumes. At 10KB average object size: 10MB ÷ 10KB = ~1024 objects.

| Parameter           | Shard Default                        | Effect at 10KB/object    | Rationale                                                                           |
|---------------------|--------------------------------------|--------------------------|-------------------------------------------------------------------------------------|
| `SnapshotThreshold` | 1024                                 | ~10MB before snapshot    | Matches `defaultMemTableThreshold` (10MB) for typical 10KB objects                  |
| `SnapshotInterval`  | 30s                                  | Timely snapshots         | Ensures truncation even during slow ingestion periods                               |
| `TrailingLogs`      | 4096 (pre-Phase 5), 0 (post-Phase 5)| ~41MB retained pre-Phase 5 | WAL has zero trailing entries; RAFT needs some only until out-of-band bootstrap exists |

**Key files:**
- `cluster/shard/store.go:56` — add `TrailingLogs` field to `StoreConfig`
- `cluster/shard/store.go:220` — set `TrailingLogs` in `raftConfig()`
- `usecases/config/environment.go` — add `SHARD_RAFT_SNAPSHOT_THRESHOLD`, `SHARD_RAFT_SNAPSHOT_INTERVAL`, `SHARD_RAFT_TRAILING_LOGS` environment variables
- `usecases/config/config_handler.go` — add shard RAFT config fields

### Acceptance Criteria (1d)
- [ ] `StoreConfig` includes `TrailingLogs` field, set in `raftConfig()`
- [ ] Separate environment variables for shard-level RAFT config
- [ ] Defaults are 1024/30s/4096 (not schema-level 8192/120s/10240)
- [ ] Unit tests verify shard config is independent of schema config

---

## Phase 1e: RAFT Log Entry Compression

**Goal:** Reduce raft.db size and replication network bandwidth by compressing RAFT log entries at the application level.

### Problem

Each RAFT log entry contains a full serialized `ApplyRequest` with object bytes (JSON properties + float32 vectors). These are highly compressible.

### Solution

Application-level compression using `s2` (improved Snappy) from `github.com/klauspost/compress` (`go.mod:59` — already a dependency).

**Key files:**
- `cluster/shard/proto/messages.proto:19-38` — add `bool compressed = 6` to `ApplyRequest`
- `cluster/shard/replicator.go` — compress `subCmd` with `s2.Encode()` after marshaling
- `cluster/shard/fsm.go` — decompress `req.SubCommand` with `s2.Decode()` if `req.Compressed`
- Run `buf generate` from `cluster/shard/proto` to regenerate Go code

**Backwards compatibility:** The `compressed` field defaults to `false` in protobuf. Old uncompressed entries replay correctly — the FSM only decompresses when `req.Compressed == true`.

**Expected impact:** ~2x compression on typical mixed objects (JSON + float vectors). Reduces both raft.db size and replication network bandwidth.

### Acceptance Criteria (1e)
- [ ] Round-trip test: compress in replicator → RAFT → decompress in FSM → verify object equality
- [ ] Handles both compressed and uncompressed entries (rolling upgrade safety)
- [ ] Benchmark: measure raft.db size with/without compression

---

## Phase 2: Snapshot-Flush Safety

**Goal:** Enforce the critical invariant: *RAFT log entries must not be truncated until all their data is flushed to LSM segments.* This is the safety mechanism that makes WAL elimination safe.

### 2a: Expand the `shard` Interface

The FSM needs to flush memtables before creating a snapshot. Extend the `shard` interface (`cluster/shard/fsm.go:31-37`) with a `FlushMemtables()` method so the FSM can trigger a flush.

**Key files:**
- `cluster/shard/fsm.go` — expand `shard` interface
- `adapters/repos/db/shard.go` (or wherever the shard satisfies this interface) — implement `FlushMemtables()`

### 2b: Flush-Before-Snapshot in FSM.Snapshot()

Modify `FSM.Snapshot()` to call `shard.FlushMemtables()` before capturing the snapshot. If flush fails, return an error — RAFT will retry later and will NOT truncate the log.

The existing `Store.FlushMemtables()` at `lsmkv/store_backup.go:74` is battle-tested (used for backups) and handles concurrent flush coordination.

**Critical context (Phase 1d interaction):** With `SnapshotThreshold=1024` (Phase 1d), snapshots happen frequently. Each snapshot calls `FlushMemtables()`, which is the mechanism that enables aggressive log truncation — flushing memtables ensures all applied entries are durably in segments before the log is truncated. This is the bridge between RAFT durability and LSM durability: RAFT guarantees entries survive until snapshot, snapshot guarantees entries are in segments, segments guarantee entries survive after log truncation.

**Key files:**
- `cluster/shard/fsm.go` — modify `Snapshot()` to flush first

### Acceptance Criteria
- [ ] `shard` interface includes `FlushMemtables()` method
- [ ] `FSM.Snapshot()` flushes memtables before creating snapshot data
- [ ] Failed flush causes `Snapshot()` to return error (prevents log truncation)
- [ ] Successful flush guarantees all applied entries are in LSM segments
- [ ] Tests verifying flush-before-snapshot behavior

---

## Phase 3: Write Path Integration

**Goal:** Wire WAL-less mode into the shard write path so that RAFT-replicated shards actually create their LSM buckets without WALs, and skip unnecessary WAL flush calls.

### 3a: Shard RAFT Detection

Add a method to the shard (e.g., `isRaftReplicated()`) that returns whether this shard is backed by a RAFT cluster. This is determined by whether `index.raft != nil` (set at `index.go:415`).

**Key files:**
- `adapters/repos/db/shard.go` — add detection method
- `adapters/repos/db/index.go` — ensure `raft` field is accessible to shards

### 3b: Propagate WAL-Disabled to Bucket Creation

When a shard is RAFT-replicated, `makeDefaultBucketOptions()` (`shard_bucket_options.go:20`) should append `WithWALDisabled()` to all bucket options. This covers the objects bucket, all property buckets (filterable, searchable, rangeable), and auxiliary buckets.

**Key files:**
- `adapters/repos/db/shard_bucket_options.go` — conditionally append `WithWALDisabled()`

### 3c: Guard WAL Flush Calls

The write path calls `store.WriteWALs()` after object writes (`shard_write_put.go`). When WAL is disabled, these calls should be no-ops or guarded. Review all `WriteWALs()` call sites:
- `shard_write_put.go` (single put)
- `shard_write_batch_*.go` (batch puts)
- Any other write path that explicitly flushes WALs

**Key files:**
- `adapters/repos/db/shard_write_put.go` — guard `WriteWALs()`
- `adapters/repos/db/lsmkv/store.go` — make `WriteWALs()` aware of WAL-less mode, or guard at call sites

### Acceptance Criteria
- [ ] RAFT-replicated shards create all buckets with WAL disabled
- [ ] Non-RAFT shards continue creating buckets with WAL enabled (no regression)
- [ ] `WriteWALs()` is correctly handled when WALs are disabled
- [ ] Integration test: RAFT-replicated shard ingests objects without `.wal` files appearing
- [ ] Integration test: non-RAFT shard still produces `.wal` files as before

---

## Phase 4: Expand FSM Operations

**Goal:** Add the remaining CRUD operations to the FSM so that all object mutations go through RAFT consensus. Currently only `TYPE_PUT_OBJECT` is implemented.

### 4a: DeleteObject

Add `TYPE_DELETE_OBJECT` to the FSM and replicator. This involves:
- New protobuf message type for delete (or reuse existing with UUID-only payload)
- `FSM.Apply()` dispatch for delete
- `shard` interface expansion with `DeleteObject()`
- `Replicator.DeleteObject()` to serialize and route through RAFT

**Key files:**
- `cluster/shard/proto/messages.proto` — add message types
- `cluster/shard/fsm.go` — add dispatch case
- `cluster/shard/replicator.go` — add `DeleteObject()` method

### 4b: MergeObject

Add `TYPE_MERGE_OBJECT` for partial object updates. Similar structure to PutObject but calls the shard's merge path.

### 4c: AddReferences

Add `TYPE_ADD_REFERENCES` for cross-reference additions. References have their own serialization and shard write path.

### 4d: Batch Operations

Add `TYPE_PUT_OBJECT_BATCH` for batch ingestion. This is reserved in the proto but not implemented. Batch operations are critical for performance — a single RAFT log entry containing N objects avoids N consensus rounds.

### Acceptance Criteria
- [ ] All four operation types dispatch correctly in `FSM.Apply()`
- [ ] Each operation has a corresponding `Replicator` method
- [ ] The `shard` interface exposes all needed methods
- [ ] Protobuf messages are defined and generated
- [ ] Unit tests for each FSM operation type
- [ ] Idempotency verified for each operation during replay

---

## Phase 5: Out-of-Band State Transfer

**Goal:** Enable new replicas to join a RAFT cluster by receiving shard data via out-of-band segment streaming, then catching up via RAFT log replay.

### Why Not RAFT Snapshots for Shard Data?

The schema-level RAFT (`cluster/`) embeds full data in snapshots because schema is small (KB). For shard data (potentially GB+), this approach is not viable:

- `FileSnapshotStore` retains `nRetainedSnapshots=3` snapshots (`cluster/shard/store.go:39`)
- Each snapshot containing full shard data → **3x duplication** of the entire shard on disk
- Example: a 10GB shard would require 30GB just for retained snapshots

**Decision:** RAFT snapshots remain metadata-only (`lastAppliedIndex`). Shard data is transferred out-of-band. This follows a similar pattern to CockroachDB (RAFT for ordering, SSTable transfer for bulk data).

### 5a: Segment Streaming RPC

Define a gRPC streaming RPC for transferring LSM segment files from leader to follower. The leader:
1. Calls `FlushMemtables()` to ensure all data is in segments
2. Records `lastAppliedIndex` at time of flush
3. Streams segment files (all buckets: objects, properties, etc.) to the follower
4. Includes shard metadata: `indexcounter` state, property length tracker, shard config

**Key files:**
- `cluster/shard/proto/` — define streaming RPC for segment transfer
- `cluster/shard/server.go` — implement server-side streaming
- `cluster/shard/client.go` — implement client-side receiving

### 5b: Receiver-Side Segment Restoration

The follower receives streamed segments and writes them to the shard's data directory:
1. Create shard data directory structure
2. Write received segment files to appropriate bucket directories
3. Restore `indexcounter` and property length tracker state
4. Initialize shard with received segments (no WAL recovery needed)
5. Begin RAFT log replay from `lastAppliedIndex + 1`

**Key files:**
- `cluster/shard/fsm.go` — `FSM.Restore()` triggers out-of-band transfer request
- `adapters/repos/db/shard_init_lsm.go` — shard initialization from received segments

### 5c: Streaming Optimization

Optimize the segment transfer for large shards:
- Chunked transfer with configurable chunk size
- Progress tracking and resumability (record last transferred segment)
- `s2` compression during transfer (reuse from Phase 1e)
- Concurrent transfer of independent bucket segments

### 5d: Reduce TrailingLogs to 0

After out-of-band data transfer is implemented, slow followers no longer need to catch up via RAFT log replay from scratch — they bootstrap via segment streaming instead. This enables:

- Reduce `SHARD_RAFT_TRAILING_LOGS` default from 4096 to 0
- WAL has zero trailing entries after cleanup; RAFT now matches this
- Steady-state duplication drops from ~51MB to ~10MB per shard (only `SnapshotThreshold` entries)
- Entries exist in raft.db only between snapshots, not retained indefinitely

### Acceptance Criteria
- [ ] gRPC streaming RPC defined and implemented for segment transfer
- [ ] Follower can receive segments and initialize a functional shard from them
- [ ] After segment transfer, follower replays RAFT log from `lastAppliedIndex + 1` to reach consistency
- [ ] A new node joining the cluster bootstraps via segment streaming (not full log replay)
- [ ] Large shard transfers stream without excessive memory usage
- [ ] `TrailingLogs=0` is safe after this phase (verified by test: kill follower, add data, bring back, verify consistency via streaming + replay)

---

## Phase 6: Auxiliary State & Cleanup

**Goal:** Ensure all shard-level persistent state beyond object data is correctly handled under WAL-less RAFT mode.

### 6a: DocID Counter Persistence

The `indexcounter.Counter` is persisted independently. During RAFT replay, `determineInsertStatus()` re-derives docIDs. Ensure that:
- The counter is included in out-of-band segment transfer (Phase 5a), OR
- The counter can be derived from max docID in existing segments, OR
- Counter persistence is WAL-independent (it already is — it uses its own file)

Evaluate whether the current counter file approach is sufficient or needs integration with RAFT.

### 6b: Property Length Tracker

The `propLenTracker` is flushed independently. During RAFT replay, property lengths are recomputed from objects. Verify this is idempotent and assess whether it should be included in the out-of-band segment transfer (Phase 5a).

### 6c: Hashtree Consideration

The shard's `hashtree` is used for async replication consistency verification. With RAFT providing strong consistency:
- Determine whether hashtree updates should be gated on async replication being enabled
- If hashtree is still needed, ensure `mayUpsertObjectHashTree()` works correctly under RAFT replay

### 6d: Vector Index Queue

Vector index updates via `VectorIndexQueue` are asynchronous and not part of the RAFT log. This is correct (vector indices can be rebuilt from objects). Verify:
- HNSW graph rebuilds correctly after RAFT replay on crash recovery
- No special handling needed beyond existing queue behavior

### Acceptance Criteria
- [ ] DocID counter is consistent after RAFT replay and snapshot restore
- [ ] Property length tracker is consistent after RAFT replay
- [ ] Hashtree behavior is appropriate for RAFT-replicated shards
- [ ] Vector index recovers correctly without WAL

---

## Phase 7: Multi-Tenancy Compatibility

**Goal:** Ensure RAFT-replicated shards work correctly with multi-tenant lazy-loading behavior.

### 7a: Tenant Activation via RAFT Catch-Up

Multi-tenant shards use lazy loading (`WithLazySegmentLoading`). When a tenant shard is activated:
- Currently: buckets are initialized and WAL recovery happens
- With RAFT: activation must trigger RAFT catch-up instead of WAL recovery
- The RAFT cluster for a tenant shard must be able to pause/resume

### 7b: Lazy Bucket Initialization Compatibility

Verify that lazy bucket initialization is compatible with WAL-less mode. When a tenant shard is loaded lazily:
- Segment files should be loaded normally
- No WAL recovery should be attempted
- RAFT catch-up should replay any entries since last snapshot

### Acceptance Criteria
- [ ] Tenant activation triggers RAFT catch-up, not WAL recovery
- [ ] Lazy-loaded buckets work correctly in WAL-less mode
- [ ] Tenant deactivation preserves RAFT state for later reactivation
- [ ] Tests for tenant activate/deactivate cycle under RAFT

---

## Notes & Decisions Log

_Record key decisions, trade-offs, and lessons learned as each phase progresses._

| Date | Phase | Note |
|------|-------|------|
|      |       |      |
