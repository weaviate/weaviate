# Research: Storage Duplication in RAFT-Based Object Replication

## 1. Problem Statement

With the introduction of per-shard RAFT-based object replication (`cluster/shard/`), every ingested object is durably stored in **three** locations:

| Layer | Location | Format | Purpose |
|-------|----------|--------|---------|
| RAFT log | `raft.db` (BoltDB) | Protobuf `ApplyRequest` wrapping `storobj.MarshalBinary()` | Consensus ordering + replication durability |
| LSM WAL | `.wal` per bucket | CRC32-checksummed binary entries | Crash recovery for in-memory memtables |
| LSM segments | `.db` per bucket | Compacted sorted segments | Read-optimized persistent storage |

The WAL and RAFT log serve overlapping purposes — both exist to recover data that hasn't yet been persisted to segments. This creates redundant disk I/O, disk space usage, and recovery complexity.

### Scale of Redundancy

A single `PutObject` call triggers WAL writes to **all** of these buckets:
- `objects` bucket (the serialized object)
- One filterable bucket per indexed property (`{prop}_set`)
- One searchable bucket per text property (`{prop}_searchable`)
- One rangeable bucket per range-indexed property (`{prop}_range`)
- Property length tracking buckets
- Property null state buckets
- Dimension tracking bucket

A class with 10 indexed properties could trigger **15+ separate WAL writes** per object, all of which are redundant when the RAFT log already contains the full object and RAFT replay via `FSM.Apply()` would regenerate all derived indices.

---

## 2. How the Current Storage Layers Work

### 2.1 RAFT Log Write Path

```
Replicator.PutObject()
  → obj.MarshalBinary()                        [serialize object]
  → proto.Marshal(PutObjectRequest{Object: bytes})  [wrap in protobuf]
  → proto.Marshal(ApplyRequest{SubCommand: ...})    [wrap in RAFT command]
  → raft.Apply(cmdBytes, timeout)               [write to BoltDB + replicate]
  → [quorum reached]
  → FSM.Apply(raft.Log)                         [on each replica]
    → proto.Unmarshal → storobj.FromBinary()    [deserialize]
    → shard.PutObject(ctx, obj)                 [write to LSM — triggers WAL]
```

**Key files:** `cluster/shard/replicator.go:111-136`, `cluster/shard/fsm.go:81-140`

### 2.2 LSM WAL Write Path (triggered by FSM.Apply → shard.PutObject)

```
shard.PutObject(obj)
  → putObjectLSM(obj)
    → bucket("objects").Put(uuid, objBinary)     [WAL write #1]
    → updateInvertedIndexLSM(obj, status)
      → bucket("prop_set").RoaringSetAddOne()    [WAL write #2]
      → bucket("prop_searchable").Append()       [WAL write #3]
      → ... [WAL write #N for each property]
    → store.WriteWALs()                          [fsync all WAL buffers]
```

Each `bucket.Put/Append/RoaringSetAddOne` call goes through:
```
memtable.put(node)
  → commitlog.put(node)    [write to .wal file: type + version + length + data + CRC32]
  → insert into BST        [in-memory]
```

**Key files:** `adapters/repos/db/shard_write_put.go:45-87`, `adapters/repos/db/lsmkv/memtable.go:244-290`, `adapters/repos/db/lsmkv/commitlogger.go:300-345`

### 2.3 WAL Recovery (what the RAFT log would replace)

On startup, each bucket scans for `.wal` files and replays them:
```
bucket.mayRecoverFromCommitLogs()
  → scan for .wal files
  → for each: parse entries → apply to new memtable → flush to segment
```

**Key file:** `adapters/repos/db/lsmkv/bucket_recover_from_wal.go`

### 2.4 RAFT Recovery (the replacement mechanism)

On startup, hashicorp/raft replays committed log entries from `lastSnapshotIndex+1`:
```
raft library init
  → load BoltDB log store
  → for each committed entry since last snapshot:
    → FSM.Apply(entry)
      → shard.PutObject(obj)  [rebuilds memtable state]
```

This is functionally equivalent to WAL recovery, but also provides replication ordering.

---

## 3. Analysis of Approaches

### Approach A: Eliminate LSM WAL, RAFT Log as Durability Mechanism

**Concept:** Disable WAL in all LSM buckets when RAFT is active. The RAFT log serves as the crash-recovery mechanism. On restart, RAFT replays committed entries via `FSM.Apply()`, which calls `shard.PutObject()` to rebuild memtable state.

**Advantages:**
- Eliminates all redundant WAL writes (15+ per object)
- Reduces disk I/O significantly on the write path
- Simplifies recovery to a single mechanism
- No `.wal` files on disk, saving space
- Clean abstraction: the `memtableCommitLogger` interface already supports this via a no-op implementation (precedent: `adapters/repos/db/vector/hnsw/commit_logger_noop.go`)

**Challenges:**
- **Snapshot-truncation safety**: RAFT snapshots trigger log truncation. If memtable data from truncated entries hasn't been flushed to segments, data is lost. Solution: force-flush all memtables before snapshot (the existing `Store.FlushMemtables()` at `lsmkv/store_backup.go:74` does exactly this).
- **Recovery latency**: RAFT replay is sequential (one `FSM.Apply()` per entry). For large backlogs, this is slower than WAL replay (which is local-only and doesn't involve deserialization from protobuf). However, RAFT only replays entries since the last snapshot, so this is bounded.
- **Snapshot blocks writes**: The `FSM.Snapshot()` and `FSM.Apply()` are called from the same goroutine in hashicorp/raft. Force-flushing memtables during snapshot blocks new writes. Flush duration depends on memtable size.

**Components requiring change:**

| Component | File | What Changes |
|-----------|------|-------------|
| No-op WAL implementation | `lsmkv/commitlogger.go` | Add `nopCommitLogger` satisfying `memtableCommitLogger` interface (line 34) |
| Bucket WAL toggle | `lsmkv/bucket_options.go` | Add `WithWALDisabled()` option |
| Bucket memtable creation | `lsmkv/bucket.go:1304` | Use `nopCommitLogger` when `walDisabled` in `createNewActiveMemtable()` |
| WAL recovery skip | `lsmkv/bucket_recover_from_wal.go` | Early-return in `mayRecoverFromCommitLogs()` when `walDisabled` |
| Shard bucket init | `shard_bucket_options.go:20` | Append `WithWALDisabled()` in `makeDefaultBucketOptions()` when RAFT-replicated |
| Write path WAL flush | `shard_write_put.go`, `shard_write_batch_*.go` | Guard `store.WriteWALs()` calls |
| FSM snapshot safety | `cluster/shard/fsm.go:145` | Flush all memtables before snapshot, expand `shard` interface |
| Shard RAFT detection | `shard.go` | Add `isRaftReplicated()` based on `index.raft != nil` (set at `index.go:415`) |

### Approach B: Thin RAFT Log (References Only, Not Full Objects)

**Concept:** Store only object references (UUID + version/hash) in the RAFT log, not the full serialized object. RAFT ensures ordering and consensus on "which version wins." Object data lives only in the LSM store.

**Advantages:**
- Much smaller RAFT log — BoltDB grows slowly
- Faster RAFT consensus (less data to replicate over network)
- Lower memory pressure in RAFT log cache (currently 512 entries)

**Challenges:**
- **Followers need object data separately**: After RAFT commits a reference, followers must fetch the actual object from the leader (or from the client). This adds a second network round-trip and requires a separate data transfer protocol.
- **Atomicity**: The object must be durably stored before RAFT can reference it, creating a chicken-and-egg problem. If the object is stored in LSM before RAFT commits, a RAFT rejection means rolling back an LSM write.
- **Significantly more complex**: Requires a two-phase approach (store data, then replicate reference) that partially reintroduces the complexity of 2PC.
- **WAL still needed**: Since RAFT can't replay object data on recovery, the LSM WAL must remain for crash durability.

**Verdict:** This approach trades storage efficiency for protocol complexity. It does NOT solve the WAL duplication problem and introduces harder coordination challenges.

### Approach C: RAFT Log as Primary Store (Replace LSM Entirely)

**Concept:** Use the RAFT log as the single source of truth. Build read indices (inverted, vector) as derived views from the log.

**Advantages:**
- Single write path, true single source of truth
- No duplication whatsoever

**Challenges:**
- **RAFT log is not optimized for reads**: BoltDB is a B+tree key-value store, not a columnar/LSM store. Range scans, filtered queries, and vector search would be extremely slow.
- **Loses LSM compaction**: The LSM store's tiered compaction strategy is critical for write amplification management and read performance.
- **Massive refactor**: Would require rewriting the entire storage engine, inverted index, and vector index layers.
- **Log size**: Without compaction, the RAFT log grows unboundedly with all historical versions of every object.

**Verdict:** Not viable. The LSM store's read optimizations (sorted segments, bloom filters, compaction, secondary indices) are fundamental to Weaviate's query performance.

### Approach D: Hybrid — Delayed WAL Write (WAL as Backup to RAFT)

**Concept:** Keep both RAFT log and WAL, but make WAL writes asynchronous/batched rather than synchronous. The RAFT log provides primary durability; the WAL is written lazily as a secondary safety net.

**Advantages:**
- Reduced write latency (WAL writes no longer on critical path)
- Extra safety layer during RAFT snapshots
- Minimal code changes

**Challenges:**
- Still writes to two places (just asynchronously)
- Adds complexity around async WAL consistency
- Doesn't fully solve the disk space duplication

**Verdict:** Half-measure. Reduces latency but doesn't eliminate the fundamental duplication.

---

## 4. Recommended Approach: A (Eliminate LSM WAL)

Approach A is the cleanest solution. It leverages the existing `memtableCommitLogger` interface for zero-behavior WAL, uses the existing `Store.FlushMemtables()` for snapshot safety, and cleanly separates concerns: RAFT for durability + ordering, LSM segments for reads.

### Critical Safety Mechanism: Snapshot-Flush Coordination

The invariant "RAFT log entries must not be truncated until all data is in segments" is enforced by flushing all memtables inside `FSM.Snapshot()`. This works because:

1. hashicorp/raft serializes `Snapshot()` and `Apply()` calls — no new applies during flush
2. `Store.FlushMemtables()` is already battle-tested (used for backup operations)
3. If flush fails, `Snapshot()` returns an error, RAFT doesn't truncate, and retries later
4. After flush succeeds, all data from `lastAppliedIndex` and earlier is in segments — safe to truncate

### Idempotency of RAFT Replay

RAFT replay after crash calls `shard.PutObject()` for each committed entry. This is safe because:

| Operation | Idempotency Mechanism |
|-----------|----------------------|
| Object bucket `Put()` | UUID-keyed overwrite — same UUID writes are idempotent |
| Inverted index `RoaringSetAddOne()` | Set-add — adding existing element is a no-op |
| Searchable index `Append()` | `determineInsertStatus()` (`shard_write_put.go:366`) detects prior state and computes deltas |
| Vector queue `Insert()` | Queue handles deduplication |
| DocID allocation via `counter` | Counter is persisted independently; replayed objects get same docIDs via `determineInsertStatus()` |

### Feature Toggle: Per-Shard, Not Global

WAL-less mode should be gated by whether the shard is RAFT-replicated (`index.raft != nil`), not by a global config flag. This allows:
- Non-replicated shards to continue using WALs
- Both modes on the same node during migration
- Individual shard migration

---

## 5. Broader Refactoring Map

Beyond WAL elimination, taking RAFT-based object replication to production involves these additional areas (each worthy of its own analysis):

### 5.1 Operations Not Yet in RAFT FSM

Currently only `TYPE_PUT_OBJECT` is implemented in `FSM.Apply()` (line 100-106). Production requires:
- `DeleteObject` — must go through RAFT for consistency
- `MergeObject` — partial object updates
- `AddReferences` — cross-reference additions
- `PutObjectBatch` — batch operations (reserved in proto but not implemented)

Each of these has corresponding `shard.prepare*` methods in `shard_replication.go` that would need RAFT equivalents.

### 5.2 Snapshot State Transfer

The current snapshot (`fsm.go:184-190`) only stores `lastAppliedIndex`. A new replica joining the cluster has no way to get the actual object data — it would need to replay the entire RAFT log from the beginning, which is impractical.

Production requires snapshot-based state transfer: streaming LSM segment files from leader to follower via `raft.InstallSnapshot`. This is analogous to how the schema-level RAFT (in `cluster/`) includes the full schema in its snapshots.

### 5.3 Relationship with Async Replication / Hashtree

The shard currently maintains a hashtree (`shard.go`: `hashtree hashtree.AggregatedHashTree`) for async replication consistency verification. With RAFT providing strong consistency:
- The hashtree may become redundant for intra-RAFT-cluster replicas
- It may still be useful for cross-region/async replication scenarios
- The `mayUpsertObjectHashTree()` call in `putObjectLSM()` could be gated on whether async replication is also enabled

### 5.4 DocID Counter Persistence

The `indexcounter.Counter` (`shard_init_lsm.go:184`) is persisted independently. During RAFT replay, `determineInsertStatus()` re-derives docIDs from existing state. However, if the counter file is lost but segments exist, there's a risk of docID collision. With WAL-less mode, the counter must either:
- Be persisted to segments (e.g., as metadata in the objects bucket)
- Be included in RAFT snapshots
- Be derivable from the max docID in existing segments

### 5.5 Property Length Tracker

The `propLenTracker` (`inverted.JsonShardMetaData`) is persisted separately via `propLenTracker.Flush()`. During RAFT replay, property lengths are recomputed from objects. This is already idempotent but adds recovery time.

### 5.6 Vector Index Queue Interaction

Vector index updates are asynchronous via `VectorIndexQueue`. They are NOT part of the RAFT log (vectors are embedded in the object, but HNSW graph construction is separate). This is correct — vector indices can be rebuilt from objects — but means:
- HNSW graph state may lag behind RAFT-applied state
- On crash recovery, vector queue must be replayed or rebuilt from objects
- This is the existing behavior and is unaffected by WAL elimination

### 5.7 Multi-Tenancy Considerations

Multi-tenant shards use lazy loading (`WithLazySegmentLoading`). When a tenant shard is activated, buckets are initialized and WAL recovery happens. With RAFT-replicated tenants:
- Activation must trigger RAFT catch-up instead of WAL recovery
- Lazy bucket initialization must be compatible with WAL-less mode
- The RAFT cluster for a tenant shard must be able to pause/resume

---

## 6. RAFT Log Size and BoltDB Growth

Without WAL, the RAFT log must retain entries longer (until all memtables flush to segments). BoltDB growth is bounded by:
- `SnapshotThreshold`: number of entries before snapshot (triggers log truncation)
- `SnapshotInterval`: time between snapshots
- Object size: each entry contains `storobj.MarshalBinary()` output

**Example sizing:** 1000 objects at 1KB average = ~1MB of RAFT log between snapshots. At 100K objects at 10KB = ~1GB. The `SnapshotThreshold` must be tuned relative to memtable flush thresholds to keep BoltDB bounded.

The `LogCache` (512 entries in memory, `store.go:36`) provides read performance for recent entries but doesn't affect disk usage.

---

## 7. Summary of Key Files

### Storage Layer (LSM)
- `adapters/repos/db/lsmkv/commitlogger.go:34` — `memtableCommitLogger` interface (the WAL abstraction)
- `adapters/repos/db/lsmkv/bucket.go:1304` — `createNewActiveMemtable()` (where WAL is created)
- `adapters/repos/db/lsmkv/bucket_recover_from_wal.go` — WAL crash recovery
- `adapters/repos/db/lsmkv/memtable.go:272` — `commitlog.put(node)` (WAL write on every operation)
- `adapters/repos/db/lsmkv/store_backup.go:74` — `FlushMemtables()` (reusable for snapshot flush)
- `adapters/repos/db/lsmkv/bucket_options.go` — bucket configuration options

### Shard Write Path
- `adapters/repos/db/shard_write_put.go:45-87` — `putOne()` orchestration
- `adapters/repos/db/shard_write_put.go:366` — `determineInsertStatus()` (idempotency logic)
- `adapters/repos/db/shard_bucket_options.go:20` — `makeDefaultBucketOptions()` (central bucket config)
- `adapters/repos/db/shard_init_lsm.go:148` — `initObjectBucket()` (bucket creation)
- `adapters/repos/db/shard_init_properties.go:59` — property bucket initialization

### RAFT Layer
- `cluster/shard/fsm.go:31-37` — `shard` interface (the FSM-to-storage bridge)
- `cluster/shard/fsm.go:81-140` — `FSM.Apply()` and `putObject()` (where RAFT entries become storage writes)
- `cluster/shard/fsm.go:145-154` — `FSM.Snapshot()` (where truncation safety must be enforced)
- `cluster/shard/store.go:85-99` — `Store` struct (per-shard RAFT cluster with BoltDB + LogCache)
- `cluster/shard/replicator.go:111-136` — `PutObject()` (serialization and routing)

### Integration Points
- `adapters/repos/db/index.go:409-429` — RAFT replicator wiring
- `adapters/repos/db/shard_raft_replicator.go` — `ShardRaftReplicator` interface
- `adapters/repos/db/shard_replication.go` — current 2PC prepare/commit methods
