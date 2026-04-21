## Deterministic Write Catchup via Change Capture Log

### Problem

The current replica movement relies on hashtree-based async replication to catch up writes that land on the source shard during the file copy (HYDRATING) phase. This approach is:

- **Timing-based**: Uses a fixed `asyncReplicationUpperTimeBound` (5s into the future) to decide which writes to propagate. Writes that slip through the boundary are caught only by the next hashbeat cycle (~30s).
- **Probabilistic**: The hashtree is a state comparison (Merkle diff), not an ordered event log. It answers "what's different?" not "what happened?", making it inherently racy when state keeps changing during comparison.
- **Unvalidated**: The core correctness test (`TestReplicaMovementTenantParallelWrites`) is commented out due to flakiness.

For production use, especially COPY scale-out (RF=1 → RF=3), a source crash during HYDRATING means data loss since there's no surviving replica and no deterministic record of in-flight writes.

### Proposed Solution: Temporary Change Capture Log

Borrow the **snapshot + log = complete state** invariant from RAFT's state transfer protocol, without implementing full shard-level RAFT consensus.

**Core idea**: When a replica movement begins, the source shard activates a temporary, append-only change capture log. Every write that lands on the source after the file snapshot gets a monotonically increasing Log Sequence Number (LSN) and is appended to this log. The target replays the log after loading the file snapshot, closing the gap deterministically.

**Lifecycle**:

1. HYDRATING begins, and the source shard activates the change capture log. An atomic `uint64` LSN counter starts.
2. `FlushMemtables` completes, file list is snapped. Record this point as `snapshotLSN`.
3. File copy proceeds. Writes continue on the source, where each is applied to the shard normally AND appended to the log with an incrementing LSN.
4. Target loads shard from copied files (state as of `snapshotLSN`).
5. Target streams the change capture log from `snapshotLSN` onward via a new gRPC endpoint (`GetChangeLog(fromLSN) → stream LogEntry`) and replays entries in order.
6. During replay, new writes keep arriving on the source and appending to the log. Target keeps pulling (streaming catchup).
7. Once replay lag is small (e.g. < 100 entries), start dual-writing (target becomes additional write replica via existing FINALIZING mechanism).
8. Final log drain from last replayed LSN to the point dual-writing began. Since dual-writing is active, everything after that point goes directly to the target.
9. Commit target to sharding state. Discard the log.

**Every write is accounted for by exactly one mechanism**: writes before `snapshotLSN` are in the file snapshot, writes between `snapshotLSN` and dual-write start are in the log, writes after dual-write start go directly to the target. No timing windows, no probabilistic diffs.

### MOVE Operations: Log Lifecycle Through DEHYDRATING

The lifecycle above describes the COPY path, which ends at step 9 (commit target, discard log). For MOVE operations, the source must be removed from the sharding state after the target is committed. The change capture log must stay active through this phase:

1. Steps 1-8 proceed identically to COPY.
2. Target is committed to sharding state (`ReplicationAddReplicaToShard`). The log is NOT discarded.
3. Enter DEHYDRATING. The source is still in the sharding state and may still receive writes (due to RAFT propagation delay or in-flight requests). These writes continue to be appended to the log.
4. Target drains the log completely, streaming until the source confirms no further entries.
5. `DeleteReplicaFromShard` removes the source from the sharding state via RAFT.
6. After the RAFT command propagates, the source stops receiving new writes. Any final writes that landed between step 4 and propagation are in the log.
7. Target does one final log drain to capture those last entries.
8. Transition to READY. Discard the log on the source. Clean up.

This replaces the current timing-based `asyncReplicationMinimumWait` (60s default) with a deterministic "drain the log completely, then remove the source." The target knows it has every write because it can verify the final LSN matches, with no timing assumption needed.

### Log Entry Format: Object Snapshots (not raw operations)

The log captures object snapshots (the resulting state), not raw write operations. Each entry is: `{LSN, UUID, objectBytes, vectorBytes, updateTimestamp}`. Deletes are: `{LSN, UUID, tombstone: true}`.

**Why snapshots over raw operations**:

- **Simpler replay**: Uses `OverwriteObjects`, which already exists and is battle-tested in async replication. Writes directly to storage, bypasses validation (source already validated).
- **Idempotent**: Replaying the same entry twice is harmless (last-write-wins by timestamp). Safe retries.
- **No write-path side effects**: Raw op replay would re-exercise the full write path (validation, indexing, counter updates) on a shard loaded from a file snapshot, where side effects may behave differently (upsert vs insert semantics, counter state). Edge case minefield.
- **Same size as raw ops**: A PUT operation and an object snapshot carry the same payload (full object + vector). No delta mechanism for either. DELETEs are marginally smaller as raw ops but negligible.

### Resource Impact

- **Log size = write throughput x HYDRATING duration**. The log only captures writes AFTER `snapshotLSN`, so there is no duplication with the file snapshot by construction.
- For multi-tenant workloads (many small shards), HYDRATING is fast (seconds), so the log is tiny.
- For large single-tenant shards, a busy shard could accumulate a meaningful log during a long file copy. But size scales with actual write volume, not shard size, so it is proportional and predictable.
- **Safety valve**: If the log exceeds a configurable max size, do an iterative approach: flush the source, incremental file copy of new segments, reset the log. Analogous to live VM migration iterating on dirty pages.
- The one overlap case (object in snapshot, then updated during HYDRATING) means the snapshot version sits in an LSM segment until compaction reclaims it. This is standard LSM write-amplification, not new overhead.

### Relationship to Shard-RAFT

This is explicitly NOT shard-raft. Key differences:

- **Single-writer log**: The source shard is the sole writer. No consensus needed for ordering.
- **Temporary**: Activated at HYDRATING start, discarded after READY. Not an always-on cost.
- **Side-channel**: The primary write path is unchanged. The log is an observer, not the write path.
- **No leader election, no quorum writes, no membership protocol.**

However, this is a natural stepping stone. When shard-raft lands, this temporary change capture log is replaced by the shard-level RAFT log, and the catchup mechanism becomes RAFT's native follower catchup. The two can coexist: shard-raft solves a broader problem (strong consistency for all replicated writes), while this solves the narrow problem of deterministic state transfer during replica movement.

### What Needs to Be Built

- A per-shard change capture log (append-only file or lsmkv bucket, activated on demand)
- An LSN counter (atomic `uint64` on the shard)
- A gRPC streaming endpoint for log tailing (`GetChangeLog(fromLSN) → stream LogEntry`)
- Replay logic on the target (apply each `LogEntry` via `OverwriteObjects`)
- Log lifecycle management (activate, deactivate, discard, size-based iterator)