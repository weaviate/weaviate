## Deterministic Write Catchup via Change Capture Log

### Problem

When we move a replica, the source shard keeps receiving writes while we copy its files to the target. We need a way for the target to catch up on those writes.

Today we use hashtree-based async replication to do this. It has three problems:

- **It uses time, not order.** We pick a 5-second window and hope writes land inside it. Writes that miss the window only get caught on the next 30-second hashbeat cycle.
- **It compares state, not events.** A hashtree tells you "these two shards differ", not "here is what happened". When the source keeps changing during the comparison, the diff is racy.
- **It is not proven correct.** The main test for this (`TestReplicaMovementTenantParallelWrites`) is disabled because it is flaky.

This matters most for COPY scale-out (RF=1 → RF=3). If the source crashes during HYDRATING, there is no other replica and no record of the writes that were in flight. We lose data.

### Proposed Solution: Temporary Change Capture Log

We borrow one idea from RAFT: **snapshot + log = complete state**. We do not implement RAFT itself.

**The core idea**: when a replica movement starts, the source turns on a small append-only log. Every write that lands on the source after the file snapshot gets a sequence number (LSN) and is appended to the log. The target loads the file snapshot, then replays the log to catch up.

**Lifecycle**:

1. HYDRATING starts. The source turns on the log. An LSN counter starts at 0.
2. `FlushMemtables` runs. The file list is snapshotted. We record this point as `snapshotLSN`.
3. File copy begins. New writes on the source are applied normally and also appended to the log with a new LSN each.
4. The target loads the shard from the copied files. Its state matches `snapshotLSN`.
5. The target streams the log from `snapshotLSN` onward via a new gRPC endpoint (`GetChangeLog(fromLSN) → stream LogEntry`) and applies entries in order.
6. Writes keep arriving on the source while the target is replaying. The target keeps pulling.
7. Once the target is close to caught up (e.g. fewer than 100 entries behind), we start dual-writing to the target via the existing FINALIZING mechanism.
8. The target drains the remaining log entries up to the point dual-writing began. Everything after that goes directly to the target.
9. The target is committed to the sharding state. The log is discarded.

**Every write goes through exactly one path**:

- Writes before `snapshotLSN` → in the file snapshot.
- Writes between `snapshotLSN` and dual-write start → in the log.
- Writes after dual-write start → directly to the target.

No timing windows. No probabilistic diffs.

### MOVE Operations: Keeping the Log Active Through DEHYDRATING

The lifecycle above is for COPY. It ends at step 9: commit the target, throw away the log.

For MOVE, we also need to remove the source from the sharding state. The log must stay active during this phase, because the source can still receive writes for a short time (RAFT propagation delay, in-flight requests).

1. Steps 1-8 are the same as COPY.
2. The target is committed to the sharding state (`ReplicationAddReplicaToShard`). **The log stays active.**
3. We enter DEHYDRATING. The source is still in the sharding state and may still receive writes. These continue to be appended to the log.
4. The target drains the log until the source confirms there is nothing more to send.
5. `DeleteReplicaFromShard` removes the source from the sharding state via RAFT.
6. Once the RAFT command propagates, the source stops receiving writes. Any final writes that landed between step 4 and propagation are in the log.
7. The target does one last drain to pick up those final entries.
8. Transition to READY. Discard the log on the source. Clean up.

This replaces the current 60-second `asyncReplicationMinimumWait`. Instead of "wait 60 seconds and hope", we do "drain the log, then remove the source". The target verifies the final LSN, so it knows it has every write.

### Log Entry Format: Object Snapshots, Not Raw Operations

Each log entry is the resulting state of an object after the write, not the raw write operation:

- Update or insert: `{LSN, UUID, objectBytes, vectorBytes, updateTimestamp}`
- Delete: `{LSN, UUID, tombstone: true}`

**Why snapshots instead of raw operations**:

- **Replay is simple.** We use `OverwriteObjects`, which already exists and is used by async replication. It writes directly to storage and skips validation, since the source already validated the write.
- **Replay is idempotent.** Replaying the same entry twice is safe (last-write-wins by timestamp). Retries are safe.
- **No write-path side effects.** Raw operation replay would re-run the full write path (validation, indexing, counters) on a shard loaded from a file snapshot. Side effects can behave differently there (e.g. upsert vs insert, counter state). Many edge cases.
- **Same size as raw operations.** A PUT and an object snapshot carry the same payload. DELETEs are slightly smaller as raw operations but the difference is negligible.

### Resource Impact

- **Log size = write throughput × HYDRATING duration.** The log only captures writes after `snapshotLSN`, so there is no overlap with the file snapshot.
- For multi-tenant workloads (many small shards), HYDRATING takes seconds, so the log is tiny.
- For large single-tenant shards, a busy shard can build up a meaningful log during a long file copy. But size scales with write volume, not shard size, so it is predictable.
- **Safety valve**: if the log gets too big, we can iterate: flush the source, copy new segments, reset the log. Similar to live VM migration handling dirty pages.
- One overlap case: if an object is in the file snapshot and then updated during HYDRATING, the snapshot version stays in an LSM segment until compaction reclaims it. This is normal LSM write amplification, not new overhead.

### Relationship to Shard-RAFT

This is **not** shard-raft. The differences:

- **One writer.** The source shard is the only writer to the log. No consensus needed.
- **Temporary.** The log is on during HYDRATING and off after READY. No always-on cost.
- **Side channel.** The normal write path is unchanged. The log just observes.
- **No leader election, no quorum writes, no membership protocol.**

But this is a good stepping stone. When shard-raft lands, this log is replaced by the shard-level RAFT log, and catchup becomes RAFT's native follower catchup. The two solve different problems: shard-raft gives strong consistency for all replicated writes; this gives deterministic state transfer during replica movement.

### What Needs to Be Built

- A per-shard change capture log (append-only file or lsmkv bucket, turned on as needed)
- An LSN counter (atomic `uint64` on the shard)
- A gRPC streaming endpoint for the target to tail the log (`GetChangeLog(fromLSN) → stream LogEntry`)
- Replay logic on the target (apply each `LogEntry` via `OverwriteObjects`)
- Log lifecycle management (turn on, turn off, discard, size-based iteration)
