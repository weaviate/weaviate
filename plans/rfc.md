## Schema Mutation Guard During Replica Movement

### Problem: Learning from non-HA → HA Migration

During production scale-out (non-HA to HA via COPY), we discovered that certain schema-level operations performed during a replica movement can cause irrecoverable divergence between the source and target replicas. The source shard's on-disk state changes in ways that the file copy + catchup mechanism cannot reconcile, leading to undefined query behaviour, ranging from degraded performance to data inconsistencies between nodes.

The canonical example: enabling vector compression (PQ/BQ/SQ) during a COPY operation. The source replica's vector index gets compressed (new commit log entries with the quantizer codebook + a new `vectors_compressed` LSM bucket), while the target was built from pre-compression file data. There is no mechanism to retroactively compress the target or reconcile the structural difference. The result is one replica serving compressed queries and the other serving uncompressed, with potentially different result rankings.

This class of issue applies to any operation that changes the on-disk structure of a shard in ways that are not captured by the file snapshot + change capture log approach (Sections 1 and 2). The change capture log captures object-level writes, but schema-level structural changes (new buckets, deleted buckets, index type switches, compression codebooks) are a different category entirely.

### Operations That Must Be Blocked

The following operations modify shard-level on-disk state and must be blocked on any collection with an active replica movement. These are all schema/metadata operations, so blocking them does not affect users' ability to read from or write objects to the database. Blocking is scoped to the affected collection only; other collections are unaffected.

**Vector index structural changes:**

1. **Compression enablement (PQ/BQ/SQ/RQ on HNSW)**: Triggers an async goroutine that writes quantizer codebook to the HNSW commit log and populates a new `vectors_compressed` LSM bucket. The async goroutine can outlive the RAFT apply, meaning the source shard's files change even after the schema update is committed. Target gets pre-compression data with no way to reconcile.
2. **Adding a new named vector**: Creates new empty vector index files and LSM buckets on all nodes via RAFT. If writes to the new vector start during HYDRATING, the target won't have that data, and the change capture log (which is keyed to the shard, not individual vectors) may not capture vector-specific structural changes like new index directories.

**Tenant lifecycle operations:**

1. **Tenant FREEZE (HOT/COLD → FROZEN)**: Uploads shard data to cloud storage and deletes local files. If the shard being frozen is the one being moved, the source files disappear mid-transfer. Currently has zero movement guards.
2. **Tenant UNFREEZE (FROZEN → HOT)**: Calls `GetPartitions` to reassign `BelongsToNodes`, potentially changing which nodes own the shard. This can invalidate the movement's source-node assumption mid-operation.
3. **HOT → COLD tenant transition**: Shuts down the shard locally, breaking any active async replication or log streaming. Currently has a partial guard via `HasOngoingReplication`, but the guard is buggy: it checks the RAFT leader's node ID rather than the movement's actual source/target nodes, so it only fires when the leader happens to be involved in the movement.

**Property index changes:**

1. **Disabling property indexes (UpdateProperty)**: Physically deletes LSM bucket directories via `os.RemoveAll` on all shards. If the source's bucket is deleted mid-transfer, the target retains the copied version, causing structural divergence in the inverted index layout.

### Operation That Must Be Deferred (Not User-Initiated)

1. **Dynamic flat → HNSW auto-switch**: Unlike the above, this is not triggered by a user API call. It fires automatically when the local insert count exceeds a threshold, with zero RAFT coordination. When it fires, it deletes the flat LSM bucket directory and creates an HNSW commit log directory. If this happens mid-transfer, the file list is stale.

This cannot be blocked at the API layer since there is no API call. Instead, the upgrade check in `VectorIndexQueue.BeforeSchedule` must be deferred while a replica movement is active on that shard. The `HaltForTransfer` already pauses the vector queue (preventing new triggers), but an upgrade goroutine already in progress is NOT stopped. The fix: check for active movement before starting the upgrade goroutine, not just before queuing.

### Operations That Are Safe During Movement

The following operations make no on-disk changes and are safe to allow during replica movement:

- `ef`, `efMin`, `efMax`, `efFactor`, `flatSearchCutoff` changes (atomic in-memory, query-time only)
- BM25 `k1`/`b` parameter changes (in-memory, affects future scoring only)
- Stopwords configuration changes (in-memory detector swap)
- Replication factor changes (routing metadata only, does not move data)
- `autoTenantCreation` / `autoTenantActivation` toggles (in-memory flag)
- New tenant creation (independent shard, separate partition, no interaction with the shard being moved)
- Adding new properties (new empty LSM buckets, but shard load reconciliation via `updateIndexAddMissingProperties` handles missing buckets on the target)

### Replica Movement Must Also Be Blocked by Ongoing Structural Operations

The guard is bidirectional. Just as schema mutations must be blocked during replica movement, replica movement operations must be rejected if a dangerous structural operation is already in progress on the source shard:

1. **Ongoing compression**: If a compression goroutine (`hnsw.compress`) is actively running on the source shard (training the quantizer, writing the codebook to the commit log, populating `vectors_compressed`), then the shard's on-disk state is in flux. A file snapshot taken mid-compression would capture a partially-compressed state: perhaps the codebook is written but only half the vectors are re-encoded. The target would load a structurally inconsistent index. Replica movement must be rejected until compression completes and the shard is back to `StatusReady`.
2. **Ongoing flat → HNSW transition**: If a dynamic index upgrade goroutine (`dynamic.doUpgrade`) is in progress, the shard's directory is being restructured: flat bucket files are being deleted and HNSW commit logs are being created. A file snapshot during this window would capture a mix of old and new files, or reference files that no longer exist. Replica movement must be rejected until the upgrade completes and `dynamic.upgraded` is set to `true`.

The shard already tracks these states: compression sets the shard to `StatusReadOnly` with reason `statusReasonVectorIndexUpdate` for its duration, and the dynamic upgrade holds a write lock during the structural swap. The pre-flight validation (Section 3) and the RAFT-level admission check for new replication ops should both verify that the source shard is not in a transitional state before accepting a movement.

### Proposed Implementation

The guard operates at two levels: schema mutations blocked by movement, and movement blocked by structural operations.

**Schema mutations blocked by active movement:**

Extend the existing `HasOngoingReplication` check (already used for HOT→COLD) to cover all dangerous operations. The check should be performed at the RAFT apply level in `cluster/schema/meta_class.go`, keyed by collection name:

1. Before applying `UpdateClass` (vector config changes, property index changes): check if any non-terminal replication op exists for this collection. If so, reject with a clear error: "schema mutation blocked: replica movement in progress for collection X."
2. Before applying `UpdateTenants` (freeze/unfreeze/deactivate): same check, scoped to the specific tenant/shard being moved.
3. For the dynamic flat→HNSW switch: add an `isMovementActive(shardName)` check in `VectorIndexQueue.BeforeSchedule` that defers the upgrade until the movement completes.

**Movement blocked by active structural operations:**

1. In `ValidateReplicationReplicateShard` (the pre-registration validation): query the source shard's status. If it is `StatusReadOnly` with reason `statusReasonVectorIndexUpdate` (compression in progress), reject the movement with: "replica movement blocked: vector index update in progress on source shard."
2. In the same validation: check `dynamic.upgraded` status. If the source shard uses a dynamic index and `upgraded` is `false` but insert count is near the threshold, either reject or warn. If an upgrade goroutine is actively running (detectable via the `sync.Once` state or a dedicated flag), reject.
3. The pre-flight checks (Section 3) should also surface these states, giving operators visibility before they submit the operation.

**Bug fix for `HasOngoingReplication`:**

The existing guard checks the RAFT leader's node ID rather than the movement's actual source/target nodes. It should check whether the shard in question has ANY active movement, regardless of which node is the RAFT leader.

**Error messages:**

Returning clear, actionable error messages is important for operator experience. When a user tries to enable compression and gets a rejection, they need to understand why and know to retry after the movement completes. Equally, when a movement is rejected due to an ongoing compression, the operator needs to know to wait for compression to finish before retrying.