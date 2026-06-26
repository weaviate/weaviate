## Hard-Link Snapshots for Zero-Downtime File Transfer

### Problem

The current HYDRATING phase pauses compaction on the source shard for the entire duration of the file copy, which can take minutes to hours for large shards. This is because compaction merges and deletes segment files; if a segment is deleted mid-copy, the target receives a corrupt or incomplete file set. The current `HaltForTransfer` (`shard_backup.go`) pauses compaction, flushes memtables, deactivates vector maintenance, and holds this state until the copy finishes and `ResumeFileActivity` is called.

This is the primary cause of source-shard degradation during replica movement. L0 segments accumulate without merging, read performance degrades, and the shard is effectively running in a degraded mode for the entire transfer.

### Background: Hard-Link Snapshots in Backups

This problem was already solved for backups. The backup codepath was recently updated to use hard-linking of segment files, which avoids pausing compaction for the duration of a backup transfer. The insight: hard-links create a second directory entry pointing to the same inode. Even if compaction later deletes the original file path, the hard-linked path still references the same data blocks on disk. The data persists until all references (both the original and the hard-link) are removed.

Replica movement's file copy phase is functionally identical to a backup transfer, using the same `HaltForTransfer`, `PrepareForBackup`, and `ListBackupFiles` infrastructure. The hard-link improvement applies directly.

### Proposed Solution: Hard-Link Snapshot Before File Copy

Replace the current "freeze compaction for the entire copy" pattern with an instant hard-link snapshot:

1. Briefly pause compaction (prevent new compactions from starting, wait for in-flight to finish)
2. Flush memtables to disk
3. Switch HNSW commit logs (`PrepareForBackup`), seal vector queue chunks
4. Hard-link ALL files (LSM segments, HNSW commit logs, vector queue chunks, metadata) into a snapshot directory
5. Resume ALL maintenance immediately: compaction, flushing, vector maintenance all restart
6. Copy the hard-linked files to the target at leisure
7. Clean up the snapshot directory after transfer completes

Steps 1-5 take seconds at most. After step 5, the source shard runs at full performance. Compaction proceeds freely on live segments while the hard-linked snapshot provides a stable, immutable file set for the copy.

### Disk Space Overhead on Source

Hard-links don't consume additional space initially (they share data blocks with the originals). However, while the transfer is in progress, compaction cannot reclaim disk space from segments that have been hard-linked, since the hard-link keeps the inode alive even after compaction "deletes" the original path. This means the source node temporarily uses more disk than it would without a transfer in progress.

The overhead is bounded by: the total size of segments that compaction would have merged/deleted during the transfer window. For a shard with moderate write activity and a transfer lasting minutes, this is typically a fraction of the shard's total size. For very active shards with long transfers, it could be significant.

This has a direct implication for pre-flight validation (see Section 4): disk space checks must account for this overhead on the **source** node, not just the target.

### Snapshot Directory Cleanup

When the target crashes mid-transfer, the hard-linked snapshot directory stays on the source, pinning disk space. This doesn't require a complex lease mechanism. The snapshot directory is identified by operation ID, so cleanup is straightforward:

- **On retry**: Before creating a new snapshot, clean up any existing snapshot for the same operation.
- **On cancellation/completion**: The existing `SyncShard` or cancel codepath removes the snapshot directory.
- **Orphan protection**: A periodic sweep can remove snapshot directories whose operation ID is no longer tracked in the FSM (terminated or force-deleted).

The current `HaltForTransfer` + 5-minute inactivity timeout pattern was needed because compaction stayed frozen for the entire copy. With hard-links, the compaction freeze is seconds, so a standard `defer` is perfectly adequate for that brief window, and a target crash during those seconds is vanishingly unlikely.

### What Needs to Be Built

- Snapshot directory creation and hard-link logic (can be adapted from the existing backup hard-link implementation)
- Integration into the copier's `CopyReplicaFiles` flow, replacing the long-lived `PauseFileActivity` with a brief pause + hard-link + immediate resume
- Snapshot cleanup on retry, cancellation, and completion (keyed by operation ID)
- Orphan snapshot garbage collection (periodic sweep against FSM state)
- Source-side disk space estimation for pre-flight checks (projected compaction overhead during expected transfer duration)