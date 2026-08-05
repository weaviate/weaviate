# Phase 5: Out-of-Band State Transfer

## Context

Phases 1-4 replaced per-bucket WAL durability with RAFT log durability. The RAFT snapshot contains only metadata (`lastAppliedIndex`), not the actual shard data. When a follower falls too far behind (log entries truncated), hashicorp/raft triggers `FSM.Restore()` via `InstallSnapshot`. Currently `Restore()` only reads metadata — the follower has no way to get the actual shard data.

Phase 5 implements out-of-band state transfer using **hardlink snapshots**: the leader creates filesystem hardlinks of all segment files (instant, O(1) per file), then serves them to the follower asynchronously. Writes and compaction continue uninterrupted on the leader; only a brief (~2-3s) compaction pause is needed for snapshot creation.

## Key Design Decisions

1. **Hardlink snapshots** — LSM segments are immutable after creation. `os.Link()` creates a hardlink sharing the same inode (no data copy). Compaction can delete originals; hardlinks keep data accessible. Brief compaction pause (~2-3s) only for snapshot creation, NOT for the entire transfer.
2. **New RPCs on ShardReplicationService** — `CreateTransferSnapshot`, `GetSnapshotFile` (streaming), `ReleaseTransferSnapshot`. No reuse of `FileReplicationService` (which requires `PauseFileActivity` for full transfer duration).
3. **Follower-pull model** — `FSM.Restore()` triggers the follower to request snapshot + download files from leader.
4. **Reinit path auto-updates FSM** — `IncomingReinitShard` → `initLocalShard` → `shard_init.go:187` calls `OnShardCreated()` → `store.SetShard(newShard)`.

## Transfer Flow

```
Leader: FSM.Snapshot().Persist()
  → FlushMemtables() + write JSON {className, shardName, nodeID, lastAppliedIndex}

hashicorp/raft sends snapshot to follower via InstallSnapshot

Follower: FSM.Restore(snapshotReader)
  1. Decode JSON metadata (lastAppliedIndex)
  2. Detect foreign snapshot (snap.NodeID != f.nodeID)
  3. Call stateTransferer.TransferState(className, shardName)
     │  StateTransferer queries current leader dynamically
     │  via leaderFunc() → store.LeaderID() (atomic read, no deadlock)
     │
     ┌─────────── On Leader ───────────┐
     │ a. CreateTransferSnapshot RPC   │
     │    - FlushMemtables (~1s)       │
     │    - PauseCompaction (instant)  │
     │    - List all shard files       │
     │    - Create hardlinks to        │
     │      staging dir (~1s)          │
     │    - ResumeCompaction (instant) │  ← Total pause: ~2-3s
     │    - Return file list +         │
     │      snapshot ID                │
     └────────────────────────────────┘
     b. For each file: GetSnapshotFile RPC (streaming chunks)
        - Worker pool download with temp files, CRC32, fsync
     c. ReleaseTransferSnapshot RPC
        - Leader deletes staging directory
     d. Reinit local shard (IncomingReinitShard)
        - Shutdown old shard
        - initLocalShard from downloaded files
        - OnShardCreated → store.SetShard(newShard)
  4. Store lastAppliedIndex
  5. RAFT replays log from lastAppliedIndex+1 (all ops idempotent)
```

**Why not use snapshot nodeID for leader?** The leader may change between snapshot
creation and restore. Instead, the follower queries the current leader dynamically
via `store.LeaderID()` (which reads an atomic from hashicorp/raft). The `nodeID`
field remains in the snapshot metadata for logging/debugging only.

## Implementation

### Step 1: Add proto messages and service RPCs

**File: `cluster/shard/proto/messages.proto`**

Add to existing `ShardReplicationService`:

```protobuf
service ShardReplicationService {
  rpc Apply(ApplyRequest) returns (ApplyResponse) {}
  // NEW: State transfer for snapshot restore
  rpc CreateTransferSnapshot(CreateTransferSnapshotRequest) returns (CreateTransferSnapshotResponse) {}
  rpc GetSnapshotFile(GetSnapshotFileRequest) returns (stream SnapshotFileChunk) {}
  rpc ReleaseTransferSnapshot(ReleaseTransferSnapshotRequest) returns (ReleaseTransferSnapshotResponse) {}
}

message CreateTransferSnapshotRequest {
  string class = 1;
  string shard = 2;
}

message SnapshotFileInfo {
  string name = 1;    // relative file path
  int64 size = 2;     // file size in bytes
  uint32 crc32 = 3;   // CRC32 checksum
}

message CreateTransferSnapshotResponse {
  string snapshot_id = 1;
  repeated SnapshotFileInfo files = 2;
}

message GetSnapshotFileRequest {
  string snapshot_id = 1;
  string file_name = 2;
}

message SnapshotFileChunk {
  int64 offset = 1;
  bytes data = 2;
  bool eof = 3;
}

message ReleaseTransferSnapshotRequest {
  string snapshot_id = 1;
}

message ReleaseTransferSnapshotResponse {}
```

Then regenerate: `cd cluster/shard/proto && PATH=$PATH:~/go/bin buf generate`

### Step 2: Expand `shard` interface for snapshot support

**File: `cluster/shard/fsm.go`**

Add to the `shard` interface:

```go
type shard interface {
    // ... existing methods ...

    // CreateTransferSnapshot creates a hardlink snapshot of all shard files
    // for out-of-band state transfer. Returns the snapshot ID and staging
    // directory path. Caller must call ReleaseTransferSnapshot when done.
    CreateTransferSnapshot(ctx context.Context) (TransferSnapshot, error)

    // ReleaseTransferSnapshot deletes the staging directory for a snapshot.
    ReleaseTransferSnapshot(snapshotID string) error
}
```

Define the `TransferSnapshot` type (exported, used by server):

```go
type TransferSnapshot struct {
    ID       string
    Dir      string             // staging directory path
    Files    []TransferFileInfo // file list with sizes and checksums
}

type TransferFileInfo struct {
    Name  string // relative path within staging dir
    Size  int64
    CRC32 uint32
}
```

Add `StateTransferer` interface (no `leaderNodeID` — leader determined dynamically):

```go
type StateTransferer interface {
    TransferState(ctx context.Context, className, shardName string) error
}
```

### Step 3: Implement `CreateTransferSnapshot` on `*db.Shard`

**File: `adapters/repos/db/shard_transfer_snapshot.go` (new)**

```go
func (s *Shard) CreateTransferSnapshot(ctx context.Context) (shard.TransferSnapshot, error) {
    snapshotID := uuid.New().String()
    stagingDir := filepath.Join(s.path(), ".transfer-snapshot-"+snapshotID)

    // 1. Flush memtables (brief, ensures all in-memory data is in segments)
    if err := s.store.FlushMemtables(ctx); err != nil {
        return shard.TransferSnapshot{}, fmt.Errorf("flush memtables: %w", err)
    }

    // 2. Brief compaction pause for file listing + hardlink creation
    if err := s.store.PauseCompaction(ctx); err != nil {
        return shard.TransferSnapshot{}, fmt.Errorf("pause compaction: %w", err)
    }
    defer s.store.ResumeCompaction(ctx)

    // 3. List all shard files (LSM segments, vector indices)
    lsmFiles, err := s.store.ListFiles(ctx, s.index.Config.RootPath)
    if err != nil { ... }

    vectorFiles := []string{}
    s.ForEachVectorIndex(func(tv string, idx VectorIndex) error {
        files, _ := idx.ListFiles(ctx, s.index.Config.RootPath)
        vectorFiles = append(vectorFiles, files...)
        return nil
    })
    // Also list vector queue files via ForceSwitch

    allFiles := append(lsmFiles, vectorFiles...)

    // 4. Create staging directory and hardlinks
    os.MkdirAll(stagingDir, 0o755)
    for _, relPath := range allFiles {
        srcPath := filepath.Join(s.index.Config.RootPath, relPath)
        dstPath := filepath.Join(stagingDir, relPath)
        os.MkdirAll(filepath.Dir(dstPath), 0o755)
        os.Link(srcPath, dstPath)  // Instant, O(1)
    }

    // 5. Copy metadata files (small, not hardlinked since they're being written to)
    //    indexcount, proplengths, version → copy bytes into staging dir

    // 6. Build file info list with sizes + CRC32 checksums
    // Walk staging dir, compute CRC32 for each file

    // Compaction resumes here (defer)
    return shard.TransferSnapshot{ID: snapshotID, Dir: stagingDir, Files: fileInfos}, nil
}

func (s *Shard) ReleaseTransferSnapshot(snapshotID string) error {
    stagingDir := filepath.Join(s.path(), ".transfer-snapshot-"+snapshotID)
    return os.RemoveAll(stagingDir)
}
```

Key details:
- `store.PauseCompaction()` / `store.ResumeCompaction()` at `lsmkv/store_backup.go:20-46` — pauses only compaction, not writes
- `store.ListFiles()` at `lsmkv/bucket_backup.go:45-65` — lists `.db` segment files, ignores `.wal` and `.tmp`
- `VectorIndex.ListFiles()` — lists HNSW commit log and snapshot files
- CRC32 computed via `usecases/integrity.CRC32()` (same as copier uses)
- Metadata files (indexcount, proplengths, version) copied via `readBackupMetadata()` pattern at `shard_backup.go:290-339`

### Step 4: Add RPC handlers to server

**File: `cluster/shard/server.go`**

Add methods + snapshot tracking:

```go
type Server struct {
    shardproto.UnimplementedShardReplicationServiceServer
    registry  *Registry
    logger    logrus.FieldLogger
    snapshots sync.Map // snapshotID → *activeSnapshot
}

type activeSnapshot struct {
    dir   string // staging directory path
    class string
    shard string
}
```

**CreateTransferSnapshot handler:**
- Get Store from Registry → get shard → call `shard.CreateTransferSnapshot(ctx)`
- Store snapshot info in `snapshots` map
- Return file list + snapshot ID

**GetSnapshotFile handler (server-streaming):**
- Look up snapshot by ID
- Open file from staging directory: `os.Open(filepath.Join(snap.dir, req.FileName))`
- Stream chunks (same pattern as `file_replication_service.go:130-175`)
- Chunk size: configurable, default 64KB

**ReleaseTransferSnapshot handler:**
- Look up snapshot by ID
- Call `shard.ReleaseTransferSnapshot(snapshotID)` (removes staging dir)
- Remove from `snapshots` map

### Step 5: Implement state transfer client

**New file: `cluster/shard/state_transfer.go`**

```go
type StateTransfer struct {
    rpcClientMaker  rpcClientMaker   // creates ShardReplicationServiceClient
    addressResolver addressResolver  // resolves nodeID → address
    reinitializer   ShardReinitializer
    leaderFunc      func(className, shardName string) string // queries current leader dynamically
    rootDataPath    string
    log             logrus.FieldLogger
}
```

**Leader discovery:** `leaderFunc` is wired to `registry.Leader(className, shardName)` which calls
`store.LeaderID()` → `raft.LeaderWithID()` (atomic read, safe during Restore). The leader is
determined at transfer time, not from stale snapshot metadata. If the leader is empty (election
in progress), `TransferState` retries with backoff.

`TransferState` method:

1. **Determine current leader:** Call `leaderFunc(className, shardName)` to get the leader node ID.
   Resolve to address via `addressResolver`. Retry with backoff if no leader yet.
2. Create `ShardReplicationServiceClient` to leader via `rpcClientMaker`
3. Call `CreateTransferSnapshot(class, shard)` → get file list + snapshot ID
4. `defer client.ReleaseTransferSnapshot(snapshotID)`
5. Download files using worker pool (adapted from `copier.go:120-178` patterns):
   - `metadataWorker`: file info already in response, no separate metadata fetch needed
   - `downloadWorker`: `GetSnapshotFile(snapshotID, fileName)` → stream chunks → temp file → CRC32 validate → rename
   - Concurrent workers: `runtime.GOMAXPROCS(0)`
   - CRC32 validation via `usecases/integrity.CRC32()`
   - Pre-allocation, fsync, temp files (.tmp suffix → rename)
6. Call `reinitializer.ReinitShard(ctx, className, shardName)`

**`ShardReinitializer` interface** (exported, implemented at adapter layer):

```go
type ShardReinitializer interface {
    ReinitShard(ctx context.Context, className, shardName string) error
}
```

**Note:** The download worker logic (~100 lines) is adapted from `copier.go:273-379`. We replicate the pattern (not import copier directly) to avoid heavy dependencies. Both use the same `usecases/integrity.CRC32()` and `entities/errors.NewErrorGroupWrapper()`.

### Step 6: Modify `FSM.Restore()` and add setter

**File: `cluster/shard/fsm.go`**

Add field + setter to FSM:

```go
type FSM struct {
    // ... existing fields ...
    stateTransferer StateTransferer
}

func (f *FSM) SetStateTransferer(st StateTransferer) {
    f.mu.Lock()
    defer f.mu.Unlock()
    f.stateTransferer = st
}
```

Modify `Restore()` — leader is NOT taken from snapshot metadata; `StateTransferer` finds it dynamically:

```go
func (f *FSM) Restore(rc io.ReadCloser) error {
    defer rc.Close()
    // ... decode snapshot, verify class/shard ...

    f.mu.RLock()
    st := f.stateTransferer
    f.mu.RUnlock()

    // Trigger state transfer if this is a foreign snapshot (from another node).
    // The StateTransferer determines the current leader dynamically — we don't
    // use snap.NodeID for leader determination since the leader may have changed
    // between snapshot creation and restore.
    if snap.NodeID != f.nodeID && st != nil {
        ctx := context.Background()
        if err := st.TransferState(ctx, f.className, f.shardName); err != nil {
            return fmt.Errorf("state transfer for %s/%s: %w", f.className, f.shardName, err)
        }
    }

    f.lastAppliedIndex.Store(snap.LastAppliedIndex)
    return nil
}
```

### Step 7: Propagate StateTransferer through config chain

**File: `cluster/shard/store.go`** — Add `SetStateTransferer(st StateTransferer)` delegating to FSM.

**File: `cluster/shard/raft.go`** — Add `StateTransferer StateTransferer` to `RaftConfig`. In `OnShardCreated`, after `store.SetShard(shard)`, call `store.SetStateTransferer(r.config.StateTransferer)`.

**File: `cluster/shard/registry.go`** — Add `StateTransferer StateTransferer` to `RegistryConfig`. Propagate to `RaftConfig` in `GetOrCreateRaft`. Add `SetStateTransferer()` for late-binding (needed because `rpcClientMaker` and reinitializer may not be available at registry creation time).

**Note on `leaderFunc` wiring:** The `StateTransfer` struct's `leaderFunc` field is wired to `registry.Leader(className, shardName)` at creation time in `configure_api.go`. This uses `Registry.Leader()` (line 230 of registry.go) which traverses `Registry → Raft → Store → raft.LeaderWithID()`. The function is safe to call from `FSM.Restore()` because `raft.LeaderWithID()` reads an atomic value, not a mutex-protected field.

### Step 8: Wire in `configure_api.go`

**File: `adapters/handlers/rest/configure_api.go`**

After the ShardRegistry and DB are created:

```go
// Create the shard reinitializer (needs DB)
reinitializer := &shardReinitAdapter{db: repo}

// Create state transfer — leader is found dynamically via registry.Leader()
stateTransfer := &shard.StateTransfer{
    RpcClientMaker:  rpcClientMaker,   // from line ~380
    AddressResolver: addrResolver,     // existing NodeAddress resolver
    Reinitializer:   reinitializer,
    LeaderFunc:      appState.ShardRegistry.Leader,  // dynamically queries current leader
    RootDataPath:    dataPath,
    Log:             appState.Logger,
}

appState.ShardRegistry.SetStateTransferer(stateTransfer)
```

`shardReinitAdapter`:
```go
type shardReinitAdapter struct { db *db.DB }
func (a *shardReinitAdapter) ReinitShard(ctx context.Context, className, shardName string) error {
    idx := a.db.GetIndex(schema.ClassName(className))
    if idx == nil { return fmt.Errorf("index for class %s not found", className) }
    return idx.IncomingReinitShard(ctx, shardName)
}
```

### Step 9: Reduce TrailingLogs to 0

**File: `cluster/shard/store.go:239-246`** — Change default from 4096 to 0.

**File: `usecases/config/environment.go`** — Update `SHARD_RAFT_TRAILING_LOGS` default to 0.

### Step 10: Tests

**FSM.Restore() tests** (`cluster/shard/fsm_test.go` or `store_test.go`):
- Restore triggers state transfer when snapshot nodeID differs from FSM nodeID
- Restore skips transfer when nodeID matches (leader self-snapshot)
- Restore skips transfer when stateTransferer is nil
- Restore returns error when state transfer fails (lastAppliedIndex NOT updated)

**State transfer tests** (`cluster/shard/state_transfer_test.go`):
- Happy path: mock gRPC client returns snapshot + files, mock reinitializer succeeds
- Leader found dynamically via leaderFunc (not from snapshot metadata)
- leaderFunc returns empty initially, then returns leader on retry (backoff)
- CreateTransferSnapshot RPC fails
- File download fails (temp files cleaned up, ReleaseTransferSnapshot still called)
- CRC32 mismatch
- ReinitShard fails

**Snapshot creation tests** (`adapters/repos/db/shard_transfer_snapshot_test.go`):
- CreateTransferSnapshot creates staging directory with hardlinks
- All shard files present in staging dir
- ReleaseTransferSnapshot removes staging directory
- Multiple concurrent snapshots supported

**TrailingLogs test** (`cluster/shard/store_test.go`):
- Update expected default from 4096 to 0

## Files Summary

| File | Changes |
|------|---------|
| `cluster/shard/proto/messages.proto` | New messages + 3 RPCs on ShardReplicationService |
| `cluster/shard/proto/` (generated) | Regenerate via `buf generate` |
| `cluster/shard/fsm.go` | `StateTransferer` interface, `TransferSnapshot`/`TransferFileInfo` types, shard interface expansion, FSM field/setter, modify `Restore()` |
| `cluster/shard/server.go` | 3 new RPC handlers, snapshot tracking map |
| `cluster/shard/state_transfer.go` **(new)** | `StateTransfer` struct, `TransferState()`, file download workers, `ShardReinitializer` interface |
| `cluster/shard/store.go` | `SetStateTransferer()`; TrailingLogs default → 0 |
| `cluster/shard/raft.go` | `StateTransferer` in `RaftConfig`; wire in `OnShardCreated` |
| `cluster/shard/registry.go` | `StateTransferer` in `RegistryConfig`; `SetStateTransferer()`; propagate |
| `adapters/repos/db/shard_transfer_snapshot.go` **(new)** | `CreateTransferSnapshot`, `ReleaseTransferSnapshot` on `*Shard` |
| `adapters/handlers/rest/configure_api.go` | Wire `StateTransfer` + reinit adapter into Registry |
| `usecases/config/environment.go` | `SHARD_RAFT_TRAILING_LOGS` default → 0 |
| Tests (new + modified) | FSM.Restore, state transfer, snapshot creation, TrailingLogs |

## Implementation Sequence

1. Proto messages + buf generate (Step 1)
2. Shard interface expansion + types (Step 2)
3. Snapshot creation on `*Shard` (Step 3)
4. Server RPC handlers (Step 4)
5. State transfer client (Step 5)
6. FSM.Restore() modification (Step 6)
7. Config chain plumbing (Step 7)
8. Wiring in configure_api.go (Step 8)
9. TrailingLogs change (Step 9)
10. Tests (Step 10)

## Verification

1. `source ~/.bash_profile && cd cluster/shard/proto && PATH=$PATH:~/go/bin buf generate` — proto regeneration
2. `go test ./cluster/shard/ -v` — shard RAFT tests
3. `go test ./adapters/repos/db/ -run TestShard_TransferSnapshot -v` — snapshot tests
4. `CGO_ENABLED=0 go build ./cmd/weaviate-server` — compilation
5. `source ~/.bash_profile && golangci-lint run ./cluster/shard/... ./adapters/repos/db/...`

## Idempotency & Safety Notes

- **Downloaded files may be slightly newer** than `lastAppliedIndex` (leader accepted more writes after snapshot). RAFT replays from `lastAppliedIndex + 1`, re-applying some entries already in files. Safe because all FSM operations are idempotent.
- **HNSW commit logs** in staging dir may include post-snapshot entries (shared inode). Extra entries are handled gracefully by HNSW recovery (stops at first incomplete entry) and RAFT replay re-inserts.
- **hashicorp/raft serializes** `Restore()` and `Apply()` — no concurrent applies during restore.
- **Staging directory cleanup**: `defer ReleaseTransferSnapshot` ensures cleanup even on error. Orphaned staging dirs (from crashes) can be detected by `.transfer-snapshot-*` prefix and cleaned up on shard init.
