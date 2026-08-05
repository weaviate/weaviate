# Plan: Wait for Leader Applied Index Before Local Reads

## Context

When a Create (PutObject) is immediately followed by an Update (UpdateObject/MergeObject), the update request may land on a RAFT follower that hasn't replicated the create yet. Both `UpdateObject` (update.go:75) and `MergeObject` (merge.go:79) read the object **locally** before writing through RAFT, so they fail with "not found" when the follower is behind. This causes flakiness in the "create and update object" acceptance test.

**Root cause:** The usecases layer reads from local shard storage before replicating the write through RAFT, with no guarantee the local replica has caught up to the leader.

**Why not refactor to defer reads into the RAFT FSM instead?**

We explored whether the local read could be eliminated entirely by deferring all logic into the RAFT FSM Apply. This is not feasible for three fundamental reasons:

1. **Vectorization is non-deterministic.** Vectorizer modules (text2vec-openai, etc.) make HTTP calls to external APIs. RAFT FSM Apply must be deterministic — all nodes must produce identical results from the same
log entry. Running vectorization inside FSM would violate this guarantee.
2. **Vectorization depends on the previous object**. The reVectorizeEmbeddings() comparison (modules/compare.go) fetches the old object to check if properties changed, avoiding expensive re-vectorization when
unnecessary. This optimization requires the previous object state before the RAFT write.
3. **Property merging for MergeObject is tightly coupled to vectorization**. The usecases layer merges prev+new properties, vectorizes the merged result, then only includes changed vectors in the MergeDocument sent
through RAFT. Moving this into FSM would either lose the optimization or require passing the entire previous object through the RAFT log.

The same pattern applies to reference operations (DeleteReference, UpdateReference) which are read-modify-write at the property level. A broader refactor would require architectural changes at both usecases and
shard layers — not justified for this fix.

**Solution:** Before performing a local read in UpdateObject/MergeObject, query the RAFT leader for its current applied index, then wait until the local FSM catches up to that index before reading.

---

## Implementation Steps

### Step 1: Add channel-based index notification to FSM
**File:** `cluster/shard/fsm.go`

- Add `indexMu sync.Mutex` and `indexCond *sync.Cond` fields to `FSM` struct
- Initialize `indexCond = sync.NewCond(&f.indexMu)` in `NewFSM()`
- In `Apply()`, after `f.lastAppliedIndex.Store(l.Index)`, call `f.indexCond.Broadcast()`
- Add `WaitForIndex(ctx context.Context, targetIndex uint64) error`:
  - Fast path: if `lastAppliedIndex >= targetIndex`, return nil
  - Spawn a context-cancellation goroutine that calls `Broadcast()` on ctx.Done
  - Lock indexMu, loop `Wait()` until `lastAppliedIndex >= targetIndex` or ctx cancelled
  - Clean up the waker goroutine on success via a `stopWaker` channel

### Step 2: Expose WaitForAppliedIndex on Store
**File:** `cluster/shard/store.go`

- Add `WaitForAppliedIndex(ctx context.Context, targetIndex uint64) error`
- Delegates to `s.fsm.WaitForIndex(ctx, targetIndex)`

### Step 3: Add GetLastAppliedIndex gRPC RPC
**File:** `cluster/shard/proto/messages.proto`

Add to `ShardReplicationService`:
```protobuf
rpc GetLastAppliedIndex(GetLastAppliedIndexRequest) returns (GetLastAppliedIndexResponse) {}
```

Add messages:
```protobuf
message GetLastAppliedIndexRequest {
  string class = 1;
  string shard = 2;
}

message GetLastAppliedIndexResponse {
  uint64 last_applied_index = 1;
}
```

Then regenerate proto code: `cd cluster/shard/proto && PATH=$PATH:~/go/bin buf generate`

### Step 4: Implement GetLastAppliedIndex in gRPC Server
**File:** `cluster/shard/server.go`

```go
func (s *Server) GetLastAppliedIndex(ctx context.Context, req *shardproto.GetLastAppliedIndexRequest) (*shardproto.GetLastAppliedIndexResponse, error) {
    store := s.registry.GetStore(req.Class, req.Shard)
    if store == nil {
        return nil, status.Errorf(codes.NotFound, "store not found for %s/%s", req.Class, req.Shard)
    }
    return &shardproto.GetLastAppliedIndexResponse{
        LastAppliedIndex: store.LastAppliedIndex(),
    }, nil
}
```

### Step 5: Add WaitForShardReady on Registry
**File:** `cluster/shard/registry.go`

Add method to `Registry`:
```go
func (reg *Registry) WaitForShardReady(ctx context.Context, className, shardName string) error
```

Flow:
1. `GetStore(className, shardName)` → if nil, return nil
2. If `store.IsLeader()`, return nil (leader is always caught up)
3. `store.LeaderID()` → if empty, return nil (no leader yet)
4. `reg.RpcClientMaker(ctx, leaderID)` → create gRPC client to leader
5. `client.GetLastAppliedIndex(ctx, &req{Class, Shard})` → get leader's applied index
6. `store.WaitForAppliedIndex(ctx, resp.LastAppliedIndex)` → wait locally

### Step 6: Add EnsureReplicaCaughtUp to DB/Index layer
**File:** `adapters/repos/db/index.go`

Add to Index:
```go
func (i *Index) ensureReplicaCaughtUp(ctx context.Context, id strfmt.UUID, tenant string) error {
    if i.Config.ShardRegistry == nil {
        return nil  // RAFT not enabled
    }
    shardName, err := i.shardResolver.ResolveShardByObjectID(ctx, id, tenant)
    if err != nil {
        return nil  // let the actual read handle shard resolution errors
    }
    return i.Config.ShardRegistry.WaitForShardReady(ctx, i.Config.ClassName.String(), shardName)
}
```

**File:** `adapters/repos/db/crud.go`

Add to DB:
```go
func (db *DB) EnsureReplicaCaughtUp(ctx context.Context, class string, id strfmt.UUID, tenant string) error {
    idx := db.GetIndex(schema.ClassName(class))
    if idx == nil {
        return nil
    }
    return idx.ensureReplicaCaughtUp(ctx, id, tenant)
}
```

### Step 7: Add EnsureReplicaCaughtUp to VectorRepo interface
**File:** `usecases/objects/manager.go`

Add to `VectorRepo` interface:
```go
EnsureReplicaCaughtUp(ctx context.Context, class string, id strfmt.UUID, tenant string) error
```

### Step 8: Call EnsureReplicaCaughtUp in UpdateObject and MergeObject

**File:** `usecases/objects/update.go` (line ~75, before `getObjectFromRepo`)
```go
if err := m.vectorRepo.EnsureReplicaCaughtUp(ctx, className, id, updates.Tenant); err != nil {
    return nil, fmt.Errorf("ensure replica caught up: %w", err)
}
```

**File:** `usecases/objects/merge.go` (line ~78, replacing the TODO comment before `m.vectorRepo.Object`)
```go
if err := m.vectorRepo.EnsureReplicaCaughtUp(ctx, cls, id, updates.Tenant); err != nil {
    return &Error{"ensure replica caught up", StatusInternalServerError, err}
}
```

### Step 9: Update test mocks/fakes
Add no-op `EnsureReplicaCaughtUp` to any mocks/fakes implementing VectorRepo (grep for existing fakes).

### Step 10: Add unit tests

**File:** `cluster/shard/fsm_test.go` — Test WaitForIndex:
- Test fast path (already caught up)
- Test wait path (apply arrives after wait starts)
- Test context cancellation

**File:** `cluster/shard/store_test.go` — Test WaitForAppliedIndex end-to-end

---

## Files Modified (summary)

| File | Change |
|------|--------|
| `cluster/shard/fsm.go` | Add sync.Cond, WaitForIndex() |
| `cluster/shard/store.go` | Add WaitForAppliedIndex() |
| `cluster/shard/proto/messages.proto` | Add GetLastAppliedIndex RPC + messages |
| `cluster/shard/proto/` (generated) | Regenerate proto code |
| `cluster/shard/server.go` | Implement GetLastAppliedIndex |
| `cluster/shard/registry.go` | Add WaitForShardReady() |
| `adapters/repos/db/index.go` | Add ensureReplicaCaughtUp() |
| `adapters/repos/db/crud.go` | Add EnsureReplicaCaughtUp() |
| `usecases/objects/manager.go` | Add to VectorRepo interface |
| `usecases/objects/update.go` | Call EnsureReplicaCaughtUp before read |
| `usecases/objects/merge.go` | Call EnsureReplicaCaughtUp before read (replace TODO) |
| Various test fakes | Add no-op EnsureReplicaCaughtUp |

---

## Verification

1. **Unit tests:** `go test ./cluster/shard/... -run TestWaitForIndex`
2. **Acceptance test:** `go test -v -count=1 -tags integrationTest ./test/acceptance/replication/shard/ -run Test_RaftShardReplication/create_and_update_object`
3. Run the full shard RAFT test suite to ensure no regressions
