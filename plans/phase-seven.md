# RAFT Read Consistency: EVENTUAL / STRONG / DIRECT

## Context

The per-shard RAFT replication PoC (phases 1-6) handles writes through RAFT consensus, but reads are completely RAFT-unaware — they go straight to local LSM storage via the backing 2PC replicator. The old ONE/QUORUM/ALL consistency levels were designed for 2PC and don't map cleanly to RAFT semantics.

This plan introduces three new read consistency levels for RAFT-backed shards:
- **EVENTUAL**: Read from local shard, no consistency check. Fast, possibly stale.
- **STRONG**: Linearizable via ReadIndex protocol. Follower asks leader for current commit index, waits for local FSM to catch up, then reads locally. Distributes read I/O.
- **DIRECT**: Linearizable from leader. If local node is leader, verify + read. If not, transparently forward read to leader. Zero wait, but concentrates load.

Design decisions:
- RAFT shards reject ONE/QUORUM/ALL (error). 2PC shards accept both sets (EVENTUAL→ONE, STRONG→QUORUM, DIRECT→ALL).
- Write consistency level on RAFT shards is silently ignored.
- All read operations (GetOne, Exists, FindUUIDs, Search, VectorSearch) are covered.
- DIRECT reads on non-leader nodes are transparently forwarded server-side.

---

## Phase 1: New Consistency Level Types

Add EVENTUAL/STRONG/DIRECT to the type system across all API surfaces.

### 1a. Internal type constants

**File: `cluster/router/types/consistency_level.go`**

Add new constants and helper methods:
```go
const (
    ConsistencyLevelEventual ConsistencyLevel = "EVENTUAL"
    ConsistencyLevelStrong   ConsistencyLevel = "STRONG"
    ConsistencyLevelDirect   ConsistencyLevel = "DIRECT"
)

func (l ConsistencyLevel) IsRaft() bool {
    return l == ConsistencyLevelEventual || l == ConsistencyLevelStrong || l == ConsistencyLevelDirect
}

func (l ConsistencyLevel) Is2PC() bool {
    return l == ConsistencyLevelOne || l == ConsistencyLevelQuorum || l == ConsistencyLevelAll
}

// MapTo2PC maps RAFT CLs to 2PC equivalents (for 2PC-lenient mode).
func (l ConsistencyLevel) MapTo2PC() ConsistencyLevel {
    switch l {
    case ConsistencyLevelEventual:
        return ConsistencyLevelOne
    case ConsistencyLevelStrong:
        return ConsistencyLevelQuorum
    case ConsistencyLevelDirect:
        return ConsistencyLevelAll
    default:
        return l
    }
}
```

### 1b. gRPC proto enum

**File: `grpc/proto/v1/base.proto`**

```protobuf
enum ConsistencyLevel {
  CONSISTENCY_LEVEL_UNSPECIFIED = 0;
  CONSISTENCY_LEVEL_ONE = 1;
  CONSISTENCY_LEVEL_QUORUM = 2;
  CONSISTENCY_LEVEL_ALL = 3;
  CONSISTENCY_LEVEL_EVENTUAL = 4;
  CONSISTENCY_LEVEL_STRONG = 5;
  CONSISTENCY_LEVEL_DIRECT = 6;
}
```

Then run `make grpc` to regenerate.

### 1c. REST validation

**File: `adapters/handlers/rest/handlers_objects.go`** — `getConsistencyLevel()` (line 910)

Add EVENTUAL/STRONG/DIRECT to the switch statement:
```go
case types.ConsistencyLevelEventual, types.ConsistencyLevelStrong, types.ConsistencyLevelDirect:
    return *lvl, nil
```

Update the error message to include new values.

### 1d. gRPC extraction

**File: `adapters/handlers/grpc/v1/service.go`** — `extractReplicationProperties()` (line 377)

Add cases:
```go
case pb.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL:
    return &additional.ReplicationProperties{ConsistencyLevel: "EVENTUAL"}
case pb.ConsistencyLevel_CONSISTENCY_LEVEL_STRONG:
    return &additional.ReplicationProperties{ConsistencyLevel: "STRONG"}
case pb.ConsistencyLevel_CONSISTENCY_LEVEL_DIRECT:
    return &additional.ReplicationProperties{ConsistencyLevel: "DIRECT"}
```

Same for `adapters/handlers/grpc/v1/batch/handler.go` (line 159).

### 1e. GraphQL enum

**File: `adapters/handlers/graphql/local/get/replication.go`** — `consistencyLevelArgument()` (line 27)

Add enum values for EVENTUAL, STRONG, DIRECT.

### 1f. Description update

**File: `adapters/handlers/graphql/descriptions/get.go`** (line 47)

Update `ConsistencyLevel` description string to mention both sets.

---

## Phase 2: ReadIndex RPC

Add a lightweight RPC for the ReadIndex protocol: follower asks leader "what is your current commit index?"

### 2a. Proto messages

**File: `cluster/shard/proto/messages.proto`**

```protobuf
message ReadIndexRequest {
  string class = 1;
  string shard = 2;
}

message ReadIndexResponse {
  uint64 commit_index = 1;
}
```

Add to service:
```protobuf
service ShardReplicationService {
  // ... existing RPCs ...
  rpc ReadIndex(ReadIndexRequest) returns (ReadIndexResponse) {}
}
```

Then run `cd cluster/shard/proto && PATH=$PATH:~/go/bin buf generate`.

### 2b. Store methods

**File: `cluster/shard/store.go`**

```go
// ReadIndex returns the current commit index for linearizable reads.
// Must be called on the leader. Verifies leadership before returning.
func (s *Store) ReadIndex() (uint64, error) {
    s.mu.RLock()
    if !s.started || s.closed || s.raft == nil {
        s.mu.RUnlock()
        return 0, ErrNotStarted
    }
    r := s.raft
    s.mu.RUnlock()

    // Verify we're still the leader
    if err := r.VerifyLeader().Error(); err != nil {
        return 0, ErrNotLeader
    }

    // On a verified leader, LastAppliedIndex equals the commit index
    // because Apply() waits for FSM.Apply() before returning.
    return s.fsm.LastAppliedIndex(), nil
}

// WaitForAppliedIndex blocks until the local FSM has applied at least
// up to the given index, or the context is cancelled.
func (s *Store) WaitForAppliedIndex(ctx context.Context, index uint64) error {
    for {
        if s.fsm.LastAppliedIndex() >= index {
            return nil
        }
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-time.After(time.Millisecond):
        }
    }
}
```

### 2c. Server handler

**File: `cluster/shard/server.go`**

```go
func (s *Server) ReadIndex(ctx context.Context, req *shardproto.ReadIndexRequest) (*shardproto.ReadIndexResponse, error) {
    store := s.registry.GetStore(req.Class, req.Shard)
    if store == nil {
        return nil, status.Errorf(codes.NotFound, "store not found for %s/%s", req.Class, req.Shard)
    }
    idx, err := store.ReadIndex()
    if err != nil {
        return nil, toRPCError(err)
    }
    return &shardproto.ReadIndexResponse{CommitIndex: idx}, nil
}
```

### 2d. Registry convenience methods

**File: `cluster/shard/registry.go`**

```go
func (reg *Registry) ReadIndex(className, shardName string) (uint64, error) {
    store := reg.GetStore(className, shardName)
    if store == nil {
        return 0, fmt.Errorf("store not found for %s/%s", className, shardName)
    }
    return store.ReadIndex()
}

func (reg *Registry) WaitForAppliedIndex(ctx context.Context, className, shardName string, index uint64) error {
    store := reg.GetStore(className, shardName)
    if store == nil {
        return fmt.Errorf("store not found for %s/%s", className, shardName)
    }
    return store.WaitForAppliedIndex(ctx, index)
}

func (reg *Registry) LeaderID(className, shardName string) string {
    store := reg.GetStore(className, shardName)
    if store == nil {
        return ""
    }
    return store.LeaderID()
}
```

---

## Phase 3: Local Shard Reader Interface

The RAFT replicator needs to read from the local shard directly (for EVENTUAL and STRONG reads) without going through the 2PC Finder's network stack.

### 3a. Define shardReader interface

**File: `cluster/shard/replicator.go`** (new interface)

```go
// shardReader provides read access to a local shard.
// Implemented by adapters/repos/db.Shard.
type shardReader interface {
    ObjectByID(ctx context.Context, id strfmt.UUID, props search.SelectProperties, adds additional.Properties) (*storobj.Object, error)
    Exists(ctx context.Context, id strfmt.UUID) (bool, error)
    FindUUIDs(ctx context.Context, filters *filters.LocalFilter) ([]strfmt.UUID, error)
}

// shardReaderProvider resolves a shard name to a local shardReader.
// Returns the reader and a release function. Returns nil reader if the
// shard is not locally available.
type shardReaderProvider func(shardName string) (shardReader, func(), error)
```

### 3b. Add to RouterConfig

**File: `cluster/shard/replicator.go`**

```go
type RouterConfig struct {
    // ... existing fields ...
    // LocalShardReader resolves a shard name to a local reader for direct reads.
    LocalShardReader shardReaderProvider
}
```

### 3c. Wire in Index

**File: `adapters/repos/db/index.go`** — where the RAFT replicator is created

Provide a closure that returns the local shard:
```go
LocalShardReader: func(shardName string) (shard.ShardReader, func(), error) {
    s, release, err := i.getShardForDirectLocalOperation(ctx, tenant, shardName, localShardOperationRead)
    if err != nil || s == nil {
        return nil, nil, err
    }
    return s, release, nil  // Shard already implements ObjectByID, Exists
}
```

Note: `adapters/repos/db.Shard` already has `ObjectByID()`, `Exists()` methods in `shard_read.go`. It satisfies the `shardReader` interface.

### 3d. Update ShardRaftReplicator interface

**File: `adapters/repos/db/shard_raft_replicator.go`**

Add ReadIndex and WaitForAppliedIndex methods:
```go
type ShardRaftReplicator interface {
    // ... existing write methods ...

    // ReadIndex returns the leader's current commit index for the specified shard.
    ReadIndex(ctx context.Context, className, shardName string) (uint64, error)

    // WaitForAppliedIndex waits for the local FSM to apply up to the given index.
    WaitForAppliedIndex(ctx context.Context, className, shardName string, index uint64) error

    // LeaderID returns the leader's node ID for the specified shard.
    LeaderID(className, shardName string) string
}
```

---

## Phase 4: RAFT-Aware Read Methods

Override the read methods on the RAFT replicator to handle EVENTUAL/STRONG/DIRECT.

### 4a. Core read routing logic

**File: `cluster/shard/replicator.go`** — new methods

```go
// GetOne overrides the backing replicator for RAFT-backed shards.
func (r *replicator) GetOne(ctx context.Context, l routerTypes.ConsistencyLevel, shard string, id strfmt.UUID, props search.SelectProperties, adds additional.Properties) (*storobj.Object, error) {
    // If not a RAFT CL or no RAFT store, delegate to backing replicator
    if !l.IsRaft() || r.raft.GetStore(shard) == nil {
        cl := l
        if l.IsRaft() {
            cl = l.MapTo2PC() // RAFT CL on non-RAFT shard: map to 2PC
        }
        return r.Replicator.GetOne(ctx, cl, shard, id, props, adds)
    }

    // Validate: reject 2PC CLs on RAFT shards
    if l.Is2PC() {
        return nil, fmt.Errorf("consistency level %s is not supported for RAFT-backed shards, use EVENTUAL/STRONG/DIRECT", l)
    }

    switch l {
    case routerTypes.ConsistencyLevelEventual:
        return r.readLocalObject(ctx, shard, id, props, adds)

    case routerTypes.ConsistencyLevelStrong:
        if err := r.ensureReadIndex(ctx, shard); err != nil {
            return nil, fmt.Errorf("strong read: %w", err)
        }
        return r.readLocalObject(ctx, shard, id, props, adds)

    case routerTypes.ConsistencyLevelDirect:
        return r.readFromLeader(ctx, shard, id, props, adds)
    }

    return nil, fmt.Errorf("unsupported consistency level: %s", l)
}
```

Same pattern for `Exists()` and `FindUUIDs()`.

### 4b. Helper methods

**File: `cluster/shard/replicator.go`**

```go
// readLocalObject reads from the local shard via shardReaderProvider.
func (r *replicator) readLocalObject(ctx context.Context, shard string, id strfmt.UUID, props search.SelectProperties, adds additional.Properties) (*storobj.Object, error) {
    reader, release, err := r.config.LocalShardReader(shard)
    if err != nil {
        return nil, fmt.Errorf("get local shard reader: %w", err)
    }
    if reader == nil {
        return nil, fmt.Errorf("shard %s not available locally", shard)
    }
    defer release()
    return reader.ObjectByID(ctx, id, props, adds)
}

// ensureReadIndex performs the ReadIndex protocol:
// 1. If leader: VerifyLeader (no RPC needed)
// 2. If follower: call ReadIndex RPC to leader, wait for local apply
func (r *replicator) ensureReadIndex(ctx context.Context, shardName string) error {
    store := r.raft.GetStore(shardName)
    if store == nil {
        return fmt.Errorf("raft store not found for shard %s", shardName)
    }

    // If we're the leader, just verify leadership
    if store.IsLeader() {
        return store.VerifyLeader()
    }

    // Follower: ask leader for commit index via RPC
    leaderID := store.LeaderID()
    if leaderID == "" {
        return ErrNoLeaderFound
    }

    client, err := r.rpcClientMaker(ctx, leaderID)
    if err != nil {
        return fmt.Errorf("create RPC client for leader %s: %w", leaderID, err)
    }

    resp, err := client.ReadIndex(ctx, &shardproto.ReadIndexRequest{
        Class: r.class,
        Shard: shardName,
    })
    if err != nil {
        return fmt.Errorf("read index from leader: %w", err)
    }

    // Wait for local FSM to catch up
    return store.WaitForAppliedIndex(ctx, resp.CommitIndex)
}

// readFromLeader reads from the leader for DIRECT consistency.
// If this node is the leader, reads locally after verifying leadership.
// If not, forwards the read to the leader node via the backing replicator.
func (r *replicator) readFromLeader(ctx context.Context, shard string, id strfmt.UUID, props search.SelectProperties, adds additional.Properties) (*storobj.Object, error) {
    store := r.raft.GetStore(shard)
    if store == nil {
        return nil, fmt.Errorf("raft store not found for shard %s", shard)
    }

    if store.IsLeader() {
        if err := store.VerifyLeader(); err != nil {
            return nil, fmt.Errorf("verify leader: %w", err)
        }
        return r.readLocalObject(ctx, shard, id, props, adds)
    }

    // Forward to leader using the backing replicator's NodeObject
    leaderID := store.LeaderID()
    if leaderID == "" {
        return nil, ErrNoLeaderFound
    }
    return r.Replicator.NodeObject(ctx, leaderID, shard, id, props, adds)
}
```

### 4c. CheckConsistency override

**File: `cluster/shard/replicator.go`**

For RAFT CLs, consistency is ensured at read time (ReadIndex or leader read), so skip the post-read digest check:
```go
func (r *replicator) CheckConsistency(ctx context.Context, l routerTypes.ConsistencyLevel, objs []*storobj.Object) error {
    if l.IsRaft() {
        return nil // Consistency already ensured by ReadIndex/VerifyLeader
    }
    return r.Replicator.CheckConsistency(ctx, l, objs)
}
```

---

## Phase 5: Search Integration

Search operations don't go through `GetOne()` — they read from shards directly and then call `CheckConsistency()`. We need to add a RAFT-aware "prepare" step before search reads.

### 5a. PrepareRead method on replicator

**File: `cluster/shard/replicator.go`**

```go
// PrepareRead prepares a shard for a consistent read under RAFT CLs.
// For EVENTUAL: no-op, returns local node.
// For STRONG: performs ReadIndex protocol, returns local node.
// For DIRECT: verifies leadership or returns leader node name.
// Returns the node name to read from.
func (r *replicator) PrepareRead(ctx context.Context, shardName string, cl routerTypes.ConsistencyLevel) (readFromNode string, err error) {
    if !cl.IsRaft() || r.raft.GetStore(shardName) == nil {
        return r.config.NodeID, nil // Not RAFT, proceed normally
    }

    switch cl {
    case routerTypes.ConsistencyLevelEventual:
        return r.config.NodeID, nil

    case routerTypes.ConsistencyLevelStrong:
        if err := r.ensureReadIndex(ctx, shardName); err != nil {
            return "", err
        }
        return r.config.NodeID, nil

    case routerTypes.ConsistencyLevelDirect:
        store := r.raft.GetStore(shardName)
        if store.IsLeader() {
            if err := store.VerifyLeader(); err != nil {
                return "", err
            }
            return r.config.NodeID, nil
        }
        leaderID := store.LeaderID()
        if leaderID == "" {
            return "", ErrNoLeaderFound
        }
        return leaderID, nil // Caller should forward search to this node
    }

    return r.config.NodeID, nil
}
```

### 5b. Add PrepareRead to Replicator interface

**File: `cluster/shard/replicator.go`** — Replicator interface

```go
type Replicator interface {
    // ... existing methods ...
    PrepareRead(ctx context.Context, shardName string, cl routerTypes.ConsistencyLevel) (readFromNode string, err error)
}
```

The backing 2PC replicator must also implement this (as a no-op returning local node).

### 5c. Index search path integration

**File: `adapters/repos/db/index.go`** — `objectSearch()` and `objectVectorSearch()`

Before searching each shard, call `PrepareRead`:
```go
readNode, err := i.replicator.PrepareRead(ctx, shardName, cl)
if err != nil {
    return nil, err
}
if readNode == localNodeName {
    // Read from local shard (existing path)
} else {
    // Forward search to readNode (for DIRECT on non-leader)
    // Use i.remote.SearchShard(ctx, readNode, ...) or similar
}
```

The existing `CheckConsistency` call after search will be a no-op for RAFT CLs (Phase 4c).

---

## Phase 6: CL Validation at the Index Layer

Reject 2PC CLs on RAFT-backed shards early.

### 6a. Validation helper

**File: `adapters/repos/db/index.go`**

```go
func (i *Index) validateConsistencyLevel(cl routerTypes.ConsistencyLevel, shardName string) error {
    isRaftShard := i.shardRaftReplicator != nil && i.shardRaftReplicator.IsLeader(i.Config.ClassName.String(), shardName)
    // Better: check if RAFT store exists (regardless of leadership)
    // Need a HasStore(className, shardName) bool method on the registry

    if isRaftShard && cl.Is2PC() {
        return fmt.Errorf("consistency level %s is not supported for RAFT-backed shards; use EVENTUAL, STRONG, or DIRECT", cl)
    }
    return nil
}
```

### 6b. Write CL handling

In the write path (`preparePutObject`, etc.), when a RAFT-backed shard receives a write with any CL, silently ignore the CL and proceed through RAFT consensus. No code changes needed — the existing RAFT replicator already ignores the `l routerTypes.ConsistencyLevel` parameter.

---

## Phase 7: Testing

### 7a. Unit tests

- **`cluster/router/types/consistency_level_test.go`**: Test `IsRaft()`, `Is2PC()`, `MapTo2PC()`.
- **`cluster/shard/store_test.go`**: Test `ReadIndex()`, `WaitForAppliedIndex()` with test RAFT clusters.
- **`cluster/shard/replicator_test.go`**: Test `GetOne`, `Exists`, `FindUUIDs` routing for all 6 CLs on RAFT and non-RAFT shards. Mock `shardReaderProvider` and RPC client.

### 7b. Integration tests

- **`cluster/shard/`**: Multi-node integration test: write via RAFT, read with EVENTUAL (might be stale), STRONG (linearizable), DIRECT (from leader).
- Verify that STRONG reads on a follower see writes that completed before the read.
- Verify that EVENTUAL reads may not see very recent writes.

### 7c. Acceptance tests

- **`test/acceptance/`**: End-to-end test with 3-node cluster, RAFT-backed class:
  - Write object, STRONG read from follower → sees it.
  - Write object, DIRECT read → sees it.
  - Write object, EVENTUAL read → may or may not see it (non-deterministic, just verify no error).

---

## Files Modified (Summary)

| File | Change |
|------|--------|
| `cluster/router/types/consistency_level.go` | Add EVENTUAL/STRONG/DIRECT constants, IsRaft(), Is2PC(), MapTo2PC() |
| `cluster/shard/proto/messages.proto` | Add ReadIndexRequest, ReadIndexResponse, ReadIndex RPC |
| `cluster/shard/store.go` | Add ReadIndex(), WaitForAppliedIndex() |
| `cluster/shard/server.go` | Add ReadIndex handler |
| `cluster/shard/registry.go` | Add ReadIndex(), WaitForAppliedIndex(), LeaderID() convenience methods |
| `cluster/shard/replicator.go` | Add shardReader interface, shardReaderProvider, override GetOne/Exists/FindUUIDs/CheckConsistency, add PrepareRead, ensureReadIndex, readLocalObject, readFromLeader |
| `adapters/repos/db/shard_raft_replicator.go` | Add ReadIndex, WaitForAppliedIndex, LeaderID to interface |
| `adapters/repos/db/index.go` | Wire LocalShardReader into RouterConfig, call PrepareRead in search paths, add CL validation |
| `grpc/proto/v1/base.proto` | Add EVENTUAL/STRONG/DIRECT to ConsistencyLevel enum |
| `adapters/handlers/rest/handlers_objects.go` | Accept new CLs in getConsistencyLevel() |
| `adapters/handlers/grpc/v1/service.go` | Add extraction cases for new CLs |
| `adapters/handlers/grpc/v1/batch/handler.go` | Add extraction cases for new CLs |
| `adapters/handlers/graphql/local/get/replication.go` | Add new CL enum values |
| `adapters/handlers/graphql/descriptions/get.go` | Update description string |

## Verification

1. `go build ./...` — ensure compilation
2. `go test ./cluster/shard/...` — RAFT unit tests
3. `go test ./cluster/router/types/...` — CL type tests
4. `go test -tags integrationTest ./cluster/shard/...` — multi-node integration
5. `go test -tags integrationTest ./test/acceptance/...` — end-to-end acceptance
6. `source ~/.bash_profile && golangci-lint run` — linting
