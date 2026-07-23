# RAFT Read Consistency: EVENTUAL / STRONG / DIRECT

## Context

The per-shard RAFT replication PoC (phases 1-6) handles writes through RAFT consensus, but reads are completely RAFT-unaware — they go straight to local LSM storage via the backing 2PC replicator. The old ONE/QUORUM/ALL consistency levels were designed for 2PC and don't map cleanly to RAFT semantics.

Since the original plan was written, the strong-consistency-on-writes (SC-on-writes) work landed. It introduced `EnsureReplicaCaughtUp` to prevent dirty reads during read-modify-write operations (UPDATE, MERGE, reference ops) and built significant ReadIndex-protocol infrastructure that this plan now reuses instead of duplicating.

This plan introduces three new read consistency levels for RAFT-backed shards:
- **EVENTUAL**: Read from local shard, no consistency check. Fast, possibly stale.
- **STRONG**: Linearizable via ReadIndex protocol. Follower asks leader for current commit index (with leadership verification), waits for local FSM to catch up, then reads locally. Distributes read I/O.
- **DIRECT**: Linearizable from leader. If local node is leader, verify + read. If not, transparently forward read to leader. Zero wait, but concentrates load.

Design decisions:
- RAFT shards reject ONE/QUORUM/ALL (error). 2PC shards accept both sets (EVENTUAL→ONE, STRONG→QUORUM, DIRECT→ALL).
- Write consistency level on RAFT shards is silently ignored.
- All read operations (GetOne, Exists, FindUUIDs, Search, VectorSearch) are covered.
- DIRECT reads on non-leader nodes are transparently forwarded server-side.
- Default CL for RAFT-backed shards is EVENTUAL (matches current fast-local-read behaviour).

### Existing infrastructure reused (from SC-on-writes)

| Component | File | What it does |
|-----------|------|------------|
| `GetLastAppliedIndex` RPC | `cluster/shard/server.go:196` | Returns leader's applied index |
| `GetLastAppliedIndex` proto | `cluster/shard/proto/messages.proto:187-195` | Request/Response messages |
| `Store.WaitForAppliedIndex()` | `cluster/shard/store.go:466` | Blocks until local FSM catches up |
| `FSM.WaitForIndex()` | `cluster/shard/fsm.go:437-470` | Condition-variable-based wait with context cancellation |
| `Registry.WaitForShardReady()` | `cluster/shard/registry.go:298-327` | Full ReadIndex flow: query leader → wait locally |
| `VerifyLeaderForRead()` | `cluster/shard/replicator.go:465` | Leadership verification on the replicator |

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

// MapTo2PC maps RAFT CLs to 2PC equivalents (for non-RAFT shards receiving RAFT CLs).
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

Also update `ToInt()` to add explicit cases for RAFT CLs (all map to 1) for clarity, even though the default case already returns 1.

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

Same for `adapters/handlers/grpc/v1/batch/handler.go` — `extractReplicationProperties()` (line 159).

### 1e. GraphQL enum

**File: `adapters/handlers/graphql/local/get/replication.go`** — `consistencyLevelArgument()` (line 27)

Add enum values for EVENTUAL, STRONG, DIRECT.

### 1f. Description update

**File: `adapters/handlers/graphql/descriptions/get.go`** (line 47)

Update `ConsistencyLevel` description string to mention both sets.

---

## Phase 2: Linearizable ReadIndex via GetLastAppliedIndex

The existing `GetLastAppliedIndex` RPC (built for SC-on-writes) already implements the core ReadIndex flow. The only gap is that the server handler does **not** call `VerifyLeader()` before returning the index. For true linearizability (STRONG reads), the leader must confirm it's still leader before reporting its commit index — without this, a stale leader could return an outdated index.

We cannot unconditionally add `VerifyLeader()` because the existing write-path (`EnsureReplicaCaughtUp` → `WaitForShardReady`) also calls this RPC. If the leader steps down in the window after committing a write but before responding, the write-path would fail unnecessarily — it just needs the index, not a leadership guarantee.

### 2a. Proto field addition

**File: `cluster/shard/proto/messages.proto`**

Add a `verify_leader` field to the existing request:
```protobuf
message GetLastAppliedIndexRequest {
  string class = 1;
  string shard = 2;
  bool verify_leader = 3;  // If true, verify leadership before returning (for linearizable reads)
}
```

Then run `cd cluster/shard/proto && PATH=$PATH:~/go/bin buf generate`.

### 2b. Server handler update

**File: `cluster/shard/server.go`** — `GetLastAppliedIndex` handler (line 196)

Conditionally verify leadership:
```go
func (s *Server) GetLastAppliedIndex(ctx context.Context, req *shardproto.GetLastAppliedIndexRequest) (*shardproto.GetLastAppliedIndexResponse, error) {
    store := s.registry.GetStore(req.Class, req.Shard)
    if store == nil {
        return nil, status.Errorf(codes.NotFound, "store not found for %s/%s", req.Class, req.Shard)
    }
    if req.VerifyLeader {
        if err := store.VerifyLeader(); err != nil {
            return nil, toRPCError(err)
        }
    }
    return &shardproto.GetLastAppliedIndexResponse{
        LastAppliedIndex: store.LastAppliedIndex(),
    }, nil
}
```

Existing write-path callers (`WaitForShardReady`) don't set `VerifyLeader` (defaults to false), so they are unaffected.

### 2c. Registry linearizable read method

**File: `cluster/shard/registry.go`**

Add a new method alongside the existing `WaitForShardReady`:
```go
// WaitForLinearizableRead performs the ReadIndex protocol with leadership verification.
// Used for STRONG consistency reads. Unlike WaitForShardReady (used in the write path),
// this method requests VerifyLeader=true to guarantee linearizability.
func (reg *Registry) WaitForLinearizableRead(ctx context.Context, className, shardName string) error {
    store := reg.GetStore(className, shardName)
    if store == nil {
        return nil // RAFT not configured for this shard
    }

    if store.IsLeader() {
        return store.VerifyLeader() // Leader must verify for linearizability
    }

    leaderID := store.LeaderID()
    if leaderID == "" {
        return ErrNoLeaderFound
    }

    client, err := reg.RpcClientMaker(ctx, leaderID)
    if err != nil {
        return fmt.Errorf("create RPC client for leader %s: %w", leaderID, err)
    }

    resp, err := client.GetLastAppliedIndex(ctx, &shardproto.GetLastAppliedIndexRequest{
        Class:        className,
        Shard:        shardName,
        VerifyLeader: true,
    })
    if err != nil {
        return fmt.Errorf("get leader applied index: %w", err)
    }

    return store.WaitForAppliedIndex(ctx, resp.LastAppliedIndex)
}
```

**No new RPCs, no new Store methods needed.** One new proto field and one new Registry method.

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
    // Registry is the shard RAFT registry (for ReadIndex protocol).
    Registry *Registry
}
```

### 3c. Wire in Index

**File: `adapters/repos/db/index.go`** — where the RAFT replicator is created (~line 400)

Provide a closure that returns the local shard:
```go
LocalShardReader: func(shardName string) (shard.ShardReader, func(), error) {
    s, release, err := i.getShardForDirectLocalOperation(ctx, tenant, shardName, localShardOperationRead)
    if err != nil || s == nil {
        return nil, nil, err
    }
    return s, release, nil  // Shard already implements ObjectByID, Exists
},
Registry: cfg.ShardRegistry,
```

Note: `adapters/repos/db.Shard` already has `ObjectByID()`, `Exists()` methods in `shard_read.go`. It satisfies the `shardReader` interface.

---

## Phase 4: RAFT-Aware Read Methods

Override the read methods on the RAFT replicator to handle EVENTUAL/STRONG/DIRECT.

### 4a. Core read routing logic

**File: `cluster/shard/replicator.go`** — new methods

**IMPORTANT**: The original plan had a logic bug in the routing condition. Using `!l.IsRaft() || r.raft.GetStore(shard) == nil` as the first check means 2PC CLs (where `!l.IsRaft()` is true) on RAFT shards silently pass through to the backing replicator instead of being rejected. The fix is to check shard type first, then CL type:

```go
// GetOne overrides the backing replicator for RAFT-backed shards.
func (r *replicator) GetOne(ctx context.Context, l routerTypes.ConsistencyLevel, shard string, id strfmt.UUID, props search.SelectProperties, adds additional.Properties) (*storobj.Object, error) {
    isRaftShard := r.raft.GetStore(shard) != nil

    // Non-RAFT shard: delegate to backing replicator (mapping RAFT CLs to 2PC equivalents)
    if !isRaftShard {
        cl := l
        if l.IsRaft() {
            cl = l.MapTo2PC()
        }
        return r.Replicator.GetOne(ctx, cl, shard, id, props, adds)
    }

    // RAFT shard: reject 2PC CLs
    if l.Is2PC() {
        return nil, fmt.Errorf("consistency level %s is not supported for RAFT-backed shards; use EVENTUAL, STRONG, or DIRECT", l)
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

// ensureReadIndex performs the ReadIndex protocol for STRONG consistency.
// Delegates to Registry.WaitForLinearizableRead which handles:
// - Leader: VerifyLeader (no RPC needed)
// - Follower: GetLastAppliedIndex RPC with VerifyLeader=true → wait for local FSM
func (r *replicator) ensureReadIndex(ctx context.Context, shardName string) error {
    return r.config.Registry.WaitForLinearizableRead(ctx, r.config.ClassName, shardName)
}

// readFromLeader reads from the leader for DIRECT consistency.
// If this node is the leader, reads locally after verifying leadership.
// If not, forwards the read to the leader node via the backing replicator's NodeObject.
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

Search operations don't go through `GetOne()` — they read from shards directly via `objectSearchByShard` using a `ReadRoutingPlan`, then call `CheckConsistency()`. We need to add a RAFT-aware "prepare" step before search reads.

The search path uses `buildReadRoutingPlan()` → `ReadRoutingPlan` → `IntConsistencyLevel` (from `ToInt()`). `ToInt()` only knows ONE/QUORUM/ALL; RAFT CLs all map to 1 (the default). For RAFT shards, replica-counting logic doesn't apply — consistency is ensured by the ReadIndex protocol (STRONG) or leadership verification (DIRECT), not by querying N replicas.

### 5a. PrepareRead method on replicator

**File: `cluster/shard/replicator.go`**

```go
// PrepareRead prepares a shard for a consistent read under RAFT CLs.
// For EVENTUAL: no-op, returns local node.
// For STRONG: performs ReadIndex protocol (via WaitForLinearizableRead), returns local node.
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

**File: `adapters/repos/db/index.go`** — `objectSearchByShard()` (line 1773)

In the `localSearch` closure, before calling `shard.ObjectSearch()`, call `PrepareRead`:
```go
readNode, err := i.replicator.PrepareRead(ctx, shardName, cl)
if err != nil {
    return err
}
if readNode == localNodeName {
    // Read from local shard (existing path)
} else {
    // Forward search to readNode (for DIRECT on non-leader)
    // Use remoteSearch targeting the leader node via i.remote.SearchShard
}
```

The existing `CheckConsistency` call after search will be a no-op for RAFT CLs (Phase 4c).

---

## Phase 6: Default CL for RAFT Shards

**File: `adapters/repos/db/index.go`**

The current `defaultConsistency()` (line 3200) returns `QUORUM`. Since RAFT shards reject 2PC CLs, any read path that doesn't explicitly set a CL (e.g. `objectByID` at line 1451) will fail on RAFT shards. We need a RAFT-aware default.

Convert `defaultConsistency` from a package-level function to an `Index` method:
```go
func (i *Index) defaultConsistency(defaultOverride ...routerTypes.ConsistencyLevel) *additional.ReplicationProperties {
    rp := &additional.ReplicationProperties{}
    if len(defaultOverride) != 0 {
        rp.ConsistencyLevel = string(defaultOverride[0])
    } else if i.isRaftBacked() {
        rp.ConsistencyLevel = string(routerTypes.ConsistencyLevelEventual)
    } else {
        rp.ConsistencyLevel = string(routerTypes.ConsistencyLevelQuorum)
    }
    return rp
}

func (i *Index) isRaftBacked() bool {
    return i.Config.RaftReplicationEnabled && i.Config.ShardRegistry != nil
}
```

---

## Phase 7: CL Validation at the Index Layer

Reject 2PC CLs on RAFT-backed shards early. This is a defence-in-depth measure — Phase 4 already rejects at the replicator level.

### 7a. Validation helper

**File: `adapters/repos/db/index.go`**

```go
func (i *Index) validateConsistencyLevel(cl routerTypes.ConsistencyLevel) error {
    if i.isRaftBacked() && cl.Is2PC() {
        return fmt.Errorf("consistency level %s is not supported for RAFT-backed shards; use EVENTUAL, STRONG, or DIRECT", cl)
    }
    return nil
}
```

### 7b. Write CL handling

In the write path (`preparePutObject`, etc.), when a RAFT-backed shard receives a write with any CL, silently ignore the CL and proceed through RAFT consensus. No code changes needed — the existing RAFT replicator already ignores the `l routerTypes.ConsistencyLevel` parameter.

---

## Phase 8: Testing

### 8a. Unit tests

- **`cluster/router/types/consistency_level_test.go`**: Test `IsRaft()`, `Is2PC()`, `MapTo2PC()`, `ToInt()` for all 6 CLs.
- **`cluster/shard/replicator_test.go`**: Test `GetOne`, `Exists`, `FindUUIDs` routing for all 6 CLs on RAFT and non-RAFT shards. Mock `shardReaderProvider` and RPC client.

### 8b. Integration tests

- **`cluster/shard/`**: Multi-node integration test: write via RAFT, read with EVENTUAL (might be stale), STRONG (linearizable), DIRECT (from leader).
- Verify that STRONG reads on a follower see writes that completed before the read.
- Verify that EVENTUAL reads may not see very recent writes.

### 8c. Acceptance tests

- **`test/acceptance/`**: End-to-end test with 3-node cluster, RAFT-backed class:
  - Write object, STRONG read from follower → sees it.
  - Write object, DIRECT read → sees it.
  - Write object, EVENTUAL read → may or may not see it (non-deterministic, just verify no error).

---

## Files Modified (Summary)

| File | Change |
|------|--------|
| `cluster/router/types/consistency_level.go` | Add EVENTUAL/STRONG/DIRECT constants, IsRaft(), Is2PC(), MapTo2PC(), update ToInt() |
| `cluster/shard/proto/messages.proto` | Add `verify_leader` field to GetLastAppliedIndexRequest → regenerate |
| `cluster/shard/server.go` | Conditionally VerifyLeader() in GetLastAppliedIndex handler |
| `cluster/shard/registry.go` | Add `WaitForLinearizableRead()` method |
| `cluster/shard/replicator.go` | Add shardReader interface, shardReaderProvider, LocalShardReader + Registry in RouterConfig, override GetOne/Exists/FindUUIDs/CheckConsistency, add PrepareRead, ensureReadIndex, readLocalObject, readFromLeader |
| `adapters/repos/db/index.go` | Wire LocalShardReader + Registry into RouterConfig, call PrepareRead in search paths, add CL validation, RAFT-aware defaultConsistency() |
| `grpc/proto/v1/base.proto` | Add EVENTUAL/STRONG/DIRECT to ConsistencyLevel enum → `make grpc` |
| `adapters/handlers/rest/handlers_objects.go` | Accept new CLs in getConsistencyLevel() |
| `adapters/handlers/grpc/v1/service.go` | Add extraction cases for new CLs |
| `adapters/handlers/grpc/v1/batch/handler.go` | Add extraction cases for new CLs |
| `adapters/handlers/graphql/local/get/replication.go` | Add new CL enum values |
| `adapters/handlers/graphql/descriptions/get.go` | Update description string |

## Verification

1. `go build ./...` — ensure compilation
2. `go test ./cluster/router/types/...` — CL type tests
3. `go test ./cluster/shard/...` — RAFT replicator + registry read tests
4. `go test ./adapters/repos/db/...` — index layer tests
5. `golangci-lint run` — linting
