# RAFT-Based Object Replication Refactoring Plan

## Goal
Refactor `cluster/data/` to establish a clear hierarchy:
- **Store** (one per shard) - Handles RAFT ring lifecycle for a single shard
- **Raft** (one per index) - Manages all shard Stores within an index
- **Registry** (global) - Manages all per-index Raft instances
- **Router** - Routes through per-index Raft to shard Stores
- **Server** - Handles gRPC requests via Registry

## Current State

| File | Struct | Role |
|------|--------|------|
| `store.go` | `Store` | Global manager for ALL shard RAFT clusters (sync.Map: "class_shard" -> `*Raft`) |
| `raft.go` | `Raft` | Single shard's RAFT cluster (HashiCorp RAFT, FSM, BoltDB) |
| `router.go` | `Router` | Routes via global `Store`, one Router per index |
| `server.go` | `Server` | gRPC server using global `Store` |

## Target State

| File | Struct | Role |
|------|--------|------|
| `store.go` | `Store` | **One per SHARD** - RAFT ring lifecycle (renamed from current `Raft`) |
| `raft.go` | `Raft` | **One per INDEX** - Manages shard Stores (NEW) |
| `registry.go` | `Registry` | **Global** - Manages per-index Rafts (NEW, replaces current `Store` concept) |
| `router.go` | `Router` | Routes via per-index `Raft` |
| `server.go` | `Server` | gRPC server using `Registry` |

---

## Implementation Steps

### Phase 1: Create New Store (Rename from Raft)

**File: `cluster/data/store.go`** - Replace contents

1. Copy all content from current `raft.go`
2. Rename:
   - `Raft` -> `Store`
   - `RaftConfig` -> `StoreConfig`
   - `NewRaft` -> `NewStore`
3. Update internal log component name to `"shard_raft_store"`
4. Keep all methods unchanged: `SetShard`, `Start`, `Stop`, `Apply`, `IsLeader`, `Leader`, `LeaderID`, `VerifyLeader`, `State`, `LastAppliedIndex`

### Phase 2: Create New Raft (Per-Index Manager)

**File: `cluster/data/raft.go`** - Replace contents with new per-index manager

```go
// RaftConfig - configuration for per-index RAFT manager
type RaftConfig struct {
    ClassName          string
    NodeID             string
    Logger             *logrus.Logger
    AddressResolver    addressResolver
    RaftPort           int
    ApplyTimeout       time.Duration
    HeartbeatTimeout   time.Duration
    ElectionTimeout    time.Duration
    LeaderLeaseTimeout time.Duration
    SnapshotInterval   time.Duration
    SnapshotThreshold  uint64
}

// Raft - manages all shard Stores for a single index
type Raft struct {
    config  RaftConfig
    log     logrus.FieldLogger
    stores  sync.Map  // shardName -> *Store
    started bool
    startMu sync.Mutex
}
```

**Key Methods:**
- `NewRaft(config RaftConfig) *Raft`
- `Start() error`
- `Shutdown() error`
- `GetOrCreateStore(ctx, shardName, members, dataPath) (*Store, error)`
- `GetStore(shardName) *Store`
- `StopStore(shardName) error`
- `OnShardCreated(ctx, shardName, members, dataPath, shard) error`
- `OnShardDeleted(shardName) error`
- `IsLeader(shardName) bool`
- `VerifyLeaderForRead(ctx, shardName) error`
- `LeaderAddress(shardName) string`
- `Leader(shardName) string`

### Phase 3: Create Registry (Global Manager)

**File: `cluster/data/registry.go`** - NEW file

```go
// RegistryConfig - configuration for global registry
type RegistryConfig struct {
    NodeID             string
    Logger             *logrus.Logger
    AddressResolver    addressResolver
    RaftPort           int
    ApplyTimeout       time.Duration
    MaxMsgSize         int
    RpcClientMaker     rpcClientMaker
    HeartbeatTimeout   time.Duration
    ElectionTimeout    time.Duration
    LeaderLeaseTimeout time.Duration
    SnapshotInterval   time.Duration
    SnapshotThreshold  uint64
}

// Registry - manages all per-index Raft instances
type Registry struct {
    config  RegistryConfig
    log     logrus.FieldLogger
    indices sync.Map  // className -> *Raft
    started bool
    startMu sync.Mutex
}
```

**Key Methods:**
- `NewRegistry(config RegistryConfig) *Registry`
- `Start() error`
- `Shutdown() error`
- `GetOrCreateRaft(className) (*Raft, error)`
- `GetRaft(className) *Raft`
- `DeleteRaft(className) error`
- `GetStore(className, shardName) *Store` (convenience)
- `IsLeader(className, shardName) bool`
- `VerifyLeaderForRead(ctx, className, shardName) error`
- `LeaderAddress(className, shardName) string`
- `Leader(className, shardName) string`
- `Stats() map[string]interface{}`

### Phase 4: Update Router

**File: `cluster/data/router.go`**

1. Change `RouterConfig.Store *Store` to `RouterConfig.Raft *Raft`
2. Update `Router` struct field `store *Store` to `raft *Raft`
3. Simplify method signatures (no className needed - Raft knows its class):
   - `IsLeader(shardName) bool` (was `IsLeader(className, shardName)`)
   - `VerifyLeaderForRead(ctx, shardName)` (was with className)
   - `LeaderAddress(shardName)` (was with className)
4. Update `PutObject` to use `r.raft.GetStore(shard)` instead of `r.store.GetCluster(r.class, shard)`
5. Update `forwardToLeader` signature: `raft *Raft` -> `store *Store`

### Phase 5: Update gRPC Server

**File: `cluster/data/server.go`**

1. Change `Server.store *Store` to `Server.registry *Registry`
2. Update `NewShardRPCServer(store *Store, ...)` to `NewShardRPCServer(registry *Registry, ...)`
3. Update `Apply` method:
   ```go
   func (s *Server) Apply(ctx context.Context, req *shardproto.ApplyRequest) (*shardproto.ApplyResponse, error) {
       store := s.registry.GetStore(req.Class, req.Shard)
       if store == nil {
           return nil, status.Error(codes.NotFound, fmt.Sprintf("store not found for %s/%s", req.Class, req.Shard))
       }
       return store.Apply(ctx, req)
   }
   ```

### Phase 6: Update Integration Points

**File: `adapters/handlers/rest/state/state.go`**
- Change `ShardRaftStore *data.Store` to `ShardRaftRegistry *data.Registry`

**File: `adapters/handlers/rest/configure_api.go`**
- Update initialization to create `Registry` instead of `Store`:
  ```go
  appState.ShardRaftRegistry = data.NewRegistry(data.RegistryConfig{...})
  ```

**File: `adapters/handlers/rest/clusterapi/grpc/server.go`**
- Update gRPC server registration:
  ```go
  if state.ShardRaftRegistry != nil {
      shardRPCServer := data.NewShardRPCServer(state.ShardRaftRegistry, state.Logger)
      shardproto.RegisterShardReplicationServiceServer(s, shardRPCServer)
  }
  ```

**File: `adapters/handlers/rest/clusterapi/serve.go`**
- Update startup/shutdown to use `ShardRaftRegistry.Start()` / `ShardRaftRegistry.Shutdown()`

**File: `adapters/repos/db/repo.go`**
- Change `Config.ShardRaftStore` to `Config.ShardRaftRegistry`

**File: `adapters/repos/db/index.go`**
- Add `raft *data.Raft` field to `Index` struct
- Update `NewIndex` to get/create per-index Raft:
  ```go
  if cfg.RaftReplicationEnabled && cfg.ShardRaftRegistry != nil {
      indexRaft, err := cfg.ShardRaftRegistry.GetOrCreateRaft(cfg.ClassName.String())
      index.raft = indexRaft
      index.replicator = data.NewRouter(data.RouterConfig{
          Raft: indexRaft,  // Changed from Store
          ...
      })
  }
  ```

**File: `adapters/repos/db/shard_init.go`** (lines 177-204)
- Change `index.Config.ShardRaftStore.OnShardCreated(...)` to `index.raft.OnShardCreated(...)`
- Remove className parameter (Raft knows its class)

**File: `adapters/repos/db/shard_shutdown.go`**
- Change `index.Config.ShardRaftStore.OnShardDeleted(...)` to `index.raft.OnShardDeleted(...)`
- Remove className parameter

---

## Files to Modify

| File | Change Type |
|------|-------------|
| `cluster/data/store.go` | **REPLACE** - Rename from raft.go content |
| `cluster/data/raft.go` | **REPLACE** - New per-index manager |
| `cluster/data/registry.go` | **CREATE** - New global registry |
| `cluster/data/router.go` | **MODIFY** - Use per-index Raft |
| `cluster/data/server.go` | **MODIFY** - Use Registry |
| `cluster/data/fsm.go` | No changes |
| `cluster/data/client.go` | No changes |
| `cluster/data/replicator.go` | No changes |
| `adapters/handlers/rest/state/state.go` | **MODIFY** - ShardRaftRegistry |
| `adapters/handlers/rest/configure_api.go` | **MODIFY** - Registry init |
| `adapters/handlers/rest/clusterapi/grpc/server.go` | **MODIFY** - Registry ref |
| `adapters/handlers/rest/clusterapi/serve.go` | **MODIFY** - Registry lifecycle |
| `adapters/repos/db/repo.go` | **MODIFY** - Config field |
| `adapters/repos/db/index.go` | **MODIFY** - Add raft field, update NewIndex |
| `adapters/repos/db/init.go` | **MODIFY** - Pass registry in config |
| `adapters/repos/db/shard_init.go` | **MODIFY** - Use index.raft |
| `adapters/repos/db/shard_shutdown.go` | **MODIFY** - Use index.raft |
| `cluster/data/store_test.go` | **MODIFY** - Update for new hierarchy |

---

## Verification

1. **Unit Tests**: Run `go test ./cluster/data/...`
2. **Build**: Ensure `go build ./...` succeeds
3. **Integration**: Test with `docker-compose-raft.yml` 3-node cluster:
   - Create class with RAFT replication enabled
   - Insert objects and verify consensus
   - Kill leader and verify failover
   - Verify gRPC forwarding works

---

## Request Flow After Refactoring

```
Client PutObject Request
        ↓
Index.replicator.PutObject()  [Router]
        ↓
Router.raft.GetStore(shard)   [Per-index Raft]
        ↓
    Is Leader? ───────────────┐
        │ YES                 │ NO
        ↓                     ↓
Store.Apply()           Forward to Leader
   [RAFT consensus]           ↓
        ↓              ShardRPCClient.ForwardPutObject()
FSM.Apply()                   ↓
        ↓              Server.Apply() [gRPC]
Shard.PutObject()             ↓
                       Registry.GetStore(class, shard)
                              ↓
                       Store.Apply() [Leader]
```
