# Plan: Multiplexed Transport for Per-Shard RAFT

## Context

`cluster/shard/store.go:238` calls `raft.NewRaft()` with a nil `s.transport`, causing a panic. Transport was intentionally deferred (TODO at line 237). The core challenge: each shard needs its own `raft.Transport` with its own consumer channel, but we can't have a separate TCP connection per shard since there could be millions. We need multiplexed transport where node pairs share a bounded number of connections supporting N parallel shard RAFT clusters.

**Solution**: Use **hashicorp/yamux** for stream multiplexing. One TCP listener per node, yamux sessions to peers (2 TCP connections per peer pair — one per direction), per-shard virtual `StreamLayer` that routes via shard key headers. Each shard gets its own `raft.NetworkTransport` wrapping its virtual stream layer, reusing all of hashicorp/raft's proven wire protocol, pipelining, and heartbeat handling.

---

## Architecture

```
MuxTransport (per-node, owned by Registry)
  ├── TCP Listener (single port: raftCfg.Port + 1)
  ├── Session Pool: map[peerAddr]*yamux.Session (outbound, one per peer)
  ├── Accept Loop: incoming TCP → yamux.Server → dispatch streams by shard key
  └── ShardStreamLayer registry: map[shardKey]*ShardStreamLayer

Per Shard:
  ShardStreamLayer (implements raft.StreamLayer)
    ├── Accept(): reads from dispatch channel (populated by MuxTransport)
    ├── Dial(): opens yamux stream → writes shard key header → returns stream
    └── Wraps in: raft.NetworkTransport (standard hashicorp/raft transport)
         └── Used by Store as raft.Transport
```

**Wire protocol per yamux stream:**
```
[2 bytes: shard key length, uint16 big-endian]
[N bytes: shard key string ("className/shardName")]
[... standard hashicorp/raft NetworkTransport msgpack protocol ...]
```

**Connection model**: At most 2 TCP connections per peer pair (one per direction). All shard RAFT clusters multiplex over these via yamux streams.

---

## Implementation Steps

### Step 1: Add yamux dependency

```bash
go get github.com/hashicorp/yamux
```

### Step 2: Create `cluster/shard/transport.go`

This is the core new file containing three types:

#### `ShardAddressProvider`

Implements `raft.ServerAddressProvider`. Resolves node ID → `host:shardRaftPort` using the existing `addressResolver` interface. Pattern mirrors `cluster/resolver/raft.go:60-84`.

```go
type ShardAddressProvider struct {
    resolver addressResolver
    raftPort int
}

func (p *ShardAddressProvider) ServerAddr(id raft.ServerID) (raft.ServerAddress, error) {
    addr := p.resolver.NodeAddress(string(id))
    if addr == "" {
        return "", fmt.Errorf("could not resolve node %s", id)
    }
    return raft.ServerAddress(fmt.Sprintf("%s:%d", addr, p.raftPort)), nil
}
```

#### `MuxTransport`

Per-node singleton managing the shared TCP listener and yamux session pool.

```go
type MuxTransport struct {
    listener    net.Listener
    advertise   net.Addr
    addrProvider *ShardAddressProvider
    logger      logrus.FieldLogger

    sessions   map[string]*yamux.Session  // peerAddr → outbound session
    sessionsMu sync.RWMutex

    shardLayers   map[string]*ShardStreamLayer  // shardKey → layer
    shardLayersMu sync.RWMutex

    shutdownCh chan struct{}
}
```

Key methods:
- `NewMuxTransport(bindAddr string, advertise net.Addr, provider *ShardAddressProvider, logger) (*MuxTransport, error)` — binds TCP listener, starts `acceptLoop` goroutine
- `acceptLoop()` — accepts TCP connections, wraps each in `yamux.Server(conn, cfg)`, starts `handleSession` goroutine per session
- `handleSession(session)` — accepts yamux streams from session, reads shard key header via `readShardKeyHeader()`, dispatches stream to matching `ShardStreamLayer.incomingCh`. If shard key not found (race during startup), logs warning and closes stream.
- `getOrDialSession(address raft.ServerAddress) (*yamux.Session, error)` — returns existing outbound yamux session or dials new TCP connection + `yamux.Client(conn, cfg)`. Checks `session.IsClosed()` to handle stale sessions from peer restarts.
- `CreateShardTransport(className, shardName string, logger) (raft.Transport, error)` — creates `ShardStreamLayer`, registers it, wraps in `raft.NewNetworkTransportWithConfig()` with `ShardAddressProvider` and `maxPool=3`
- `DestroyShardTransport(className, shardName string)` — unregisters `ShardStreamLayer`, closes it
- `Close() error` — closes listener, all sessions, signals shutdown

#### `ShardStreamLayer`

Per-shard virtual stream layer implementing `raft.StreamLayer` (`net.Listener` + `Dial`).

```go
type ShardStreamLayer struct {
    shardKey   string
    mux        *MuxTransport
    advertise  net.Addr
    incomingCh chan net.Conn  // buffered (64), populated by MuxTransport.handleSession
    closeCh    chan struct{}
}
```

Methods:
- `Accept() (net.Conn, error)` — blocks on `incomingCh`, returns `ErrTransportShutdown` when `closeCh` closed
- `Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error)` — calls `mux.getOrDialSession(address)`, opens yamux stream via `session.Open()`, writes shard key header via `writeShardKeyHeader()`, returns stream as `net.Conn`
- `Close() error` — closes `closeCh`, drains `incomingCh`
- `Addr() net.Addr` — returns advertise address (used by `NetworkTransport.LocalAddr()`)

#### Helper functions

```go
func writeShardKeyHeader(w io.Writer, shardKey string) error  // uint16 BE length + bytes
func readShardKeyHeader(r io.Reader) (string, error)          // inverse
func shardKey(className, shardName string) string             // "className/shardName"
```

### Step 3: Modify `cluster/shard/registry.go`

- Add `muxTransport *MuxTransport` field to `Registry` struct (line 66-75)
- In `Start()` (lines 89-109): after resolving advertise address, create `MuxTransport`:
  ```go
  advertiseAddr := fmt.Sprintf("%s:%d", advertiseAddr, reg.config.RaftPort)
  tcpAddr, _ := net.ResolveTCPAddr("tcp", advertiseAddr)
  bindAddr := fmt.Sprintf("0.0.0.0:%d", reg.config.RaftPort)
  provider := &ShardAddressProvider{resolver: reg.config.AddressResolver, raftPort: reg.config.RaftPort}
  reg.muxTransport, err = NewMuxTransport(bindAddr, tcpAddr, provider, reg.log)
  ```
- In `Shutdown()` (lines 112-132): after stopping all Raft instances, call `reg.muxTransport.Close()`
- In `GetOrCreateRaft()` (lines 148-160): add `MuxTransport: reg.muxTransport` to `RaftConfig`

### Step 4: Modify `cluster/shard/raft.go`

- Add `MuxTransport *MuxTransport` field to `RaftConfig` (lines 24-44)
- In `GetOrCreateStore()` (lines 127-141): after building `StoreConfig`, create shard transport:
  ```go
  transport, err := r.config.MuxTransport.CreateShardTransport(r.config.ClassName, shardName, r.config.Logger)
  storeConfig.Transport = transport
  ```
- In `Shutdown()` (lines 85-105): after stopping each store, call `r.config.MuxTransport.DestroyShardTransport(r.config.ClassName, shardName)`

### Step 5: Modify `cluster/shard/store.go`

- Remove the `// TODO: init transport properly` comment at line 237
- No other changes — `Store` already receives and uses `Transport` from `StoreConfig`

### Step 6: Create `cluster/shard/transport_test.go`

Unit and integration tests:

1. **TestShardKeyHeader_RoundTrip** — write + read shard key header, verify correctness
2. **TestMuxTransport_SingleShard_ThreeNodes** — 3 MuxTransport instances on different ports, one shard each, form RAFT cluster, verify leader election
3. **TestMuxTransport_MultipleShards_SharedConnections** — 2 MuxTransport instances, 10 shards each, verify all 10 RAFT clusters elect leaders (validates multiplexing)
4. **TestMuxTransport_SessionReconnect** — close a yamux session, verify next Dial creates new session
5. **TestShardAddressProvider_Resolution** — verify node ID → host:port resolution

### Step 7: Verify no regressions

- `go test ./cluster/shard/...` — existing tests use `InmemTransport` directly via `StoreConfig`, bypassing `MuxTransport` entirely
- `go test -race ./cluster/shard/...` — race detection

---

## Files Modified

| File | Change |
|------|--------|
| `cluster/shard/transport.go` | **NEW** — MuxTransport, ShardStreamLayer, ShardAddressProvider, helpers |
| `cluster/shard/transport_test.go` | **NEW** — transport unit + integration tests |
| `cluster/shard/registry.go` | Add `muxTransport` field, create in `Start()`, close in `Shutdown()`, pass to `RaftConfig` |
| `cluster/shard/raft.go` | Add `MuxTransport` to `RaftConfig`, call `CreateShardTransport` in `GetOrCreateStore()`, destroy in `Shutdown()` |
| `cluster/shard/store.go` | Remove TODO comment (line 237) |
| `go.mod` / `go.sum` | Add `github.com/hashicorp/yamux` |

## Existing Code Reused

- `raft.NewNetworkTransportWithConfig()` — wraps our `ShardStreamLayer`, provides all RAFT wire protocol, pooling, pipelining
- `raft.NetworkTransportConfig.ServerAddressProvider` — pluggable address resolution (same pattern as `cluster/resolver/raft.go`)
- `addressResolver` interface (registry.go:30-32) — already available, resolves node names to addresses
- `cluster/log.NewHCLogrusLogger()` — existing logrus → hclog adapter
- `raft.NewInmemTransport()` — tests continue using this unchanged

## Verification

1. **Unit tests**: `go test -v -race ./cluster/shard/... -run TestMuxTransport`
2. **Existing tests**: `go test -v -race ./cluster/shard/...` (no regressions)
3. **Acceptance test**: Start 3-node cluster via `docker-compose-raft.yml`, run `go test ./test/acceptance/replication/shard/...`
   - Ensure `docker-compose-raft.yml` exposes shard RAFT port (schema RAFT port + 1)
4. **Build**: `go build ./cmd/weaviate-server/` (compile check)

## Edge Cases Handled

- **Stale yamux sessions** (peer restart): `getOrDialSession` checks `session.IsClosed()`, re-dials if stale
- **Unknown shard key on accept** (startup race): log warning, close stream — remote RAFT retries automatically
- **Full incomingCh buffer**: use select with default to avoid blocking `handleSession`, close stream if buffer full
- **Graceful shutdown ordering**: Stop all Raft/Store instances first (closes NetworkTransports → ShardStreamLayers), then close MuxTransport (listener + sessions)
- **Concurrent store creation**: `GetOrCreateStore` already uses `LoadOrStore` for thread safety; `RegisterShardLayer` uses mutex
