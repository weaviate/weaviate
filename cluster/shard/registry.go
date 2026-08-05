//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package shard

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	shardproto "github.com/weaviate/weaviate/cluster/shard/proto"
	"github.com/weaviate/weaviate/cluster/shard/sharedlog"
	"go.etcd.io/raft/v3/raftpb"
)

type addressResolver interface {
	NodeAddress(nodeName string) string
}

// groupRouter lets per-class Raft managers register their Stores in the
// node-wide group table the Registry uses to route inbound raft messages.
type groupRouter interface {
	registerGroup(groupID uint64, s *Store)
	unregisterGroup(groupID uint64)
}

// sharedRaftLogName is the WAL directory holding every shard group's raft
// log (the node-wide segmented shared log).
const sharedRaftLogName = "shard-raft-log"

// legacyBboltLogName is the bbolt file a previous build of this unreleased
// branch stored the shared log in. It is ignored (with a warning) — the WAL
// bootstraps fresh, which is crash-equivalent for raft state.
const legacyBboltLogName = "shard-raft-log.db"

// rpcClientMaker creates a gRPC client to the shard replication service on
// the node identified by nodeID. The closure resolves the nodeID to the
// correct gRPC address internally.
type rpcClientMaker func(ctx context.Context, nodeID string) (shardproto.ShardReplicationServiceClient, error)

// RegistryConfig holds configuration for the global Registry.
type RegistryConfig struct {
	// NodeID is the local node's identifier.
	NodeID string
	// Logger is the logger to use.
	Logger *logrus.Logger
	// AddressResolver resolves node names to addresses.
	AddressResolver addressResolver
	// DataPath is the node data root; the shared raft log and snapshot
	// directory live under it.
	DataPath string
	// RaftPort is the single port used for all shard RAFT traffic (multiplexed).
	RaftPort       int
	RpcClientMaker rpcClientMaker

	// RAFT timing configuration
	HeartbeatTimeout       time.Duration
	ElectionTimeout        time.Duration
	SnapshotThreshold      uint64
	SnapshotBytesThreshold uint64
	// SnapshotMinInterval is the age floor for small groups: retained
	// entries older than this trigger a snapshot even when the entry/byte
	// thresholds never fire. 0 disables the age trigger.
	SnapshotMinInterval time.Duration

	// MaxConcurrentSnapshots bounds the per-node snapshot worker pool.
	MaxConcurrentSnapshots int

	// StateTransferer handles out-of-band state transfer for snapshot restore.
	StateTransferer StateTransferer

	// IsLocalCluster indicates whether the cluster is running on a single host.
	IsLocalCluster bool
	// NodeNameToPortMap maps node names to their shard RAFT ports (for local clusters).
	NodeNameToPortMap map[string]int
}

// Registry manages all per-index Raft instances on a node.
// This is the top-level entry point for RAFT-based replication. It owns the
// node-wide shared raft infrastructure (shared log, snapshot pool, node-ID
// table) and routes inbound raft messages to the owning Store.
type Registry struct {
	config         RegistryConfig
	log            logrus.FieldLogger
	RpcClientMaker rpcClientMaker

	// muxTransport is written by Start and Shutdown (under startMu) but also
	// read by unregisterGroup — which runs on shard unload/drop paths that
	// may race Shutdown and must NOT take startMu (Shutdown holds it while
	// the raft.Shutdown chain calls unregisterGroup) — hence atomic.
	muxTransport atomic.Pointer[MuxTransport]
	sharedLog    *sharedlog.Store
	snapshotter  *Snapshotter
	nodeIDs      *nodeIDMap

	indices sync.Map // key: className -> *Raft
	groups  sync.Map // key: groupID uint64 -> *Store (the message-routing table)

	started bool
	startMu sync.Mutex
}

// NewRegistry creates a new global registry for managing RAFT replication.
func NewRegistry(config RegistryConfig) *Registry {
	return &Registry{
		config: config,
		log: config.Logger.WithFields(logrus.Fields{
			"component": "shard_raft_registry",
		}),
		RpcClientMaker: config.RpcClientMaker,
	}
}

// Start initializes the registry.
func (reg *Registry) Start() error {
	reg.startMu.Lock()
	defer reg.startMu.Unlock()

	if reg.started {
		return nil
	}

	// Get the advertise address from the resolver for the local node
	advertiseAddr := reg.config.AddressResolver.NodeAddress(reg.config.NodeID)
	if advertiseAddr == "" {
		return fmt.Errorf("could not resolve advertise address for local node %s", reg.config.NodeID)
	}

	// Create the multiplexed transport for shard RAFT traffic
	advertiseAddrStr := fmt.Sprintf("%s:%d", advertiseAddr, reg.config.RaftPort)
	tcpAddr, err := net.ResolveTCPAddr("tcp", advertiseAddrStr)
	if err != nil {
		return fmt.Errorf("resolve advertise addr %s: %w", advertiseAddrStr, err)
	}

	bindAddr := fmt.Sprintf("0.0.0.0:%d", reg.config.RaftPort)
	provider := &ShardAddressProvider{
		resolver:          reg.config.AddressResolver,
		raftPort:          reg.config.RaftPort,
		isLocalCluster:    reg.config.IsLocalCluster,
		nodeNameToPortMap: reg.config.NodeNameToPortMap,
	}

	// Node-wide shared raft infrastructure consumed by every Store.
	reg.nodeIDs = newNodeIDMap()

	legacy := filepath.Join(reg.config.DataPath, legacyBboltLogName)
	if fi, statErr := os.Stat(legacy); statErr == nil && !fi.IsDir() {
		reg.log.Warnf("ignoring legacy bbolt shared raft log at %s: superseded by the segmented WAL at %s; delete the file to reclaim disk space",
			legacy, filepath.Join(reg.config.DataPath, sharedRaftLogName))
	}

	reg.sharedLog, err = sharedlog.Open(sharedlog.Options{
		Path:   filepath.Join(reg.config.DataPath, sharedRaftLogName),
		Logger: reg.log,
	})
	if err != nil {
		return fmt.Errorf("open shared raft log: %w", err)
	}

	reg.snapshotter = NewSnapshotter(SnapshotterOptions{
		RootDataPath: reg.config.DataPath,
		Logger:       reg.log,
		Workers:      reg.config.MaxConcurrentSnapshots,
	})

	mt, err := NewMuxTransport(bindAddr, tcpAddr, provider, reg.nodeIDs, reg, reg.log, 0)
	if err != nil {
		_ = reg.snapshotter.Close()
		_ = reg.sharedLog.Close()
		return fmt.Errorf("create mux transport: %w", err)
	}
	reg.muxTransport.Store(mt)

	reg.started = true
	reg.log.WithFields(logrus.Fields{
		"port":      reg.config.RaftPort,
		"advertise": advertiseAddr,
	}).Info("shard RAFT registry started")
	return nil
}

// Shutdown stops all Raft instances managed by this registry.
func (reg *Registry) Shutdown() error {
	reg.startMu.Lock()
	defer reg.startMu.Unlock()

	var lastErr error

	// Stop all index Raft instances
	reg.indices.Range(func(key, value interface{}) bool {
		raft := value.(*Raft)
		if err := raft.Shutdown(); err != nil {
			reg.log.WithError(err).WithField("class", key).Error("error shutting down index raft")
			lastErr = err
		}
		reg.indices.Delete(key)
		return true
	})

	// Close shared infrastructure after all Stores' Ready loops have drained:
	// transport first (no more inbound routing), then the snapshot pool, then
	// the shared log.
	if mt := reg.muxTransport.Swap(nil); mt != nil {
		if err := mt.Close(); err != nil {
			reg.log.Errorf("error closing mux transport: %v", err)
			lastErr = err
		}
	}
	if reg.snapshotter != nil {
		if err := reg.snapshotter.Close(); err != nil {
			reg.log.WithError(err).Error("error closing snapshotter")
			lastErr = err
		}
		reg.snapshotter = nil
	}
	if reg.sharedLog != nil {
		if err := reg.sharedLog.Close(); err != nil {
			reg.log.WithError(err).Error("error closing shared raft log")
			lastErr = err
		}
		reg.sharedLog = nil
	}

	reg.started = false
	reg.log.Info("shard RAFT registry shutdown complete")
	return lastErr
}

// GetOrCreateRaft gets or creates a Raft instance for the specified class/index.
func (reg *Registry) GetOrCreateRaft(className string) (*Raft, error) {
	reg.startMu.Lock()
	if !reg.started {
		reg.startMu.Unlock()
		return nil, fmt.Errorf("shard RAFT registry not started")
	}
	reg.startMu.Unlock()

	// Check if Raft already exists
	if existing, ok := reg.indices.Load(className); ok {
		return existing.(*Raft), nil
	}

	raftConfig := RaftConfig{
		ClassName:              className,
		NodeID:                 reg.config.NodeID,
		Logger:                 reg.config.Logger,
		HeartbeatTimeout:       reg.config.HeartbeatTimeout,
		ElectionTimeout:        reg.config.ElectionTimeout,
		SnapshotThreshold:      reg.config.SnapshotThreshold,
		SnapshotBytesThreshold: reg.config.SnapshotBytesThreshold,
		SnapshotMinInterval:    reg.config.SnapshotMinInterval,
		StateTransferer:        reg.config.StateTransferer,
		MuxTransport:           reg.muxTransport.Load(),
		SharedLog:              reg.sharedLog,
		Snapshotter:            reg.snapshotter,
		NodeIDs:                reg.nodeIDs,
		Resolver:               reg.config.AddressResolver,
		GroupRouter:            reg,
	}

	raft := NewRaft(raftConfig)

	// Start the Raft instance
	if err := raft.Start(); err != nil {
		return nil, fmt.Errorf("start index raft: %w", err)
	}

	// Store the Raft (use LoadOrStore to handle concurrent creation)
	actual, loaded := reg.indices.LoadOrStore(className, raft)
	if loaded {
		// Another goroutine created the Raft first, shut down ours and return that one
		raft.Shutdown()
		return actual.(*Raft), nil
	}

	reg.log.WithField("class", className).Info("created per-index RAFT manager")
	return raft, nil
}

// GetRaft returns an existing Raft for a class, or nil if not found.
func (reg *Registry) GetRaft(className string) *Raft {
	if raft, ok := reg.indices.Load(className); ok {
		return raft.(*Raft)
	}
	return nil
}

// DeleteRaft removes a Raft instance when an index is dropped: every shard
// Store is stopped, unregistered from message routing, and its persisted
// group state purged (shared log + snapshot directory), so a later same-name
// re-creation or catch-up replay cannot resurrect dead groups. Idempotent;
// nil if no Raft exists for the class.
//
// Concurrent GetOrCreateRaft for the same class is serialized upstream by
// the schema apply pipeline (a class delete fully applies before a same-name
// create), so DeleteRaft does not guard against it.
func (reg *Registry) DeleteRaft(className string) error {
	if raft, ok := reg.indices.LoadAndDelete(className); ok {
		return raft.(*Raft).Drop()
	}
	return nil
}

// GetStore retrieves a Store by class and shard name (convenience method).
func (reg *Registry) GetStore(className, shardName string) *Store {
	raft := reg.GetRaft(className)
	if raft == nil {
		return nil
	}
	return raft.GetStore(shardName)
}

// IsLeader checks if this node is leader for a specific shard.
func (reg *Registry) IsLeader(className, shardName string) bool {
	raft := reg.GetRaft(className)
	if raft == nil {
		return false
	}
	return raft.IsLeader(shardName)
}

// VerifyLeaderForRead verifies leader status for linearizable reads.
func (reg *Registry) VerifyLeaderForRead(ctx context.Context, className, shardName string) error {
	raft := reg.GetRaft(className)
	if raft == nil {
		return fmt.Errorf("raft not found for class %s", className)
	}
	return raft.VerifyLeaderForRead(ctx, shardName)
}

// LeaderAddress returns the leader address for a shard.
func (reg *Registry) LeaderAddress(className, shardName string) string {
	raft := reg.GetRaft(className)
	if raft == nil {
		return ""
	}
	return raft.LeaderAddress(shardName)
}

// SetStateTransferer sets the state transferrer for late-binding. This is
// needed because the StateTransfer depends on components (DB, reinitializer)
// that may not be available at Registry creation time.
func (reg *Registry) SetStateTransferer(st StateTransferer) {
	reg.config.StateTransferer = st

	// Also propagate to any already-created Raft instances
	reg.indices.Range(func(key, value interface{}) bool {
		r := value.(*Raft)
		r.config.StateTransferer = st
		// Propagate to existing stores
		r.stores.Range(func(key, value interface{}) bool {
			store := value.(*Store)
			store.SetStateTransferer(st)
			return true
		})
		return true
	})
}

// WaitForShardReady ensures the local replica for a shard has caught up to
// every acknowledged write, so a local read that follows a write does not
// observe stale state. Apply acks at quorum commit, so even the leader's
// local state can lag an acked write — the leader waits for its own apply
// pipeline to cover the committed-staged watermark; a follower asks the
// leader for that watermark and waits for it locally.
func (reg *Registry) WaitForShardReady(ctx context.Context, className, shardName string) error {
	store := reg.GetStore(className, shardName)
	if store == nil {
		return nil // RAFT not configured for this shard
	}

	if store.IsLeader() {
		return store.WaitForAppliedIndex(ctx, store.CommittedIndex())
	}

	leaderID := store.LeaderID()
	if leaderID == "" {
		return nil // no leader yet, let the actual operation handle the error
	}

	client, err := reg.RpcClientMaker(ctx, leaderID)
	if err != nil {
		return fmt.Errorf("create RPC client for leader %s: %w", leaderID, err)
	}

	resp, err := client.GetLastAppliedIndex(ctx, &shardproto.GetLastAppliedIndexRequest{
		Class: className,
		Shard: shardName,
	})
	if err != nil {
		return fmt.Errorf("get leader applied index: %w", err)
	}

	return store.WaitForAppliedIndex(ctx, resp.LastAppliedIndex)
}

// WaitForLinearizableRead performs the ReadIndex protocol with leadership verification.
// Used for STRONG consistency reads. Unlike WaitForShardReady (used in the write path),
// this method requests VerifyLeader=true to guarantee linearizability.
func (reg *Registry) WaitForLinearizableRead(ctx context.Context, className, shardName string) error {
	store := reg.GetStore(className, shardName)
	if store == nil {
		return nil // RAFT not configured for this shard
	}

	if store.IsLeader() {
		return store.VerifyLeader(ctx) // Leader must verify for linearizability
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

// Leader returns the leader node ID for a shard.
func (reg *Registry) Leader(className, shardName string) string {
	raft := reg.GetRaft(className)
	if raft == nil {
		return ""
	}
	return raft.Leader(shardName)
}

// Stats returns statistics about all managed indices and shards.
func (reg *Registry) Stats() map[string]interface{} {
	stats := make(map[string]interface{})
	var indexCount int
	var totalStores int
	var totalLeaders int

	reg.indices.Range(func(key, value interface{}) bool {
		indexCount++
		raft := value.(*Raft)
		raftStats := raft.Stats()
		if stores, ok := raftStats["total_stores"].(int); ok {
			totalStores += stores
		}
		if leaders, ok := raftStats["leader_stores"].(int); ok {
			totalLeaders += leaders
		}
		return true
	})

	stats["total_indices"] = indexCount
	stats["total_stores"] = totalStores
	stats["leader_stores"] = totalLeaders
	if reg.sharedLog != nil {
		// Groups quarantined by WAL boot validation (stores refuse to start;
		// see sharedlog validateGroups / ErrGroupPoisoned).
		stats["poisoned_groups"] = reg.sharedLog.PoisonedGroupCount()
	}
	return stats
}

// RouteMessage delivers an inbound raft message to the Store that owns the
// group. Implements MessageRouter for the MuxTransport. A message for an
// unknown group is dropped silently — that is normal during startup races
// before the Store has registered.
func (reg *Registry) RouteMessage(groupID uint64, msg raftpb.Message) error {
	if v, ok := reg.groups.Load(groupID); ok {
		shardRaftMessages.WithLabelValues("route", msgClass(msg.Type), groupLabel(groupID)).Inc()
		v.(*Store).step(msg)
		return nil
	}
	// Unknown group: normal only in the short window between a peer creating
	// a group and this node registering its store. Persistent drops here mean
	// a ghost group or registration loss — count and say so.
	shardRaftDropped.WithLabelValues(dropSiteRouteUnknownGroup).Inc()
	if routeDropLog.Allow(groupLabel(groupID)) {
		reg.log.WithFields(logrus.Fields{
			"group": groupID,
			"type":  msg.Type.String(),
			"from":  msg.From,
		}).Warn("dropping inbound raft message for unknown group")
	}
	return nil
}

// routeDropLog rate-limits unknown-group WARNs per group ID.
var routeDropLog = newLogLimiter(time.Second)

// registerGroup adds a Store to the node-wide message-routing table.
// A groupID collision between distinct shards is an unrecoverable hash
// collision (see hashGroupID) and panics.
func (reg *Registry) registerGroup(groupID uint64, s *Store) {
	if prev, loaded := reg.groups.LoadOrStore(groupID, s); loaded {
		ps := prev.(*Store)
		if ps != s {
			panic(fmt.Sprintf("shard: group ID collision: %d shared by %s/%s and %s/%s",
				groupID, ps.config.ClassName, ps.config.ShardName,
				s.config.ClassName, s.config.ShardName))
		}
	}
}

// unregisterGroup removes a Store from the message-routing table and retires
// the group's transport bulk stripes (queued frames discarded and counted at
// send_group_removed, writer goroutines and streams reaped). Callers must
// have stopped the Store first — Stop waits for the Ready loop, the group's
// only Send source — or a racing Send re-creates a stripe that then idles
// until transport Close. Safe against a concurrent Registry.Shutdown: the
// transport pointer is read atomically, and removeGroup on a closing
// transport is a no-op (Close discards every lane and reaps every writer).
func (reg *Registry) unregisterGroup(groupID uint64) {
	reg.groups.Delete(groupID)
	if mt := reg.muxTransport.Load(); mt != nil {
		mt.removeGroup(groupID)
	}
}
