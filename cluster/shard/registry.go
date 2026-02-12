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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	shardproto "github.com/weaviate/weaviate/cluster/shard/proto"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type addressResolver interface {
	NodeAddress(nodeName string) string
}

type rpcClientMaker func(ctx context.Context, address string) (shardproto.ShardReplicationServiceClient, error)

// RegistryConfig holds configuration for the global Registry.
type RegistryConfig struct {
	// NodeID is the local node's identifier.
	NodeID string
	// Logger is the logger to use.
	Logger *logrus.Logger
	// AddressResolver resolves node names to addresses.
	AddressResolver addressResolver
	// RaftPort is the single port used for all shard RAFT traffic (multiplexed).
	RaftPort int
	// ApplyTimeout is the timeout for RAFT Apply operations.
	ApplyTimeout time.Duration
	// MaxMsgSize is the maximum message size for gRPC calls.
	MaxMsgSize     int
	RpcClientMaker rpcClientMaker

	// RAFT timing configuration
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	LeaderLeaseTimeout time.Duration
	SnapshotInterval   time.Duration
	SnapshotThreshold  uint64
	TrailingLogs       uint64

	// StateTransferer handles out-of-band state transfer for snapshot restore.
	StateTransferer StateTransferer
}

// Registry manages all per-index Raft instances on a node.
// This is the top-level entry point for RAFT-based replication.
type Registry struct {
	config         RegistryConfig
	log            logrus.FieldLogger
	RpcClientMaker rpcClientMaker

	indices sync.Map // key: className -> *Raft

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
		ClassName:          className,
		NodeID:             reg.config.NodeID,
		Logger:             reg.config.Logger,
		ApplyTimeout:       reg.config.ApplyTimeout,
		HeartbeatTimeout:   reg.config.HeartbeatTimeout,
		ElectionTimeout:    reg.config.ElectionTimeout,
		LeaderLeaseTimeout: reg.config.LeaderLeaseTimeout,
		SnapshotInterval:   reg.config.SnapshotInterval,
		SnapshotThreshold:  reg.config.SnapshotThreshold,
		TrailingLogs:       reg.config.TrailingLogs,
		StateTransferer:    reg.config.StateTransferer,
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

// DeleteRaft removes a Raft instance when an index is dropped.
func (reg *Registry) DeleteRaft(className string) error {
	if raft, ok := reg.indices.LoadAndDelete(className); ok {
		return raft.(*Raft).Shutdown()
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
	return stats
}

func (reg *Registry) Execute(ctx context.Context, req *shardproto.ApplyRequest) (uint64, error) {
	t := prometheus.NewTimer(
		monitoring.GetMetrics().SchemaWrites.WithLabelValues(
			req.Type.String(),
		))
	defer t.ObserveDuration()

	var schemaVersion uint64
	err := backoff.Retry(func() error {
		var err error
		store := reg.GetStore(req.Class, req.Shard)
		if store == nil {
			err = fmt.Errorf("raft store not found for shard %s/%s", req.Class, req.Shard)
			reg.log.Warnf("apply: %s", err)
			return backoff.Permanent(err)
		}

		// Validate the apply first
		if _, ok := shardproto.ApplyRequest_Type_name[int32(req.Type.Number())]; !ok {
			err = types.ErrUnknownCommand
			// This is an invalid apply command, don't retry
			return backoff.Permanent(err)
		}

		// We are the leader, let's apply
		if store.IsLeader() {
			schemaVersion, err = store.Apply(ctx, req)
			// We might fail due to leader not found as we are losing or transferring leadership, retry
			if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
				return err
			}
			return backoff.Permanent(err)
		}

		leader := store.Leader()
		if leader == "" {
			err = reg.leaderErr(req.Class, req.Shard)
			reg.log.Warnf("apply: could not find leader: %s", err)
			return err
		}

		client, err := reg.RpcClientMaker(ctx, leader)
		if err != nil {
			err = fmt.Errorf("create RPC client for leader %s: %w", leader, err)
			reg.log.Warnf("apply: %s", err)
			return backoff.Permanent(err)
		}

		var resp *shardproto.ApplyResponse
		resp, err = client.Apply(ctx, req)
		if err != nil {
			// Don't retry if the actual apply to the leader failed, we have retry at the network layer already
			return backoff.Permanent(err)
		}
		schemaVersion = resp.Version
		return nil
		// pass in the election timeout after applying multiplier
	}, backoffConfig(ctx, reg.config.ElectionTimeout))

	return schemaVersion, err
}

// leaderErr decorates ErrLeaderNotFound by distinguishing between
// normal election happening and there is no leader been chosen yet
// and if it can't reach the other nodes either for intercluster
// communication issues or other nodes were down.
func (reg *Registry) leaderErr(class, shard string) error {
	// store := reg.GetStore(class, shard)
	// if store != nil && store.raftResolver != nil && len(store.raftResolver.NotResolvedNodes()) > 0 {
	// 	var nodes []string
	// 	for n := range store.raftResolver.NotResolvedNodes() {
	// 		nodes = append(nodes, string(n))
	// 	}

	// 	return fmt.Errorf("%w, can not resolve nodes [%s]", types.ErrLeaderNotFound, strings.Join(nodes, ","))
	// }
	return types.ErrLeaderNotFound
}
