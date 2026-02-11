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
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// RaftConfig holds configuration for a per-index RAFT manager.
type RaftConfig struct {
	// ClassName is the name of the class/index this Raft manages.
	ClassName string
	// NodeID is the local node's identifier.
	NodeID string
	// Logger is the logger to use.
	Logger *logrus.Logger
	// ApplyTimeout is the timeout for RAFT Apply operations.
	ApplyTimeout time.Duration

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

// Raft manages all per-shard RAFT clusters (Stores) for a single index.
// Each index has one Raft instance that contains multiple Store instances,
// one for each shard in the index.
type Raft struct {
	config RaftConfig
	log    logrus.FieldLogger

	stores sync.Map // key: shardName -> *Store

	started bool
	startMu sync.Mutex
}

// NewRaft creates a new per-index RAFT manager.
func NewRaft(config RaftConfig) *Raft {
	return &Raft{
		config: config,
		log: config.Logger.WithFields(logrus.Fields{
			"component": "index_raft_manager",
			"class":     config.ClassName,
		}),
	}
}

// Start initializes the per-index RAFT manager.
func (r *Raft) Start() error {
	r.startMu.Lock()
	defer r.startMu.Unlock()

	if r.started {
		return nil
	}

	r.started = true
	r.log.Info("per-index RAFT manager started")
	return nil
}

// Shutdown stops all Store instances managed by this Raft.
func (r *Raft) Shutdown() error {
	r.startMu.Lock()
	defer r.startMu.Unlock()

	var lastErr error

	// Stop all shard stores
	r.stores.Range(func(key, value interface{}) bool {
		store := value.(*Store)
		if err := store.Stop(); err != nil {
			r.log.WithError(err).WithField("shard", key).Error("error stopping store")
			lastErr = err
		}
		r.stores.Delete(key)
		return true
	})

	r.started = false
	r.log.Info("per-index RAFT manager shutdown complete")
	return lastErr
}

// GetOrCreateStore gets or creates a Store for the specified shard.
// The members list should be the BelongsToNodes for the physical shard.
func (r *Raft) GetOrCreateStore(
	ctx context.Context,
	shardName string,
	members []string,
	dataPath string,
) (*Store, error) {
	r.startMu.Lock()
	if !r.started {
		r.startMu.Unlock()
		return nil, fmt.Errorf("per-index RAFT manager not started")
	}
	r.startMu.Unlock()

	// Check if store already exists
	if existing, ok := r.stores.Load(shardName); ok {
		return existing.(*Store), nil
	}

	storeConfig := StoreConfig{
		ClassName:          r.config.ClassName,
		ShardName:          shardName,
		NodeID:             r.config.NodeID,
		DataPath:           dataPath,
		Members:            members,
		Logger:             r.config.Logger,
		ApplyTimeout:       r.config.ApplyTimeout,
		HeartbeatTimeout:   r.config.HeartbeatTimeout,
		ElectionTimeout:    r.config.ElectionTimeout,
		LeaderLeaseTimeout: r.config.LeaderLeaseTimeout,
		SnapshotInterval:   r.config.SnapshotInterval,
		SnapshotThreshold:  r.config.SnapshotThreshold,
		TrailingLogs:       r.config.TrailingLogs,
	}

	store, err := NewStore(storeConfig)
	if err != nil {
		return nil, fmt.Errorf("create shard raft store: %w", err)
	}

	// Store the new store (use LoadOrStore to handle concurrent creation)
	actual, loaded := r.stores.LoadOrStore(shardName, store)
	if loaded {
		// Another goroutine created the store first, return that one
		return actual.(*Store), nil
	}

	r.log.WithField("shard", shardName).Info("created shard RAFT store")
	return store, nil
}

// GetStore returns an existing Store for a shard, or nil if not found.
func (r *Raft) GetStore(shardName string) *Store {
	if store, ok := r.stores.Load(shardName); ok {
		return store.(*Store)
	}
	return nil
}

// StopStore stops and removes a shard's Store.
func (r *Raft) StopStore(shardName string) error {
	if store, ok := r.stores.LoadAndDelete(shardName); ok {
		return store.(*Store).Stop()
	}
	return nil
}

// OnShardCreated handles the creation of a new shard.
// This should be called when a shard is created on this node.
func (r *Raft) OnShardCreated(
	ctx context.Context,
	shardName string,
	members []string,
	dataPath string,
	shard shard,
) error {
	store, err := r.GetOrCreateStore(ctx, shardName, members, dataPath)
	if err != nil {
		return fmt.Errorf("get or create store: %w", err)
	}

	// Set the shard operator
	store.SetShard(shard)

	// Set the state transferer for out-of-band snapshot restore
	if r.config.StateTransferer != nil {
		store.SetStateTransferer(r.config.StateTransferer)
	}

	// Start the store
	if err := store.Start(ctx); err != nil {
		return fmt.Errorf("start store: %w", err)
	}

	return nil
}

// OnShardDeleted handles the deletion of a shard.
// This should be called when a shard is deleted from this node.
func (r *Raft) OnShardDeleted(shardName string) error {
	return r.StopStore(shardName)
}

// OnMembershipChanged handles changes to a shard's replica membership.
// This should be called when Physical.BelongsToNodes changes.
func (r *Raft) OnMembershipChanged(ctx context.Context, shardName string, newMembers []string) error {
	store := r.GetStore(shardName)
	if store == nil {
		r.log.WithField("shard", shardName).Warn("store not found for membership change")
		return nil
	}

	// TODO: Implement RAFT membership change
	// This involves adding/removing voters from the RAFT cluster configuration.
	r.log.WithFields(logrus.Fields{
		"shard":      shardName,
		"newMembers": newMembers,
	}).Info("membership change requested (not yet implemented)")

	return nil
}

// IsLeader returns true if this node is the leader for the specified shard.
func (r *Raft) IsLeader(shardName string) bool {
	store := r.GetStore(shardName)
	if store == nil {
		return false
	}
	return store.IsLeader()
}

// VerifyLeaderForRead verifies this node is the leader for linearizable reads.
func (r *Raft) VerifyLeaderForRead(ctx context.Context, shardName string) error {
	store := r.GetStore(shardName)
	if store == nil {
		return fmt.Errorf("raft store not found for %s/%s", r.config.ClassName, shardName)
	}
	return store.VerifyLeader()
}

// LeaderAddress returns the current leader's address for a shard.
func (r *Raft) LeaderAddress(shardName string) string {
	store := r.GetStore(shardName)
	if store == nil {
		return ""
	}
	return store.Leader()
}

// Leader returns the current leader's node ID for a shard.
func (r *Raft) Leader(shardName string) string {
	store := r.GetStore(shardName)
	if store == nil {
		return ""
	}
	return store.LeaderID()
}

// ClassName returns the class name this Raft manager is responsible for.
func (r *Raft) ClassName() string {
	return r.config.ClassName
}

// Stats returns statistics about all managed Stores.
func (r *Raft) Stats() map[string]interface{} {
	stats := make(map[string]interface{})
	var count int
	var leaders int

	r.stores.Range(func(key, value interface{}) bool {
		count++
		store := value.(*Store)
		if store.IsLeader() {
			leaders++
		}
		return true
	})

	stats["total_stores"] = count
	stats["leader_stores"] = leaders
	stats["class"] = r.config.ClassName
	return stats
}
