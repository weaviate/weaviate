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
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/shard/sharedlog"
	"go.etcd.io/raft/v3/raftpb"
)

// This file exposes a handful of package internals to the external shard_test
// package. The mock shard lives in cluster/shard/mocks, which imports
// cluster/shard — so mock-using tests must stay in package shard_test, and
// reach internals (the message router, nodeID map, store wiring) through here.

// memRouter routes inbound raft messages to per-group Stores on one node — the
// test-side MessageRouter for a MemTransport.
type memRouter struct {
	mu     sync.Mutex
	stores map[uint64]*Store
}

func newMemRouter() *memRouter {
	return &memRouter{stores: make(map[uint64]*Store)}
}

func (r *memRouter) add(s *Store) {
	r.mu.Lock()
	r.stores[s.GroupID()] = s
	r.mu.Unlock()
}

func (r *memRouter) RouteMessage(groupID uint64, msg raftpb.Message) error {
	r.mu.Lock()
	s := r.stores[groupID]
	r.mu.Unlock()
	if s != nil {
		s.step(msg)
	}
	return nil
}

// BuildTestStore builds a fully-wired, unstarted single-node Store over an
// in-process MemTransport. members is the raft membership — extra members
// beyond nodeID are phantom (never reachable), useful for no-quorum tests.
func BuildTestStore(t *testing.T, class, shardName, nodeID string, members []string, sh shard) *Store {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	sl, err := sharedlog.Open(sharedlog.Options{
		Path:   filepath.Join(t.TempDir(), sharedRaftLogName),
		Logger: logger,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sl.Close() })

	snap := NewSnapshotter(SnapshotterOptions{RootDataPath: t.TempDir(), Logger: logger})
	t.Cleanup(func() { _ = snap.Close() })

	nodeIDs := newNodeIDMap()
	net := NewMemNetwork()
	router := newMemRouter()
	transport := net.NewTransport(nodeIDs.register(nodeID), router, logger)
	t.Cleanup(func() { _ = transport.Close() })

	store, err := NewStore(StoreConfig{
		ClassName:         class,
		ShardName:         shardName,
		NodeID:            nodeID,
		Members:           members,
		Logger:            logger,
		Transport:         transport,
		SharedLog:         sl,
		Snapshotter:       snap,
		NodeIDs:           nodeIDs,
		TickInterval:      20 * time.Millisecond,
		HeartbeatTimeout:  40 * time.Millisecond,
		ElectionTimeout:   80 * time.Millisecond,
		SnapshotThreshold: 1024,
	})
	require.NoError(t, err)

	if sh != nil {
		store.SetShard(sh)
	}
	router.add(store)
	t.Cleanup(func() { _ = store.Stop() })

	return store
}

// RestoreFSM calls FSM.RestoreFromSnapshot with a snapshot built from the given
// fields — shardSnapshotData is unexported, so external tests go through here.
func RestoreFSM(fsm *FSM, className, shardName, nodeID string, lastAppliedIndex uint64) error {
	return fsm.RestoreFromSnapshot(shardSnapshotData{
		ClassName:        className,
		ShardName:        shardName,
		NodeID:           nodeID,
		LastAppliedIndex: lastAppliedIndex,
	})
}
