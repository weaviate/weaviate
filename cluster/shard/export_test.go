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

// MissedTicks exposes the tick-replay arithmetic for table tests.
func MissedTicks(last, now time.Time, interval time.Duration, maxTicks int) (int, time.Time) {
	return missedTicks(last, now, interval, maxTicks)
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

// TestStoreSpec describes one node of a BuildTestClusterWithOptions cluster.
type TestStoreSpec struct {
	NodeID string
	// Shard is the node's FSM target; nil leaves it unset.
	Shard shard
	// WrapTransport, when non-nil, wraps the node's MemTransport before the
	// Store sees it — for latency injection and outbound message counting.
	WrapTransport func(Transport) Transport
}

// TestClusterOptions carries the raft timing knobs BuildTestCluster hardcodes.
type TestClusterOptions struct {
	TickInterval      time.Duration
	HeartbeatTimeout  time.Duration
	ElectionTimeout   time.Duration
	SnapshotThreshold uint64
}

// BuildTestClusterWithOptions mirrors BuildTestCluster with per-node shard
// wiring, transport wrapping, and explicit raft timing.
func BuildTestClusterWithOptions(t *testing.T, class, shardName string, specs []TestStoreSpec, opts TestClusterOptions) []*Store {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	net := NewMemNetwork()
	ids := newNodeIDMap()
	members := make([]string, len(specs))
	for i := range specs {
		members[i] = specs[i].NodeID
	}

	stores := make([]*Store, len(specs))
	for i, spec := range specs {
		sl, err := sharedlog.Open(sharedlog.Options{
			Path:   filepath.Join(t.TempDir(), sharedRaftLogName),
			Logger: logger,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sl.Close() })

		snap := NewSnapshotter(SnapshotterOptions{RootDataPath: t.TempDir(), Logger: logger})
		t.Cleanup(func() { _ = snap.Close() })

		router := newMemRouter()
		var transport Transport = net.NewTransport(ids.register(spec.NodeID), router, logger)
		t.Cleanup(func() { _ = transport.Close() })
		if spec.WrapTransport != nil {
			transport = spec.WrapTransport(transport)
		}

		store, err := NewStore(StoreConfig{
			ClassName:         class,
			ShardName:         shardName,
			NodeID:            spec.NodeID,
			Members:           members,
			Logger:            logger,
			Transport:         transport,
			SharedLog:         sl,
			Snapshotter:       snap,
			NodeIDs:           ids,
			TickInterval:      opts.TickInterval,
			HeartbeatTimeout:  opts.HeartbeatTimeout,
			ElectionTimeout:   opts.ElectionTimeout,
			SnapshotThreshold: opts.SnapshotThreshold,
		})
		require.NoError(t, err)

		if spec.Shard != nil {
			store.SetShard(spec.Shard)
		}
		router.add(store)
		stores[i] = store
		t.Cleanup(func() { _ = store.Stop() })
	}
	return stores
}

// BuildTestStoreAt builds a single-node Store whose shared log and snapshot
// state live at explicit paths, so a test can stop it, rebuild at the same
// paths, and exercise restart recovery. The returned closeInfra is idempotent
// and must be called before rebuilding at the same paths (bbolt holds an
// exclusive file lock); it is also registered as a t.Cleanup guard.
func BuildTestStoreAt(t *testing.T, class, shardName, nodeID, logPath, snapRoot string, snapshotThreshold uint64, sh shard) (*Store, func()) {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	sl, err := sharedlog.Open(sharedlog.Options{Path: logPath, Logger: logger})
	require.NoError(t, err)
	snap := NewSnapshotter(SnapshotterOptions{RootDataPath: snapRoot, Logger: logger})

	nodeIDs := newNodeIDMap()
	net := NewMemNetwork()
	router := newMemRouter()
	transport := net.NewTransport(nodeIDs.register(nodeID), router, logger)

	store, err := NewStore(StoreConfig{
		ClassName:         class,
		ShardName:         shardName,
		NodeID:            nodeID,
		Members:           []string{nodeID},
		Logger:            logger,
		Transport:         transport,
		SharedLog:         sl,
		Snapshotter:       snap,
		NodeIDs:           nodeIDs,
		TickInterval:      20 * time.Millisecond,
		HeartbeatTimeout:  40 * time.Millisecond,
		ElectionTimeout:   80 * time.Millisecond,
		SnapshotThreshold: snapshotThreshold,
	})
	require.NoError(t, err)

	if sh != nil {
		store.SetShard(sh)
	}
	router.add(store)

	var once sync.Once
	closeInfra := func() {
		once.Do(func() {
			_ = store.Stop()
			_ = transport.Close()
			_ = snap.Close()
			_ = sl.Close()
		})
	}
	t.Cleanup(closeInfra)
	return store, closeInfra
}

// BuildTestCluster wires n fully-connected, unstarted Stores over one shared
// MemNetwork (one shared nodeIDMap, Members = all nodeIDs). Mirrors
// BuildTestStore for multi-node tests. Callers that need a per-node shard wire
// it via Store.SetShard before Start.
func BuildTestCluster(t *testing.T, class, shardName string, nodeIDs []string) []*Store {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	net := NewMemNetwork()
	ids := newNodeIDMap()
	stores := make([]*Store, len(nodeIDs))
	for i, nodeID := range nodeIDs {
		sl, err := sharedlog.Open(sharedlog.Options{
			Path:   filepath.Join(t.TempDir(), sharedRaftLogName),
			Logger: logger,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sl.Close() })

		snap := NewSnapshotter(SnapshotterOptions{RootDataPath: t.TempDir(), Logger: logger})
		t.Cleanup(func() { _ = snap.Close() })

		router := newMemRouter()
		transport := net.NewTransport(ids.register(nodeID), router, logger)
		t.Cleanup(func() { _ = transport.Close() })

		store, err := NewStore(StoreConfig{
			ClassName:         class,
			ShardName:         shardName,
			NodeID:            nodeID,
			Members:           nodeIDs,
			Logger:            logger,
			Transport:         transport,
			SharedLog:         sl,
			Snapshotter:       snap,
			NodeIDs:           ids,
			TickInterval:      20 * time.Millisecond,
			HeartbeatTimeout:  40 * time.Millisecond,
			ElectionTimeout:   200 * time.Millisecond,
			SnapshotThreshold: 1024,
		})
		require.NoError(t, err)

		router.add(store)
		stores[i] = store
		t.Cleanup(func() { _ = store.Stop() })
	}
	return stores
}
