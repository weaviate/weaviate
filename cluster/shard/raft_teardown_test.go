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
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/shard/sharedlog"
	"go.etcd.io/raft/v3/raftpb"
)

// recordingGroupRouter implements groupRouter and MessageRouter, tracking
// unregistrations so tests can assert routing teardown.
type recordingGroupRouter struct {
	mu           sync.Mutex
	registered   map[uint64]*Store
	unregistered map[uint64]bool
}

func newRecordingGroupRouter() *recordingGroupRouter {
	return &recordingGroupRouter{
		registered:   make(map[uint64]*Store),
		unregistered: make(map[uint64]bool),
	}
}

func (r *recordingGroupRouter) registerGroup(groupID uint64, s *Store) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.registered[groupID] = s
}

func (r *recordingGroupRouter) unregisterGroup(groupID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.registered, groupID)
	r.unregistered[groupID] = true
}

func (r *recordingGroupRouter) RouteMessage(groupID uint64, msg raftpb.Message) error {
	r.mu.Lock()
	s := r.registered[groupID]
	r.mu.Unlock()
	if s != nil {
		s.step(msg)
	}
	return nil
}

func (r *recordingGroupRouter) wasUnregistered(groupID uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.unregistered[groupID]
}

type staticTeardownResolver struct{}

func (staticTeardownResolver) NodeAddress(string) string { return "127.0.0.1" }

// buildTeardownRaft wires an started per-index Raft manager over a real
// MuxTransport on an ephemeral port (single node; nothing dials out).
func buildTeardownRaft(t *testing.T, className string) (*Raft, *recordingGroupRouter, *sharedlog.Store, string) {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)
	dataPath := t.TempDir()

	sl, err := sharedlog.Open(sharedlog.Options{
		Path:   filepath.Join(dataPath, sharedRaftLogName),
		Logger: logger,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sl.Close() })

	snap := NewSnapshotter(SnapshotterOptions{RootDataPath: dataPath, Logger: logger})
	t.Cleanup(func() { _ = snap.Close() })

	ids := newNodeIDMap()
	router := newRecordingGroupRouter()

	tcpAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	provider := &ShardAddressProvider{resolver: staticTeardownResolver{}}
	mux, err := NewMuxTransport("127.0.0.1:0", tcpAddr, provider, ids, router, logger, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mux.Close() })

	r := NewRaft(RaftConfig{
		ClassName:        className,
		NodeID:           "node1",
		Logger:           logger,
		HeartbeatTimeout: 40 * time.Millisecond,
		ElectionTimeout:  200 * time.Millisecond,
		MuxTransport:     mux,
		SharedLog:        sl,
		Snapshotter:      snap,
		NodeIDs:          ids,
		Resolver:         staticTeardownResolver{},
		GroupRouter:      router,
	})
	require.NoError(t, r.Start())
	t.Cleanup(func() { _ = r.Shutdown() })

	return r, router, sl, dataPath
}

// startStoreAndAwaitLeader creates a single-voter Store for shardName and
// waits until it elects itself, which guarantees its HardState reached the
// shared log (processReady persists before surfacing SoftState).
func startStoreAndAwaitLeader(t *testing.T, r *Raft, shardName string) *Store {
	t.Helper()

	store, err := r.GetOrCreateStore(context.Background(), shardName, []string{"node1"})
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	require.Eventually(t, store.IsLeader, 5*time.Second, 10*time.Millisecond,
		"single-voter group must elect itself")
	return store
}

func TestRaft_OnShardDropped(t *testing.T) {
	const className = "TeardownClass"

	tests := []struct {
		name      string
		shardName string
		// setup produces the pre-drop state and returns the Store, or nil
		// for the storeless case.
		setup func(t *testing.T, r *Raft, sl *sharedlog.Store, dataPath string) *Store
		// wantUnregistered: the drop must remove the group from routing
		// (only meaningful when a Store existed).
		wantUnregistered bool
	}{
		{
			name:      "loaded store: stopped, unregistered, state purged",
			shardName: "s1",
			setup: func(t *testing.T, r *Raft, _ *sharedlog.Store, dataPath string) *Store {
				store := startStoreAndAwaitLeader(t, r, "s1")
				// Seed a snapshot file so the purge assertion is not vacuous.
				dir := filepath.Join(dataPath, "raft-snapshots", className, "s1")
				require.NoError(t, os.MkdirAll(dir, 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(dir, "00000000000000000004.snap"), []byte("{}"), 0o644))
				return store
			},
			wantUnregistered: true,
		},
		{
			name:      "no store loaded: persisted state still purged",
			shardName: "s2",
			setup: func(t *testing.T, _ *Raft, sl *sharedlog.Store, _ string) *Store {
				// Persisted state from an earlier incarnation, no live Store —
				// the unloaded-shard drop path.
				require.NoError(t, sl.Append(context.Background(), sharedlog.GroupWrite{
					GroupID:   hashGroupID(className, "s2"),
					HardState: &raftpb.HardState{Term: 5, Commit: 3},
				}))
				return nil
			},
			wantUnregistered: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, router, sl, dataPath := buildTeardownRaft(t, className)
			store := tt.setup(t, r, sl, dataPath)
			gid := hashGroupID(className, tt.shardName)

			has, err := sl.HasGroup(gid)
			require.NoError(t, err)
			require.True(t, has, "group must have persisted state before drop")

			require.NoError(t, r.OnShardDropped(tt.shardName))

			require.Nil(t, r.GetStore(tt.shardName), "store must be gone after drop")
			require.Equal(t, tt.wantUnregistered, router.wasUnregistered(gid))
			if store != nil {
				require.Equal(t, ShardStateShutdown, store.State(), "ready loop must be stopped")
			}

			has, err = sl.HasGroup(gid)
			require.NoError(t, err)
			require.False(t, has, "persisted group state must be purged")

			_, statErr := os.Stat(filepath.Join(dataPath, "raft-snapshots", className, tt.shardName))
			require.True(t, os.IsNotExist(statErr), "snapshot dir must be removed")

			// Idempotent: a second drop of the same shard is a no-op.
			require.NoError(t, r.OnShardDropped(tt.shardName))
		})
	}
}

// TestRaft_Drop_PurgesUnloadedShardGroups pins the class-delete journey for a
// shard that was unloaded earlier in the process (e.g. a tenant that went
// HOT -> COLD): its Store is gone from the manager but its persisted group
// state remains, and Drop must still purge it or the group resurrects when
// the class/tenant name is reused.
func TestRaft_Drop_PurgesUnloadedShardGroups(t *testing.T) {
	const className = "TeardownClass"

	r, _, sl, _ := buildTeardownRaft(t, className)
	store := startStoreAndAwaitLeader(t, r, "s1")
	gid := store.GroupID()

	// HOT -> COLD: store removed, persisted state deliberately kept.
	require.NoError(t, r.OnShardUnloaded("s1"))
	has, err := sl.HasGroup(gid)
	require.NoError(t, err)
	require.True(t, has)

	// Class delete: Drop must purge the unloaded shard's group too.
	require.NoError(t, r.Drop())

	has, err = sl.HasGroup(gid)
	require.NoError(t, err)
	require.False(t, has,
		"class drop must purge group state of shards unloaded earlier in the process")
}

// TestRaft_OnShardUnloaded_KeepsPersistedState pins the unload/drop split: an
// unload (node shutdown, tenant offload) stops and unregisters the Store but
// must keep the group's persisted state so a restart can recover it.
func TestRaft_OnShardUnloaded_KeepsPersistedState(t *testing.T) {
	const className = "TeardownClass"

	r, router, sl, _ := buildTeardownRaft(t, className)
	store := startStoreAndAwaitLeader(t, r, "s1")
	gid := store.GroupID()

	require.NoError(t, r.OnShardUnloaded("s1"))

	require.Nil(t, r.GetStore("s1"))
	require.True(t, router.wasUnregistered(gid))
	require.Equal(t, ShardStateShutdown, store.State())

	has, err := sl.HasGroup(gid)
	require.NoError(t, err)
	require.True(t, has, "unload must keep persisted group state for restart recovery")
}
