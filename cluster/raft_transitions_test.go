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

package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/mocks"
	"github.com/weaviate/weaviate/cluster/utils"
)

func TestFSMReachedIndexWaitsForTheLastCommand(t *testing.T) {
	entries := []raft.LogType{raft.LogConfiguration, raft.LogCommand, raft.LogCommand, raft.LogNoop, raft.LogConfiguration}
	tests := []struct {
		name      string
		compacted uint64
		hole      uint64
		applied   uint64
		index     uint64
		want      bool
	}{
		{name: "the FSM is at the index", applied: 5, index: 5, want: true},
		{name: "the last command is applied", applied: 3, index: 5, want: true},
		{name: "the last command is queued", applied: 2, index: 5},
		{name: "the log does not reach the index", applied: 3, index: 6},
		{name: "a snapshot holds every command", compacted: 3, applied: 3, index: 5, want: true},
		{name: "a hole below the index", hole: 4, applied: 3, index: 5, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := NewMockStore(t, "node1", 0).store
			for r := attachLeaderRaft(t, st); r.AppliedIndex() < 5; {
				require.NoError(t, r.Barrier(5*time.Second).Error())
			}
			logs := raft.NewInmemStore()
			for i, typ := range entries {
				require.NoError(t, logs.StoreLog(&raft.Log{Index: uint64(i + 1), Type: typ}))
			}
			if tt.compacted > 0 {
				require.NoError(t, logs.DeleteRange(1, tt.compacted))
			}
			if tt.hole > 0 {
				require.NoError(t, logs.DeleteRange(tt.hole, tt.hole))
			}
			cache, err := raft.NewLogCache(16, logs)
			require.NoError(t, err)
			st.logCache = cache
			st.lastAppliedIndex.Store(tt.applied)

			got, err := st.fsmReachedIndex(tt.index)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

type failingLogStore struct{ raft.LogStore }

func (failingLogStore) GetLog(uint64, *raft.Log) error { return errors.New("disk read failed") }

// A failed read must not pass for a compacted log, because raft's applied index already covers the barrier.
func TestFSMReachedIndexFailsClosedOnALogReadError(t *testing.T) {
	st := NewMockStore(t, "node1", 0).store
	r := attachLeaderRaft(t, st)
	logs := raft.NewInmemStore()
	for i := uint64(1); i <= 3; i++ {
		require.NoError(t, logs.StoreLog(&raft.Log{Index: i, Type: raft.LogCommand}))
	}
	cache, err := raft.NewLogCache(16, failingLogStore{logs})
	require.NoError(t, err)
	st.logCache = cache

	index := r.AppliedIndex()
	require.NotZero(t, index)
	got, err := st.fsmReachedIndex(index)
	require.ErrorContains(t, err, "disk read failed")
	assert.False(t, got)

	st.logCache = nil
	_, err = st.fsmReachedIndex(index)
	require.ErrorContains(t, err, "raft log is not open")
}

func TestRestoreAdvancesTheAppliedIndex(t *testing.T) {
	source := NewMockStore(t, "source-node", utils.MustGetFreeTCPPort())
	snapshot, err := source.store.Snapshot()
	require.NoError(t, err)
	sink := &mocks.SnapshotSink{Buffer: bytes.NewBuffer(nil)}
	require.NoError(t, snapshot.Persist(sink))

	target := NewMockStore(t, "target-node", utils.MustGetFreeTCPPort())
	target.store.init()
	target.parser.On("ParseClass", mock.Anything).Return(nil)
	target.indexer.On("TriggerSchemaUpdateCallbacks").Return()
	target.indexer.On("RestoreClassDir", mock.Anything).Return(nil)
	target.indexer.On("UpdateShardStatus", mock.Anything).Return(nil)
	target.indexer.On("AddClass", mock.Anything).Return(nil)
	snapshots, err := raft.NewFileSnapshotStore(t.TempDir(), 1, io.Discard)
	require.NoError(t, err)
	_, transport := raft.NewInmemTransport("")
	created, err := snapshots.Create(raft.SnapshotVersionMax, 7, 1, raft.Configuration{}, 1, transport)
	require.NoError(t, err)
	require.NoError(t, created.Close())
	target.store.snapshotStore = snapshots

	require.NoError(t, target.store.Restore(io.NopCloser(bytes.NewReader(sink.Buffer.Bytes()))))
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, target.store.WaitForAppliedIndex(canceled, time.Millisecond, 7))

	target.store.cfg.MetadataOnlyVoters = true
	target.store.lastAppliedIndex.Store(0)
	require.NoError(t, target.store.Restore(io.NopCloser(bytes.NewReader(sink.Buffer.Bytes()))))
	require.NoError(t, target.store.WaitForAppliedIndex(canceled, time.Millisecond, 7))
}

// attachLeaderRaft gives store a bootstrapped single-node raft that has won its election.
func attachLeaderRaft(t *testing.T, store *Store) *raft.Raft {
	t.Helper()
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID("node1")
	cfg.Logger = hclog.NewNullLogger()
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond
	logs := raft.NewInmemStore()
	snapshots := raft.NewInmemSnapshotStore()
	addr, transport := raft.NewInmemTransport("")
	require.NoError(t, raft.BootstrapCluster(cfg, logs, logs, snapshots, transport,
		raft.Configuration{Servers: []raft.Server{{ID: cfg.LocalID, Address: addr}}}))

	r, err := raft.NewRaft(cfg, store, logs, logs, snapshots, transport)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Shutdown().Error()) })
	require.Eventually(t, func() bool { return r.State() == raft.Leader }, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, r.Barrier(5*time.Second).Error())

	store.raft.Store(r)
	return r
}
