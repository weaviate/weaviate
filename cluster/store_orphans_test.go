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
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	clustermocks "github.com/weaviate/weaviate/cluster/mocks"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestStoreLeavesNoOrphanedClassData pins that every startup and restore
// journey drops the data of a class the schema no longer names.
func TestStoreLeavesNoOrphanedClassData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T, ms MockStore)
	}{
		{
			name: "startup catch-up replays the delete",
			run: func(t *testing.T, ms MockStore) {
				ms.store.lastAppliedIndexToDB.Store(2)
				applyOK(t, ms.store, orphanAddClass(1))
				applyOK(t, ms.store, orphanDeleteClass(2))
				waitLoaded(t, ms.store)
			},
		},
		{
			name: "delete lands while the startup load runs",
			run: func(t *testing.T, ms MockStore) {
				ms.store.lastAppliedIndexToDB.Store(1)
				release := holdReload(t, ms)
				applyOK(t, ms.store, orphanAddClass(1))
				<-release.loading
				applyOK(t, ms.store, orphanDeleteClass(2))
				close(release.release)
				waitLoaded(t, ms.store)
			},
		},
		{
			name: "runtime snapshot restore without the class",
			run: func(t *testing.T, ms MockStore) {
				ms.store.dbLoad.markDone()
				applyOK(t, ms.store, orphanAddClass(1))
				restoreOK(t, ms, emptySnapshot(t))
			},
		},
		{
			name: "snapshot restore without the class while the startup load runs",
			run: func(t *testing.T, ms MockStore) {
				applyOK(t, ms.store, orphanAddClass(1))
				held := holdReload(t, ms)
				ms.store.dbLoad.run(ms.store.loadDBFromSchema)
				<-held.loading
				restoreOK(t, ms, emptySnapshot(t))
				close(held.release)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ms := newOrphanStore(t)
			test.run(t, ms)
			require.True(t, tryNTimesWithWait(200, 10*time.Millisecond, func() bool {
				return called(ms.indexer, "DropOrphanedClass")
			}), "class C's data was never dropped: it stays on disk with nothing to name it")
			ms.indexer.AssertCalled(t, "DropOrphanedClass", mock.Anything, "C", false)
		})
	}
}

type heldReload struct{ loading, release chan struct{} }

// holdReload holds the first ReloadLocalDB open without starting the load.
func holdReload(t *testing.T, ms MockStore) heldReload {
	t.Helper()
	h := heldReload{loading: make(chan struct{}), release: make(chan struct{})}
	var first atomic.Bool
	ms.indexer.ReloadLocalDBHook = func(ctx context.Context) {
		if first.CompareAndSwap(false, true) {
			close(h.loading)
			select {
			case <-h.release:
			case <-ctx.Done():
			}
		}
	}
	return h
}

func newOrphanStore(t *testing.T) MockStore {
	t.Helper()
	ms := NewMockStore(t, "node1", utils.MustGetFreeTCPPort())
	orphanMocks(t, ms)
	return ms
}

func orphanMocks(t *testing.T, ms MockStore) {
	t.Helper()
	require.NoError(t, ms.store.init())
	ms.store.raft = &raft.Raft{}
	ms.parser.On("ParseClass", mock.Anything).Return(nil)
	ms.indexer.On("Open", mock.Anything).Return(nil)
	ms.indexer.On("TriggerSchemaUpdateCallbacks").Return()
	ms.indexer.On("AddClass", mock.Anything).Return(nil)
	ms.indexer.On("DeleteClass", mock.Anything, mock.Anything).Return(nil)
	ms.indexer.On("DropOrphanedClass", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	ms.replicationFSM.EXPECT().DeleteReplicationsByCollection(mock.Anything).Return(nil).Maybe()
}

func orphanAddClass(index uint64) *raft.Log {
	return logEntry(index, "C", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{
		Class: &models.Class{Class: "C"},
		State: &sharding.State{Physical: map[string]sharding.Physical{
			"S0": {Name: "S0", BelongsToNodes: []string{"node1"}},
		}},
	}, nil)
}

func orphanDeleteClass(index uint64) *raft.Log {
	return logEntry(index, "C", cmd.ApplyRequest_TYPE_DELETE_CLASS, nil, nil)
}

func applyOK(t *testing.T, st *Store, l *raft.Log) {
	t.Helper()
	resp, ok := st.Apply(l).(Response)
	require.True(t, ok)
	require.NoError(t, resp.Error)
}

func waitLoaded(t *testing.T, st *Store) {
	t.Helper()
	require.True(t, tryNTimesWithWait(500, 10*time.Millisecond, st.dbLoad.done), "the load must finish")
}

// emptySnapshot is a leader's snapshot taken after every class was deleted.
func emptySnapshot(t *testing.T) []byte {
	t.Helper()
	src := NewMockStore(t, "leader", utils.MustGetFreeTCPPort())
	snap, err := src.store.Snapshot()
	require.NoError(t, err)
	sink := &clustermocks.SnapshotSink{Buffer: bytes.NewBuffer(nil)}
	require.NoError(t, snap.Persist(sink))
	return sink.Buffer.Bytes()
}

func restoreOK(t *testing.T, ms MockStore, snapshot []byte) {
	t.Helper()
	require.NoError(t, ms.store.Restore(io.NopCloser(bytes.NewReader(snapshot))))
}

// TestStoreRefusesSnapshotsUntilStartupLoad pins that a restarted node takes no
// snapshot before its load is done. Deletes replayed or deferred until then
// only reach the DB after the load; a snapshot covering them would leave a
// restart nothing to replay, and their data on disk.
func TestStoreRefusesSnapshotsUntilStartupLoad(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		run     func(t *testing.T, ms MockStore)
		wantErr bool
	}{
		{
			name: "fresh node",
			run:  func(t *testing.T, ms MockStore) { applyOK(t, ms.store, orphanAddClass(1)) },
		},
		{
			name: "restarted, catching up past a delete",
			run: func(t *testing.T, ms MockStore) {
				ms.store.lastAppliedIndexToDB.Store(10)
				applyOK(t, ms.store, orphanAddClass(1))
				applyOK(t, ms.store, orphanDeleteClass(2))
			},
			wantErr: true,
		},
		{
			name: "restarted, delete deferred behind the load",
			run: func(t *testing.T, ms MockStore) {
				ms.store.lastAppliedIndexToDB.Store(1)
				held := holdReload(t, ms)
				applyOK(t, ms.store, orphanAddClass(1))
				<-held.loading
				applyOK(t, ms.store, orphanDeleteClass(2))
				t.Cleanup(func() { close(held.release) })
			},
			wantErr: true,
		},
		{
			name: "restarted, loaded",
			run: func(t *testing.T, ms MockStore) {
				ms.store.lastAppliedIndexToDB.Store(2)
				applyOK(t, ms.store, orphanAddClass(1))
				applyOK(t, ms.store, orphanDeleteClass(2))
				waitLoaded(t, ms.store)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ms := newOrphanStore(t)
			test.run(t, ms)

			_, err := ms.store.Snapshot()
			if test.wantErr {
				require.ErrorIs(t, err, errStartupLoadPending)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestStoreRestoreDuringLoadRunsQueuedWrites pins that a snapshot restored
// mid-load does not drop the writes queued behind the cancelled load. They
// predate the snapshot, and the restored schema no longer names what they
// deleted, so nothing else would remove its data.
func TestStoreRestoreDuringLoadRunsQueuedWrites(t *testing.T) {
	t.Parallel()

	ms := newOrphanStore(t)
	applyOK(t, ms.store, orphanAddClass(1))

	held := holdReload(t, ms)
	ms.store.dbLoad.run(ms.store.loadDBFromSchema)
	<-held.loading
	applyOK(t, ms.store, orphanDeleteClass(2)) // queued behind the load

	restoreOK(t, ms, emptySnapshot(t))
	close(held.release)

	ms.indexer.AssertCalled(t, "DropOrphanedClass", mock.Anything, "C", false)
}
