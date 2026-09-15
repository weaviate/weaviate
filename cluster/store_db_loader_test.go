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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"

	clustermocks "github.com/weaviate/weaviate/cluster/mocks"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/utils"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestDBLoaderRunsQueuedWritesOnceInOrder pins that every queued write runs
// once, in order.
func TestDBLoaderRunsQueuedWritesOnceInOrder(t *testing.T) {
	t.Parallel()

	l := newDBLoader(logrus.New())
	ctx, ok := l.begin()
	require.True(t, ok)

	var got []int
	var queue func(i int)
	queue = func(i int) {
		require.True(t, l.deferWrite(func() {
			got = append(got, i)
			if i == 1 {
				queue(3)
			}
		}), "a write landing mid-load must be queued")
	}
	queue(1)
	queue(2)

	l.drain(ctx)

	require.Equal(t, []int{1, 2, 3}, got)
	require.False(t, l.inFlight.Load(), "an empty queue ends the load")
	require.False(t, l.deferWrite(func() { t.Fatal("ran a write queued after the load") }),
		"the load is over: the caller writes now")
}

func TestDBLoaderIsOneShot(t *testing.T) {
	t.Parallel()

	l := newDBLoader(logrus.New())
	require.False(t, l.deferStoreWrite("op", func() error { return nil }), "no load in flight: nothing to defer")

	ctx, ok := l.begin()
	require.True(t, ok)
	l.drain(ctx)

	require.False(t, l.deferStoreWrite("op", func() error { return nil }), "the load is over: nothing to defer")
	require.Empty(t, l.queued, "a write that was not deferred must not be queued")

	_, ok = l.begin()
	require.False(t, ok, "the load is one-shot")
}

func TestDBLoaderDropsQueueOnShutdown(t *testing.T) {
	t.Parallel()

	l := newDBLoader(logrus.New())
	ctx, ok := l.begin()
	require.True(t, ok)
	require.True(t, l.deferWrite(func() { t.Fatal("ran a write after shutdown") }))

	l.cancel()
	l.drain(ctx)
	require.False(t, l.inFlight.Load())
}

// TestDBLoaderHandoverUnderStress checks that every write the loader took
// runs.
func TestDBLoaderHandoverUnderStress(t *testing.T) {
	t.Parallel()

	const rounds = 2000

	for i := 0; i < rounds; i++ {
		var (
			l        = newDBLoader(logrus.New())
			queued   atomic.Int64
			ran      atomic.Int64
			refused  atomic.Int64
			start    = make(chan struct{})
			wg       sync.WaitGroup
			ctx, ok  = l.begin()
			commands = 4
		)
		require.True(t, ok, "round %d: the load must start idle", i)

		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			<-start
			l.drain(ctx)
		}, l.log)

		for j := 0; j < commands; j++ {
			wg.Add(1)
			enterrors.GoWrapper(func() {
				defer wg.Done()
				<-start
				if l.deferWrite(func() { ran.Add(1) }) {
					queued.Add(1)
				} else {
					refused.Add(1)
				}
			}, l.log)
		}

		close(start)
		wg.Wait()

		require.False(t, l.inFlight.Load(), "round %d: the loader must leave the load idle", i)
		require.Equal(t, int64(commands), queued.Load()+refused.Load())
		require.Equal(t, queued.Load(), ran.Load(),
			"round %d: %d write(s) queued but %d ran; the rest never reach the DB", i, queued.Load(), ran.Load())
	}
}

// TestStoreDeferredDBWritesReachTheDB pins that a DB write deferred behind
// the load is applied afterwards, whatever the command type. UpdateIndex
// alone does not reconcile class config, shard status or replica removal.
func TestStoreDeferredDBWritesReachTheDB(t *testing.T) {
	t.Parallel()

	cls := &models.Class{Class: "C", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}}
	newState := func() *sharding.State {
		return &sharding.State{
			PartitioningEnabled: true,
			Physical: map[string]sharding.Physical{"T0": {
				Name: "T0", BelongsToNodes: []string{"node1"}, Status: models.TenantActivityStatusHOT,
			}},
		}
	}

	tests := []struct {
		name   string
		log    func(index uint64) *raft.Log
		method string
	}{
		{
			name: "update class",
			log: func(i uint64) *raft.Log {
				return logEntry(i, "C", cmd.ApplyRequest_TYPE_UPDATE_CLASS, cmd.UpdateClassRequest{Class: cls}, nil)
			},
			method: "UpdateClass",
		},
		{
			name: "add property",
			log: func(i uint64) *raft.Log {
				return logEntry(i, "C", cmd.ApplyRequest_TYPE_ADD_PROPERTY,
					cmd.AddPropertyRequest{Properties: []*models.Property{{Name: "p", DataType: []string{"text"}}}}, nil)
			},
			method: "AddProperty",
		},
		{
			name: "update shard status",
			log: func(i uint64) *raft.Log {
				return logEntry(i, "C", cmd.ApplyRequest_TYPE_UPDATE_SHARD_STATUS,
					cmd.UpdateShardStatusRequest{Class: "C", Shard: "T0", Status: "READONLY"}, nil)
			},
			method: "UpdateShardStatus",
		},
		{
			name: "add tenant",
			log: func(i uint64) *raft.Log {
				return logEntry(i, "C", cmd.ApplyRequest_TYPE_ADD_TENANT, nil, &cmd.AddTenantsRequest{
					ClusterNodes: []string{"node1"},
					Tenants:      []*cmd.Tenant{{Name: "T1", Status: models.TenantActivityStatusHOT}},
				})
			},
			method: "AddTenants",
		},
		{
			name: "delete class",
			log: func(i uint64) *raft.Log {
				return logEntry(i, "C", cmd.ApplyRequest_TYPE_DELETE_CLASS, nil, nil)
			},
			method: "DropOrphanedClass",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ms := newDeferralStore(t)
			st := ms.store
			applied := func(l *raft.Log) {
				t.Helper()
				resp, ok := st.Apply(l).(Response)
				require.True(t, ok)
				require.NoError(t, resp.Error)
			}
			applied(logEntry(1, "C", cmd.ApplyRequest_TYPE_ADD_CLASS,
				cmd.AddClassRequest{Class: cls, State: newState()}, nil))
			require.True(t, called(ms.indexer, "AddClass"), "precondition: the class reached the DB before the load")

			release := holdLoad(t, &ms)
			applied(test.log(2))
			require.False(t, called(ms.indexer, test.method),
				"%s reached the DB while the load was still running", test.method)

			close(release)
			require.True(t, tryNTimesWithWait(200, 10*time.Millisecond, st.dbLoaded.Load),
				"the load must finish")
			require.True(t, called(ms.indexer, test.method),
				"%s applied mid-load never reached the DB", test.method)
		})
	}
}

func TestStoreDeferredDBWritesKeepLogOrder(t *testing.T) {
	t.Parallel()

	cls := &models.Class{Class: "C", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}}
	addClass := func(i uint64) *raft.Log {
		return logEntry(i, "C", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{
			Class: cls, State: &sharding.State{PartitioningEnabled: true, Physical: map[string]sharding.Physical{}},
		}, nil)
	}

	ms := newDeferralStore(t)
	st := ms.store
	apply := func(l *raft.Log) {
		t.Helper()
		resp, ok := st.Apply(l).(Response)
		require.True(t, ok)
		require.NoError(t, resp.Error)
	}
	apply(addClass(1))
	require.True(t, called(ms.indexer, "AddClass"), "precondition: the class reached the DB before the load")

	release := holdLoad(t, &ms)
	apply(logEntry(2, "C", cmd.ApplyRequest_TYPE_DELETE_CLASS, nil, nil))
	apply(addClass(3))
	apply(logEntry(4, "C", cmd.ApplyRequest_TYPE_DELETE_CLASS, nil, nil))

	close(release)
	require.True(t, tryNTimesWithWait(200, 10*time.Millisecond, st.dbLoaded.Load), "the load must finish")

	var got []string
	for _, c := range ms.indexer.Calls {
		switch c.Method {
		case "AddClass", "DropOrphanedClass", "DeleteClass":
			got = append(got, c.Method)
		}
	}
	require.Equal(t, []string{"AddClass", "DropOrphanedClass", "AddClass", "DropOrphanedClass"}, got)
}

func newDeferralStore(t *testing.T) MockStore {
	t.Helper()
	ms := NewMockStore(t, "node1", utils.MustGetFreeTCPPort())
	ms.store.raft = &raft.Raft{}
	ms.parser.On("ParseClass", mock.Anything).Return(nil)
	ms.parser.On("ParseClassUpdate", mock.Anything, mock.Anything).Return(mock.Anything, nil)
	for _, m := range []string{"AddClass", "UpdateClass", "UpdateShardStatus"} {
		ms.indexer.On(m, mock.Anything).Return(nil)
	}
	for _, m := range []string{"AddProperty", "AddTenants", "DeleteClass"} {
		ms.indexer.On(m, mock.Anything, mock.Anything).Return(nil)
	}
	ms.indexer.On("DropOrphanedClass", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	ms.indexer.On("Open", mock.Anything).Return(nil)
	ms.indexer.On("TriggerSchemaUpdateCallbacks").Return()
	ms.replicationFSM.EXPECT().HasActiveReplicationForCollection(mock.Anything).Return(false).Maybe()
	ms.replicationFSM.EXPECT().DeleteReplicationsByCollection(mock.Anything).Return(nil).Maybe()
	return ms
}

// holdLoad starts the load and holds it in ReloadLocalDB until release is closed.
func holdLoad(t *testing.T, ms *MockStore) chan struct{} {
	t.Helper()
	release, loading := make(chan struct{}), make(chan struct{})
	ms.indexer.ReloadLocalDBHook = func(context.Context) {
		close(loading)
		<-release
	}
	ms.store.dbLoad.start(ms.store.reloadDBFromSchema)
	<-loading
	return release
}

func logEntry(index uint64, class string, typ cmd.ApplyRequest_Type, jsonSub any, rpcSub protoreflect.ProtoMessage) *raft.Log {
	return &raft.Log{Index: index, Type: raft.LogCommand, Data: cmdAsBytes(class, typ, jsonSub, rpcSub)}
}

func called(m *fakes.MockSchemaExecutor, method string) bool {
	for _, c := range m.Calls {
		if c.Method == method {
			return true
		}
	}
	return false
}

// TestStoreRestoreDuringLoadSupersedesIt pins that a snapshot restored mid-load
// cancels the load and reloads before returning, never beside it.
func TestStoreRestoreDuringLoadSupersedesIt(t *testing.T) {
	source := NewMockStore(t, "restore-during-load-source", utils.MustGetFreeTCPPort())
	setupTestSchema(t, source)
	snapshot, err := source.store.Snapshot()
	require.NoError(t, err)
	sink := &clustermocks.SnapshotSink{Buffer: bytes.NewBuffer(nil)}
	require.NoError(t, snapshot.Persist(sink))

	target := NewMockStore(t, "restore-during-load-target", utils.MustGetFreeTCPPort())
	require.NoError(t, target.store.init())
	target.store.raft = &raft.Raft{} // runtime InstallSnapshot
	target.parser.On("ParseClass", mock.Anything).Return(nil)
	target.indexer.On("TriggerSchemaUpdateCallbacks").Return()
	target.indexer.On("RestoreClassDir", mock.Anything).Return(nil)

	var (
		calls, active, maxActive atomic.Int32
		loading                  = make(chan struct{})
	)
	target.indexer.ReloadLocalDBHook = func(ctx context.Context) {
		n := active.Add(1)
		defer active.Add(-1)
		for m := maxActive.Load(); n > m && !maxActive.CompareAndSwap(m, n); m = maxActive.Load() {
		}
		if calls.Add(1) == 1 {
			close(loading)
			<-ctx.Done()
		}
	}

	target.store.dbLoad.start(target.store.reloadDBFromSchema)
	<-loading

	restored := make(chan error, 1)
	enterrors.GoWrapper(func() {
		restored <- target.store.Restore(io.NopCloser(bytes.NewReader(sink.Buffer.Bytes())))
	}, target.logger)
	select {
	case err := <-restored:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Restore did not cancel the running load")
	}

	require.True(t, target.store.dbLoaded.Load())
	require.Equal(t, int32(2), calls.Load(), "the restored schema must be loaded")
	require.Equal(t, int32(1), maxActive.Load(), "the restore's reload ran beside the load")
}
