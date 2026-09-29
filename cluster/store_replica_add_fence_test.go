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
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

var errAddReachedApply = errors.New("add reached apply")

func newReplicaAddFenceStore(t *testing.T, commitOnce ...uint64) (*Store, *MockStore) {
	t.Helper()
	srv, m := newBarrierTestStore(t)
	for _, id := range commitOnce {
		m.replicationFSM.EXPECT().SetUnCancellable(id).Return(nil).Once()
	}
	m.replicationFSM.EXPECT().SetUnCancellable(mock.Anything).Return(errAddReachedApply).Maybe()
	m.indexer.On("ReconcileAsyncReplicationForShard", mock.Anything, mock.Anything).Return(nil).Maybe()
	require.NoError(t, srv.store.waitLeaderFSMCaughtUp())
	return srv.store, m
}

func registerFenceOp(t *testing.T, fsm *replication.ShardReplicationFSM, id uint64, states ...api.ShardReplicationState) strfmt.UUID {
	t.Helper()
	opUUID := strfmt.UUID(uuid.NewString())
	require.NoError(t, fsm.Replicate(id, &api.ReplicationReplicateShardRequest{
		Uuid:             opUUID,
		SourceNode:       "Node-1",
		SourceCollection: fmt.Sprintf("Class%d", id),
		SourceShard:      "S1",
		TargetNode:       "Node-2",
		TransferType:     api.COPY.String(),
	}))
	for _, s := range states {
		require.NoError(t, fsm.UpdateReplicationOpStatus(&api.ReplicationUpdateOpStateRequest{Id: id, State: s}))
	}
	return opUUID
}

func addReplicaCmd(t *testing.T, opID uint64) *api.ApplyRequest {
	t.Helper()
	class := fmt.Sprintf("Class%d", opID)
	sub, err := json.Marshal(&api.ReplicationAddReplicaToShard{OpId: opID, Class: class, Shard: "S1", TargetNode: "Node-2"})
	require.NoError(t, err)
	return &api.ApplyRequest{Type: api.ApplyRequest_TYPE_REPLICATION_REPLICATE_ADD_REPLICA_TO_SHARD, Class: class, SubCommand: sub}
}

func updateOpStateCmd(t *testing.T, opID uint64, state api.ShardReplicationState) *api.ApplyRequest {
	t.Helper()
	sub, err := json.Marshal(&api.ReplicationUpdateOpStateRequest{Id: opID, State: state})
	require.NoError(t, err)
	return &api.ApplyRequest{Type: api.ApplyRequest_TYPE_REPLICATION_REPLICATE_UPDATE_STATE, SubCommand: sub}
}

func addShardedClassCmd(t *testing.T, class, shard, node string) *api.ApplyRequest {
	t.Helper()
	sub, err := json.Marshal(&api.AddClassRequest{
		Class: &models.Class{Class: class},
		State: &sharding.State{Physical: map[string]sharding.Physical{shard: {Name: shard, BelongsToNodes: []string{node}}}},
	})
	require.NoError(t, err)
	return &api.ApplyRequest{Type: api.ApplyRequest_TYPE_ADD_CLASS, Class: class, SubCommand: sub}
}

func receiveErr(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for Execute")
		return nil
	}
}

func opLockRefs(l *opKeyLocks, id uint64) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if k, ok := l.locks[id]; ok {
		return k.refs
	}
	return 0
}

func TestReplicaAddFence_ProposeAdmission(t *testing.T) {
	st, _ := newReplicaAddFenceStore(t)
	fsm := st.replicationManager.GetReplicationFSM()

	tests := []struct {
		name    string
		setup   func(id uint64)
		wantErr error
	}{
		{
			name:    "op missing",
			setup:   func(uint64) {},
			wantErr: replicationTypes.ErrAddReplicaOpNotFinalizing,
		},
		{
			name:    "registered",
			setup:   func(id uint64) { registerFenceOp(t, fsm, id) },
			wantErr: replicationTypes.ErrAddReplicaOpNotFinalizing,
		},
		{
			name:    "hydrating",
			setup:   func(id uint64) { registerFenceOp(t, fsm, id, api.HYDRATING) },
			wantErr: replicationTypes.ErrAddReplicaOpNotFinalizing,
		},
		{
			name:    "rewound from finalizing to hydrating",
			setup:   func(id uint64) { registerFenceOp(t, fsm, id, api.HYDRATING, api.FINALIZING, api.HYDRATING) },
			wantErr: replicationTypes.ErrAddReplicaOpNotFinalizing,
		},
		{
			name:    "integrating",
			setup:   func(id uint64) { registerFenceOp(t, fsm, id, api.HYDRATING, api.FINALIZING, api.INTEGRATING) },
			wantErr: replicationTypes.ErrAddReplicaOpNotFinalizing,
		},
		{
			name: "finalizing with cancel requested",
			setup: func(id uint64) {
				opUUID := registerFenceOp(t, fsm, id, api.HYDRATING, api.FINALIZING)
				require.NoError(t, fsm.CancelReplication(&api.ReplicationCancelRequest{Uuid: opUUID}))
			},
			wantErr: replicationTypes.ErrOpCancellationInFlight,
		},
		{
			name:    "cancelled",
			setup:   func(id uint64) { registerFenceOp(t, fsm, id, api.HYDRATING, api.CANCELLED) },
			wantErr: replicationTypes.ErrOpCancellationInFlight,
		},
		{
			name:    "finalizing",
			setup:   func(id uint64) { registerFenceOp(t, fsm, id, api.HYDRATING, api.FINALIZING) },
			wantErr: errAddReachedApply,
		},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opID := uint64(1000 + i)
			tc.setup(opID)
			before := settledLogIndex(t, st)

			_, err := st.Execute(addReplicaCmd(t, opID))

			require.ErrorIs(t, err, tc.wantErr)
			if errors.Is(tc.wantErr, errAddReachedApply) {
				require.Equal(t, before+1, st.raft.Load().LastIndex())
				return
			}
			require.Equal(t, before, st.raft.Load().LastIndex(), "a refused add must not be appended")
		})
	}
	require.Zero(t, st.replicaOpLocks.len())
}

func TestReplicaAddFence_FinalizingRetryAfterCommittedAddIsAdmitted(t *testing.T) {
	st, _ := newReplicaAddFenceStore(t, 7)
	registerFenceOp(t, st.replicationManager.GetReplicationFSM(), 7, api.HYDRATING, api.FINALIZING)
	_, err := st.Execute(addShardedClassCmd(t, "Class7", "S1", "Node-1"))
	require.NoError(t, err)

	_, err = st.Execute(addReplicaCmd(t, 7))
	require.NoError(t, err)
	replicas, err := st.SchemaReader().ShardReplicas("Class7", "S1")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"Node-1", "Node-2"}, replicas)

	_, err = st.Execute(addReplicaCmd(t, 7))
	require.ErrorIs(t, err, errAddReachedApply)
}

func TestReplicaAddFence_OpLockIsTakenBeforeTheBarrier(t *testing.T) {
	st, m := newReplicaAddFenceStore(t)
	registerFenceOp(t, st.replicationManager.GetReplicationFSM(), 9, api.HYDRATING, api.FINALIZING)
	m.indexer.ExpectedCalls = nil
	m.indexer.On("Open", mock.Anything).Return(nil)
	m.indexer.On("Close", mock.Anything).Return(nil)
	m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
	m.indexer.On("AddClass", mock.Anything).Run(func(mock.Arguments) { time.Sleep(300 * time.Millisecond) }).Return(nil)

	st.replicaOpLocks.lock(9)
	addErr := make(chan error, 1)
	enterrors.GoWrapper(func() {
		_, err := st.Execute(addReplicaCmd(t, 9))
		addErr <- err
	}, st.log)
	require.Eventually(t, func() bool { return opLockRefs(st.replicaOpLocks, 9) == 2 }, 5*time.Second, 5*time.Millisecond)

	slow, err := proto.Marshal(addClassCmd(t, "Slow9"))
	require.NoError(t, err)
	rewind, err := proto.Marshal(updateOpStateCmd(t, 9, api.HYDRATING))
	require.NoError(t, err)
	slowFut := st.raft.Load().Apply(slow, st.applyTimeout)
	rewindFut := st.raft.Load().Apply(rewind, st.applyTimeout)
	st.fsmCaughtUpTerm.Store(0)
	st.replicaOpLocks.unlock(9)

	require.ErrorIs(t, receiveErr(t, addErr), replicationTypes.ErrAddReplicaOpNotFinalizing)
	require.NoError(t, slowFut.Error())
	require.NoError(t, rewindFut.Error())
}

func TestReplicaAddFence_RewindWaitsForAnAdmittedAdd(t *testing.T) {
	st, _ := newReplicaAddFenceStore(t)
	fsm := st.replicationManager.GetReplicationFSM()
	registerFenceOp(t, fsm, 1, api.HYDRATING, api.FINALIZING)
	registerFenceOp(t, fsm, 2, api.HYDRATING)

	admitted := make(chan struct{})
	release := make(chan struct{})
	releaseAdd := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseAdd)
	st.replicaAddAdmittedHook = func() {
		close(admitted)
		<-release
	}

	var mu sync.Mutex
	var order []string
	record := func(s string) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, s)
	}
	addErr := make(chan error, 1)
	rewindErr := make(chan error, 1)

	enterrors.GoWrapper(func() {
		_, err := st.Execute(addReplicaCmd(t, 1))
		record("add")
		addErr <- err
	}, st.log)
	select {
	case <-admitted:
	case <-time.After(5 * time.Second):
		t.Fatal("add was never admitted")
	}

	enterrors.GoWrapper(func() {
		_, err := st.Execute(updateOpStateCmd(t, 1, api.HYDRATING))
		record("rewind")
		rewindErr <- err
	}, st.log)

	otherErr := make(chan error, 1)
	enterrors.GoWrapper(func() {
		_, err := st.Execute(updateOpStateCmd(t, 2, api.FINALIZING))
		otherErr <- err
	}, st.log)
	require.NoError(t, receiveErr(t, otherErr), "another op must not wait on this op's lock")
	op2, ok := fsm.GetOpById(2)
	require.True(t, ok)
	require.Equal(t, api.FINALIZING, op2.Status.GetCurrentState())

	select {
	case err := <-rewindErr:
		t.Fatalf("rewind appended between the add's check and its append: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	op1, ok := fsm.GetOpById(1)
	require.True(t, ok)
	require.Equal(t, api.FINALIZING, op1.Status.GetCurrentState())

	releaseAdd()
	require.ErrorIs(t, receiveErr(t, addErr), errAddReachedApply)
	require.NoError(t, receiveErr(t, rewindErr))
	require.Equal(t, []string{"add", "rewind"}, order)
	op1, ok = fsm.GetOpById(1)
	require.True(t, ok)
	require.Equal(t, api.HYDRATING, op1.Status.GetCurrentState())
	require.Zero(t, st.replicaOpLocks.len())
}

func TestReplicaAddFence_AddAfterRewindIsRefused(t *testing.T) {
	st, _ := newReplicaAddFenceStore(t)
	registerFenceOp(t, st.replicationManager.GetReplicationFSM(), 3, api.HYDRATING, api.FINALIZING)

	_, err := st.Execute(updateOpStateCmd(t, 3, api.HYDRATING))
	require.NoError(t, err)
	before := settledLogIndex(t, st)

	_, err = st.Execute(addReplicaCmd(t, 3))

	require.ErrorIs(t, err, replicationTypes.ErrAddReplicaOpNotFinalizing)
	require.Equal(t, before, st.raft.Load().LastIndex())
}

func TestOpKeyLocks(t *testing.T) {
	tests := []struct {
		name    string
		ids     int
		callers int
	}{
		{name: "one id many callers", ids: 1, callers: 64},
		{name: "many ids", ids: 32, callers: 4},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l := newOpKeyLocks()
			logger, _ := logrustest.NewNullLogger()
			held := make([]int, tc.ids)
			var wg sync.WaitGroup
			for id := 0; id < tc.ids; id++ {
				for c := 0; c < tc.callers; c++ {
					wg.Add(1)
					enterrors.GoWrapper(func() {
						defer wg.Done()
						l.lock(uint64(id))
						held[id]++
						assert.Equal(t, 1, held[id])
						held[id]--
						l.unlock(uint64(id))
					}, logger)
				}
			}
			wg.Wait()
			require.Zero(t, l.len())
		})
	}
}
