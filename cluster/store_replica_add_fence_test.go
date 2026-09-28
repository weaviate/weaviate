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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

var errAddReachedApply = errors.New("add reached apply")

func newReplicaAddFenceStore(t *testing.T) *Store {
	t.Helper()
	srv, m := newBarrierTestStore(t)
	m.replicationFSM.EXPECT().SetUnCancellable(mock.Anything).Return(errAddReachedApply).Maybe()
	require.NoError(t, srv.store.waitLeaderFSMCaughtUp())
	return srv.store
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

func TestReplicaAddFence_ProposeAdmission(t *testing.T) {
	st := newReplicaAddFenceStore(t)
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
	st := newReplicaAddFenceStore(t)
	registerFenceOp(t, st.replicationManager.GetReplicationFSM(), 7, api.HYDRATING, api.FINALIZING)

	for i := 0; i < 2; i++ {
		_, err := st.Execute(addReplicaCmd(t, 7))
		require.ErrorIs(t, err, errAddReachedApply)
	}
}

func TestReplicaAddFence_RewindWaitsForAnAdmittedAdd(t *testing.T) {
	st := newReplicaAddFenceStore(t)
	fsm := st.replicationManager.GetReplicationFSM()
	registerFenceOp(t, fsm, 1, api.HYDRATING, api.FINALIZING)
	registerFenceOp(t, fsm, 2, api.HYDRATING)

	admitted := make(chan struct{})
	release := make(chan struct{})
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
	<-admitted

	enterrors.GoWrapper(func() {
		_, err := st.Execute(updateOpStateCmd(t, 1, api.HYDRATING))
		record("rewind")
		rewindErr <- err
	}, st.log)

	_, err := st.Execute(updateOpStateCmd(t, 2, api.FINALIZING))
	require.NoError(t, err, "another op must not wait on this op's lock")
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

	close(release)
	require.ErrorIs(t, <-addErr, errAddReachedApply)
	require.NoError(t, <-rewindErr)
	require.Equal(t, []string{"add", "rewind"}, order)
	op1, ok = fsm.GetOpById(1)
	require.True(t, ok)
	require.Equal(t, api.HYDRATING, op1.Status.GetCurrentState())
	require.Zero(t, st.replicaOpLocks.len())
}

func TestReplicaAddFence_AddAfterRewindIsRefused(t *testing.T) {
	st := newReplicaAddFenceStore(t)
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
						require.Equal(t, 1, held[id])
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
