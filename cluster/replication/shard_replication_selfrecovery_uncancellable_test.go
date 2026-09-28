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

package replication_test

import (
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type cancelAction string

const (
	actionCancel      cancelAction = "cancel"
	actionDelete      cancelAction = "delete"
	actionDeleteAll   cancelAction = "delete all"
	actionErrorBudget cancelAction = "error budget"
)

const (
	selfRecoveryColl       = "TestClass"
	selfRecoveryShard      = "shard1"
	selfRecoveryDonorID    = "node1"
	selfRecoveryTarget     = "node2"
	selfRecoveryAnyOtherID = "node3"
	selfRecoveryOpID       = uint64(3)
)

func newShardSchemaReader(collection, shardName string, nodes ...string) schema.SchemaReader {
	parser := fakes.NewMockParser()
	parser.On("ParseClass", mock.Anything).Return(nil)
	schemaManager := schema.NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
	schemaManager.AddClass(
		buildApplyRequest(collection, api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
			Class: &models.Class{Class: collection, MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
			State: &sharding.State{
				Physical: map[string]sharding.Physical{shardName: {BelongsToNodes: nodes}},
			},
		}), "node1", true, false)
	return schemaManager.NewSchemaReader()
}

func applyCancelAction(t *testing.T, m *replication.Manager, uuid strfmt.UUID, opID uint64, action cancelAction) error {
	t.Helper()
	fsm := m.GetReplicationFSM()
	switch action {
	case actionCancel:
		return fsm.CancelReplication(&api.ReplicationCancelRequest{Uuid: uuid})
	case actionDelete:
		return fsm.DeleteReplication(&api.ReplicationDeleteRequest{Uuid: uuid})
	case actionDeleteAll:
		return fsm.DeleteAllReplications(&api.ReplicationDeleteAllRequest{})
	case actionErrorBudget:
		var err error
		for i := 0; i <= replication.MaxErrors; i++ {
			err = m.RegisterError(buildApplyRequest(selfRecoveryColl, api.ApplyRequest_TYPE_REPLICATION_REPLICATE_REGISTER_ERROR,
				api.ReplicationRegisterErrorRequest{Id: opID, Error: fmt.Sprintf("copy failed %d", i)}))
		}
		return err
	}
	t.Fatalf("unknown action %q", action)
	return nil
}

func completeAcceptedCancel(t *testing.T, fsm *replication.ShardReplicationFSM, opID uint64) {
	t.Helper()
	op, ok := fsm.GetOpById(opID)
	require.True(t, ok)
	switch {
	case op.Status.ShouldDelete:
		require.NoError(t, fsm.RemoveReplicationOp(&api.ReplicationRemoveOpRequest{Id: opID}))
	case op.Status.ShouldCancel:
		require.NoError(t, fsm.CancellationComplete(&api.ReplicationCancellationCompleteRequest{Id: opID}))
	}
}

func targetRouted(fsm *replication.ShardReplicationFSM) (bool, bool) {
	replicas := []string{selfRecoveryDonorID, selfRecoveryTarget, selfRecoveryAnyOtherID}
	read := fsm.FilterOneShardReplicasRead(selfRecoveryColl, selfRecoveryShard, replicas)
	write := fsm.FilterOneShardReplicasWrite(selfRecoveryColl, selfRecoveryShard, replicas)
	contains := func(s []string) bool {
		for _, n := range s {
			if n == selfRecoveryTarget {
				return true
			}
		}
		return false
	}
	return contains(read), contains(write)
}

func TestSelfRecoveryUncancellableAfterFinalizing(t *testing.T) {
	type row struct {
		name         string
		transfer     api.ShardReplicationTransferType
		path         []api.ShardReplicationState
		cancelBefore bool
		action       cancelAction
		wantErr      error
		wantCancel   bool
		wantUncanc   bool
		wantRouted   bool
	}
	pastFinalizing := map[string][]api.ShardReplicationState{
		"FINALIZING":  {api.HYDRATING, api.FINALIZING},
		"INTEGRATING": {api.HYDRATING, api.FINALIZING, api.INTEGRATING},
	}
	refusal := map[cancelAction]error{
		actionCancel:      types.ErrCancellationImpossible,
		actionDelete:      types.ErrDeletionImpossible,
		actionDeleteAll:   nil,
		actionErrorBudget: types.ErrCancellationImpossible,
	}
	actions := []cancelAction{actionCancel, actionDelete, actionDeleteAll, actionErrorBudget}
	var rows []row
	for stateName, path := range pastFinalizing {
		for _, a := range actions {
			rows = append(rows, row{
				name:       fmt.Sprintf("SELF_RECOVERY %s refuses %s", stateName, a),
				transfer:   api.SELF_RECOVERY,
				path:       path,
				action:     a,
				wantErr:    refusal[a],
				wantUncanc: true,
				wantRouted: path[len(path)-1] == api.INTEGRATING,
			})
		}
	}
	for _, a := range actions {
		rows = append(rows,
			row{
				name:       fmt.Sprintf("SELF_RECOVERY HYDRATING allows %s", a),
				transfer:   api.SELF_RECOVERY,
				path:       []api.ShardReplicationState{api.HYDRATING},
				action:     a,
				wantCancel: true,
				wantRouted: true,
			},
			row{
				name:         fmt.Sprintf("SELF_RECOVERY cancel won before FINALIZING allows %s", a),
				transfer:     api.SELF_RECOVERY,
				path:         []api.ShardReplicationState{api.HYDRATING, api.FINALIZING},
				cancelBefore: true,
				action:       a,
				wantCancel:   true,
				wantRouted:   true,
			},
			row{
				name:       fmt.Sprintf("MOVE HYDRATING from a recovering source allows %s", a),
				transfer:   api.MOVE,
				path:       []api.ShardReplicationState{api.HYDRATING},
				action:     a,
				wantCancel: true,
				wantRouted: true,
			},
			row{
				name:       fmt.Sprintf("COPY FINALIZING before the add allows %s", a),
				transfer:   api.COPY,
				path:       []api.ShardReplicationState{api.HYDRATING, api.FINALIZING},
				action:     a,
				wantCancel: true,
				wantRouted: true,
			},
		)
	}
	rows = append(rows,
		row{
			name:       "SELF_RECOVERY READY allows delete",
			transfer:   api.SELF_RECOVERY,
			path:       []api.ShardReplicationState{api.HYDRATING, api.FINALIZING, api.INTEGRATING, api.READY},
			action:     actionDelete,
			wantCancel: true,
			wantUncanc: true,
			wantRouted: true,
		},
		row{
			name:       "SELF_RECOVERY READY allows delete all",
			transfer:   api.SELF_RECOVERY,
			path:       []api.ShardReplicationState{api.HYDRATING, api.FINALIZING, api.INTEGRATING, api.READY},
			action:     actionDeleteAll,
			wantCancel: true,
			wantUncanc: true,
			wantRouted: true,
		},
	)

	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			m := replication.NewManager(newShardSchemaReader(selfRecoveryColl, selfRecoveryShard, selfRecoveryDonorID, selfRecoveryTarget, selfRecoveryAnyOtherID),
				mocks.NewMockNodeSelector("localhost"), prometheus.NewPedanticRegistry())
			fsm := m.GetReplicationFSM()
			uuid := seedOpFull(t, fsm, selfRecoveryOpID, selfRecoveryDonorID, selfRecoveryTarget, selfRecoveryColl, selfRecoveryShard, tc.transfer)
			for i, st := range tc.path {
				if tc.cancelBefore && i == len(tc.path)-1 {
					require.NoError(t, fsm.CancelReplication(&api.ReplicationCancelRequest{Uuid: uuid}))
				}
				driveToState(t, fsm, selfRecoveryOpID, st)
			}

			err := applyCancelAction(t, m, uuid, selfRecoveryOpID, tc.action)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}

			op, ok := fsm.GetOpById(selfRecoveryOpID)
			require.True(t, ok)
			require.Equal(t, tc.wantCancel, op.Status.ShouldCancel || op.Status.ShouldDelete)
			require.Equal(t, tc.wantUncanc, op.Status.UnCancellable)

			completeAcceptedCancel(t, fsm, selfRecoveryOpID)
			read, write := targetRouted(fsm)
			require.Equal(t, tc.wantRouted, read)
			require.Equal(t, tc.wantRouted, write)
		})
	}
}

func TestSelfRecoveryUncancellableSurvivesSnapshot(t *testing.T) {
	fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
	uuid := seedOpFull(t, fsm, selfRecoveryOpID, selfRecoveryDonorID, selfRecoveryTarget, selfRecoveryColl, selfRecoveryShard, api.SELF_RECOVERY)
	driveToState(t, fsm, selfRecoveryOpID, api.HYDRATING)
	driveToState(t, fsm, selfRecoveryOpID, api.FINALIZING)

	snap, err := fsm.Snapshot()
	require.NoError(t, err)
	restored := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
	require.NoError(t, restored.Restore(snap))

	require.ErrorIs(t, restored.CancelReplication(&api.ReplicationCancelRequest{Uuid: uuid}), types.ErrCancellationImpossible)
	op, ok := restored.GetOpById(selfRecoveryOpID)
	require.True(t, ok)
	require.True(t, op.Status.UnCancellable)
}
