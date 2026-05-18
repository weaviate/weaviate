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

package replication

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication/types"
)

func seedOp(t *testing.T, fsm *ShardReplicationFSM, opID uint64) strfmt.UUID {
	t.Helper()
	return seedOpOfType(t, fsm, opID, api.COPY)
}

func seedOpOfType(t *testing.T, fsm *ShardReplicationFSM, opID uint64, transferType api.ShardReplicationTransferType) strfmt.UUID {
	t.Helper()
	uuid := strfmt.UUID("00000000-0000-0000-0000-00000000000" + string(rune('0'+opID%10)))
	require.NoError(t, fsm.Replicate(opID, &api.ReplicationReplicateShardRequest{
		Version:          api.ReplicationCommandVersionV0,
		Uuid:             uuid,
		SourceNode:       "node1",
		SourceCollection: "TestClass",
		SourceShard:      "shard1",
		TargetNode:       "node2",
		TransferType:     transferType.String(),
	}))
	return uuid
}

// driveToState advances the op via UpdateReplicationOpStatus. CANCELLED
// requires CancellationComplete (the FSM rejects it here) — see driveToCancelled.
func driveToState(t *testing.T, fsm *ShardReplicationFSM, opID uint64, state api.ShardReplicationState) {
	t.Helper()
	if state == api.REGISTERED {
		return
	}
	err := fsm.UpdateReplicationOpStatus(&api.ReplicationUpdateOpStateRequest{
		Version: api.ReplicationCommandVersionV0,
		Id:      opID,
		State:   state,
	}, 0)
	require.NoError(t, err)
}

func driveToCancelled(t *testing.T, fsm *ShardReplicationFSM, opID uint64) {
	t.Helper()
	require.NoError(t, fsm.CancellationComplete(&api.ReplicationCancellationCompleteRequest{
		Version: api.ReplicationCommandVersionV0,
		Id:      opID,
	}))
}

// TestShardReplicationFSM_CancelReplication firewalls the cancel-conflict
// contract: once UnCancellable flips, CancelReplication returns
// ErrCancellationImpossible regardless of state.
func TestShardReplicationFSM_CancelReplication(t *testing.T) {
	cases := []struct {
		name             string
		state            api.ShardReplicationState
		unCancellable    bool
		driveCancelled   bool
		wantErr          error
		wantShouldCancel bool
		wantShouldDelete bool
	}{
		{
			name:             "cancellable HYDRATING op succeeds",
			state:            api.HYDRATING,
			unCancellable:    false,
			wantErr:          nil,
			wantShouldCancel: true,
			wantShouldDelete: false,
		},
		{
			name:          "uncancellable FINALIZING op rejects",
			state:         api.FINALIZING,
			unCancellable: true,
			wantErr:       types.ErrCancellationImpossible,
		},
		{
			name:          "uncancellable DEHYDRATING op rejects",
			state:         api.DEHYDRATING,
			unCancellable: true,
			wantErr:       types.ErrCancellationImpossible,
		},
		{
			name:          "uncancellable READY op rejects (UnCancellable persists past READY)",
			state:         api.READY,
			unCancellable: true,
			wantErr:       types.ErrCancellationImpossible,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fsm := NewShardReplicationFSM(prometheus.NewRegistry())
			const opID uint64 = 1
			uuid := seedOp(t, fsm, opID)
			driveToState(t, fsm, opID, tc.state)
			if tc.driveCancelled {
				driveToCancelled(t, fsm, opID)
			}
			if tc.unCancellable {
				require.NoError(t, fsm.SetUnCancellable(opID))
			}

			err := fsm.CancelReplication(&api.ReplicationCancelRequest{
				Version: api.ReplicationCommandVersionV0,
				Uuid:    uuid,
			})

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}

			status, ok := fsm.statusById[opID]
			require.True(t, ok)
			require.Equal(t, tc.wantShouldCancel, status.ShouldCancel, "ShouldCancel")
			require.Equal(t, tc.wantShouldDelete, status.ShouldDelete, "ShouldDelete")
		})
	}

	t.Run("unknown UUID wraps ErrReplicationOperationNotFound", func(t *testing.T) {
		fsm := NewShardReplicationFSM(prometheus.NewRegistry())
		err := fsm.CancelReplication(&api.ReplicationCancelRequest{
			Version: api.ReplicationCommandVersionV0,
			Uuid:    strfmt.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
		})
		require.ErrorIs(t, err, types.ErrReplicationOperationNotFound)
	})
}

// TestShardReplicationFSM_DeleteReplication firewalls the delete-conflict
// contract: delete is rejected only when UnCancellable=true AND state != READY.
// CANCELLED is included in the rejection — it is not READY.
func TestShardReplicationFSM_DeleteReplication(t *testing.T) {
	cases := []struct {
		name             string
		state            api.ShardReplicationState
		unCancellable    bool
		driveCancelled   bool
		wantErr          error
		wantShouldCancel bool
		wantShouldDelete bool
	}{
		{
			name:             "cancellable HYDRATING op succeeds",
			state:            api.HYDRATING,
			unCancellable:    false,
			wantErr:          nil,
			wantShouldCancel: true,
			wantShouldDelete: true,
		},
		{
			name:          "uncancellable FINALIZING op rejects",
			state:         api.FINALIZING,
			unCancellable: true,
			wantErr:       types.ErrDeletionImpossible,
		},
		{
			name:          "uncancellable DEHYDRATING op rejects",
			state:         api.DEHYDRATING,
			unCancellable: true,
			wantErr:       types.ErrDeletionImpossible,
		},
		{
			name:             "uncancellable READY op succeeds (READY is the documented exception)",
			state:            api.READY,
			unCancellable:    true,
			wantErr:          nil,
			wantShouldCancel: true,
			wantShouldDelete: true,
		},
		{
			name:           "uncancellable CANCELLED op rejects (CANCELLED is not READY)",
			driveCancelled: true,
			unCancellable:  true,
			wantErr:        types.ErrDeletionImpossible,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fsm := NewShardReplicationFSM(prometheus.NewRegistry())
			const opID uint64 = 1
			uuid := seedOp(t, fsm, opID)
			if !tc.driveCancelled {
				driveToState(t, fsm, opID, tc.state)
			} else {
				driveToCancelled(t, fsm, opID)
			}
			if tc.unCancellable {
				require.NoError(t, fsm.SetUnCancellable(opID))
			}

			err := fsm.DeleteReplication(&api.ReplicationDeleteRequest{
				Version: api.ReplicationCommandVersionV0,
				Uuid:    uuid,
			})

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}

			status, ok := fsm.statusById[opID]
			require.True(t, ok)
			require.Equal(t, tc.wantShouldCancel, status.ShouldCancel, "ShouldCancel")
			require.Equal(t, tc.wantShouldDelete, status.ShouldDelete, "ShouldDelete")
		})
	}

	t.Run("unknown UUID wraps ErrReplicationOperationNotFound", func(t *testing.T) {
		fsm := NewShardReplicationFSM(prometheus.NewRegistry())
		err := fsm.DeleteReplication(&api.ReplicationDeleteRequest{
			Version: api.ReplicationCommandVersionV0,
			Uuid:    strfmt.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
		})
		require.ErrorIs(t, err, types.ErrReplicationOperationNotFound)
	})
}

// TestShardReplicationFSM_SetUnCancellable smoke-tests the helper itself;
// the atomicity with addReplicaToShard lives in the SchemaManager test.
func TestShardReplicationFSM_SetUnCancellable(t *testing.T) {
	t.Run("flips UnCancellable on a known op", func(t *testing.T) {
		fsm := NewShardReplicationFSM(prometheus.NewRegistry())
		const opID uint64 = 7
		seedOp(t, fsm, opID)

		require.False(t, fsm.statusById[opID].UnCancellable, "should start cancellable")
		require.NoError(t, fsm.SetUnCancellable(opID))
		require.True(t, fsm.statusById[opID].UnCancellable)
	})

	t.Run("unknown id wraps ErrReplicationOperationNotFound", func(t *testing.T) {
		fsm := NewShardReplicationFSM(prometheus.NewRegistry())
		err := fsm.SetUnCancellable(999)
		require.ErrorIs(t, err, types.ErrReplicationOperationNotFound)
	})
}
