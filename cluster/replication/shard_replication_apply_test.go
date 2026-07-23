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
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/replication/types"
)

func seedOp(t *testing.T, fsm *replication.ShardReplicationFSM, opID uint64) strfmt.UUID {
	t.Helper()
	return seedOpOfType(t, fsm, opID, api.COPY)
}

func seedOpOfType(t *testing.T, fsm *replication.ShardReplicationFSM, opID uint64, transferType api.ShardReplicationTransferType) strfmt.UUID {
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

func seedOpFull(t *testing.T, fsm *replication.ShardReplicationFSM, opID uint64, srcNode, tgtNode, collection, shard string, transferType api.ShardReplicationTransferType) strfmt.UUID {
	t.Helper()
	uuid := strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", opID))
	require.NoError(t, fsm.Replicate(opID, &api.ReplicationReplicateShardRequest{
		Version:          api.ReplicationCommandVersionV0,
		Uuid:             uuid,
		SourceNode:       srcNode,
		SourceCollection: collection,
		SourceShard:      shard,
		TargetNode:       tgtNode,
		TransferType:     transferType.String(),
	}))
	return uuid
}

// driveToState advances the op via UpdateReplicationOpStatus. CANCELLED
// requires CancellationComplete (the FSM rejects it here) — see driveToCancelled.
func driveToState(t *testing.T, fsm *replication.ShardReplicationFSM, opID uint64, state api.ShardReplicationState) {
	t.Helper()
	if state == api.REGISTERED {
		return
	}
	err := fsm.UpdateReplicationOpStatus(&api.ReplicationUpdateOpStateRequest{
		Version: api.ReplicationCommandVersionV0,
		Id:      opID,
		State:   state,
	})
	require.NoError(t, err)
}

func driveToCancelled(t *testing.T, fsm *replication.ShardReplicationFSM, opID uint64) {
	t.Helper()
	require.NoError(t, fsm.CancellationComplete(&api.ReplicationCancellationCompleteRequest{
		Version: api.ReplicationCommandVersionV0,
		Id:      opID,
	}))
}

// admissionOp declares one op for a TestReplicate_Admission row: it is seeded via
// Replicate (which is itself subject to admission), then driven to its state.
type admissionOp struct {
	id           uint64
	srcNode      string
	tgtNode      string
	transferType api.ShardReplicationTransferType
	state        api.ShardReplicationState
}

func TestReplicate_Admission(t *testing.T) {
	const (
		coll  = "TestClass"
		shard = "shard1"
	)

	cases := []struct {
		name string
		// setup ops are admitted (must all succeed) and driven to their state, then
		// final is the op under test.
		setup   []admissionOp
		final   admissionOp
		wantErr bool // true ⇒ final must be rejected with ErrShardAlreadyReplicating
	}{
		// --- source-guard isolation: shared source FQDN, disjoint targets ---
		{
			name:    "source: two active MOVEs rejected",
			setup:   []admissionOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.MOVE, state: api.HYDRATING}},
			final:   admissionOp{id: 2, srcNode: "node1", tgtNode: "node3", transferType: api.MOVE, state: api.REGISTERED},
			wantErr: true,
		},
		{
			name:    "source: cancelled MOVE then new MOVE allowed",
			setup:   []admissionOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.MOVE, state: api.CANCELLED}},
			final:   admissionOp{id: 2, srcNode: "node1", tgtNode: "node3", transferType: api.MOVE, state: api.REGISTERED},
			wantErr: false,
		},
		{
			name:    "source: completed COPY (READY) then new MOVE allowed",
			setup:   []admissionOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.COPY, state: api.READY}},
			final:   admissionOp{id: 2, srcNode: "node1", tgtNode: "node3", transferType: api.MOVE, state: api.REGISTERED},
			wantErr: false,
		},
		{
			name:    "source: active COPY then COPY allowed",
			setup:   []admissionOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.COPY, state: api.HYDRATING}},
			final:   admissionOp{id: 2, srcNode: "node1", tgtNode: "node3", transferType: api.COPY, state: api.REGISTERED},
			wantErr: false,
		},
		// --- target-guard isolation: shared target FQDN, disjoint sources ---
		{
			name:    "target: two active MOVEs from distinct sources rejected",
			setup:   []admissionOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.MOVE, state: api.HYDRATING}},
			final:   admissionOp{id: 2, srcNode: "node3", tgtNode: "node2", transferType: api.MOVE, state: api.REGISTERED},
			wantErr: true,
		},
		{
			name:    "target: active COPY then MOVE from distinct source rejected",
			setup:   []admissionOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.COPY, state: api.HYDRATING}},
			final:   admissionOp{id: 2, srcNode: "node3", tgtNode: "node2", transferType: api.MOVE, state: api.REGISTERED},
			wantErr: true,
		},
		{
			name:    "target: cancelled MOVE then active MOVE from distinct source allowed",
			setup:   []admissionOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.MOVE, state: api.CANCELLED}},
			final:   admissionOp{id: 2, srcNode: "node3", tgtNode: "node2", transferType: api.MOVE, state: api.REGISTERED},
			wantErr: false,
		},
		{
			name:    "target: completed COPY (READY) then active COPY from distinct source allowed",
			setup:   []admissionOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.COPY, state: api.READY}},
			final:   admissionOp{id: 2, srcNode: "node3", tgtNode: "node2", transferType: api.COPY, state: api.REGISTERED},
			wantErr: false,
		},
		{
			name:    "target: two active COPYs from distinct sources allowed",
			setup:   []admissionOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.COPY, state: api.HYDRATING}},
			final:   admissionOp{id: 2, srcNode: "node3", tgtNode: "node2", transferType: api.COPY, state: api.REGISTERED},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
			for _, op := range tc.setup {
				seedOpFull(t, fsm, op.id, op.srcNode, op.tgtNode, coll, shard, op.transferType)
				switch op.state {
				case api.REGISTERED:
				case api.CANCELLED:
					driveToCancelled(t, fsm, op.id)
				default:
					driveToState(t, fsm, op.id, op.state)
				}
			}

			uuid := strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", tc.final.id))
			err := fsm.Replicate(tc.final.id, &api.ReplicationReplicateShardRequest{
				Version:          api.ReplicationCommandVersionV0,
				Uuid:             uuid,
				SourceNode:       tc.final.srcNode,
				SourceCollection: coll,
				SourceShard:      shard,
				TargetNode:       tc.final.tgtNode,
				TransferType:     tc.final.transferType.String(),
			})

			if tc.wantErr {
				require.ErrorIs(t, err, replication.ErrShardAlreadyReplicating)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestReplicate_RejectsSourceFromInFlightTarget(t *testing.T) {
	const (
		coll  = "TestClass"
		shard = "shard1"
	)
	cases := []struct {
		name    string
		setup   []admissionOp
		final   admissionOp
		wantErr bool
	}{
		{
			name:    "MOVE sourcing a replica still being moved in is rejected",
			setup:   []admissionOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.MOVE, state: api.INTEGRATING}},
			final:   admissionOp{id: 2, srcNode: "node2", tgtNode: "node3", transferType: api.MOVE, state: api.REGISTERED},
			wantErr: true,
		},
		{
			name:    "COPY sourcing a replica still being moved in is rejected (incomplete read)",
			setup:   []admissionOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.MOVE, state: api.INTEGRATING}},
			final:   admissionOp{id: 2, srcNode: "node2", tgtNode: "node3", transferType: api.COPY, state: api.REGISTERED},
			wantErr: true,
		},
		{
			name:    "MOVE sourcing a replica still being copied in is rejected",
			setup:   []admissionOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.COPY, state: api.HYDRATING}},
			final:   admissionOp{id: 2, srcNode: "node2", tgtNode: "node3", transferType: api.MOVE, state: api.REGISTERED},
			wantErr: true,
		},
		{
			name:    "MOVE sourcing a FINALIZING target is rejected",
			setup:   []admissionOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.MOVE, state: api.FINALIZING}},
			final:   admissionOp{id: 2, srcNode: "node2", tgtNode: "node3", transferType: api.MOVE, state: api.REGISTERED},
			wantErr: true,
		},
		{
			// DEHYDRATING n2 is complete (its drain finished before this state), but we
			// err closed and reject until READY — pins the conservative boundary.
			name:    "MOVE sourcing a DEHYDRATING target is rejected (conservative)",
			setup:   []admissionOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.MOVE, state: api.DEHYDRATING}},
			final:   admissionOp{id: 2, srcNode: "node2", tgtNode: "node3", transferType: api.MOVE, state: api.REGISTERED},
			wantErr: true,
		},
		{
			name:    "sourcing a READY target is allowed",
			setup:   []admissionOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.MOVE, state: api.READY}},
			final:   admissionOp{id: 2, srcNode: "node2", tgtNode: "node3", transferType: api.MOVE, state: api.REGISTERED},
			wantErr: false,
		},
		{
			name:    "sourcing a CANCELLED target is allowed",
			setup:   []admissionOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.MOVE, state: api.CANCELLED}},
			final:   admissionOp{id: 2, srcNode: "node2", tgtNode: "node3", transferType: api.MOVE, state: api.REGISTERED},
			wantErr: false,
		},
		{
			name:    "disjoint moves are allowed",
			setup:   []admissionOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.MOVE, state: api.INTEGRATING}},
			final:   admissionOp{id: 2, srcNode: "node3", tgtNode: "node4", transferType: api.MOVE, state: api.REGISTERED},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
			for _, op := range tc.setup {
				seedOpFull(t, fsm, op.id, op.srcNode, op.tgtNode, coll, shard, op.transferType)
				switch op.state {
				case api.REGISTERED:
				case api.CANCELLED:
					driveToCancelled(t, fsm, op.id)
				default:
					driveToState(t, fsm, op.id, op.state)
				}
			}

			uuid := strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", tc.final.id))
			err := fsm.Replicate(tc.final.id, &api.ReplicationReplicateShardRequest{
				Version:          api.ReplicationCommandVersionV0,
				Uuid:             uuid,
				SourceNode:       tc.final.srcNode,
				SourceCollection: coll,
				SourceShard:      shard,
				TargetNode:       tc.final.tgtNode,
				TransferType:     tc.final.transferType.String(),
			})

			if tc.wantErr {
				require.ErrorIs(t, err, replication.ErrShardAlreadyReplicating)
			} else {
				require.NoError(t, err)
			}
		})
	}
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
			fsm := replication.NewShardReplicationFSM(prometheus.NewRegistry())
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

			op, ok := fsm.GetOpById(opID)
			require.True(t, ok)
			require.Equal(t, tc.wantShouldCancel, op.Status.ShouldCancel, "ShouldCancel")
			require.Equal(t, tc.wantShouldDelete, op.Status.ShouldDelete, "ShouldDelete")
		})
	}

	t.Run("unknown UUID wraps ErrReplicationOperationNotFound", func(t *testing.T) {
		fsm := replication.NewShardReplicationFSM(prometheus.NewRegistry())
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
			fsm := replication.NewShardReplicationFSM(prometheus.NewRegistry())
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

			op, ok := fsm.GetOpById(opID)
			require.True(t, ok)
			require.Equal(t, tc.wantShouldCancel, op.Status.ShouldCancel, "ShouldCancel")
			require.Equal(t, tc.wantShouldDelete, op.Status.ShouldDelete, "ShouldDelete")
		})
	}

	t.Run("unknown UUID wraps ErrReplicationOperationNotFound", func(t *testing.T) {
		fsm := replication.NewShardReplicationFSM(prometheus.NewRegistry())
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
		fsm := replication.NewShardReplicationFSM(prometheus.NewRegistry())
		const opID uint64 = 7
		seedOp(t, fsm, opID)

		op, ok := fsm.GetOpById(opID)
		require.True(t, ok)
		require.False(t, op.Status.UnCancellable, "should start cancellable")
		require.NoError(t, fsm.SetUnCancellable(opID))

		op, ok = fsm.GetOpById(opID)
		require.True(t, ok)
		require.True(t, op.Status.UnCancellable)
	})

	t.Run("unknown id wraps ErrReplicationOperationNotFound", func(t *testing.T) {
		fsm := replication.NewShardReplicationFSM(prometheus.NewRegistry())
		err := fsm.SetUnCancellable(999)
		require.ErrorIs(t, err, types.ErrReplicationOperationNotFound)
	})
}
