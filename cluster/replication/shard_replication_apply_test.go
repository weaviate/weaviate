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
)

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
	require.NoError(t, fsm.UpdateReplicationOpStatus(&api.ReplicationUpdateOpStateRequest{
		Version: api.ReplicationCommandVersionV0,
		Id:      opID,
		State:   state,
	}))
}

func driveToCancelled(t *testing.T, fsm *replication.ShardReplicationFSM, opID uint64) {
	t.Helper()
	require.NoError(t, fsm.CancellationComplete(&api.ReplicationCancellationCompleteRequest{
		Version: api.ReplicationCommandVersionV0,
		Id:      opID,
	}))
}

// seededOp declares one op a table row wants present before the assertion: it is
// admitted via Replicate (itself subject to admission), then driven to its state.
type seededOp struct {
	id           uint64
	srcNode      string
	tgtNode      string
	transferType api.ShardReplicationTransferType
	state        api.ShardReplicationState
}

func seedAll(t *testing.T, fsm *replication.ShardReplicationFSM, collection, shard string, ops []seededOp) {
	t.Helper()
	for _, op := range ops {
		seedOpFull(t, fsm, op.id, op.srcNode, op.tgtNode, collection, shard, op.transferType)
		if op.state == api.CANCELLED {
			driveToCancelled(t, fsm, op.id)
			continue
		}
		driveToState(t, fsm, op.id, op.state)
	}
}

func TestReplicate_Admission(t *testing.T) {
	const (
		coll  = "TestClass"
		shard = "shard1"
	)

	cases := []struct {
		name string
		// setup ops are admitted (all must succeed) and driven to their state, then
		// final is the op under test.
		setup   []seededOp
		final   seededOp
		wantErr bool // true ⇒ final must be rejected with ErrShardAlreadyReplicating
	}{
		// --- source-guard isolation: shared source FQDN, disjoint targets ---
		{
			name:    "source: two active MOVEs rejected",
			setup:   []seededOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.MOVE, state: api.HYDRATING}},
			final:   seededOp{id: 2, srcNode: "node1", tgtNode: "node3", transferType: api.MOVE, state: api.REGISTERED},
			wantErr: true,
		},
		{
			name:    "source: cancelled MOVE then new MOVE allowed",
			setup:   []seededOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.MOVE, state: api.CANCELLED}},
			final:   seededOp{id: 2, srcNode: "node1", tgtNode: "node3", transferType: api.MOVE, state: api.REGISTERED},
			wantErr: false,
		},
		{
			name:    "source: completed COPY (READY) then new MOVE allowed",
			setup:   []seededOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.COPY, state: api.READY}},
			final:   seededOp{id: 2, srcNode: "node1", tgtNode: "node3", transferType: api.MOVE, state: api.REGISTERED},
			wantErr: false,
		},
		{
			name:    "source: active COPY then COPY allowed",
			setup:   []seededOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.COPY, state: api.HYDRATING}},
			final:   seededOp{id: 2, srcNode: "node1", tgtNode: "node3", transferType: api.COPY, state: api.REGISTERED},
			wantErr: false,
		},
		// --- target-guard isolation: shared target FQDN, disjoint sources. Every row
		// here is admitted unconditionally before the fix: the guard keyed its status
		// lookup on the incoming op's own id, which is never already in statusById.
		{
			name:    "target: two active MOVEs from distinct sources rejected",
			setup:   []seededOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.MOVE, state: api.HYDRATING}},
			final:   seededOp{id: 2, srcNode: "node3", tgtNode: "node2", transferType: api.MOVE, state: api.REGISTERED},
			wantErr: true,
		},
		{
			name:    "target: active COPY then MOVE from distinct source rejected",
			setup:   []seededOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.COPY, state: api.HYDRATING}},
			final:   seededOp{id: 2, srcNode: "node3", tgtNode: "node2", transferType: api.MOVE, state: api.REGISTERED},
			wantErr: true,
		},
		{
			name:    "target: cancelled MOVE then active MOVE from distinct source allowed",
			setup:   []seededOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.MOVE, state: api.CANCELLED}},
			final:   seededOp{id: 2, srcNode: "node3", tgtNode: "node2", transferType: api.MOVE, state: api.REGISTERED},
			wantErr: false,
		},
		{
			name:    "target: completed COPY (READY) then active COPY from distinct source allowed",
			setup:   []seededOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.COPY, state: api.READY}},
			final:   seededOp{id: 2, srcNode: "node3", tgtNode: "node2", transferType: api.COPY, state: api.REGISTERED},
			wantErr: false,
		},
		{
			name:    "target: two active COPYs from distinct sources allowed",
			setup:   []seededOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.COPY, state: api.HYDRATING}},
			final:   seededOp{id: 2, srcNode: "node3", tgtNode: "node2", transferType: api.COPY, state: api.REGISTERED},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
			seedAll(t, fsm, coll, shard, tc.setup)

			err := fsm.Replicate(tc.final.id, &api.ReplicationReplicateShardRequest{
				Version:          api.ReplicationCommandVersionV0,
				Uuid:             strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", tc.final.id)),
				SourceNode:       tc.final.srcNode,
				SourceCollection: coll,
				SourceShard:      shard,
				TargetNode:       tc.final.tgtNode,
				TransferType:     tc.final.transferType.String(),
			})

			if tc.wantErr {
				require.ErrorIs(t, err, replication.ErrShardAlreadyReplicating)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestReplicate_RejectsSourceFromInFlightTarget pins the chained-move guard: a move
// A→B followed by B→C must not be admitted while B is still being hydrated by the
// first leg. Both legs running at once seals the second leg's change-capture log
// before the first has drained its writes into B, and those writes never reach C.
func TestReplicate_RejectsSourceFromInFlightTarget(t *testing.T) {
	const (
		coll  = "TestClass"
		shard = "shard1"
	)
	cases := []struct {
		name    string
		setup   []seededOp
		final   seededOp
		wantErr bool
	}{
		{
			name:    "MOVE sourcing a replica still being moved in is rejected",
			setup:   []seededOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.MOVE, state: api.HYDRATING}},
			final:   seededOp{id: 2, srcNode: "node2", tgtNode: "node3", transferType: api.MOVE, state: api.REGISTERED},
			wantErr: true,
		},
		{
			name:    "COPY sourcing a replica still being moved in is rejected (incomplete read)",
			setup:   []seededOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.MOVE, state: api.HYDRATING}},
			final:   seededOp{id: 2, srcNode: "node2", tgtNode: "node3", transferType: api.COPY, state: api.REGISTERED},
			wantErr: true,
		},
		{
			name:    "MOVE sourcing a replica still being copied in is rejected",
			setup:   []seededOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.COPY, state: api.HYDRATING}},
			final:   seededOp{id: 2, srcNode: "node2", tgtNode: "node3", transferType: api.MOVE, state: api.REGISTERED},
			wantErr: true,
		},
		{
			name:    "MOVE sourcing a FINALIZING target is rejected",
			setup:   []seededOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.MOVE, state: api.FINALIZING}},
			final:   seededOp{id: 2, srcNode: "node2", tgtNode: "node3", transferType: api.MOVE, state: api.REGISTERED},
			wantErr: true,
		},
		{
			name:    "MOVE sourcing a REGISTERED target is rejected",
			setup:   []seededOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.MOVE, state: api.REGISTERED}},
			final:   seededOp{id: 2, srcNode: "node2", tgtNode: "node3", transferType: api.MOVE, state: api.REGISTERED},
			wantErr: true,
		},
		{
			// DEHYDRATING n2 is complete (its drain finished before this state), but we
			// err closed and reject until READY — pins the conservative boundary.
			name:    "MOVE sourcing a DEHYDRATING target is rejected (conservative)",
			setup:   []seededOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.MOVE, state: api.DEHYDRATING}},
			final:   seededOp{id: 2, srcNode: "node2", tgtNode: "node3", transferType: api.MOVE, state: api.REGISTERED},
			wantErr: true,
		},
		{
			name:    "sourcing a READY target is allowed",
			setup:   []seededOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.MOVE, state: api.READY}},
			final:   seededOp{id: 2, srcNode: "node2", tgtNode: "node3", transferType: api.MOVE, state: api.REGISTERED},
			wantErr: false,
		},
		{
			name:    "sourcing a CANCELLED target is allowed",
			setup:   []seededOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.MOVE, state: api.CANCELLED}},
			final:   seededOp{id: 2, srcNode: "node2", tgtNode: "node3", transferType: api.MOVE, state: api.REGISTERED},
			wantErr: false,
		},
		{
			name:    "disjoint moves are allowed",
			setup:   []seededOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.MOVE, state: api.HYDRATING}},
			final:   seededOp{id: 2, srcNode: "node3", tgtNode: "node4", transferType: api.MOVE, state: api.REGISTERED},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
			seedAll(t, fsm, coll, shard, tc.setup)

			err := fsm.Replicate(tc.final.id, &api.ReplicationReplicateShardRequest{
				Version:          api.ReplicationCommandVersionV0,
				Uuid:             strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", tc.final.id)),
				SourceNode:       tc.final.srcNode,
				SourceCollection: coll,
				SourceShard:      shard,
				TargetNode:       tc.final.tgtNode,
				TransferType:     tc.final.transferType.String(),
			})

			if tc.wantErr {
				require.ErrorIs(t, err, replication.ErrShardAlreadyReplicating)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestReplicationFSM_HasOngoingReplication_TargetCoexistence covers the target arm of
// HasOngoingReplication, which gates HOT→COLD tenant deactivation in
// schema.metaClass.UpdateTenants. A false negative deactivates and unloads the shard
// out from under a replication op still hydrating into it.
//
// HasOngoingReplication has no equivalent on v1.38/main, so the upstream fix carries
// no coverage for this path.
func TestReplicationFSM_HasOngoingReplication_TargetCoexistence(t *testing.T) {
	const (
		coll  = "TestClass"
		shard = "shard1"
	)

	cases := []struct {
		name  string
		ops   []seededOp
		query string
		want  bool
	}{
		{
			name: "settled COPY beside a hydrating COPY into the same target is ongoing",
			ops: []seededOp{
				{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.COPY, state: api.HYDRATING},
				{id: 2, srcNode: "node3", tgtNode: "node2", transferType: api.COPY, state: api.READY},
			},
			query: "node2",
			want:  true,
		},
		{
			name: "both settled into the same target is not ongoing",
			ops: []seededOp{
				{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.COPY, state: api.READY},
				{id: 2, srcNode: "node3", tgtNode: "node2", transferType: api.COPY, state: api.READY},
			},
			query: "node2",
			want:  false,
		},
		{
			name:  "single hydrating target is ongoing",
			ops:   []seededOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.COPY, state: api.HYDRATING}},
			query: "node2",
			want:  true,
		},
		{
			name:  "single settled target is not ongoing",
			ops:   []seededOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.COPY, state: api.READY}},
			query: "node2",
			want:  false,
		},
		{
			name:  "uninvolved replica is not ongoing",
			ops:   []seededOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.COPY, state: api.HYDRATING}},
			query: "node4",
			want:  false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
			seedAll(t, fsm, coll, shard, tc.ops)
			require.Equal(t, tc.want, fsm.HasOngoingReplication(coll, shard, tc.query))
		})
	}
}
