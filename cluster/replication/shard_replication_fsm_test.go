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
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
)

// TestShardReplicationFSM_FilterOneReplica_Coexistence pins the OR-fold over the ops
// sharing a target replica. Before the fix the index held one op per target FQDN, so
// the last op written decided routing for the whole replica.
func TestShardReplicationFSM_FilterOneReplica_Coexistence(t *testing.T) {
	const (
		coll  = "TestClass"
		shard = "shard1"
	)
	replicas := []string{"node2"}

	cases := []struct {
		name string
		ops  []seededOp
		want []string
	}{
		{
			// The cancelled recopy is written last, so before the fix it is the op the
			// single slot retains and node2 drops out of read and write routing even
			// though the completed copy makes it a perfectly good replica.
			name: "completed COPY beside a cancelled recopy stays routable",
			ops: []seededOp{
				{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.COPY, state: api.READY},
				{id: 2, srcNode: "node3", tgtNode: "node2", transferType: api.COPY, state: api.CANCELLED},
			},
			want: []string{"node2"},
		},
		{
			name: "hydrating COPY beside a completed COPY stays routable",
			ops: []seededOp{
				{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.COPY, state: api.READY},
				{id: 2, srcNode: "node3", tgtNode: "node2", transferType: api.COPY, state: api.HYDRATING},
			},
			want: []string{"node2"},
		},
		{
			name: "cancelled only is excluded",
			ops:  []seededOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.COPY, state: api.CANCELLED}},
			want: []string{},
		},
		{
			name: "single completed COPY is routable",
			ops:  []seededOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.COPY, state: api.READY}},
			want: []string{"node2"},
		},
		{
			name: "single hydrating COPY is not yet routable",
			ops:  []seededOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.COPY, state: api.HYDRATING}},
			want: []string{},
		},
		{
			name: "single finalizing MOVE is not yet routable",
			ops:  []seededOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.MOVE, state: api.FINALIZING}},
			want: []string{},
		},
		{
			name: "single dehydrating MOVE is routable",
			ops:  []seededOp{{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.MOVE, state: api.DEHYDRATING}},
			want: []string{"node2"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
			seedAll(t, fsm, coll, shard, tc.ops)

			require.Equal(t, tc.want, fsm.FilterOneShardReplicasRead(coll, shard, replicas))
			write, _ := fsm.FilterOneShardReplicasWrite(coll, shard, replicas)
			require.Equal(t, tc.want, write)
		})
	}
}

// TestShardReplicationFSM_RemoveOneOfTwoTargetOps pins the per-target slice remove path.
// Before the fix removal deleted the whole target key, so removing either of two ops
// sharing a replica also dropped the survivor — and with no entry left, routing fell
// through to the source check and happily routed reads to a still-hydrating replica.
func TestShardReplicationFSM_RemoveOneOfTwoTargetOps(t *testing.T) {
	const (
		coll  = "TestClass"
		shard = "shard1"
	)
	replicas := []string{"node2"}

	fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
	remove := func(id uint64) {
		t.Helper()
		require.NoError(t, fsm.RemoveReplicationOp(&api.ReplicationRemoveOpRequest{
			Version: api.ReplicationCommandVersionV0,
			Id:      id,
		}))
	}
	requireRoutable := func(msg string, want []string) {
		t.Helper()
		require.Equal(t, want, fsm.FilterOneShardReplicasRead(coll, shard, replicas), msg)
		write, _ := fsm.FilterOneShardReplicasWrite(coll, shard, replicas)
		require.Equal(t, want, write, msg)
	}

	seedAll(t, fsm, coll, shard, []seededOp{
		{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.MOVE, state: api.CANCELLED},
		{id: 2, srcNode: "node3", tgtNode: "node2", transferType: api.MOVE, state: api.HYDRATING},
	})
	require.Len(t, fsm.GetOpsForTarget("node2"), 2)
	requireRoutable("node2 is still hydrating, so it is not routable", []string{})

	// Removing the cancelled op must leave the hydrating op governing the replica.
	remove(1)
	require.Len(t, fsm.GetOpsForTarget("node2"), 1)
	requireRoutable("the hydrating survivor still keeps node2 out of routing", []string{})

	// Removing the last op empties the slice; the key must be deleted so routing falls
	// through to the source check rather than OR-folding an empty slice to (false,false).
	remove(2)
	require.Empty(t, fsm.GetOpsForTarget("node2"))
	requireRoutable("with no op left node2 routes as an ordinary replica", []string{"node2"})
}

// TestShardReplicationFSM_RestoreIsOrderIndependent pins snapshot restore as a total,
// order-independent rebuild. snap.Ops is a map, so each restore ranges it in a fresh
// random order. Before the fix restore replayed ops through the admission-validating
// write path, which both rejected legally-committed snapshots (failing the RAFT restore
// and crashlooping the node) and let whichever op landed last win the target index,
// leaving nodes to route the same shard differently.
func TestShardReplicationFSM_RestoreIsOrderIndependent(t *testing.T) {
	const (
		coll  = "TestClass"
		shard = "shard1"
	)

	cases := []struct {
		name string
		// seeded in id order; a settled op must precede the active op it coexists with.
		ops []seededOp
		// per target replica: how many ops the per-node index holds, and whether the
		// replica is read+write routable.
		wantOpsPerTarget map[string]int
		wantRoutable     map[string]bool
	}{
		{
			// Admission accepts a MOVE off a source whose only other op is a completed
			// COPY. Replayed MOVE-first, the old restore path saw the MOVE and rejected
			// the COPY, so ~half of all restores failed outright.
			name: "completed COPY and ongoing MOVE sharing a source",
			ops: []seededOp{
				{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.COPY, state: api.READY},
				{id: 2, srcNode: "node1", tgtNode: "node3", transferType: api.MOVE, state: api.HYDRATING},
			},
			wantOpsPerTarget: map[string]int{"node2": 1, "node3": 1},
			wantRoutable:     map[string]bool{"node2": true, "node3": false},
		},
		{
			// Both ops share target node2, so the single-valued index kept only one.
			name: "completed COPY and cancelled recopy sharing a target",
			ops: []seededOp{
				{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.COPY, state: api.READY},
				{id: 2, srcNode: "node3", tgtNode: "node2", transferType: api.COPY, state: api.CANCELLED},
			},
			wantOpsPerTarget: map[string]int{"node2": 2},
			wantRoutable:     map[string]bool{"node2": true},
		},
		{
			name: "combined snapshot mixing both coexistences",
			ops: []seededOp{
				{id: 1, srcNode: "node1", tgtNode: "node2", transferType: api.COPY, state: api.READY},
				{id: 2, srcNode: "node1", tgtNode: "node3", transferType: api.MOVE, state: api.HYDRATING},
				{id: 3, srcNode: "node4", tgtNode: "node5", transferType: api.COPY, state: api.READY},
				{id: 4, srcNode: "node6", tgtNode: "node5", transferType: api.COPY, state: api.CANCELLED},
			},
			wantOpsPerTarget: map[string]int{"node2": 1, "node3": 1, "node5": 2},
			wantRoutable:     map[string]bool{"node2": true, "node3": false, "node5": true},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
			seedAll(t, src, coll, shard, tc.ops)
			blob, err := src.Snapshot()
			require.NoError(t, err)

			const restores = 100
			var prevStatus map[replication.ShardReplicationOp]replication.ShardReplicationOpStatus
			for i := 1; i <= restores; i++ {
				dst := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
				require.NoErrorf(t, dst.Restore(blob), "restore %d/%d must not error", i, restores)

				gotStatus := dst.GetStatusByOps()
				require.Lenf(t, gotStatus, len(tc.ops), "restore %d: op count", i)
				for _, op := range tc.ops {
					_, ok := dst.GetOpById(op.id)
					require.Truef(t, ok, "restore %d: op %d present", i, op.id)
				}

				for node, count := range tc.wantOpsPerTarget {
					require.Lenf(t, dst.GetOpsForTarget(node), count,
						"restore %d: per-node target index for %s", i, node)
				}

				for node, routable := range tc.wantRoutable {
					want := []string{}
					if routable {
						want = []string{node}
					}
					require.Equalf(t, want, dst.FilterOneShardReplicasRead(coll, shard, []string{node}),
						"restore %d: %s read routing", i, node)
					write, _ := dst.FilterOneShardReplicasWrite(coll, shard, []string{node})
					require.Equalf(t, want, write, "restore %d: %s write routing", i, node)
				}

				if prevStatus != nil {
					require.Equalf(t, prevStatus, gotStatus, "restore %d state must match restore %d", i, i-1)
				}
				prevStatus = gotStatus
			}
		})
	}
}
