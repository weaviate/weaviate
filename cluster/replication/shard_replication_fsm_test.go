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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
)

// TestShardReplicationFSM_FilterReplicas_ByState: read/write filter
// contract across a COPY lifecycle. The target receives no direct writes while
// it catches up (HYDRATING/FINALIZING) — the change-capture-log is the sole,
// ordered, LWW-safe catchup path — and is promoted to a counted read+write
// replica only at INTEGRATING.
func TestShardReplicationFSM_FilterReplicas_ByState(t *testing.T) {
	const (
		class  = "TestClass"
		shard  = "shard1"
		source = "node1"
		target = "node2"
	)
	replicas := []string{source, target}

	cases := []struct {
		name      string
		state     api.ShardReplicationState
		wantRead  []string
		wantWrite []string
	}{
		{
			name:      "REGISTERED: target not yet routable",
			state:     api.REGISTERED,
			wantRead:  []string{source},
			wantWrite: []string{source},
		},
		{
			name:      "HYDRATING: target not yet routable",
			state:     api.HYDRATING,
			wantRead:  []string{source},
			wantWrite: []string{source},
		},
		{
			name:      "FINALIZING: target receives no direct writes (CCL-only catchup)",
			state:     api.FINALIZING,
			wantRead:  []string{source},
			wantWrite: []string{source},
		},
		{
			name:      "INTEGRATING: target is a counted read+write replica, not additional",
			state:     api.INTEGRATING,
			wantRead:  []string{source, target},
			wantWrite: []string{source, target},
		},
		{
			name:      "READY: target fully promoted",
			state:     api.READY,
			wantRead:  []string{source, target},
			wantWrite: []string{source, target},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
			seedOp(t, fsm, 1)
			driveToState(t, fsm, 1, tc.state)

			gotRead := fsm.FilterOneShardReplicasRead(class, shard, replicas)
			assert.ElementsMatch(t, tc.wantRead, gotRead, "read replicas")

			gotWrite := fsm.FilterOneShardReplicasWrite(class, shard, replicas)
			assert.ElementsMatch(t, tc.wantWrite, gotWrite, "write replicas")
		})
	}
}

// TestShardReplicationFSM_AllPeersAtLeast firewalls the convergence barrier that
// gates the move/copy cutovers: the source change-capture log is sealed at
// INTEGRATING and the source replica removed at DEHYDRATING only once every node
// has applied the transition. PerNodeState is keyed per node and populated only
// by committed NodeReachedState broadcasts, so a node that has not (yet) reported
// is absent. AllPeersAtLeast must treat an absent expected node as not satisfied
// — otherwise a partial/empty map reports convergence and a node still routing
// under the old topology loses writes after the cutover.
//
// The expected set is the full cluster membership (here node1=source,
// node2=target, node3=a non-replica that can still coordinate a write), not just
// the shard replicas.
func TestShardReplicationFSM_AllPeersAtLeast(t *testing.T) {
	const opID uint64 = 1

	cases := []struct {
		name          string
		expectedNodes []string
		reached       map[string]api.ShardReplicationState
		target        api.ShardReplicationState
		want          bool
	}{
		{
			name:          "no node has reported is not convergence",
			expectedNodes: []string{"node1", "node2"},
			reached:       nil,
			target:        api.INTEGRATING,
			want:          false,
		},
		{
			name:          "only the target reported, source silent is not convergence",
			expectedNodes: []string{"node1", "node2"},
			reached:       map[string]api.ShardReplicationState{"node2": api.INTEGRATING},
			target:        api.INTEGRATING,
			want:          false,
		},
		{
			name:          "every expected node reached target",
			expectedNodes: []string{"node1", "node2"},
			reached:       map[string]api.ShardReplicationState{"node1": api.INTEGRATING, "node2": api.INTEGRATING},
			target:        api.INTEGRATING,
			want:          true,
		},
		{
			name:          "a peer below target blocks",
			expectedNodes: []string{"node1", "node2"},
			reached:       map[string]api.ShardReplicationState{"node1": api.INTEGRATING, "node2": api.HYDRATING},
			target:        api.INTEGRATING,
			want:          false,
		},
		{
			name:          "peers past target still satisfy it",
			expectedNodes: []string{"node1", "node2"},
			reached:       map[string]api.ShardReplicationState{"node1": api.READY, "node2": api.DEHYDRATING},
			target:        api.INTEGRATING,
			want:          true,
		},
		{
			name:          "non-replica coordinator that has not reported blocks",
			expectedNodes: []string{"node1", "node2", "node3"},
			reached:       map[string]api.ShardReplicationState{"node1": api.INTEGRATING, "node2": api.INTEGRATING},
			target:        api.INTEGRATING,
			want:          false,
		},
		{
			name:          "DEHYDRATING converges only when every node reports it",
			expectedNodes: []string{"node1", "node2", "node3"},
			reached:       map[string]api.ShardReplicationState{"node1": api.DEHYDRATING, "node2": api.DEHYDRATING, "node3": api.DEHYDRATING},
			target:        api.DEHYDRATING,
			want:          true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fsm := replication.NewShardReplicationFSM(prometheus.NewRegistry())
			seedOp(t, fsm, opID) // source node1, target node2
			for node, state := range tc.reached {
				require.NoError(t, fsm.NodeReachedState(&api.ReplicationNodeReachedStateRequest{
					Version: api.ReplicationCommandVersionV0,
					Id:      opID,
					NodeId:  node,
					State:   state,
				}))
			}
			require.Equal(t, tc.want, fsm.AllPeersAtLeast(opID, tc.target, tc.expectedNodes))
		})
	}

	t.Run("unknown op is never converged", func(t *testing.T) {
		fsm := replication.NewShardReplicationFSM(prometheus.NewRegistry())
		require.False(t, fsm.AllPeersAtLeast(999, api.INTEGRATING, []string{"node1"}))
	})
}
