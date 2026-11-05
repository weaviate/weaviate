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

package types_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
)

func TestReadRoutingPlan_ShardPlans(t *testing.T) {
	replica := func(shard, node string) types.Replica {
		return types.Replica{ShardName: shard, NodeName: node, HostAddr: node + ":8080"}
	}

	tests := []struct {
		name     string
		replicas []types.Replica
		cl       types.ConsistencyLevel
		want     []types.ReadRoutingPlan
	}{
		{
			name:     "no replicas",
			replicas: nil,
			cl:       types.ConsistencyLevelAll,
			want:     []types.ReadRoutingPlan{},
		},
		{
			name:     "one shard on one replica",
			replicas: []types.Replica{replica("A", "n1")},
			cl:       types.ConsistencyLevelAll,
			want: []types.ReadRoutingPlan{
				{
					Shard:               "A",
					ReplicaSet:          types.ReadReplicaSet{Replicas: []types.Replica{replica("A", "n1")}},
					ConsistencyLevel:    types.ConsistencyLevelAll,
					IntConsistencyLevel: 1,
				},
			},
		},
		{
			// The collection-wide plan is sorted local-replicas-first across all
			// shards, so a shard's replicas do not arrive contiguously.
			name: "interleaved shards keep their own replicas in order",
			replicas: []types.Replica{
				replica("A", "n1"), replica("B", "n1"), replica("C", "n1"),
				replica("A", "n2"), replica("B", "n2"), replica("C", "n2"),
			},
			cl: types.ConsistencyLevelAll,
			want: []types.ReadRoutingPlan{
				{
					Shard:               "A",
					ReplicaSet:          types.ReadReplicaSet{Replicas: []types.Replica{replica("A", "n1"), replica("A", "n2")}},
					ConsistencyLevel:    types.ConsistencyLevelAll,
					IntConsistencyLevel: 2,
				},
				{
					Shard:               "B",
					ReplicaSet:          types.ReadReplicaSet{Replicas: []types.Replica{replica("B", "n1"), replica("B", "n2")}},
					ConsistencyLevel:    types.ConsistencyLevelAll,
					IntConsistencyLevel: 2,
				},
				{
					Shard:               "C",
					ReplicaSet:          types.ReadReplicaSet{Replicas: []types.Replica{replica("C", "n1"), replica("C", "n2")}},
					ConsistencyLevel:    types.ConsistencyLevelAll,
					IntConsistencyLevel: 2,
				},
			},
		},
		{
			// A plan spanning shards of unequal width cannot be validated as a
			// whole, but each shard on its own can.
			name: "level counts each shard's own replicas",
			replicas: []types.Replica{
				replica("A", "n1"), replica("B", "n1"), replica("B", "n2"), replica("B", "n3"),
			},
			cl: types.ConsistencyLevelQuorum,
			want: []types.ReadRoutingPlan{
				{
					Shard:               "A",
					ReplicaSet:          types.ReadReplicaSet{Replicas: []types.Replica{replica("A", "n1")}},
					ConsistencyLevel:    types.ConsistencyLevelQuorum,
					IntConsistencyLevel: 1,
				},
				{
					Shard:               "B",
					ReplicaSet:          types.ReadReplicaSet{Replicas: []types.Replica{replica("B", "n1"), replica("B", "n2"), replica("B", "n3")}},
					ConsistencyLevel:    types.ConsistencyLevelQuorum,
					IntConsistencyLevel: 2,
				},
			},
		},
		{
			name: "ONE reaches a single replica per shard",
			replicas: []types.Replica{
				replica("A", "n1"), replica("A", "n2"),
			},
			cl: types.ConsistencyLevelOne,
			want: []types.ReadRoutingPlan{
				{
					Shard:               "A",
					ReplicaSet:          types.ReadReplicaSet{Replicas: []types.Replica{replica("A", "n1"), replica("A", "n2")}},
					ConsistencyLevel:    types.ConsistencyLevelOne,
					IntConsistencyLevel: 1,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := types.ReadRoutingPlan{
				LocalHostname:       "n1",
				Tenant:              "t1",
				ReplicaSet:          types.ReadReplicaSet{Replicas: tt.replicas},
				ConsistencyLevel:    types.ConsistencyLevelOne,
				IntConsistencyLevel: 1,
			}

			got := plan.ShardPlans(tt.cl)

			want := make([]types.ReadRoutingPlan, len(tt.want))
			for i, p := range tt.want {
				p.LocalHostname = "n1"
				p.Tenant = "t1"
				want[i] = p
			}
			require.Equal(t, want, got)
		})
	}
}

func TestReadRoutingPlan_SpreadReplicas(t *testing.T) {
	replicas := func(shard string, nodes ...string) []types.Replica {
		out := make([]types.Replica, 0, len(nodes))
		for _, node := range nodes {
			out = append(out, types.Replica{ShardName: shard, NodeName: node, HostAddr: node + ":8080"})
		}
		return out
	}

	tests := []struct {
		name     string
		local    string
		replicas [][]types.Replica
		// want is the node each shard must land on; nil when only maxLoad is fixed
		want    map[string]string
		maxLoad int
	}{
		{
			name:    "no replicas",
			local:   "n1",
			want:    map[string]string{},
			maxLoad: 0,
		},
		{
			name:     "single replicas stay where they are, even on one node",
			local:    "n1",
			replicas: [][]types.Replica{replicas("A", "n2"), replicas("B", "n2")},
			want:     map[string]string{"A": "n2", "B": "n2"},
			maxLoad:  2,
		},
		{
			name:     "the local node wins a tie",
			local:    "n1",
			replicas: [][]types.Replica{replicas("A", "n2", "n3", "n1")},
			want:     map[string]string{"A": "n1"},
			maxLoad:  1,
		},
		{
			name:     "the local node takes one shard and the rest go elsewhere",
			local:    "n1",
			replicas: [][]types.Replica{replicas("A", "n1", "n2", "n3"), replicas("B", "n1", "n2", "n3"), replicas("C", "n1", "n2", "n3")},
			maxLoad:  1,
		},
		{
			name:     "a shard with fewer replicas picks first",
			local:    "n9",
			replicas: [][]types.Replica{replicas("A", "n1", "n2"), replicas("B", "n2")},
			want:     map[string]string{"A": "n1", "B": "n2"},
			maxLoad:  1,
		},
		{
			name:  "four shards at rf 3 on five nodes land on four nodes",
			local: "n1",
			replicas: [][]types.Replica{
				replicas("A", "n1", "n2", "n3"), replicas("B", "n1", "n3", "n4"),
				replicas("C", "n1", "n4", "n5"), replicas("D", "n2", "n3", "n5"),
			},
			maxLoad: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var all []types.Replica
			for _, r := range tt.replicas {
				all = append(all, r...)
			}
			plan := types.ReadRoutingPlan{LocalHostname: tt.local, ReplicaSet: types.ReadReplicaSet{Replicas: all}}

			// ties break at random, so every run must hold
			for range 100 {
				got := map[string]string{}
				load := map[string]int{}
				for _, r := range plan.SpreadReplicas() {
					require.NotContains(t, got, r.ShardName, "one replica per shard")
					require.Contains(t, all, r)
					got[r.ShardName] = r.NodeName
					load[r.NodeName]++
				}
				require.Len(t, got, len(tt.replicas))
				for node, n := range load {
					require.LessOrEqual(t, n, tt.maxLoad, "node %s", node)
				}
				if tt.want != nil {
					require.Equal(t, tt.want, got)
				}
			}
		})
	}
}
