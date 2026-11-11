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

func TestReadRoutingPlan_LocalShardCount(t *testing.T) {
	replica := func(shard, node string) types.Replica {
		return types.Replica{ShardName: shard, NodeName: node}
	}

	tests := []struct {
		name     string
		replicas []types.Replica
		want     int
	}{
		{name: "no replicas", want: 0},
		{name: "one local shard", replicas: []types.Replica{replica("A", "local")}, want: 1},
		{name: "remote shards only", replicas: []types.Replica{replica("A", "n2"), replica("B", "n3")}, want: 0},
		{
			name:     "local and remote shards",
			replicas: []types.Replica{replica("A", "local"), replica("B", "local"), replica("C", "n2")},
			want:     2,
		},
		{
			name:     "replicas of a local shard count once",
			replicas: []types.Replica{replica("A", "local"), replica("A", "local"), replica("A", "n2"), replica("B", "n2")},
			want:     1,
		},
		{
			// ExecuteForEachShard sends this shard to n2 and never reaches its local replica.
			name:     "a shard whose first replica is remote is not read locally",
			replicas: []types.Replica{replica("A", "n2"), replica("A", "local")},
			want:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := types.ReadRoutingPlan{
				LocalHostname: "local",
				ReplicaSet:    types.ReadReplicaSet{Replicas: tt.replicas},
			}
			require.Equal(t, tt.want, plan.LocalShardCount())
		})
	}
}

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
