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

package router_test

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/router"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// fakeReplicaHealth reports the hosts it was given as unable to serve
type fakeReplicaHealth map[string]bool

func (f fakeReplicaHealth) Unhealthy(hostAddr string) bool { return f[hostAddr] }

// A replica that is a live member but cannot serve must be tried last, never dropped.
func TestReadRoutingPlanOrdersUnhealthyReplicasLast(t *testing.T) {
	nodes := []string{"node1", "node2", "node3"}

	tests := []struct {
		name   string
		health types.ReplicaHealth
		want   []string
	}{
		{
			name:   "no health view keeps the preferred node first",
			health: nil,
			want:   []string{"node1", "node2", "node3"},
		},
		{
			name:   "the preferred node is demoted when it cannot serve",
			health: fakeReplicaHealth{"node1": true},
			want:   []string{"node2", "node3", "node1"},
		},
		{
			name:   "healthy replicas keep their relative order",
			health: fakeReplicaHealth{"node2": true},
			want:   []string{"node1", "node3", "node2"},
		},
		{
			name:   "every replica unhealthy leaves the order untouched, so the shard stays readable",
			health: fakeReplicaHealth{"node1": true, "node2": true, "node3": true},
			want:   []string{"node1", "node2", "node3"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockSchemaGetter := schema.NewMockSchemaGetter(t)
			mockSchemaReader := schema.NewMockSchemaReader(t)
			mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
			mockNodeSelector := mocks.NewMockNodeSelector(nodes...)

			state := createShardingStateWithShards([]string{"shard1"})
			mockSchemaReader.EXPECT().Shards(mock.Anything).Return(state.AllPhysicalShards(), nil).Maybe()
			mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
				func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
					return readFunc(&models.Class{Class: className}, state)
				}).Maybe()
			mockSchemaReader.EXPECT().ShardReplicas("TestClass", "shard1").Return(nodes, nil).Maybe()
			mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "shard1", nodes).
				Return(nodes).Maybe()

			r := router.NewBuilder("TestClass", false, mockNodeSelector, mockSchemaGetter,
				mockSchemaReader, mockReplicationFSM).
				WithReplicaHealth(test.health).
				Build()

			plan, err := r.BuildReadRoutingPlan(types.RoutingPlanBuildOptions{
				Shard:            "shard1",
				ConsistencyLevel: types.ConsistencyLevelOne,
			})
			require.NoError(t, err)

			got := make([]string, 0, len(plan.ReplicaSet.Replicas))
			for _, replica := range plan.ReplicaSet.Replicas {
				got = append(got, replica.NodeName)
			}
			require.Equal(t, test.want, got, "every replica must stay in the plan, only the order may change")
		})
	}
}
