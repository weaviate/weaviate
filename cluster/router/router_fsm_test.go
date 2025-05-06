package router_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/router"
	"github.com/weaviate/weaviate/cluster/router/types"
	schemaTypes "github.com/weaviate/weaviate/cluster/schema/types"
	clusterMocks "github.com/weaviate/weaviate/usecases/cluster/mocks"
)

func TestReadRoutingWithFSM(t *testing.T) {
	testCases := []struct {
		name                 string
		allShardNodes        []string
		opStatus             api.ShardReplicationState
		preRoutingPlanAction func(fsm *replication.ShardReplicationFSM)
		expectedReplicas     []string
		expectedErrorStr     string
	}{
		{
			name:             "registered",
			allShardNodes:    []string{"node1", "node2"},
			opStatus:         api.REGISTERED,
			expectedReplicas: []string{"node1"},
		},
		{
			name:             "hydrating",
			allShardNodes:    []string{"node1", "node2"},
			opStatus:         api.HYDRATING,
			expectedReplicas: []string{"node1"},
		},
		{
			name:             "finalizing",
			allShardNodes:    []string{"node1", "node2"},
			opStatus:         api.FINALIZING,
			expectedReplicas: []string{"node1"},
		},
		{
			name:             "ready",
			allShardNodes:    []string{"node1", "node2"},
			opStatus:         api.READY,
			expectedReplicas: []string{"node1", "node2"},
		},
		{
			name:             "dehydrating",
			allShardNodes:    []string{"node1", "node2"},
			opStatus:         api.DEHYDRATING,
			expectedErrorStr: "no replicas found for class collection1 shard shard1",
		},
		{
			name:             "aborted",
			allShardNodes:    []string{"node1", "node2"},
			opStatus:         api.ABORTED,
			expectedReplicas: []string{"node1"},
		},
		{
			name:          "ready deleted",
			allShardNodes: []string{"node1", "node2"},
			opStatus:      api.READY,
			preRoutingPlanAction: func(fsm *replication.ShardReplicationFSM) {
				fsm.DeleteReplicationOp(&api.ReplicationDeleteOpRequest{
					Version: api.ReplicationCommandVersionV0,
					Id:      1,
				})
			},
			expectedReplicas: []string{"node1", "node2"},
		},
		{
			name:          "hydrating deleted",
			allShardNodes: []string{"node1", "node2"},
			opStatus:      api.HYDRATING,
			preRoutingPlanAction: func(fsm *replication.ShardReplicationFSM) {
				fsm.DeleteReplicationOp(&api.ReplicationDeleteOpRequest{
					Version: api.ReplicationCommandVersionV0,
					Id:      1,
				})
			},
			expectedReplicas: []string{"node1", "node2"},
		},
		{
			name:             "registered extra node",
			allShardNodes:    []string{"node1", "node2", "node3"},
			opStatus:         api.REGISTERED,
			expectedReplicas: []string{"node1", "node3"},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			reg := prometheus.NewRegistry()
			shardReplicationFSM := replication.NewShardReplicationFSM(reg)
			clusterState := clusterMocks.NewMockNodeSelector(testCase.allShardNodes...)
			schemaReaderMock := schemaTypes.NewMockSchemaReader(t)
			schemaReaderMock.On("ShardReplicas", mock.Anything, mock.Anything).Return(func(class string, shard string) ([]string, error) {
				return testCase.allShardNodes, nil
			})
			myRouter := router.New(logger, clusterState, schemaReaderMock, shardReplicationFSM)
			shardReplicationFSM.Replicate(1, &api.ReplicationReplicateShardRequest{
				Version:          api.ReplicationCommandVersionV0,
				SourceNode:       "node1",
				SourceCollection: "collection1",
				SourceShard:      "shard1",
				TargetNode:       "node2",
				Uuid:             "00000000-0000-0000-0000-000000000000",
			})
			err := shardReplicationFSM.UpdateReplicationOpStatus(&api.ReplicationUpdateOpStateRequest{
				Version: api.ReplicationCommandVersionV0,
				Id:      1,
				State:   testCase.opStatus,
			})
			require.NoError(t, err)
			if testCase.preRoutingPlanAction != nil {
				testCase.preRoutingPlanAction(shardReplicationFSM)
			}
			routingPlan, err := myRouter.BuildReadRoutingPlan(types.RoutingPlanBuildOptions{
				Collection: "collection1",
				Shard:      "shard1",
			})
			if testCase.expectedErrorStr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), testCase.expectedErrorStr)
			} else {
				require.NoError(t, err)
				require.Equal(t, testCase.expectedReplicas, routingPlan.Replicas, "test case: %s", testCase.name)
			}
		})
	}
}
