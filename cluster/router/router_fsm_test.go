//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

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
			expectedReplicas: []string{"node2"},
		},
		{
			name:             "cancelled",
			allShardNodes:    []string{"node1", "node2"},
			opStatus:         api.CANCELLED,
			expectedReplicas: []string{"node1"},
		},
		{
			name:          "ready deleted",
			allShardNodes: []string{"node1", "node2"},
			opStatus:      api.READY,
			preRoutingPlanAction: func(fsm *replication.ShardReplicationFSM) {
				fsm.CancelReplication(&api.ReplicationCancelRequest{
					Version: api.ReplicationCommandVersionV0,
					Uuid:    "00000000-0000-0000-0000-000000000000",
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

			// Setup the FSM with the right state
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

			// Build the routing plan
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

func TestWriteRoutingWithFSM(t *testing.T) {
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
			expectedReplicas: []string{"node2"},
		},
		{
			name:             "cancelled",
			allShardNodes:    []string{"node1", "node2"},
			opStatus:         api.CANCELLED,
			expectedReplicas: []string{"node1"},
		},
		{
			name:          "ready deleted",
			allShardNodes: []string{"node1", "node2"},
			opStatus:      api.READY,
			preRoutingPlanAction: func(fsm *replication.ShardReplicationFSM) {
				fsm.CancelReplication(&api.ReplicationCancelRequest{
					Version: api.ReplicationCommandVersionV0,
					Uuid:    "00000000-0000-0000-0000-000000000000",
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

			// Setup the FSM with the right state
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

			// Build the routing plan
			routingPlan, err := myRouter.BuildWriteRoutingPlan(types.RoutingPlanBuildOptions{
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
