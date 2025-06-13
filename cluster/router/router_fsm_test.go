//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package router_test

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/weaviate/weaviate/entities/models"

	"github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
	"github.com/weaviate/weaviate/usecases/sharding/config"

	"github.com/prometheus/client_golang/prometheus"
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
		partitioningEnabled  bool
		allShardNodes        []string
		opStatus             api.ShardReplicationState
		preRoutingPlanAction func(fsm *replication.ShardReplicationFSM)
		expectedReplicas     types.ReplicaSet
		expectedErrorStr     string
	}{
		{
			name:                "registered",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2"},
			opStatus:            api.REGISTERED,
			expectedReplicas:    types.ReplicaSet{Replicas: []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}}},
		},
		{
			name:                "hydrating",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2"},
			opStatus:            api.HYDRATING,
			expectedReplicas:    types.ReplicaSet{Replicas: []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}}},
		},
		{
			name:                "finalizing",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2"},
			opStatus:            api.FINALIZING,
			expectedReplicas:    types.ReplicaSet{Replicas: []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}}},
		},
		{
			name:                "ready",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2"},
			opStatus:            api.READY,
			expectedReplicas:    types.ReplicaSet{Replicas: []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}, {NodeName: "node2", ShardName: "shard1", HostAddr: "node2"}}},
		},
		{
			name:                "dehydrating",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2"},
			opStatus:            api.DEHYDRATING,
			expectedReplicas:    types.ReplicaSet{Replicas: []types.Replica{{NodeName: "node2", ShardName: "shard1", HostAddr: "node2"}}},
		},
		{
			name:                "cancelled",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2"},
			opStatus:            api.CANCELLED,
			expectedReplicas:    types.ReplicaSet{Replicas: []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}}},
		},
		{
			name:                "ready deleted",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2"},
			opStatus:            api.READY,
			preRoutingPlanAction: func(fsm *replication.ShardReplicationFSM) {
				fsm.CancelReplication(&api.ReplicationCancelRequest{
					Version: api.ReplicationCommandVersionV0,
					Uuid:    "00000000-0000-0000-0000-000000000000",
				})
			},
			expectedReplicas: types.ReplicaSet{Replicas: []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}, {NodeName: "node2", ShardName: "shard1", HostAddr: "node2"}}},
		},
		{
			name:                "registered extra node",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2", "node3"},
			opStatus:            api.REGISTERED,
			expectedReplicas:    types.ReplicaSet{Replicas: []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}, {NodeName: "node3", ShardName: "shard1", HostAddr: "node3"}}},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name+"_partitioning_enabled_"+strconv.FormatBool(testCase.partitioningEnabled), func(t *testing.T) {
			reg := prometheus.NewRegistry()
			shardReplicationFSM := replication.NewShardReplicationFSM(reg)
			clusterState := clusterMocks.NewMockNodeSelector(testCase.allShardNodes...)
			schemaReaderMock := schemaTypes.NewMockSchemaReader(t)
			schemaGetterMock := schema.NewMockSchemaGetter(t)
			schemaGetterMock.EXPECT().OptimisticTenantStatus(mock.Anything, "collection1", "shard1").Return(
				map[string]string{
					"shard1": models.TenantActivityStatusHOT,
				}, nil).Maybe()
			schemaReaderMock.EXPECT().CopyShardingState("collection1").Return(&sharding.State{
				IndexID: "index-001",
				Config: config.Config{
					VirtualPerPhysical:  0,
					DesiredCount:        1,
					ActualCount:         1,
					DesiredVirtualCount: 0,
					ActualVirtualCount:  0,
					Key:                 "",
					Strategy:            "",
					Function:            "",
				},
				Physical: map[string]sharding.Physical{
					"shard1": {
						Name:                                 "shard1",
						OwnsVirtual:                          []string{},
						OwnsPercentage:                       100,
						LegacyBelongsToNodeForBackwardCompat: "",
						BelongsToNodes:                       testCase.expectedReplicas.NodeNames(),
						Status:                               testCase.opStatus.String(),
					},
				},
				Virtual:             []sharding.Virtual{},
				PartitioningEnabled: false,
				ReplicationFactor:   1,
			}).Maybe()
			schemaReaderMock.On("ShardReplicas", mock.Anything, mock.Anything).Return(func(class string, shard string) ([]string, error) {
				return testCase.allShardNodes, nil
			})
			myRouter := router.NewBuilder("collection1", testCase.partitioningEnabled, clusterState, schemaGetterMock, schemaReaderMock, shardReplicationFSM).Build()

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
				Shard: "shard1",
			})
			if testCase.expectedErrorStr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), testCase.expectedErrorStr)
			} else {
				require.NoError(t, err)
				require.Equal(t, testCase.expectedReplicas, routingPlan.ReplicaSet, "test case: %s", testCase.name)
			}
		})
	}
}

func TestWriteRoutingWithFSM(t *testing.T) {
	testCases := []struct {
		name                 string
		partitioningEnabled  bool
		allShardNodes        []string
		opStatus             api.ShardReplicationState
		preRoutingPlanAction func(fsm *replication.ShardReplicationFSM)
		expectedReplicas     types.ReplicaSet
		expectedErrorStr     string
	}{
		{
			name:                "registered",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2"},
			opStatus:            api.REGISTERED,
			expectedReplicas:    types.ReplicaSet{Replicas: []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}}},
		},
		{
			name:                "hydrating",
			partitioningEnabled: true,
			allShardNodes:       []string{"node1", "node2"},
			opStatus:            api.HYDRATING,
			expectedReplicas:    types.ReplicaSet{Replicas: []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}}},
		},
		{
			name:                "finalizing",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2"},
			opStatus:            api.FINALIZING,
			expectedReplicas:    types.ReplicaSet{Replicas: []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}}},
		},
		{
			name:                "ready",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2"},
			opStatus:            api.READY,
			expectedReplicas:    types.ReplicaSet{Replicas: []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}, {NodeName: "node2", ShardName: "shard1", HostAddr: "node2"}}},
		},
		{
			name:                "dehydrating",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2"},
			opStatus:            api.DEHYDRATING,
			expectedReplicas:    types.ReplicaSet{Replicas: []types.Replica{{NodeName: "node2", ShardName: "shard1", HostAddr: "node2"}}},
		},
		{
			name:                "cancelled",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2"},
			opStatus:            api.CANCELLED,
			expectedReplicas:    types.ReplicaSet{Replicas: []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}}},
		},
		{
			name:                "ready deleted",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2"},
			opStatus:            api.READY,
			preRoutingPlanAction: func(fsm *replication.ShardReplicationFSM) {
				fsm.CancelReplication(&api.ReplicationCancelRequest{
					Version: api.ReplicationCommandVersionV0,
					Uuid:    "00000000-0000-0000-0000-000000000000",
				})
			},
			expectedReplicas: types.ReplicaSet{Replicas: []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}, {NodeName: "node2", ShardName: "shard1", HostAddr: "node2"}}},
		},
		{
			name:                "registered extra node",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2", "node3"},
			opStatus:            api.REGISTERED,
			expectedReplicas:    types.ReplicaSet{Replicas: []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}, {NodeName: "node3", ShardName: "shard1", HostAddr: "node3"}}},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name+"_partitioning_enabled_"+strconv.FormatBool(testCase.partitioningEnabled), func(t *testing.T) {
			reg := prometheus.NewRegistry()
			shardReplicationFSM := replication.NewShardReplicationFSM(reg)
			clusterState := clusterMocks.NewMockNodeSelector(testCase.allShardNodes...)
			schemaReaderMock := schemaTypes.NewMockSchemaReader(t)
			schemaGetterMock := schema.NewMockSchemaGetter(t)
			schemaGetterMock.EXPECT().OptimisticTenantStatus(mock.Anything, "collection1", "shard1").Return(
				map[string]string{
					"shard1": models.TenantActivityStatusHOT,
				}, nil).Maybe()
			schemaReaderMock.EXPECT().CopyShardingState("collection1").Return(&sharding.State{
				IndexID: "index-001",
				Config:  config.Config{},
				Physical: map[string]sharding.Physical{
					"shard1": {
						Name:                                 "shard1",
						OwnsVirtual:                          []string{},
						OwnsPercentage:                       100,
						LegacyBelongsToNodeForBackwardCompat: "",
						BelongsToNodes:                       testCase.allShardNodes,
						Status:                               testCase.opStatus.String(),
					},
				},
				Virtual:             []sharding.Virtual{},
				PartitioningEnabled: false,
				ReplicationFactor:   1,
			}).Maybe()
			schemaReaderMock.On("ShardReplicas", mock.Anything, mock.Anything).Return(func(class string, shard string) ([]string, error) {
				return testCase.allShardNodes, nil
			})
			myRouter := router.NewBuilder("collection1", testCase.partitioningEnabled, clusterState, schemaGetterMock, schemaReaderMock, shardReplicationFSM).Build()

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
				Shard: "shard1",
			})
			if testCase.expectedErrorStr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), testCase.expectedErrorStr)
			} else {
				require.NoError(t, err)
				require.Equal(t, testCase.expectedReplicas, routingPlan.ReplicaSet, "test case: %s", testCase.name)
			}
		})
	}
}
