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

	"github.com/weaviate/weaviate/usecases/sharding"
	"github.com/weaviate/weaviate/usecases/sharding/config"

	"github.com/weaviate/weaviate/entities/models"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/router"
	"github.com/weaviate/weaviate/cluster/router/types"
	clusterMocks "github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/schema"
)

func TestReadRoutingWithFSM(t *testing.T) {
	testCases := []struct {
		name                 string
		partitioningEnabled  bool
		allShardNodes        []string
		opStatus             api.ShardReplicationState
		preRoutingPlanAction func(fsm *replication.ShardReplicationFSM)
		directCandidate      string
		localNodeName        string
		expectedReplicas     types.ReadReplicaSet
		expectedErrorStr     string
	}{
		{
			name:                "registered",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2"},
			opStatus:            api.REGISTERED,
			expectedReplicas:    types.ReadReplicaSet{Replicas: []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}}},
			directCandidate:     "node1",
			localNodeName:       "node1",
		},
		{
			name:                "hydrating",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2"},
			opStatus:            api.HYDRATING,
			expectedReplicas:    types.ReadReplicaSet{Replicas: []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}}},
			directCandidate:     "node1",
			localNodeName:       "node1",
		},
		{
			name:                "finalizing",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2"},
			opStatus:            api.FINALIZING,
			expectedReplicas:    types.ReadReplicaSet{Replicas: []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}}},
			directCandidate:     "node1",
			localNodeName:       "node1",
		},
		{
			name:                "ready",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2"},
			opStatus:            api.READY,
			expectedReplicas:    types.ReadReplicaSet{Replicas: []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}, {NodeName: "node2", ShardName: "shard1", HostAddr: "node2"}}},
			directCandidate:     "node1",
			localNodeName:       "node1",
		},
		{
			name:                "dehydrating",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2"},
			opStatus:            api.DEHYDRATING,
			expectedReplicas:    types.ReadReplicaSet{Replicas: []types.Replica{{NodeName: "node2", ShardName: "shard1", HostAddr: "node2"}}},
			directCandidate:     "node1",
			localNodeName:       "node1",
		},
		{
			name:                "cancelled",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2"},
			opStatus:            api.CANCELLED,
			expectedReplicas:    types.ReadReplicaSet{Replicas: []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}}},
			directCandidate:     "node1",
			localNodeName:       "node1",
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
			expectedReplicas: types.ReadReplicaSet{Replicas: []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}, {NodeName: "node2", ShardName: "shard1", HostAddr: "node2"}}},
			directCandidate:  "node1",
			localNodeName:    "node1",
		},
		{
			name:                "registered extra node",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2", "node3"},
			opStatus:            api.REGISTERED,
			expectedReplicas:    types.ReadReplicaSet{Replicas: []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}, {NodeName: "node3", ShardName: "shard1", HostAddr: "node3"}}},
			directCandidate:     "node1",
			localNodeName:       "node1",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name+"_partitioning_enabled_"+strconv.FormatBool(testCase.partitioningEnabled), func(t *testing.T) {
			reg := prometheus.NewRegistry()
			shardReplicationFSM := replication.NewShardReplicationFSM(reg)
			clusterState := clusterMocks.NewMockNodeSelector(testCase.allShardNodes...)
			schemaReaderMock := schema.NewMockSchemaReader(t)
			schemaGetterMock := schema.NewMockSchemaGetter(t)
			schemaGetterMock.EXPECT().OptimisticTenantStatus(mock.Anything, "collection1", "shard1").Return(
				map[string]string{
					"shard1": models.TenantActivityStatusHOT,
				}, nil).Maybe()
			state := &sharding.State{
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
			}
			schemaReaderMock.EXPECT().Shards(mock.Anything).Return(state.AllPhysicalShards(), nil).Maybe()
			schemaReaderMock.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(func(className string, readFunc func(*models.Class, *sharding.State) error) error {
				class := &models.Class{Class: className}
				return readFunc(class, state)
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

			tenant := ""
			if testCase.partitioningEnabled {
				tenant = "shard1"
			}
			// Build the routing plan
			readPlan, err := myRouter.BuildReadRoutingPlan(types.RoutingPlanBuildOptions{
				Shard:  "shard1",
				Tenant: tenant,
			})
			if testCase.expectedErrorStr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), testCase.expectedErrorStr)
			} else {
				require.NoError(t, err)
				require.Equal(t, testCase.expectedReplicas, readPlan.ReplicaSet, "test case: %s", testCase.name)
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
		directCandidate      string
		localNodeName        string
		expectedReplicas     []types.Replica
		expectedErrorStr     string
	}{
		{
			name:                "registered",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2"},
			opStatus:            api.REGISTERED,
			expectedReplicas:    []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}},
			directCandidate:     "node1",
			localNodeName:       "node1",
		},
		{
			name:                "hydrating",
			partitioningEnabled: true,
			allShardNodes:       []string{"node1", "node2"},
			opStatus:            api.HYDRATING,
			expectedReplicas:    []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}},
			directCandidate:     "node1",
			localNodeName:       "node1",
		},
		{
			name:                "finalizing",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2"},
			opStatus:            api.FINALIZING,
			expectedReplicas:    []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}},
			directCandidate:     "node1",
			localNodeName:       "node1",
		},
		{
			name:                "ready",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2"},
			opStatus:            api.READY,
			expectedReplicas:    []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}, {NodeName: "node2", ShardName: "shard1", HostAddr: "node2"}},
			directCandidate:     "node1",
			localNodeName:       "node1",
		},
		{
			name:                "dehydrating",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2"},
			opStatus:            api.DEHYDRATING,
			expectedReplicas:    []types.Replica{{NodeName: "node2", ShardName: "shard1", HostAddr: "node2"}},
			directCandidate:     "node1",
			localNodeName:       "node1",
		},
		{
			name:                "cancelled",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2"},
			opStatus:            api.CANCELLED,
			expectedReplicas:    []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}},
			directCandidate:     "node1",
			localNodeName:       "node1",
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
			expectedReplicas: []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}, {NodeName: "node2", ShardName: "shard1", HostAddr: "node2"}},
			directCandidate:  "node1",
			localNodeName:    "node1",
		},
		{
			name:                "registered extra node",
			partitioningEnabled: rand.Uint64()%2 == 0,
			allShardNodes:       []string{"node1", "node2", "node3"},
			opStatus:            api.REGISTERED,
			expectedReplicas:    []types.Replica{{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"}, {NodeName: "node3", ShardName: "shard1", HostAddr: "node3"}},
			directCandidate:     "node1",
			localNodeName:       "node1",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name+"_partitioning_enabled_"+strconv.FormatBool(testCase.partitioningEnabled), func(t *testing.T) {
			reg := prometheus.NewRegistry()
			shardReplicationFSM := replication.NewShardReplicationFSM(reg)
			clusterState := clusterMocks.NewMockNodeSelector(testCase.allShardNodes...)
			schemaReaderMock := schema.NewMockSchemaReader(t)
			schemaGetterMock := schema.NewMockSchemaGetter(t)
			schemaGetterMock.EXPECT().OptimisticTenantStatus(mock.Anything, "collection1", "shard1").Return(
				map[string]string{
					"shard1": models.TenantActivityStatusHOT,
				}, nil).Maybe()
			state := &sharding.State{
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
			}
			schemaReaderMock.EXPECT().Shards(mock.Anything).Return(state.AllPhysicalShards(), nil).Maybe()
			schemaReaderMock.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(func(className string, readFunc func(*models.Class, *sharding.State) error) error {
				class := &models.Class{Class: className}
				return readFunc(class, state)
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

			tenant := ""
			if testCase.partitioningEnabled {
				tenant = "shard1"
			}
			ws, err := myRouter.GetWriteReplicasLocation("collection1", tenant, "shard1")
			if testCase.expectedErrorStr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), testCase.expectedErrorStr)
			} else {
				require.NoError(t, err)
				require.Equal(t, testCase.expectedReplicas, ws.Replicas, "test case: %s", testCase.name)
			}
		})
	}
}
