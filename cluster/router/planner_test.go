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
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/router"
	"github.com/weaviate/weaviate/cluster/router/types"
)

func testReadReplicaSet() types.ReadReplicaSet {
	return types.ReadReplicaSet{Replicas: []types.Replica{
		{NodeName: "A", ShardName: "S1", HostAddr: "10.12.135.19"},
		{NodeName: "B", ShardName: "S1", HostAddr: "10.12.135.20"},
		{NodeName: "C", ShardName: "S2", HostAddr: "10.12.135.21"},
		{NodeName: "D", ShardName: "S2", HostAddr: "10.12.135.22"},
	}}
}

func testSelectedReadReplicas() types.ReadReplicaSet {
	return types.ReadReplicaSet{Replicas: []types.Replica{
		{NodeName: "B", ShardName: "S1", HostAddr: "10.12.135.20"},
		{NodeName: "C", ShardName: "S2", HostAddr: "10.12.135.21"},
	}}
}

func TestNewReadPlanner_WithCustomStrategy(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	mockStrategy := types.NewMockReadReplicaStrategy(t)
	collection := "TestCollection"

	// WHEN
	planner := router.NewReadPlanner(mockRouter, collection, mockStrategy, "", "")

	// THEN
	require.NotNil(t, planner)
}

func TestNewReadPlanner_WithDefaultStrategy(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	collection := "TestCollection"
	directCandidate := "node-42"
	localNode := "local-node"

	// WHEN
	planner := router.NewReadPlanner(mockRouter, collection, nil, directCandidate, localNode)

	// THEN
	require.NotNil(t, planner)
}

func TestReadPlanner_Plan_Success(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	mockStrategy := types.NewMockReadReplicaStrategy(t)
	collection := "TestCollection"
	shard := "TestShard"

	readReplicas := testReadReplicaSet()
	selectedReplicas := testSelectedReadReplicas()

	mockRouter.EXPECT().GetReadReplicasLocation(collection, "", shard).Return(readReplicas, nil)
	mockStrategy.EXPECT().Apply(readReplicas, mock.Anything).Return(selectedReplicas)

	planner := router.NewReadPlanner(mockRouter, collection, mockStrategy, "", "")

	params := types.RoutingPlanBuildOptions{
		Shard:            shard,
		ConsistencyLevel: types.ConsistencyLevelOne,
	}

	// WHEN
	plan, err := planner.Plan(params)

	// THEN
	require.NoError(t, err)
	require.Equal(t, shard, plan.Shard)
	require.Equal(t, selectedReplicas, plan.ReplicaSet)
	require.Equal(t, types.ConsistencyLevelOne, plan.ConsistencyLevel)
}

func TestReadPlanner_Plan_RouterError(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	mockStrategy := types.NewMockReadReplicaStrategy(t)
	collection := "TestCollection"
	shard := "TestShard"

	expectedError := errors.New("router error")
	mockRouter.EXPECT().GetReadReplicasLocation(collection, "", shard).Return(types.ReadReplicaSet{}, expectedError)

	planner := router.NewReadPlanner(mockRouter, collection, mockStrategy, "", "")

	params := types.RoutingPlanBuildOptions{
		Shard:            shard,
		ConsistencyLevel: types.ConsistencyLevelOne,
	}

	// WHEN
	plan, err := planner.Plan(params)

	// THEN
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to get read replicas")
	require.Contains(t, err.Error(), "router error")
	require.Equal(t, types.ReadRoutingPlan{}, plan)
}

func TestReadPlanner_Plan_EmptyReplicas(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	mockStrategy := types.NewMockReadReplicaStrategy(t)
	collection := "TestCollection"
	shard := "TestShard"

	emptyReplicas := types.ReadReplicaSet{Replicas: []types.Replica{}}
	mockRouter.EXPECT().GetReadReplicasLocation(collection, "", shard).Return(emptyReplicas, nil)

	planner := router.NewReadPlanner(mockRouter, collection, mockStrategy, "", "")

	params := types.RoutingPlanBuildOptions{
		Shard:            shard,
		ConsistencyLevel: "ONE",
	}

	// WHEN
	plan, err := planner.Plan(params)

	// THEN
	require.Error(t, err)
	require.Contains(t, err.Error(), "no replicas available")
	require.Equal(t, types.ReadRoutingPlan{}, plan)
}

func TestReadPlanner_IntegrationWithDirectCandidateStrategy(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	collection := "TestCollection"
	shard := "TestShard"
	directCandidate := "B"
	localNode := "local-node"

	readReplicas := testReadReplicaSet()
	mockRouter.EXPECT().GetReadReplicasLocation(collection, "", shard).Return(readReplicas, nil)

	planner := router.NewReadPlanner(mockRouter, collection, nil, directCandidate, localNode)

	params := types.RoutingPlanBuildOptions{
		Shard:            shard,
		ConsistencyLevel: types.ConsistencyLevelOne,
	}

	// WHEN
	plan, err := planner.Plan(params)

	// THEN
	require.NoError(t, err)
	require.Equal(t, shard, plan.Shard)
	require.Len(t, plan.ReplicaSet.Replicas, 4) // All replicas from testReadReplicaSet()

	s1Replicas := findReplicasByShard(plan.ReplicaSet.Replicas, "S1")
	s2Replicas := findReplicasByShard(plan.ReplicaSet.Replicas, "S2")
	require.Len(t, s1Replicas, 2)                 // S1 has replicas A and B
	require.Len(t, s2Replicas, 2)                 // S2 has replicas C and D
	require.Equal(t, "B", s1Replicas[0].NodeName) // B (direct candidate) should be first for S1
	require.Equal(t, "A", s1Replicas[1].NodeName) // A should be second for S1
}

func findReplicasByShard(replicas []types.Replica, shardName string) []types.Replica {
	var result []types.Replica
	for _, replica := range replicas {
		if replica.ShardName == shardName {
			result = append(result, replica)
		}
	}
	return result
}
