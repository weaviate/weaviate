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

func testWriteReplicaSet() types.WriteReplicaSet {
	return types.WriteReplicaSet{
		Replicas: []types.Replica{
			{NodeName: "A", ShardName: "S1", HostAddr: "10.12.135.19"},
			{NodeName: "B", ShardName: "S1", HostAddr: "10.12.135.20"},
			{NodeName: "C", ShardName: "S2", HostAddr: "10.12.135.21"},
		},
		AdditionalReplicas: []types.Replica{
			{NodeName: "E", ShardName: "S1", HostAddr: "10.12.135.23"},
		},
	}
}

func testSelectedReadReplicas() types.ReadReplicaSet {
	return types.ReadReplicaSet{Replicas: []types.Replica{
		{NodeName: "B", ShardName: "S1", HostAddr: "10.12.135.20"},
		{NodeName: "C", ShardName: "S2", HostAddr: "10.12.135.21"},
	}}
}

func testOrderedWriteReplicas() types.WriteReplicaSet {
	return types.WriteReplicaSet{
		Replicas: []types.Replica{
			{NodeName: "B", ShardName: "S1", HostAddr: "10.12.135.20"}, // B first
			{NodeName: "A", ShardName: "S1", HostAddr: "10.12.135.19"},
			{NodeName: "C", ShardName: "S2", HostAddr: "10.12.135.21"},
		},
		AdditionalReplicas: []types.Replica{
			{NodeName: "E", ShardName: "S1", HostAddr: "10.12.135.23"},
		},
	}
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

	mockRouter.EXPECT().GetReadReplicasLocation(collection, shard).Return(readReplicas, nil)
	mockStrategy.EXPECT().Apply(readReplicas).Return(selectedReplicas)

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
	mockRouter.EXPECT().GetReadReplicasLocation(collection, shard).Return(types.ReadReplicaSet{}, expectedError)

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
	mockRouter.EXPECT().GetReadReplicasLocation(collection, shard).Return(emptyReplicas, nil)

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

func TestNewWritePlanner_WithCustomStrategy(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	mockStrategy := types.NewMockWriteReplicaStrategy(t)
	collection := "TestCollection"

	// WHEN
	planner := router.NewWritePlanner(mockRouter, collection, mockStrategy, "", "")

	// THEN
	require.NotNil(t, planner)
}

func TestNewWritePlanner_WithDefaultStrategy(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	collection := "TestCollection"
	directCandidate := "node-42"
	localNode := "local-node"

	// WHEN
	planner := router.NewWritePlanner(mockRouter, collection, nil, directCandidate, localNode)

	// THEN
	require.NotNil(t, planner)
}

func TestWritePlanner_Plan_Success(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	mockStrategy := types.NewMockWriteReplicaStrategy(t)
	collection := "TestCollection"
	shard := "TestShard"

	writeReplicas := testWriteReplicaSet()
	orderedReplicas := testOrderedWriteReplicas()

	mockRouter.EXPECT().GetWriteReplicasLocation(collection, shard).Return(writeReplicas, nil)
	mockStrategy.EXPECT().Apply(writeReplicas).Return(orderedReplicas)

	planner := router.NewWritePlanner(mockRouter, collection, mockStrategy, "", "")

	params := types.RoutingPlanBuildOptions{
		Shard:            shard,
		ConsistencyLevel: types.ConsistencyLevelQuorum,
	}

	// WHEN
	plan, err := planner.Plan(params)

	// THEN
	require.NoError(t, err)
	require.Equal(t, shard, plan.Shard)
	require.Equal(t, orderedReplicas, plan.ReplicaSet)
	require.Equal(t, types.ConsistencyLevelQuorum, plan.ConsistencyLevel)
}

func TestWritePlanner_Plan_RouterError(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	mockStrategy := types.NewMockWriteReplicaStrategy(t)
	collection := "TestCollection"
	shard := "TestShard"

	expectedError := errors.New("router error")
	mockRouter.EXPECT().GetWriteReplicasLocation(collection, shard).Return(types.WriteReplicaSet{}, expectedError)

	planner := router.NewWritePlanner(mockRouter, collection, mockStrategy, "", "")

	params := types.RoutingPlanBuildOptions{
		Shard:            shard,
		ConsistencyLevel: types.ConsistencyLevelQuorum,
	}

	// WHEN
	plan, err := planner.Plan(params)

	// THEN
	require.Error(t, err)
	require.Contains(t, err.Error(), "error while getting write replicas")
	require.Contains(t, err.Error(), "router error")
	require.Equal(t, types.WriteRoutingPlan{}, plan)
}

func TestWritePlanner_Plan_EmptyReplicas(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	mockStrategy := types.NewMockWriteReplicaStrategy(t)
	collection := "TestCollection"
	shard := "TestShard"

	emptyReplicas := types.WriteReplicaSet{Replicas: []types.Replica{}}
	mockRouter.EXPECT().GetWriteReplicasLocation(collection, shard).Return(emptyReplicas, nil)

	planner := router.NewWritePlanner(mockRouter, collection, mockStrategy, "", "")

	params := types.RoutingPlanBuildOptions{
		Shard:            shard,
		ConsistencyLevel: types.ConsistencyLevelQuorum,
	}

	// WHEN
	plan, err := planner.Plan(params)

	// THEN
	require.Error(t, err)
	require.Contains(t, err.Error(), "no write replicas available")
	require.Equal(t, types.WriteRoutingPlan{}, plan)
}

func TestReadPlanner_IntegrationWithDirectCandidateStrategy(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	collection := "TestCollection"
	shard := "TestShard"
	directCandidate := "B"
	localNode := "local-node"

	readReplicas := testReadReplicaSet()
	mockRouter.EXPECT().GetReadReplicasLocation(collection, shard).Return(readReplicas, nil)

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
	require.Len(t, plan.ReplicaSet.Replicas, 2) // One per shard

	s1Replica := findReplicaByShard(plan.ReplicaSet.Replicas, "S1")
	s2Replica := findReplicaByShard(plan.ReplicaSet.Replicas, "S2")

	require.NotNil(t, s1Replica)
	require.NotNil(t, s2Replica)
	require.Equal(t, "B", s1Replica.NodeName) // Should prefer B for S1
	// S2 could be C or D, both are valid
}

func TestWritePlanner_IntegrationWithDirectCandidateStrategy(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	collection := "TestCollection"
	shard := "TestShard"
	directCandidate := "B"
	localNode := "local-node"

	writeReplicas := testWriteReplicaSet()
	mockRouter.EXPECT().GetWriteReplicasLocation(collection, shard).Return(writeReplicas, nil)

	planner := router.NewWritePlanner(mockRouter, collection, nil, directCandidate, localNode)

	params := types.RoutingPlanBuildOptions{
		Shard:            shard,
		ConsistencyLevel: types.ConsistencyLevelQuorum,
	}

	// WHEN
	plan, err := planner.Plan(params)

	// THEN
	require.NoError(t, err)
	require.Equal(t, shard, plan.Shard)
	require.Len(t, plan.ReplicaSet.Replicas, 3)                 // All replicas
	require.Equal(t, "B", plan.ReplicaSet.Replicas[0].NodeName) // B should be first
	require.ElementsMatch(t, writeReplicas.Replicas, plan.ReplicaSet.Replicas)
	require.ElementsMatch(t, writeReplicas.AdditionalReplicas, plan.ReplicaSet.AdditionalReplicas)
}

func findReplicaByShard(replicas []types.Replica, shardName string) *types.Replica {
	for _, replica := range replicas {
		if replica.ShardName == shardName {
			return &replica
		}
	}
	return nil
}
