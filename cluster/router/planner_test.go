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
	require.Len(t, plan.ReplicaSet.Replicas, 2, "should have exactly one replica per shard")
	shardCounts := make(map[string]int)
	for _, replica := range plan.ReplicaSet.Replicas {
		shardCounts[replica.ShardName]++
	}
	require.Equal(t, 1, shardCounts["S1"], "should have exactly one replica for shard S1")
	require.Equal(t, 1, shardCounts["S2"], "should have exactly one replica for shard S2")

	s1Replica := findReplicaByShard(plan.ReplicaSet.Replicas, "S1")
	s2Replica := findReplicaByShard(plan.ReplicaSet.Replicas, "S2")
	require.NotNil(t, s1Replica, "should have one replica for S1")
	require.NotNil(t, s2Replica, "should have one replica for S2")

	// S1 should select B (the direct candidate)
	require.Equal(t, "B", s1Replica.NodeName, "S1 should select direct candidate B")

	// S2 should select C (first available since B is not available in S2)
	require.Equal(t, "C", s2Replica.NodeName, "S2 should select first available replica C")
}

func findReplicaByShard(replicas []types.Replica, shardName string) *types.Replica {
	for _, replica := range replicas {
		if replica.ShardName == shardName {
			return &replica
		}
	}
	return nil
}

func TestReadPlanner_IntegrationWithEmptyShard(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	collection := "TestCollection"
	shard := ""
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
	require.Len(t, plan.ReplicaSet.Replicas, 2, "should have exactly one replica per shard even when querying all shards")

	s1Replica := findReplicaByShard(plan.ReplicaSet.Replicas, "S1")
	s2Replica := findReplicaByShard(plan.ReplicaSet.Replicas, "S2")
	require.Equal(t, "B", s1Replica.NodeName, "S1 should select direct candidate B")
	require.Equal(t, "C", s2Replica.NodeName, "S2 should select first available replica C")
}

func TestReadPlanner_NoReplicasAvailable(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	collection := "TestCollection"
	shard := "TestShard"
	directCandidate := "B"
	localNode := "local-node"

	emptyReplicas := types.ReadReplicaSet{Replicas: []types.Replica{}}
	mockRouter.EXPECT().GetReadReplicasLocation(collection, "", shard).Return(emptyReplicas, nil)

	planner := router.NewReadPlanner(mockRouter, collection, nil, directCandidate, localNode)

	params := types.RoutingPlanBuildOptions{
		Shard:            shard,
		ConsistencyLevel: types.ConsistencyLevelOne,
	}

	// WHEN
	plan, err := planner.Plan(params)

	// THEN
	require.Error(t, err)
	require.Contains(t, err.Error(), "no replicas available")
	require.Empty(t, plan.ReplicaSet.Replicas)
}

// Test to verify router error propagation
func TestReadPlanner_RouterError(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	collection := "TestCollection"
	shard := "TestShard"
	directCandidate := "B"
	localNode := "local-node"

	routerError := errors.New("router connection failed")
	mockRouter.EXPECT().GetReadReplicasLocation(collection, "", shard).Return(types.ReadReplicaSet{}, routerError)

	planner := router.NewReadPlanner(mockRouter, collection, nil, directCandidate, localNode)

	params := types.RoutingPlanBuildOptions{
		Shard:            shard,
		ConsistencyLevel: types.ConsistencyLevelOne,
	}

	// WHEN
	plan, err := planner.Plan(params)

	// THEN
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to get read replicas")
	require.Contains(t, err.Error(), "router connection failed")
	require.Empty(t, plan.ReplicaSet.Replicas)
}
