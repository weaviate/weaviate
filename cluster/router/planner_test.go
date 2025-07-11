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

func testMultiShardRoutingPlan() types.ReadRoutingPlan {
	return types.ReadRoutingPlan{
		Shard:  "",
		Tenant: "",
		ReplicaSet: types.ReadReplicaSet{Replicas: []types.Replica{
			{NodeName: "A", ShardName: "S1", HostAddr: "10.12.135.19"},
			{NodeName: "B", ShardName: "S1", HostAddr: "10.12.135.20"},
			{NodeName: "C", ShardName: "S2", HostAddr: "10.12.135.21"},
			{NodeName: "D", ShardName: "S2", HostAddr: "10.12.135.22"},
		}},
		ConsistencyLevel:    types.ConsistencyLevelOne,
		IntConsistencyLevel: 1,
	}
}

func testSingleShardRoutingPlan() types.ReadRoutingPlan {
	return types.ReadRoutingPlan{
		Shard:  "S1",
		Tenant: "",
		ReplicaSet: types.ReadReplicaSet{Replicas: []types.Replica{
			{NodeName: "A", ShardName: "S1", HostAddr: "10.12.135.19"},
			{NodeName: "B", ShardName: "S1", HostAddr: "10.12.135.20"},
		}},
		ConsistencyLevel:    types.ConsistencyLevelOne,
		IntConsistencyLevel: 1,
	}
}

func testTenantRoutingPlan() types.ReadRoutingPlan {
	return types.ReadRoutingPlan{
		Shard:  "tenant1",
		Tenant: "tenant1",
		ReplicaSet: types.ReadReplicaSet{Replicas: []types.Replica{
			{NodeName: "A", ShardName: "tenant1", HostAddr: "10.12.135.19"},
			{NodeName: "B", ShardName: "tenant1", HostAddr: "10.12.135.20"},
		}},
		ConsistencyLevel:    types.ConsistencyLevelOne,
		IntConsistencyLevel: 1,
	}
}

func TestNewReadPlanner(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	collection := "TestCollection"

	// WHEN
	planner := router.NewReadPlanner(mockRouter, collection)

	// THEN
	require.NotNil(t, planner)
}

func TestReadPlanner_Plan_Success_MultipleShards(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	collection := "TestCollection"
	shard := ""
	tenant := ""

	routingPlan := testMultiShardRoutingPlan()
	mockRouter.EXPECT().BuildReadRoutingPlan(types.RoutingPlanBuildOptions{
		Shard:            shard,
		Tenant:           tenant,
		ConsistencyLevel: types.ConsistencyLevelOne,
	}).Return(routingPlan, nil)

	planner := router.NewReadPlanner(mockRouter, collection)

	params := types.RoutingPlanBuildOptions{
		Shard:            shard,
		Tenant:           tenant,
		ConsistencyLevel: types.ConsistencyLevelOne,
	}

	// WHEN
	plan, err := planner.Plan(params)

	// THEN
	require.NoError(t, err)
	require.Equal(t, shard, plan.Shard)
	require.Equal(t, tenant, plan.Tenant)
	require.Equal(t, types.ConsistencyLevelOne, plan.ConsistencyLevel)
	require.Equal(t, 1, plan.IntConsistencyLevel)
	require.Len(t, plan.ReplicaSet.Replicas, 2, "should have exactly one replica per shard")

	shardCounts := make(map[string]int)
	for _, replica := range plan.ReplicaSet.Replicas {
		shardCounts[replica.ShardName]++
	}
	require.Equal(t, 1, shardCounts["S1"], "should have exactly one replica for shard S1")
	require.Equal(t, 1, shardCounts["S2"], "should have exactly one replica for shard S2")

	s1Replica := findReplicaByShard(plan.ReplicaSet.Replicas, "S1")
	s2Replica := findReplicaByShard(plan.ReplicaSet.Replicas, "S2")
	require.NotNil(t, s1Replica)
	require.NotNil(t, s2Replica)
	require.Equal(t, "A", s1Replica.NodeName, "should select first replica A for shard S1")
	require.Equal(t, "C", s2Replica.NodeName, "should select first replica C for shard S2")
}

func TestReadPlanner_Plan_Success_SingleShard(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	collection := "TestCollection"
	shard := "S1"
	tenant := ""

	routingPlan := testSingleShardRoutingPlan()
	mockRouter.EXPECT().BuildReadRoutingPlan(types.RoutingPlanBuildOptions{
		Shard:            shard,
		Tenant:           tenant,
		ConsistencyLevel: types.ConsistencyLevelOne,
	}).Return(routingPlan, nil)

	planner := router.NewReadPlanner(mockRouter, collection)

	params := types.RoutingPlanBuildOptions{
		Shard:            shard,
		Tenant:           tenant,
		ConsistencyLevel: types.ConsistencyLevelOne,
	}

	// WHEN
	plan, err := planner.Plan(params)

	// THEN
	require.NoError(t, err)
	require.Equal(t, shard, plan.Shard)
	require.Equal(t, tenant, plan.Tenant)
	require.Equal(t, types.ConsistencyLevelOne, plan.ConsistencyLevel)
	require.Equal(t, 1, plan.IntConsistencyLevel)
	require.Len(t, plan.ReplicaSet.Replicas, 1, "should have exactly one replica for single shard")
	require.Equal(t, "A", plan.ReplicaSet.Replicas[0].NodeName, "should select first replica A")
	require.Equal(t, "S1", plan.ReplicaSet.Replicas[0].ShardName, "should be for shard S1")
}

func TestReadPlanner_Plan_RouterError(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	collection := "TestCollection"
	shard := "TestShard"
	tenant := ""

	expectedError := errors.New("no replicas found")
	mockRouter.EXPECT().BuildReadRoutingPlan(types.RoutingPlanBuildOptions{
		Shard:            shard,
		Tenant:           tenant,
		ConsistencyLevel: types.ConsistencyLevelOne,
	}).Return(types.ReadRoutingPlan{}, expectedError)

	planner := router.NewReadPlanner(mockRouter, collection)

	params := types.RoutingPlanBuildOptions{
		Shard:            shard,
		Tenant:           tenant,
		ConsistencyLevel: types.ConsistencyLevelOne,
	}

	// WHEN
	plan, err := planner.Plan(params)

	// THEN
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to build read routing plan")
	require.Contains(t, err.Error(), "no replicas found")
	require.Equal(t, types.ReadRoutingPlan{}, plan)
}

func TestReadPlanner_Plan_EmptyReplicas(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	collection := "TestCollection"
	shard := "TestShard"
	tenant := ""

	emptyPlan := types.ReadRoutingPlan{
		Shard:               shard,
		Tenant:              tenant,
		ReplicaSet:          types.ReadReplicaSet{Replicas: []types.Replica{}},
		ConsistencyLevel:    types.ConsistencyLevelOne,
		IntConsistencyLevel: 1,
	}
	mockRouter.EXPECT().BuildReadRoutingPlan(types.RoutingPlanBuildOptions{
		Shard:            shard,
		Tenant:           tenant,
		ConsistencyLevel: types.ConsistencyLevelOne,
	}).Return(emptyPlan, nil)

	planner := router.NewReadPlanner(mockRouter, collection)

	params := types.RoutingPlanBuildOptions{
		Shard:            shard,
		Tenant:           tenant,
		ConsistencyLevel: types.ConsistencyLevelOne,
	}

	// WHEN
	plan, err := planner.Plan(params)

	// THEN
	require.NoError(t, err)
	require.Equal(t, shard, plan.Shard)
	require.Equal(t, tenant, plan.Tenant)
	require.Empty(t, plan.ReplicaSet.Replicas, "should handle empty replicas gracefully")
}

func TestReadPlanner_Plan_WithDirectCandidate(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	collection := "TestCollection"
	shard := "S1"
	tenant := ""
	directCandidate := "B"

	routingPlan := types.ReadRoutingPlan{
		Shard:  shard,
		Tenant: tenant,
		ReplicaSet: types.ReadReplicaSet{Replicas: []types.Replica{
			{NodeName: "B", ShardName: "S1", HostAddr: "10.12.135.20"}, // Preferred first
			{NodeName: "A", ShardName: "S1", HostAddr: "10.12.135.19"},
		}},
		ConsistencyLevel:    types.ConsistencyLevelOne,
		IntConsistencyLevel: 1,
	}

	mockRouter.EXPECT().BuildReadRoutingPlan(types.RoutingPlanBuildOptions{
		Shard:               shard,
		Tenant:              tenant,
		DirectCandidateNode: directCandidate,
		ConsistencyLevel:    types.ConsistencyLevelOne,
	}).Return(routingPlan, nil)

	planner := router.NewReadPlanner(mockRouter, collection)

	params := types.RoutingPlanBuildOptions{
		Shard:               shard,
		Tenant:              tenant,
		DirectCandidateNode: directCandidate,
		ConsistencyLevel:    types.ConsistencyLevelOne,
	}

	// WHEN
	plan, err := planner.Plan(params)

	// THEN
	require.NoError(t, err)
	require.Equal(t, shard, plan.Shard)
	require.Equal(t, tenant, plan.Tenant)
	require.Len(t, plan.ReplicaSet.Replicas, 1, "should have exactly one replica")
	require.Equal(t, "B", plan.ReplicaSet.Replicas[0].NodeName, "should select preferred node B")
}

func TestReadPlanner_Plan_WithTenant(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	collection := "TestCollection"
	shard := "tenant1"
	tenant := "tenant1"

	routingPlan := testTenantRoutingPlan()
	mockRouter.EXPECT().BuildReadRoutingPlan(types.RoutingPlanBuildOptions{
		Shard:            shard,
		Tenant:           tenant,
		ConsistencyLevel: types.ConsistencyLevelOne,
	}).Return(routingPlan, nil)

	planner := router.NewReadPlanner(mockRouter, collection)

	params := types.RoutingPlanBuildOptions{
		Shard:            shard,
		Tenant:           tenant,
		ConsistencyLevel: types.ConsistencyLevelOne,
	}

	// WHEN
	plan, err := planner.Plan(params)

	// THEN
	require.NoError(t, err)
	require.Equal(t, shard, plan.Shard)
	require.Equal(t, tenant, plan.Tenant)
	require.Len(t, plan.ReplicaSet.Replicas, 1, "should have exactly one replica for tenant shard")
	require.Equal(t, "A", plan.ReplicaSet.Replicas[0].NodeName, "should select first replica A")
	require.Equal(t, "tenant1", plan.ReplicaSet.Replicas[0].ShardName, "should be for tenant shard")
}

func TestReadPlanner_Plan_ComplexMultiShard(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	collection := "TestCollection"
	shard := ""
	tenant := ""

	// Router returns multiple replicas per shard, planner should select one per shard.
	routingPlan := types.ReadRoutingPlan{
		Shard:  shard,
		Tenant: tenant,
		ReplicaSet: types.ReadReplicaSet{Replicas: []types.Replica{
			{NodeName: "A", ShardName: "S1", HostAddr: "10.12.135.19"},
			{NodeName: "B", ShardName: "S1", HostAddr: "10.12.135.20"},
			{NodeName: "C", ShardName: "S1", HostAddr: "10.12.135.21"},

			{NodeName: "B", ShardName: "S2", HostAddr: "10.12.135.20"},
			{NodeName: "C", ShardName: "S2", HostAddr: "10.12.135.21"},
			{NodeName: "A", ShardName: "S2", HostAddr: "10.12.135.19"},

			{NodeName: "C", ShardName: "S3", HostAddr: "10.12.135.21"},
			{NodeName: "B", ShardName: "S3", HostAddr: "10.12.135.20"},
			{NodeName: "A", ShardName: "S3", HostAddr: "10.12.135.19"},
		}},
		ConsistencyLevel:    types.ConsistencyLevelOne,
		IntConsistencyLevel: 1,
	}

	mockRouter.EXPECT().BuildReadRoutingPlan(types.RoutingPlanBuildOptions{
		Shard:            shard,
		Tenant:           tenant,
		ConsistencyLevel: types.ConsistencyLevelOne,
	}).Return(routingPlan, nil)

	planner := router.NewReadPlanner(mockRouter, collection)

	params := types.RoutingPlanBuildOptions{
		Shard:            shard,
		Tenant:           tenant,
		ConsistencyLevel: types.ConsistencyLevelOne,
	}

	// WHEN
	plan, err := planner.Plan(params)

	// THEN
	require.NoError(t, err)
	require.Equal(t, shard, plan.Shard)
	require.Equal(t, tenant, plan.Tenant)

	require.Len(t, plan.ReplicaSet.Replicas, 3, "should have exactly one replica per shard")
	shardCounts := make(map[string]int)
	selectedNodes := make(map[string]string)
	for _, replica := range plan.ReplicaSet.Replicas {
		shardCounts[replica.ShardName]++
		selectedNodes[replica.ShardName] = replica.NodeName
	}

	require.Equal(t, 1, shardCounts["S1"], "should have exactly one replica for shard S1")
	require.Equal(t, 1, shardCounts["S2"], "should have exactly one replica for shard S2")
	require.Equal(t, 1, shardCounts["S3"], "should have exactly one replica for shard S3")
	require.Equal(t, "A", selectedNodes["S1"], "should select first replica A for shard S1")
	require.Equal(t, "B", selectedNodes["S2"], "should select first replica B for shard S2")
	require.Equal(t, "C", selectedNodes["S3"], "should select first replica C for shard S3")
}

func TestReadPlanner_Plan_MultiTenant_SpecificShard(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	collection := "TestCollection"
	shard := "luke"
	tenant := "luke"

	routingPlan := types.ReadRoutingPlan{
		Shard:  shard,
		Tenant: tenant,
		ReplicaSet: types.ReadReplicaSet{Replicas: []types.Replica{
			{NodeName: "C", ShardName: shard, HostAddr: "10.12.135.21"},
			{NodeName: "D", ShardName: shard, HostAddr: "10.12.135.22"},
		}},
		ConsistencyLevel:    types.ConsistencyLevelOne,
		IntConsistencyLevel: 1,
	}

	mockRouter.EXPECT().BuildReadRoutingPlan(types.RoutingPlanBuildOptions{
		Shard:            shard,
		Tenant:           tenant,
		ConsistencyLevel: types.ConsistencyLevelOne,
	}).Return(routingPlan, nil)

	planner := router.NewReadPlanner(mockRouter, collection)

	params := types.RoutingPlanBuildOptions{
		Shard:            shard,
		Tenant:           tenant,
		ConsistencyLevel: types.ConsistencyLevelOne,
	}

	// WHEN
	plan, err := planner.Plan(params)

	// THEN
	require.NoError(t, err)
	require.Equal(t, shard, plan.Shard)
	require.Equal(t, tenant, plan.Tenant)
	require.Len(t, plan.ReplicaSet.Replicas, 1, "should have exactly one replica for specific tenant shard")
	require.Equal(t, "C", plan.ReplicaSet.Replicas[0].NodeName, "should select first replica C")
	require.Equal(t, "luke", plan.ReplicaSet.Replicas[0].ShardName, "should be for luke shard")
}

func TestReadPlanner_Plan_ConsistencyLevel_QUORUM(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	collection := "TestCollection"
	shard := "S1"
	tenant := ""

	// Router returns 3 replicas, QUORUM requires 2
	routingPlan := types.ReadRoutingPlan{
		Shard:  shard,
		Tenant: tenant,
		ReplicaSet: types.ReadReplicaSet{Replicas: []types.Replica{
			{NodeName: "A", ShardName: "S1", HostAddr: "10.12.135.19"},
			{NodeName: "B", ShardName: "S1", HostAddr: "10.12.135.20"},
			{NodeName: "C", ShardName: "S1", HostAddr: "10.12.135.21"},
		}},
		ConsistencyLevel:    types.ConsistencyLevelQuorum,
		IntConsistencyLevel: 2, // 3/2 + 1 = 2
	}

	mockRouter.EXPECT().BuildReadRoutingPlan(types.RoutingPlanBuildOptions{
		Shard:            shard,
		Tenant:           tenant,
		ConsistencyLevel: types.ConsistencyLevelQuorum,
	}).Return(routingPlan, nil)

	planner := router.NewReadPlanner(mockRouter, collection)

	params := types.RoutingPlanBuildOptions{
		Shard:            shard,
		Tenant:           tenant,
		ConsistencyLevel: types.ConsistencyLevelQuorum,
	}

	// WHEN
	plan, err := planner.Plan(params)

	// THEN
	require.NoError(t, err)
	require.Equal(t, types.ConsistencyLevelQuorum, plan.ConsistencyLevel)
	require.Equal(t, 2, plan.IntConsistencyLevel)
	// Planner selects the required number of replicas based on consistency level
	require.Len(t, plan.ReplicaSet.Replicas, 2, "should select 2 replicas for QUORUM")
	require.Equal(t, "A", plan.ReplicaSet.Replicas[0].NodeName, "should select first replica (preferred)")
	require.Equal(t, "B", plan.ReplicaSet.Replicas[1].NodeName, "should select second replica")
}

func TestReadPlanner_Plan_ConsistencyLevel_ALL(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	collection := "TestCollection"
	shard := "S1"
	tenant := ""

	// Router returns 2 replicas, ALL requires 2
	routingPlan := types.ReadRoutingPlan{
		Shard:  shard,
		Tenant: tenant,
		ReplicaSet: types.ReadReplicaSet{Replicas: []types.Replica{
			{NodeName: "A", ShardName: "S1", HostAddr: "10.12.135.19"},
			{NodeName: "B", ShardName: "S1", HostAddr: "10.12.135.20"},
		}},
		ConsistencyLevel:    types.ConsistencyLevelAll,
		IntConsistencyLevel: 2, // ALL = 2
	}

	mockRouter.EXPECT().BuildReadRoutingPlan(types.RoutingPlanBuildOptions{
		Shard:            shard,
		Tenant:           tenant,
		ConsistencyLevel: types.ConsistencyLevelAll,
	}).Return(routingPlan, nil)

	planner := router.NewReadPlanner(mockRouter, collection)

	params := types.RoutingPlanBuildOptions{
		Shard:            shard,
		Tenant:           tenant,
		ConsistencyLevel: types.ConsistencyLevelAll,
	}

	// WHEN
	plan, err := planner.Plan(params)

	// THEN
	require.NoError(t, err)
	require.Equal(t, types.ConsistencyLevelAll, plan.ConsistencyLevel)
	require.Equal(t, 2, plan.IntConsistencyLevel)
	// Planner selects all replicas for ALL consistency level
	require.Len(t, plan.ReplicaSet.Replicas, 2, "should select all replicas for ALL consistency level")
	require.Equal(t, "A", plan.ReplicaSet.Replicas[0].NodeName, "should select first replica")
	require.Equal(t, "B", plan.ReplicaSet.Replicas[1].NodeName, "should select second replica")
}

func TestReadPlanner_Plan_ConsistencyLevel_InvalidQuorum(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	collection := "TestCollection"
	shard := "S1"
	tenant := ""

	// Router fails consistency validation - only 1 replica but QUORUM needs 1 (should pass)
	routingPlan := types.ReadRoutingPlan{
		Shard:  shard,
		Tenant: tenant,
		ReplicaSet: types.ReadReplicaSet{Replicas: []types.Replica{
			{NodeName: "A", ShardName: "S1", HostAddr: "10.12.135.19"},
		}},
		ConsistencyLevel:    types.ConsistencyLevelQuorum,
		IntConsistencyLevel: 1, // 1/2 + 1 = 1
	}

	mockRouter.EXPECT().BuildReadRoutingPlan(types.RoutingPlanBuildOptions{
		Shard:            shard,
		Tenant:           tenant,
		ConsistencyLevel: types.ConsistencyLevelQuorum,
	}).Return(routingPlan, nil)

	planner := router.NewReadPlanner(mockRouter, collection)

	params := types.RoutingPlanBuildOptions{
		Shard:            shard,
		Tenant:           tenant,
		ConsistencyLevel: types.ConsistencyLevelQuorum,
	}

	// WHEN
	plan, err := planner.Plan(params)

	// THEN
	require.NoError(t, err)
	require.Equal(t, types.ConsistencyLevelQuorum, plan.ConsistencyLevel)
	require.Equal(t, 1, plan.IntConsistencyLevel)
	require.Len(t, plan.ReplicaSet.Replicas, 1, "should have one replica")
}

func TestReadPlanner_Plan_ConsistencyLevel_RouterValidationError(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	collection := "TestCollection"
	shard := "S1"
	tenant := ""

	// Router returns consistency validation error
	expectedError := errors.New("impossible to satisfy consistency level (2) > available Replicas (1)")
	mockRouter.EXPECT().BuildReadRoutingPlan(types.RoutingPlanBuildOptions{
		Shard:            shard,
		Tenant:           tenant,
		ConsistencyLevel: types.ConsistencyLevelAll,
	}).Return(types.ReadRoutingPlan{}, expectedError)

	planner := router.NewReadPlanner(mockRouter, collection)

	params := types.RoutingPlanBuildOptions{
		Shard:            shard,
		Tenant:           tenant,
		ConsistencyLevel: types.ConsistencyLevelAll,
	}

	// WHEN
	plan, err := planner.Plan(params)

	// THEN
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to build read routing plan")
	require.Contains(t, err.Error(), "impossible to satisfy consistency level")
	require.Equal(t, types.ReadRoutingPlan{}, plan)
}

func TestReadPlanner_Plan_ConsistencyLevel_MultiShard_QUORUM(t *testing.T) {
	// GIVEN
	mockRouter := types.NewMockRouter(t)
	collection := "TestCollection"
	shard := ""
	tenant := ""

	// Multiple shards, each with 3 replicas (RF=3)
	routingPlan := types.ReadRoutingPlan{
		Shard:  shard,
		Tenant: tenant,
		ReplicaSet: types.ReadReplicaSet{Replicas: []types.Replica{
			{NodeName: "A", ShardName: "S1", HostAddr: "10.12.135.19"},
			{NodeName: "B", ShardName: "S1", HostAddr: "10.12.135.20"},
			{NodeName: "C", ShardName: "S1", HostAddr: "10.12.135.21"},

			{NodeName: "B", ShardName: "S2", HostAddr: "10.12.135.20"},
			{NodeName: "C", ShardName: "S2", HostAddr: "10.12.135.21"},
			{NodeName: "A", ShardName: "S2", HostAddr: "10.12.135.19"},
		}},
		ConsistencyLevel:    types.ConsistencyLevelQuorum,
		IntConsistencyLevel: 2, // QUORUM = 3/2 + 1 = 2 replicas per shard
	}

	mockRouter.EXPECT().BuildReadRoutingPlan(types.RoutingPlanBuildOptions{
		Shard:            shard,
		Tenant:           tenant,
		ConsistencyLevel: types.ConsistencyLevelQuorum,
	}).Return(routingPlan, nil)

	planner := router.NewReadPlanner(mockRouter, collection)

	params := types.RoutingPlanBuildOptions{
		Shard:            shard,
		Tenant:           tenant,
		ConsistencyLevel: types.ConsistencyLevelQuorum,
	}

	// WHEN
	plan, err := planner.Plan(params)

	// THEN
	require.NoError(t, err)
	require.Equal(t, types.ConsistencyLevelQuorum, plan.ConsistencyLevel)
	require.Equal(t, 2, plan.IntConsistencyLevel)
	require.Len(t, plan.ReplicaSet.Replicas, 4, "should select 2 replicas from each of 2 shards")

	shardCounts := make(map[string]int)
	for _, replica := range plan.ReplicaSet.Replicas {
		shardCounts[replica.ShardName]++
	}
	require.Equal(t, 2, shardCounts["S1"], "should have 2 replicas for S1 (QUORUM)")
	require.Equal(t, 2, shardCounts["S2"], "should have 2 replicas for S2 (QUORUM)")

	// Verify we got the preferred replicas (first 2) from each shard
	var s1Replicas []string
	var s2Replicas []string
	for _, replica := range plan.ReplicaSet.Replicas {
		if replica.ShardName == "S1" {
			s1Replicas = append(s1Replicas, replica.NodeName)
		} else if replica.ShardName == "S2" {
			s2Replicas = append(s2Replicas, replica.NodeName)
		}
	}
	require.Contains(t, s1Replicas, "A", "should include first replica from S1")
	require.Contains(t, s1Replicas, "B", "should include second replica from S1")
	require.Contains(t, s2Replicas, "B", "should include first replica from S2")
	require.Contains(t, s2Replicas, "C", "should include second replica from S2")
}

// Helper function to find replica by shard name
func findReplicaByShard(replicas []types.Replica, shardName string) *types.Replica {
	for _, replica := range replicas {
		if replica.ShardName == shardName {
			return &replica
		}
	}
	return nil
}
