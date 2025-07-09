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

package types_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
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

func emptyTestRoutingPlanBuildOptions() types.RoutingPlanBuildOptions {
	return types.RoutingPlanBuildOptions{}
}

func TestDirectCandidateReadStrategy_PrefersCandidate(t *testing.T) {
	// GIVEN
	dc := types.NewDirectCandidate("B", "")
	strategy := types.NewDirectCandidateReadStrategy(dc)
	rs := testReadReplicaSet()
	options := emptyTestRoutingPlanBuildOptions()

	// WHEN
	actual := strategy.Apply(rs, options)

	// THEN
	expectedShardToNode := map[string]string{
		"S1": "B", // S1 should pick node B (direct candidate)
		"S2": "C", // S2 has no candidate, should pick first available (C)
	}

	assertReadStrategyResult(t, actual, expectedShardToNode)
}

func TestDirectCandidateReadStrategy_FallsBackToLocalNode(t *testing.T) {
	// GIVEN
	dc := types.NewDirectCandidate("", "D")
	strategy := types.NewDirectCandidateReadStrategy(dc)
	rs := testReadReplicaSet()
	options := emptyTestRoutingPlanBuildOptions()

	// WHEN
	actual := strategy.Apply(rs, options)

	// THEN
	expectedShardToNode := map[string]string{
		"S1": "A", // S1 has no local node D, should pick first available (A)
		"S2": "D", // S2 should pick node D (local node)
	}

	assertReadStrategyResult(t, actual, expectedShardToNode)
}

func TestDirectCandidateReadStrategy_FallbackWhenAbsent(t *testing.T) {
	// GIVEN
	dc := types.NewDirectCandidate("unknown", "")
	strategy := types.NewDirectCandidateReadStrategy(dc)
	rs := testReadReplicaSet()
	options := emptyTestRoutingPlanBuildOptions()

	// WHEN
	actual := strategy.Apply(rs, options)

	// THEN
	expectedShardToNode := map[string]string{
		"S1": "A", // Should pick first available (A)
		"S2": "C", // Should pick first available (C)
	}

	assertReadStrategyResult(t, actual, expectedShardToNode)
}

func TestDirectCandidateReadStrategy_SingleReplica(t *testing.T) {
	// GIVEN
	rs := types.ReadReplicaSet{Replicas: []types.Replica{
		{NodeName: "Solo", ShardName: "S1", HostAddr: "1.1.1.1"},
	}}
	dc := types.NewDirectCandidate("Other", "")
	strategy := types.NewDirectCandidateReadStrategy(dc)
	options := emptyTestRoutingPlanBuildOptions()

	// WHEN
	actual := strategy.Apply(rs, options)

	// THEN
	expectedShardToNode := map[string]string{
		"S1": "Solo",
	}

	assertReadStrategyResult(t, actual, expectedShardToNode)
}

func TestDirectCandidateReadStrategy_EmptyReplicaSet(t *testing.T) {
	// GIVEN
	rs := types.ReadReplicaSet{Replicas: []types.Replica{}}
	dc := types.NewDirectCandidate("Other", "")
	strategy := types.NewDirectCandidateReadStrategy(dc)
	options := emptyTestRoutingPlanBuildOptions()

	// WHEN
	actual := strategy.Apply(rs, options)

	// THEN
	assertReadStrategyResult(t, actual, map[string]string{})
}

func TestDirectCandidateReadStrategy_SingleReplicaPerShard(t *testing.T) {
	// GIVEN
	rs := types.ReadReplicaSet{Replicas: []types.Replica{
		{NodeName: "A", ShardName: "S1", HostAddr: "10.12.135.19"},
		{NodeName: "C", ShardName: "S2", HostAddr: "10.12.135.21"},
	}}
	dc := types.NewDirectCandidate("B", "")
	strategy := types.NewDirectCandidateReadStrategy(dc)
	options := emptyTestRoutingPlanBuildOptions()

	// WHEN
	actual := strategy.Apply(rs, options)

	// THEN
	expectedShardToNode := map[string]string{
		"S1": "A", // Only A available for S1
		"S2": "C", // Only C available for S2
	}

	assertReadStrategyResult(t, actual, expectedShardToNode)
}

func TestDirectCandidateReadStrategy_OptionsOverrideStrategyPreference(t *testing.T) {
	// GIVEN
	dc := types.NewDirectCandidate("B", "")
	strategy := types.NewDirectCandidateReadStrategy(dc)
	rs := testReadReplicaSet()
	options := types.RoutingPlanBuildOptions{
		DirectCandidateNode: "A",
	}

	// WHEN
	actual := strategy.Apply(rs, options)

	// THEN
	expectedShardToNode := map[string]string{
		"S1": "A", // Should prefer A from options over B from strategy
		"S2": "C", // A not available in S2, should pick first available
	}

	assertReadStrategyResult(t, actual, expectedShardToNode)
}

func TestDirectCandidateReadStrategy_OptionsOverrideFallback(t *testing.T) {
	// GIVEN
	dc := types.NewDirectCandidate("", "D")
	strategy := types.NewDirectCandidateReadStrategy(dc)
	rs := testReadReplicaSet()
	options := types.RoutingPlanBuildOptions{
		DirectCandidateNode: "C",
	}

	// WHEN
	actual := strategy.Apply(rs, options)

	// THEN
	expectedShardToNode := map[string]string{
		"S1": "A", // C not available in S1, should pick first available
		"S2": "C", // Should prefer C from options over D from strategy
	}

	assertReadStrategyResult(t, actual, expectedShardToNode)
}

func TestDirectCandidateReadStrategy_EmptyOptionsUsesStrategyDefault(t *testing.T) {
	// GIVEN
	dc := types.NewDirectCandidate("B", "")
	strategy := types.NewDirectCandidateReadStrategy(dc)
	rs := testReadReplicaSet()
	options := types.RoutingPlanBuildOptions{
		DirectCandidateNode: "",
	}

	// WHEN
	actual := strategy.Apply(rs, options)

	// THEN
	expectedShardToNode := map[string]string{
		"S1": "B", // Should use strategy default
		"S2": "C", // B not available in S2, should pick first available
	}

	assertReadStrategyResult(t, actual, expectedShardToNode)
}

func TestDirectCandidateReadStrategy_OptionsOverrideToNonExistentNode(t *testing.T) {
	// GIVEN
	dc := types.NewDirectCandidate("B", "")
	strategy := types.NewDirectCandidateReadStrategy(dc)
	rs := testReadReplicaSet()
	options := types.RoutingPlanBuildOptions{
		DirectCandidateNode: "NonExistent",
	}

	// WHEN
	actual := strategy.Apply(rs, options)

	// THEN
	expectedShardToNode := map[string]string{
		"S1": "A", // NonExistent not available, should pick first available
		"S2": "C", // NonExistent not available, should pick first available
	}

	assertReadStrategyResult(t, actual, expectedShardToNode)
}

func assertReadStrategyResult(t *testing.T, actual types.ReadReplicaSet, expectedShardToNode map[string]string) {
	actualShards := make(map[string][]types.Replica)
	for _, replica := range actual.Replicas {
		actualShards[replica.ShardName] = append(actualShards[replica.ShardName], replica)
	}

	require.Len(t, actualShards, len(expectedShardToNode), "should have exactly the expected number of shards")

	for shard, expectedNode := range expectedShardToNode {
		actualReplicasForShard, exists := actualShards[shard]
		require.True(t, exists, fmt.Sprintf("should have replicas for shard %s", shard))
		require.Len(t, actualReplicasForShard, 1,
			fmt.Sprintf("should have exactly one replica for shard %s, got %d", shard, len(actualReplicasForShard)))
		require.Equal(t, expectedNode, actualReplicasForShard[0].NodeName,
			fmt.Sprintf("for shard %s, expected node %s but got %s", shard, expectedNode, actualReplicasForShard[0].NodeName))
	}

	expectedTotalReplicas := len(expectedShardToNode)
	require.Len(t, actual.Replicas, expectedTotalReplicas,
		fmt.Sprintf("should have exactly %d total replicas (one per shard), got %d", expectedTotalReplicas, len(actual.Replicas)))
}

func TestNewDirectCandidate_WithDirectCandidate(t *testing.T) {
	// GIVEN
	directCandidate := "node-42"
	localNode := "local-node"

	// WHEN
	dc := types.NewDirectCandidate(directCandidate, localNode)

	// THEN
	require.Equal(t, directCandidate, dc.PreferredNodeName)
}

func TestNewDirectCandidate_FallbackToLocalNode(t *testing.T) {
	// GIVEN
	directCandidate := ""
	localNode := "local-node"

	// WHEN
	dc := types.NewDirectCandidate(directCandidate, localNode)

	// THEN
	require.Equal(t, localNode, dc.PreferredNodeName)
}

func TestNewDirectCandidate_BothEmpty(t *testing.T) {
	// GIVEN
	directCandidate := ""
	localNode := ""

	// WHEN
	dc := types.NewDirectCandidate(directCandidate, localNode)

	// THEN
	require.Equal(t, "", dc.PreferredNodeName)
}
