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
		"S2": "",  // S2 has no candidate, could be either C or D
	}

	assertReadStrategyResult(t, rs, actual, expectedShardToNode)
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
		"S1": "",  // S1 has no local node D, could be A or B
		"S2": "D", // S2 should pick node D (local node)
	}

	assertReadStrategyResult(t, rs, actual, expectedShardToNode)
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
		"S1": "", // Could be A or B
		"S2": "", // Could be C or D
	}

	assertReadStrategyResult(t, rs, actual, expectedShardToNode)
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

	assertReadStrategyResult(t, rs, actual, expectedShardToNode)
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
	assertReadStrategyResult(t, rs, actual, map[string]string{})
}

func TestDirectCandidateReadStrategy_SingleReplicaPerShard(t *testing.T) {
	// GIVEN - one replica per shard
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

	assertReadStrategyResult(t, rs, actual, expectedShardToNode)
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

func assertReadStrategyResult(t *testing.T, original types.ReadReplicaSet, actual types.ReadReplicaSet, expectedShardToNode map[string]string) {
	originalShards := make(map[string][]types.Replica)
	for _, replica := range original.Replicas {
		originalShards[replica.ShardName] = append(originalShards[replica.ShardName], replica)
	}

	actualShards := make(map[string][]types.Replica)
	for _, replica := range actual.Replicas {
		actualShards[replica.ShardName] = append(actualShards[replica.ShardName], replica)
	}

	require.Len(t, actualShards, len(originalShards), "should have all shards represented")

	for shard, expectedNode := range expectedShardToNode {
		actualReplicasForShard, exists := actualShards[shard]
		require.True(t, exists, fmt.Sprintf("should have replicas for shard %s", shard))

		if expectedNode != "" {
			require.Equal(t, expectedNode, actualReplicasForShard[0].NodeName,
				fmt.Sprintf("for shard %s, expected preferred node %s to be first but got %s", shard, expectedNode, actualReplicasForShard[0].NodeName))
		}

		originalReplicasForShard := originalShards[shard]
		require.Equal(t, len(originalReplicasForShard), len(actualReplicasForShard),
			fmt.Sprintf("should return all replicas for shard %s", shard))
	}
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
		"S1": "A",
		"S2": "",
	}

	assertReadStrategyResult(t, rs, actual, expectedShardToNode)
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
		"S1": "",
		"S2": "C",
	}

	assertReadStrategyResult(t, rs, actual, expectedShardToNode)
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
		"S1": "B",
		"S2": "",
	}

	assertReadStrategyResult(t, rs, actual, expectedShardToNode)
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
		"S1": "", // Could be A or B
		"S2": "", // Could be C or D
	}

	assertReadStrategyResult(t, rs, actual, expectedShardToNode)
}
