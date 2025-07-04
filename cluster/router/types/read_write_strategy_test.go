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

func testWriteReplicaSet() types.WriteReplicaSet {
	return types.WriteReplicaSet{
		Replicas: []types.Replica{
			{NodeName: "A", ShardName: "S1", HostAddr: "10.12.135.19"},
			{NodeName: "B", ShardName: "S1", HostAddr: "10.12.135.20"},
			{NodeName: "C", ShardName: "S2", HostAddr: "10.12.135.21"},
			{NodeName: "D", ShardName: "S2", HostAddr: "10.12.135.22"},
		},
		AdditionalReplicas: []types.Replica{
			{NodeName: "E", ShardName: "S1", HostAddr: "10.12.135.23"},
		},
	}
}

func assertReadStrategyResult(t *testing.T, original types.ReadReplicaSet, actual types.ReadReplicaSet, expectedShardToNode map[string]string) {
	originalShards := make(map[string][]types.Replica)
	for _, replica := range original.Replicas {
		originalShards[replica.ShardName] = append(originalShards[replica.ShardName], replica)
	}

	actualShards := make(map[string]types.Replica)
	for _, replica := range actual.Replicas {
		actualShards[replica.ShardName] = replica
	}

	require.Len(t, actualShards, len(originalShards), "should have one replica per shard")

	for shard, expectedNode := range expectedShardToNode {
		actualReplica, exists := actualShards[shard]
		require.True(t, exists, fmt.Sprintf("should have selected a replica for shard %s", shard))

		if expectedNode != "" {
			require.Equal(t, expectedNode, actualReplica.NodeName,
				fmt.Sprintf("for shard %s, expected node %s but got %s", shard, expectedNode, actualReplica.NodeName))
		}

		originalReplicasForShard := originalShards[shard]
		validSelection := false
		for _, originalReplica := range originalReplicasForShard {
			if originalReplica.NodeName == actualReplica.NodeName &&
				originalReplica.ShardName == actualReplica.ShardName &&
				originalReplica.HostAddr == actualReplica.HostAddr {
				validSelection = true
				break
			}
		}
		require.True(t, validSelection,
			fmt.Sprintf("selected replica for shard %s was not in the original set", shard))
	}

	for shard := range actualShards {
		_, expected := originalShards[shard]
		require.True(t, expected, fmt.Sprintf("unexpected shard %s in result", shard))
	}
}

func assertWriteStrategyResult(t *testing.T, expected types.WriteReplicaSet, actual types.WriteReplicaSet, expectedFirstNodeName string) {
	require.ElementsMatch(t, expected.Replicas, actual.Replicas,
		"all replicas should be present regardless of order")

	require.ElementsMatch(t, expected.AdditionalReplicas, actual.AdditionalReplicas,
		"AdditionalReplicas should remain unchanged")

	if expectedFirstNodeName != "" && len(actual.Replicas) > 0 {
		require.Equal(t, expectedFirstNodeName, actual.Replicas[0].NodeName,
			fmt.Sprintf("expected %s to be first replica", expectedFirstNodeName))
	}
}

func TestDirectCandidateReadStrategy_PrefersCandidate(t *testing.T) {
	// GIVEN
	dc := types.NewDirectCandidate("B", "")
	strategy := types.NewDirectCandidateReadStrategy(dc)
	rs := testReadReplicaSet()

	// WHEN
	actual := strategy.Apply(rs)

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

	// WHEN
	actual := strategy.Apply(rs)

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

	// WHEN
	actual := strategy.Apply(rs)

	// THEN - should pick one replica for each shard, but we don't know which
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

	// WHEN
	actual := strategy.Apply(rs)

	// THEN
	expectedShardToNode := map[string]string{
		"S1": "Solo", // Only one replica available
	}

	assertReadStrategyResult(t, rs, actual, expectedShardToNode)
}

func TestDirectCandidateReadStrategy_EmptyReplicaSet(t *testing.T) {
	// GIVEN
	rs := types.ReadReplicaSet{Replicas: []types.Replica{}}
	dc := types.NewDirectCandidate("Other", "")
	strategy := types.NewDirectCandidateReadStrategy(dc)

	// WHEN
	actual := strategy.Apply(rs)

	// THEN
	expectedShardToNode := map[string]string{} // No shards expected
	assertReadStrategyResult(t, rs, actual, expectedShardToNode)
}

func TestDirectCandidateReadStrategy_SingleReplicaPerShard(t *testing.T) {
	// GIVEN - one replica per shard
	rs := types.ReadReplicaSet{Replicas: []types.Replica{
		{NodeName: "A", ShardName: "S1", HostAddr: "10.12.135.19"},
		{NodeName: "C", ShardName: "S2", HostAddr: "10.12.135.21"},
	}}
	dc := types.NewDirectCandidate("B", "")
	strategy := types.NewDirectCandidateReadStrategy(dc)

	// WHEN
	actual := strategy.Apply(rs)

	// THEN
	expectedShardToNode := map[string]string{
		"S1": "A", // Only A available for S1
		"S2": "C", // Only C available for S2
	}

	assertReadStrategyResult(t, rs, actual, expectedShardToNode)
}

func TestDirectCandidateWriteStrategy_PrefersCandidate(t *testing.T) {
	// GIVEN
	dc := types.NewDirectCandidate("B", "")
	strategy := types.NewDirectCandidateWriteStrategy(dc)
	ws := testWriteReplicaSet()

	// WHEN
	actual := strategy.Apply(ws)

	// THEN
	assertWriteStrategyResult(t, ws, actual, "B")
}

func TestDirectCandidateWriteStrategy_FallsBackToLocalNode(t *testing.T) {
	// GIVEN
	dc := types.NewDirectCandidate("", "C")
	strategy := types.NewDirectCandidateWriteStrategy(dc)
	ws := testWriteReplicaSet()

	// WHEN
	actual := strategy.Apply(ws)

	// THEN
	assertWriteStrategyResult(t, ws, actual, "C")
}

func TestDirectCandidateWriteStrategy_NoPreferredNode(t *testing.T) {
	// GIVEN
	dc := types.NewDirectCandidate("", "")
	strategy := types.NewDirectCandidateWriteStrategy(dc)
	ws := testWriteReplicaSet()

	// WHEN
	actual := strategy.Apply(ws)

	// THEN
	assertWriteStrategyResult(t, ws, actual, "")
	require.Equal(t, ws.Replicas, actual.Replicas, "should maintain exact original order")
}

func TestDirectCandidateWriteStrategy_PreferredNodeNotFound(t *testing.T) {
	// GIVEN
	dc := types.NewDirectCandidate("unknown", "")
	strategy := types.NewDirectCandidateWriteStrategy(dc)
	ws := testWriteReplicaSet()

	// WHEN
	actual := strategy.Apply(ws)

	// THEN
	assertWriteStrategyResult(t, ws, actual, "")
	require.Equal(t, ws.Replicas, actual.Replicas, "should maintain exact original order")
}

func TestDirectCandidateWriteStrategy_EmptyReplicaSet(t *testing.T) {
	// GIVEN
	ws := types.WriteReplicaSet{Replicas: []types.Replica{}}
	dc := types.NewDirectCandidate("A", "")
	strategy := types.NewDirectCandidateWriteStrategy(dc)

	// WHEN
	actual := strategy.Apply(ws)

	// THEN
	assertWriteStrategyResult(t, ws, actual, "")
	require.Equal(t, ws.Replicas, actual.Replicas, "empty set should remain empty")
}

func TestDirectCandidateWriteStrategy_MultiplePreferredNodes(t *testing.T) {
	// GIVEN - multiple replicas with the same preferred node
	ws := types.WriteReplicaSet{
		Replicas: []types.Replica{
			{NodeName: "A", ShardName: "S1", HostAddr: "10.12.135.19"},
			{NodeName: "B", ShardName: "S1", HostAddr: "10.12.135.20"},
			{NodeName: "B", ShardName: "S2", HostAddr: "10.12.135.21"}, // B appears twice
			{NodeName: "C", ShardName: "S3", HostAddr: "10.12.135.22"},
		},
	}
	dc := types.NewDirectCandidate("B", "")
	strategy := types.NewDirectCandidateWriteStrategy(dc)

	// WHEN
	actual := strategy.Apply(ws)

	// THEN
	assertWriteStrategyResult(t, ws, actual, "B")

	// Additional check: verify that the second replica is also B (since we have 2 B replicas)
	require.True(t, len(actual.Replicas) >= 2, "should have at least 2 replicas")
	require.Equal(t, "B", actual.Replicas[1].NodeName, "second replica should also be B")

	// Check that the two B replicas are from different shards
	bShards := []string{actual.Replicas[0].ShardName, actual.Replicas[1].ShardName}
	require.ElementsMatch(t, []string{"S1", "S2"}, bShards, "B replicas should be from S1 and S2")
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
