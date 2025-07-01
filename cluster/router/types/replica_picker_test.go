package types_test

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/router/types"
)

func deterministicZero(_ int) int { return 0 }

func testReplicaSet() types.ReplicaSet {
	return types.ReplicaSet{Replicas: []types.Replica{
		{NodeName: "A", ShardName: "S1", HostAddr: "10.12.135.19"},
		{NodeName: "B", ShardName: "S1", HostAddr: "10.12.135.20"},
		{NodeName: "C", ShardName: "S2", HostAddr: "10.12.135.21"},
		{NodeName: "D", ShardName: "S2", HostAddr: "10.12.135.22"},
	}}
}

func TestRandomReplicaPicker_AlwaysFirst(t *testing.T) {
	// GIVEN
	picker := types.NewRandomReplicaPicker(deterministicZero)
	rs := testReplicaSet()

	// WHEN
	actual := picker.Pick(rs)

	// THEN
	expected := types.ReplicaSet{Replicas: []types.Replica{
		{NodeName: "A", ShardName: "S1", HostAddr: "10.12.135.19"},
		{NodeName: "C", ShardName: "S2", HostAddr: "10.12.135.21"},
	}}

	require.Equalf(t, expected, actual, fmt.Sprintf("unexpected replicas selected.\nexpected: %+v\n actual: %+v", expected, actual))
}

func TestRandomReplicaPicker_DeterministicSeed(t *testing.T) {
	// GIVEN
	picker1 := types.NewRandomReplicaPicker(deterministicZero)
	picker2 := types.NewRandomReplicaPicker(deterministicZero)

	rs := testReplicaSet()

	// WHEN
	actual1 := picker1.Pick(rs)
	actual2 := picker2.Pick(rs)

	// THEN
	require.Equalf(t, actual1, actual2, fmt.Sprintf("expected deterministic picks with the same RNG seed\nfirst:  %+v\nsecond: %+v", actual1, actual2))
}

func TestDirectCandidateReplicaPicker_PrefersCandidate(t *testing.T) {
	// GIVEN
	fallbackReplicaPicker := types.NewRandomReplicaPicker(deterministicZero)
	directCandidateReplicaPicker := types.NewDirectCandidateReplicaPicker("B", fallbackReplicaPicker)
	rs := testReplicaSet()

	// WHEN
	actual := directCandidateReplicaPicker.Pick(rs)

	// THEN
	expected := types.ReplicaSet{Replicas: []types.Replica{
		// S1 should pick node B (direct candidate)
		{NodeName: "B", ShardName: "S1", HostAddr: "10.12.135.20"},
		// S2 has no candidate → fallbackReplicaPicker picks first (node C) as we use `deterministicZero` above
		{NodeName: "C", ShardName: "S2", HostAddr: "10.12.135.21"},
	}}

	sort.Slice(actual.Replicas, func(i, j int) bool { return actual.Replicas[i].ShardName < actual.Replicas[j].ShardName })
	sort.Slice(expected.Replicas, func(i, j int) bool { return expected.Replicas[i].ShardName < expected.Replicas[j].ShardName })
	require.Equalf(t, expected, actual, fmt.Sprintf("unexpected replicas selected.\nexpected: %+v\n actual: %+v", expected, actual))
}

func TestDirectCandidateReplicaPicker_FallbackWhenAbsent(t *testing.T) {
	// GIVEN
	fallbackReplicaPicker := types.NewRandomReplicaPicker(deterministicZero)
	directCandidateReplicaPicker := types.NewDirectCandidateReplicaPicker("unknown", fallbackReplicaPicker) // candidate Z not in set
	rs := testReplicaSet()

	// WHEN
	actual := directCandidateReplicaPicker.Pick(rs)
	expected := fallbackReplicaPicker.Pick(rs)

	// THEN
	require.Equalf(t, expected.Replicas, actual.Replicas, fmt.Sprintf("expected fallback picker to be used when candidate missing\nexpected: %+v\n actual: %+v", expected, actual))
}

func TestRandomReplicaPicker_SingleReplica(t *testing.T) {
	// GIVEN
	rs := types.ReplicaSet{Replicas: []types.Replica{
		{NodeName: "Solo", ShardName: "S1", HostAddr: "1.1.1.1"},
	}}

	// WHEN
	actual := types.NewRandomReplicaPicker(nil).Pick(rs)

	// THEN
	require.Equalf(t, rs, actual, fmt.Sprintf("single‑replica set should be returned unchanged expected: %+v actual: %+v", rs, actual))
}

func TestRandomReplicaPicker_EmptyReplicaSet(t *testing.T) {
	// GIVEN
	rs := types.ReplicaSet{Replicas: []types.Replica{}}

	// WHEN
	actual := types.NewRandomReplicaPicker(deterministicZero).Pick(rs)

	// THEN
	require.Equalf(t, rs, actual, fmt.Sprintf("expected original replica when candidate missing and only one replica exists expected: %+v actual: %+v", rs, actual))
}

func TestDirectCandidateReplicaPicker_SingleReplicaCandidatePresent(t *testing.T) {
	// GIVEN
	rs := types.ReplicaSet{Replicas: []types.Replica{
		{NodeName: "Solo", ShardName: "S1", HostAddr: "1.1.1.1"},
	}}

	// WHEN
	actual := types.NewDirectCandidateReplicaPicker("Solo", nil).Pick(rs)

	// THEN
	require.Equalf(t, rs, actual, fmt.Sprintf("expected identical set when candidate already hosts the shar expected: %+v actual: %+v", rs, actual))
}

func TestDirectCandidateReplicaPicker_SingleReplicaCandidateAbsent(t *testing.T) {
	// GIVEN
	rs := types.ReplicaSet{Replicas: []types.Replica{
		{NodeName: "Solo", ShardName: "S1", HostAddr: "1.1.1.1"},
	}}

	// WHEN
	actual := types.NewDirectCandidateReplicaPicker("Other", nil).Pick(rs)

	// THEN
	require.Equalf(t, rs, actual, fmt.Sprintf("expected original replica when candidate missing and only one replica exists expected: %+v actual: %+v", rs, actual))
}

func TestDirectCandidateReplicaPicker_EmptyReplicaSet(t *testing.T) {
	// GIVEN
	rs := types.ReplicaSet{Replicas: []types.Replica{}}

	// WHEN
	picker := types.NewDirectCandidateReplicaPicker("Other", nil)
	actual := picker.Pick(rs)

	// THEN
	require.Equalf(t, rs, actual, fmt.Sprintf("expected original replica when candidate missing and only one replica exists expected: %+v actual: %+v", rs, actual))
}

func TestRandomReplicaPicker_MultiReplicasOneShard(t *testing.T) {
	n := rand.Intn(5) + 2
	replicas := make([]types.Replica, n)
	for i := 0; i < n; i++ {
		replicas[i] = types.Replica{
			NodeName:  fmt.Sprintf("N%d", i),
			ShardName: "S1",
			HostAddr:  fmt.Sprintf("10.12.43.%d", i),
		}
	}
	rs := types.ReplicaSet{Replicas: replicas}

	picker := types.NewRandomReplicaPicker(nil) // default RNG rand.Intn
	actual := picker.Pick(rs)

	require.Lenf(t, len(actual.Replicas), 1, "unexpected number of replicas: %d", len(actual.Replicas))
	require.Equalf(t, "S1", actual.Replicas[0].ShardName, "unexpected shard name: %s", actual.Replicas[0].ShardName)
	valid := false
	for _, r := range replicas {
		if r.NodeName == actual.Replicas[0].NodeName {
			valid = true
			break
		}
	}
	require.Truef(t, valid, "unexpected replica with invalid node name: %s", actual.Replicas[0].NodeName)
}
