//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package types

import (
	"fmt"
	"strings"

	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
)

// ReadReplicaSet contains *exactly one* replica per shard and is produced by
// ReadReplicaStrategy implementations for read paths.
type ReadReplicaSet struct {
	Replicas []Replica
}

// String returns a human-readable representation of a ReplicaSet,
// showing all Replicas in the set.
func (s ReadReplicaSet) String() string {
	var b strings.Builder
	b.WriteString("[")
	for i, r := range s.Replicas {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(r.String())
	}
	b.WriteString("]")
	return b.String()
}

// NodeNames returns a list of node names contained in the ReplicaSet.
func (s ReadReplicaSet) NodeNames() []string {
	nodeNames := make([]string, 0, len(s.Replicas))
	for _, replica := range s.Replicas {
		nodeNames = append(nodeNames, replica.NodeName)
	}
	return nodeNames
}

// HostAddresses returns a list of host addresses for all Replicas in the ReplicaSet.
func (s ReadReplicaSet) HostAddresses() []string {
	hostAddresses := make([]string, 0, len(s.Replicas))
	for _, replica := range s.Replicas {
		hostAddresses = append(hostAddresses, replica.HostAddr)
	}
	return hostAddresses
}

// Shards returns a list of unique shard names for all Replicas in the ReplicaSet.
func (s ReadReplicaSet) Shards() []string {
	if len(s.Replicas) == 0 {
		return []string{}
	}

	seen := make(map[string]bool, len(s.Replicas))
	shards := make([]string, 0, len(s.Replicas))

	for _, replica := range s.Replicas {
		if !seen[replica.ShardName] {
			seen[replica.ShardName] = true
			shards = append(shards, replica.ShardName)
		}
	}

	return shards
}

func (s ReadReplicaSet) EmptyReplicas() bool {
	return len(s.Replicas) == 0
}

type WriteReplicaSet struct {
	Replicas []Replica
}

// NodeNames returns a list of node names contained in the ReplicaSet.
func (s WriteReplicaSet) NodeNames() []string {
	nodeNames := make([]string, 0, len(s.Replicas))
	for _, replica := range s.Replicas {
		nodeNames = append(nodeNames, replica.NodeName)
	}
	return nodeNames
}

// HostAddresses returns a list of host addresses for all Replicas in the ReplicaSet.
func (s WriteReplicaSet) HostAddresses() []string {
	hostAddresses := make([]string, 0, len(s.Replicas))
	for _, replica := range s.Replicas {
		hostAddresses = append(hostAddresses, replica.HostAddr)
	}
	return hostAddresses
}

// Shards returns a list of unique shard names for all Replicas in the ReplicaSet.
func (s WriteReplicaSet) Shards() []string {
	if len(s.Replicas) == 0 {
		return []string{}
	}

	seen := make(map[string]bool, len(s.Replicas))
	shards := make([]string, 0, len(s.Replicas))

	for _, replica := range s.Replicas {
		if !seen[replica.ShardName] {
			seen[replica.ShardName] = true
			shards = append(shards, replica.ShardName)
		}
	}

	return shards
}

func (s WriteReplicaSet) IsEmpty() bool {
	return len(s.Replicas) == 0
}

// validateReplicaSetConsistency answers two questions per shard. replicas holds the
// reachable replicas; replicaCounts maps each shard to its replica count N before
// unreachable replicas were dropped.
//
// Reachability: every shard in replicaCounts with N > 0 must have a minimum number of
// replicas reachable, otherwise the request is rejected with an error matching
// replicaerrors.ErrReplicas. QUORUM and ALL need a majority (N/2+1); ONE and unknown
// levels need at least one. A nil replicaCounts skips this check.
//
// Required answers: QUORUM requires a majority of N, ALL requires every reachable
// replica, ONE and unknown levels require 1. A shard without an entry in
// replicaCounts requires level.ToInt of its reachable replica count. Once the
// reachability check passes, required answers never exceed the reachable replicas.
//
// All shards must resolve to the same number of required answers.
func validateReplicaSetConsistency(replicas []Replica, level ConsistencyLevel, replicaCounts map[string]int) (int, error) {
	if len(replicas) == 0 {
		return 0, nil
	}

	reachableByShard := make(map[string]int)
	for _, replica := range replicas {
		reachableByShard[replica.ShardName]++
	}

	for shardName, n := range replicaCounts {
		// N = 0 means the shard has no replicas, so none are unreachable.
		if n == 0 {
			continue
		}
		minReachable := 1
		if level == ConsistencyLevelQuorum || level == ConsistencyLevelAll {
			minReachable = ConsistencyLevelQuorum.ToInt(n)
		}
		if reachable := reachableByShard[shardName]; reachable < minReachable {
			return 0, replicaerrors.NewNotEnoughReplicasErrorWithCounts(minReachable, reachable,
				fmt.Errorf("shard %q: %d of %d replicas reachable", shardName, reachable, n))
		}
	}

	var expectedConsistencyLevel int
	var firstShard string

	for shardName, reachable := range reachableByShard {
		resolved := level.ToInt(reachable)
		if n, ok := replicaCounts[shardName]; ok && level == ConsistencyLevelQuorum {
			resolved = level.ToInt(n)
		}

		if firstShard == "" {
			expectedConsistencyLevel = resolved
			firstShard = shardName
		} else if resolved != expectedConsistencyLevel {
			return 0, fmt.Errorf(
				"inconsistent consistency levels: shard %s resolved to %d, shard %s resolved to %d",
				firstShard, expectedConsistencyLevel, shardName, resolved)
		}
	}

	return expectedConsistencyLevel, nil
}

func (s ReadReplicaSet) ValidateConsistencyLevel(level ConsistencyLevel, replicaCounts map[string]int) (int, error) {
	return validateReplicaSetConsistency(s.Replicas, level, replicaCounts)
}

func (s WriteReplicaSet) ValidateConsistencyLevel(level ConsistencyLevel, replicaCounts map[string]int) (int, error) {
	return validateReplicaSetConsistency(s.Replicas, level, replicaCounts)
}
