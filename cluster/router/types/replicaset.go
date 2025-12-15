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

package types

import (
	"fmt"
	"strings"
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
	Replicas           []Replica
	AdditionalReplicas []Replica
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

func (s WriteReplicaSet) EmptyAdditionalReplicas() bool {
	return len(s.AdditionalReplicas) == 0
}

// AdditionalNodeNames returns a list of node names contained in the AdditionalReplicaSet.
func (s WriteReplicaSet) AdditionalNodeNames() []string {
	nodeNames := make([]string, 0, len(s.AdditionalReplicas))
	for _, replica := range s.AdditionalReplicas {
		nodeNames = append(nodeNames, replica.NodeName)
	}
	return nodeNames
}

// AdditionalHostAddresses returns a list of host addresses for all Replicas in the AdditionalReplicaSet.
func (s WriteReplicaSet) AdditionalHostAddresses() []string {
	hostAddresses := make([]string, 0, len(s.AdditionalReplicas))
	for _, replica := range s.AdditionalReplicas {
		hostAddresses = append(hostAddresses, replica.HostAddr)
	}
	return hostAddresses
}

// AdditionalShards returns a list of unique shard names for all Replicas in the AdditionalReplicaSet.
func (s WriteReplicaSet) AdditionalShards() []string {
	if len(s.AdditionalReplicas) == 0 {
		return []string{}
	}

	seen := make(map[string]bool, len(s.AdditionalReplicas))
	shards := make([]string, 0, len(s.AdditionalReplicas))

	for _, replica := range s.AdditionalReplicas {
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

// validateReplicaSetConsistency validates that the consistency level can be satisfied
// by grouping replicas by shard and validating each shard independently.
func validateReplicaSetConsistency(replicas []Replica, level ConsistencyLevel) (int, error) {
	if len(replicas) == 0 {
		return 0, nil
	}

	// Group replicas by shard
	replicasByShard := make(map[string][]Replica)
	for _, replica := range replicas {
		replicasByShard[replica.ShardName] = append(replicasByShard[replica.ShardName], replica)
	}

	var expectedConsistencyLevel int
	var firstShard string

	for shardName, shardReplicas := range replicasByShard {
		resolved := level.ToInt(len(shardReplicas))
		if resolved > len(shardReplicas) {
			return 0, fmt.Errorf(
				"shard %s: impossible to satisfy consistency level (%d) > available replicas (%d)",
				shardName, resolved, len(shardReplicas))
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

func (s ReadReplicaSet) ValidateConsistencyLevel(level ConsistencyLevel) (int, error) {
	return validateReplicaSetConsistency(s.Replicas, level)
}

func (s WriteReplicaSet) ValidateConsistencyLevel(level ConsistencyLevel) (int, error) {
	return validateReplicaSetConsistency(s.Replicas, level)
}
