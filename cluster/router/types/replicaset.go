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
)

// ReadReplicaSet contains *exactly one* replica per shard and is produced by
// ReadReplicaStrategy implementations for read paths.
type ReadReplicaSet struct {
	Replicas []Replica
	// Unreachable replicas are not contacted but count towards the consistency level.
	Unreachable []Replica
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
	Replicas    []Replica
	Unreachable []Replica
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

// validateReplicaSetConsistency validates that the consistency level can be satisfied
// by grouping replicas by shard and validating each shard independently.
func validateReplicaSetConsistency(replicas, unreachable []Replica, level ConsistencyLevel) (int, error) {
	if len(replicas) == 0 && len(unreachable) == 0 {
		return 0, nil
	}

	type shardCount struct {
		reachable   int
		unreachable []string
	}
	counts := make(map[string]*shardCount)
	var order []string
	get := func(shard string) *shardCount {
		c, ok := counts[shard]
		if !ok {
			c = &shardCount{}
			counts[shard] = c
			order = append(order, shard)
		}
		return c
	}
	for _, replica := range replicas {
		get(replica.ShardName).reachable++
	}
	for _, replica := range unreachable {
		c := get(replica.ShardName)
		c.unreachable = append(c.unreachable, replica.NodeName)
	}

	var expectedConsistencyLevel int
	var firstShard string

	for _, shardName := range order {
		c := counts[shardName]
		total := c.reachable + len(c.unreachable)
		resolved := level.ToInt(total)
		if resolved > c.reachable {
			return 0, fmt.Errorf(
				"shard %s: impossible to satisfy consistency level %s: requires %d of %d replicas, but %d unreachable %v",
				shardName, level, resolved, total, len(c.unreachable), c.unreachable)
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
	return validateReplicaSetConsistency(s.Replicas, s.Unreachable, level)
}

func (s WriteReplicaSet) ValidateConsistencyLevel(level ConsistencyLevel) (int, error) {
	return validateReplicaSetConsistency(s.Replicas, s.Unreachable, level)
}
