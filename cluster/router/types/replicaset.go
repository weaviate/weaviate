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

import "strings"

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

// Shards returns a list of shard names for all Replicas in the ReplicaSet.
func (s ReadReplicaSet) Shards() []string {
	shards := make([]string, 0, len(s.Replicas))
	for _, replica := range s.Replicas {
		shards = append(shards, replica.ShardName)
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

// Shards returns a list of shard names for all Replicas in the ReplicaSet.
func (s WriteReplicaSet) Shards() []string {
	shards := make([]string, 0, len(s.Replicas))
	for _, replica := range s.Replicas {
		shards = append(shards, replica.ShardName)
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

// AdditionalShards returns a list of shard names for all Replicas in the AdditionalReplicaSet.
func (s WriteReplicaSet) AdditionalShards() []string {
	shards := make([]string, 0, len(s.AdditionalReplicas))
	for _, replica := range s.AdditionalReplicas {
		shards = append(shards, replica.ShardName)
	}
	return shards
}

func (s WriteReplicaSet) IsEmpty() bool {
	return len(s.Replicas) == 0
}
