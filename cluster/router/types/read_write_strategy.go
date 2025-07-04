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

// DirectCandidate organizes replicas by prioritizing the
// direct candidate node first, falling back to the local node if no direct
// candidate is specified. This strategy aims to minimize network latency by
// preferring local operations when possible. This strategy can be used for
// both read and write operations.
type DirectCandidate struct {
	PreferredNodeName string
}

// NewDirectCandidate creates a new strategy that prioritizes
// the direct candidate node for operations. If directCandidate is empty,
// the strategy falls back to prioritizing the local node.
//
// Parameters:
//   - directCandidate: the preferred node name for operations
//   - localNodeName: fallback node name when directCandidate is empty
//
// Returns:
//   - DirectCandidate: a strategy instance ready to organize replicas
func NewDirectCandidate(directCandidate, localNodeName string) DirectCandidate {
	preferredNodeName := directCandidate
	if directCandidate == "" {
		preferredNodeName = localNodeName
	}
	return DirectCandidate{
		PreferredNodeName: preferredNodeName,
	}
}

// ReadReplicaStrategy defines how to select read replicas for optimal routing.
// Different strategies can prioritize local nodes, use deterministic selection, or implement
// other replica selection policies. Read strategies must select exactly one replica
// per shard to avoid reading duplicate data.
type ReadReplicaStrategy interface {
	// Apply selects exactly one replica per shard from the provided ReadReplicaSet
	// according to the strategy's logic. The returned ReadReplicaSet contains the
	// same number of unique shards but with only the selected replica for each shard.
	Apply(replicas ReadReplicaSet) ReadReplicaSet
}

// WriteReplicaStrategy defines how to organize write replicas for optimal routing.
// Different strategies can prioritize local nodes, distribute load, or implement
// other replica ordering policies.
type WriteReplicaStrategy interface {
	// Apply organizes the provided WriteReplicaSet according to the strategy's logic.
	// The returned WriteReplicaSet contains the same replicas but potentially reordered.
	Apply(replicas WriteReplicaSet) WriteReplicaSet
}

// DirectCandidateWriteStrategy organizes write replicas using the DirectCandidate
// logic to prioritize the preferred node. This strategy places replicas from the
// preferred node first in the replica list, maintaining the original order for
// all other replicas.
type DirectCandidateWriteStrategy struct {
	directCandidate DirectCandidate
}

// NewDirectCandidateWriteStrategy creates a new write strategy that uses the
// provided DirectCandidate to organize write replicas with preferred node priority.
//
// Parameters:
//   - directCandidate: the DirectCandidate strategy containing preferred node logic
//
// Returns:
//   - DirectCandidateWriteStrategy: a strategy instance ready to organize write replicas
func NewDirectCandidateWriteStrategy(directCandidate DirectCandidate) WriteReplicaStrategy {
	return &DirectCandidateWriteStrategy{directCandidate: directCandidate}
}

// Apply organizes the write replicas by placing the preferred node first.
// The preferred node is determined by directCandidate, falling back to
// localNodeName if directCandidate is empty. If no preferred node is
// specified, the original ordering is preserved.
func (s *DirectCandidateWriteStrategy) Apply(ws WriteReplicaSet) WriteReplicaSet {
	if len(ws.Replicas) == 0 {
		return ws
	}

	preferredNodeName := s.directCandidate.PreferredNodeName
	if preferredNodeName == "" {
		return ws
	}

	var orderedReplicas []Replica
	var otherReplicas []Replica
	for _, replica := range ws.Replicas {
		if replica.NodeName == preferredNodeName {
			orderedReplicas = append(orderedReplicas, replica)
		} else {
			otherReplicas = append(otherReplicas, replica)
		}
	}

	orderedReplicas = append(orderedReplicas, otherReplicas...)
	return WriteReplicaSet{
		Replicas:           orderedReplicas,
		AdditionalReplicas: ws.AdditionalReplicas,
	}
}

// DirectCandidateReadStrategy selects read replicas using the DirectCandidate
// logic to prefer the preferred node when available. When the preferred node
// is not available for a shard, this strategy falls back to selecting the
// first available replica for that shard.
type DirectCandidateReadStrategy struct {
	directCandidate DirectCandidate
}

// NewDirectCandidateReadStrategy creates a new read strategy that uses the
// provided DirectCandidate to select read replicas with preferred node priority.
//
// Parameters:
//   - directCandidate: the DirectCandidate strategy containing preferred node logic
//
// Returns:
//   - DirectCandidateReadStrategy: a strategy instance ready to select read replicas
func NewDirectCandidateReadStrategy(directCandidate DirectCandidate) ReadReplicaStrategy {
	return &DirectCandidateReadStrategy{directCandidate: directCandidate}
}

// Apply selects exactly one replica per shard, preferring the preferred node when
// available. If the preferred node is not available for a shard, the first available
// replica is selected from the available replicas for that shard. If no preferred
// node is specified, the first replica is selected for all shards.
func (s *DirectCandidateReadStrategy) Apply(rs ReadReplicaSet) ReadReplicaSet {
	replicas := rs.Replicas
	if len(replicas) == 0 {
		return rs
	}

	// If only one replica total, return it
	if len(replicas) <= 1 {
		return rs
	}

	buckets := groupByShard(replicas)
	out := make([]Replica, 0, len(buckets))

	for _, ids := range buckets {
		if len(ids) == 1 {
			out = append(out, replicas[ids[0]])
			continue
		}

		// Try to find preferred node for this shard
		preferredFound := false
		preferredNodeName := s.directCandidate.PreferredNodeName
		if preferredNodeName != "" {
			for _, i := range ids {
				if replicas[i].NodeName == preferredNodeName {
					out = append(out, replicas[i])
					preferredFound = true
					break
				}
			}
		}

		// If preferred node not found, select the first available replica
		if !preferredFound {
			out = append(out, replicas[ids[0]])
		}
	}

	return ReadReplicaSet{Replicas: out}
}

// groupByShard builds a map of shard names to indices in the original slice.
// This helper function organizes replicas by their shard membership for
// efficient per-shard processing.
func groupByShard(replicas []Replica) map[string][]int {
	m := make(map[string][]int, len(replicas))
	for i, replica := range replicas {
		m[replica.ShardName] = append(m[replica.ShardName], i)
	}
	return m
}
