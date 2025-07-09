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
// preferring local operations when possible.
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
	// according to the strategy's logic and routing options. The returned ReadReplicaSet contains the
	// same number of unique shards but with only the selected replica for each shard.
	Apply(replicas ReadReplicaSet, options RoutingPlanBuildOptions) ReadReplicaSet
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

// Apply selects exactly one replica per shard, preferring the node specified in
// options.DirectCandidateNode if provided, falling back to the strategy's configured
// preferred node when available. Uses the same organizing logic as the write strategy,
// then selects the first replica from each shard group.
func (s *DirectCandidateReadStrategy) Apply(rs ReadReplicaSet, options RoutingPlanBuildOptions) ReadReplicaSet {
	preferredNode := s.determinePreferredNode(options)
	organizedReplicas := byPreferredNode(rs.Replicas, preferredNode)
	shardGroups := groupByShard(organizedReplicas)
	var selectedReplicas []Replica

	for _, indices := range shardGroups {
		selectedReplicas = append(selectedReplicas, organizedReplicas[indices[0]])
	}

	return ReadReplicaSet{
		Replicas: selectedReplicas,
	}
}

// Helper method to determine the preferred node from options and strategy configuration
func (s *DirectCandidateReadStrategy) determinePreferredNode(options RoutingPlanBuildOptions) string {
	// Priority: options.DirectCandidateNode > strategy.directCandidate.PreferredNodeName
	if options.DirectCandidateNode != "" {
		return options.DirectCandidateNode
	}
	return s.directCandidate.PreferredNodeName
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

// byPreferredNode applies the preferred node logic to a slice of replicas.
// It groups replicas by shard and places the preferred node first within each shard group,
// maintaining the original order for other replicas.
func byPreferredNode(replicas []Replica, preferredNodeName string) []Replica {
	if len(replicas) == 0 {
		return []Replica{}
	}

	if preferredNodeName == "" {
		return replicas
	}

	buckets := groupByShard(replicas)
	out := make([]Replica, 0, len(replicas))

	processedShards := make(map[string]bool)
	for _, replica := range replicas {
		if processedShards[replica.ShardName] {
			continue
		}
		processedShards[replica.ShardName] = true

		indices := buckets[replica.ShardName]

		var preferredReplicas []Replica
		var otherReplicas []Replica

		// Separate preferred node replicas from others within this shard to keep the preferred node first
		for _, i := range indices {
			replica := replicas[i]
			if replica.NodeName == preferredNodeName {
				preferredReplicas = append(preferredReplicas, replica)
			} else {
				otherReplicas = append(otherReplicas, replica)
			}
		}

		// Add preferred replicas first, then others
		out = append(out, preferredReplicas...)
		out = append(out, otherReplicas...)
	}

	return out
}
