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
// preferred node when available.
func (s *DirectCandidateReadStrategy) Apply(rs ReadReplicaSet, options RoutingPlanBuildOptions) ReadReplicaSet {
	if len(rs.Replicas) == 0 {
		return rs
	}

	preferredNode := s.determinePreferredNode(options)
	shardToSelectedReplica := make(map[string]Replica)

	for _, replica := range rs.Replicas {
		existing, exists := shardToSelectedReplica[replica.ShardName]

		if !exists {
			shardToSelectedReplica[replica.ShardName] = replica
		} else if preferredNode != "" && replica.NodeName == preferredNode && existing.NodeName != preferredNode {
			shardToSelectedReplica[replica.ShardName] = replica
		}
	}

	selectedReplicas := make([]Replica, 0, len(shardToSelectedReplica))
	for _, replica := range shardToSelectedReplica {
		selectedReplicas = append(selectedReplicas, replica)
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
