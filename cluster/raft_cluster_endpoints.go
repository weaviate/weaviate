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

package cluster

import (
	"context"
	"fmt"
	"slices"

	"github.com/sirupsen/logrus"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

// LeaderWithID is used to return the current leader address and ID of the cluster.
// It may return empty strings if there is no current leader or the leader is unknown.
func (s *Raft) LeaderWithID() (string, string) {
	addr, id := s.store.LeaderWithID()
	return string(addr), string(id)
}

// StorageCandidates return the nodes in the raft configuration or memberlist storage nodes
// based on the current configuration of the cluster if it does have  MetadataVoterOnly nodes.
func (s *Raft) StorageCandidates() []string {
	if s.store.raft == nil {
		// get candidates from memberlist
		return s.nodeSelector.StorageCandidates()
	}

	var (
		existedRaftCandidates []string
		raftStorageCandidates []string
		memStorageCandidates  = s.nodeSelector.StorageCandidates()
		nonStorageCandidates  = s.nodeSelector.NonStorageNodes()
	)

	for _, server := range s.store.raft.GetConfiguration().Configuration().Servers {
		existedRaftCandidates = append(existedRaftCandidates, string(server.ID))
	}

	// filter non storage candidates
	for _, c := range existedRaftCandidates {
		if slices.Contains(nonStorageCandidates, c) {
			continue
		}
		raftStorageCandidates = append(raftStorageCandidates, c)
	}

	if len(memStorageCandidates) > len(raftStorageCandidates) {
		// if memberlist has more nodes then use it instead
		// this case could happen if we have MetaVoterOnly Nodes
		// in the RAFT config
		return memStorageCandidates
	}

	return s.nodeSelector.SortCandidates(raftStorageCandidates)
}

func (s *Raft) Join(ctx context.Context, id, addr string, voter bool) error {
	s.log.WithFields(logrus.Fields{
		"id":      id,
		"address": addr,
		"voter":   voter,
	}).Debug("membership.join")
	if s.store.IsLeader() {
		return s.store.Join(id, addr, voter)
	}
	leader := s.store.Leader()
	if leader == "" {
		return s.leaderErr()
	}
	req := &cmd.JoinPeerRequest{Id: id, Address: addr, Voter: voter}
	_, err := s.cl.Join(ctx, leader, req)
	return err
}

func (s *Raft) Remove(ctx context.Context, id string) error {
	s.log.WithField("id", id).Debug("membership.remove")

	var err error
	if s.store.IsLeader() {
		err = s.store.Remove(id)
	} else {
		leader := s.store.Leader()
		if leader == "" {
			return s.leaderErr()
		}
		req := &cmd.RemovePeerRequest{Id: id}
		_, err = s.cl.Remove(ctx, leader, req)
	}
	if err != nil {
		return err
	}

	// The Raft peer has been removed from the cluster configuration.
	// Now ensure that no shard replicas still reference this node and
	// that we keep satisfying the configured replication factor where possible.
	if err := s.rebalanceReplicasAfterNodeRemoval(ctx, id); err != nil {
		return fmt.Errorf("rebalance replicas after removing node %q: %w", id, err)
	}

	return nil
}

// rebalanceReplicasAfterNodeRemoval updates shard replica assignments after a node
// has been removed from the Raft cluster.
//
// For every collection where the removed node appears in the sharding state:
//   - We remove the node from all shards' BelongsToNodes
//   - We add replacement replicas on other available storage nodes to maintain
//     the replication factor where possible.
func (s *Raft) rebalanceReplicasAfterNodeRemoval(ctx context.Context, removedNode string) error {
	schemaReader := s.SchemaReader()
	states := schemaReader.States()

	// Collect class names first to avoid concurrent map iteration
	classNames := make([]string, 0, len(states))
	for className := range states {
		classNames = append(classNames, className)
	}

	for _, className := range classNames {
		shardingStateCopy := schemaReader.CopyShardingState(className)
		if shardingStateCopy == nil {
			continue
		}

		// Collect shard names from the snapshot to avoid concurrent map iteration
		shardNames := make([]string, 0, len(shardingStateCopy.Physical))
		for shardName := range shardingStateCopy.Physical {
			shardNames = append(shardNames, shardName)
		}

		// Check if any shard has the removed node
		hasRemovedNode := false
		for _, shardName := range shardNames {
			shard, exists := shardingStateCopy.Physical[shardName]
			if !exists {
				continue
			}
			for _, node := range shard.BelongsToNodes {
				if node == removedNode {
					hasRemovedNode = true
					break
				}
			}
			if hasRemovedNode {
				break
			}
		}

		if !hasRemovedNode {
			continue
		}

		classInfo := schemaReader.ClassInfo(className)
		if !classInfo.Exists || classInfo.ReplicationFactor <= 0 {
			continue
		}

		// Process each shard that contains the removed node
		for _, shardName := range shardNames {
			shard, exists := shardingStateCopy.Physical[shardName]
			if !exists {
				continue
			}
			hasNode := false
			for _, node := range shard.BelongsToNodes {
				if node == removedNode {
					hasNode = true
					break
				}
			}

			if !hasNode {
				continue
			}

			currentReplicas, err := schemaReader.ShardReplicas(className, shardName)
			if err != nil {
				return fmt.Errorf("get replicas for shard %q in collection %q: %w", shardName, className, err)
			}

			desiredRF := int(classInfo.ReplicationFactor)
			currentCount := len(currentReplicas)

			// If we're at or below the desired RF, we need to add replacement replicas first
			// before we can delete the removed node (DeleteReplicaFromShard enforces RF minimum).
			// This may temporarily increase the replica count above RF. Adding a replica updates
			// the sharding/cluster metadata here and triggers data replication to the new replica
			// asynchronously in the background; this operation does not wait for replication to complete.
			if currentCount <= desiredRF {
				availableNodes := s.StorageCandidates()
				availableSet := make(map[string]bool)
				for _, node := range availableNodes {
					if node != removedNode {
						availableSet[node] = true
					}
				}
				for _, replica := range currentReplicas {
					delete(availableSet, replica)
				}

				// Always ensure we have at least desiredRF + 1 replicas before deletion
				// because DeleteReplicaFromShard requires numberOfReplicas > ReplicationFactor
				targetCount := desiredRF + 1

				for currentCount < targetCount && len(availableSet) > 0 {
					var newNode string
					for node := range availableSet {
						newNode = node
						break
					}

					if _, err := s.AddReplicaToShard(ctx, className, shardName, newNode); err != nil {
						return fmt.Errorf("add replica %q to shard %q in collection %q: %w", newNode, shardName, className, err)
					}

					delete(availableSet, newNode)
					currentCount++
				}

				// Re-read the actual replica count after adding replicas to ensure we have
				// the current state before attempting deletion
				if currentCount <= desiredRF {
					// We couldn't add enough replicas, re-read to get the actual count
					updatedReplicas, err := schemaReader.ShardReplicas(className, shardName)
					if err != nil {
						return fmt.Errorf("get replicas for shard %q in collection %q after adding: %w", shardName, className, err)
					}
					currentCount = len(updatedReplicas)
				}
			}

			// Re-read replicas to get the current state and verify the removed node is still present
			finalReplicas, err := schemaReader.ShardReplicas(className, shardName)
			if err != nil {
				return fmt.Errorf("get replicas for shard %q in collection %q before deletion: %w", shardName, className, err)
			}

			// Check if the removed node is still in the replica list
			removedNodeStillPresent := false
			for _, replica := range finalReplicas {
				if replica == removedNode {
					removedNodeStillPresent = true
					break
				}
			}

			if !removedNodeStillPresent {
				// Removed node is no longer in the replica list, skip deletion
				continue
			}

			finalCount := len(finalReplicas)

			// After attempting to add replacement replicas, ensure that removing the
			// replica on the removed node will not violate the desired replication
			// factor. If it would, skip the deletion for now and log a warning.
			if finalCount < desiredRF {
				s.log.WithFields(logrus.Fields{
					"collection":         className,
					"shard":              shardName,
					"removed_node":       removedNode,
					"replication_factor": desiredRF,
					"current_replicas":   finalCount,
				}).Warn("skipping replica removal after node departure: insufficient nodes to maintain replication factor")
				continue
			}

			if _, err := s.DeleteReplicaFromShard(ctx, className, shardName, removedNode); err != nil {
				return fmt.Errorf("delete replica %q from shard %q in collection %q: %w", removedNode, shardName, className, err)
			}

			s.log.WithFields(logrus.Fields{
				"collection":         className,
				"shard":              shardName,
				"removed_node":       removedNode,
				"replication_factor": desiredRF,
				"current_replicas":   finalCount,
			}).Info("deleted replica from shard")
		}
	}

	return nil
}

func (s *Raft) Stats() map[string]any {
	s.log.Debug("membership.stats")
	return s.store.Stats()
}
