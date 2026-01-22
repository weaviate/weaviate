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
	for className, classState := range states {
		hasRemovedNode := false
		for _, shard := range classState.Shards.Physical {
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

		for shardName, shard := range classState.Shards.Physical {
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
			// This may temporarily increase the replica count above RF, but it does NOT
			// trigger any data replication as we only adjust the sharding metadata.
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

				targetCount := desiredRF
				if currentCount == desiredRF {
					targetCount = desiredRF + 1
				}

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
			}

			if _, err := s.DeleteReplicaFromShard(ctx, className, shardName, removedNode); err != nil {
				return fmt.Errorf("delete replica %q from shard %q in collection %q: %w", removedNode, shardName, className, err)
			}
		}
	}

	return nil
}

func (s *Raft) Stats() map[string]any {
	s.log.Debug("membership.stats")
	return s.store.Stats()
}
