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
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
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
func (s *Raft) rebalanceReplicasAfterNodeRemoval(ctx context.Context, removedNode string) error {
	schemaReader := s.SchemaReader()
	states := schemaReader.States()

	// Collect class names first to avoid concurrent map iteration
	classNames := make([]string, 0, len(states))
	for className := range states {
		classNames = append(classNames, className)
	}

	for _, className := range classNames {
		var shardingStateCopy *sharding.State
		schemaReader.Read(className, false, func(c *models.Class, s *sharding.State) error {
			shardingStateCopy = s
			return nil
		})

		if shardingStateCopy == nil {
			continue
		}

		classInfo := schemaReader.ClassInfo(className)
		if !classInfo.Exists || classInfo.ReplicationFactor <= 0 {
			continue
		}

		desiredRF := int(classInfo.ReplicationFactor)

		// Process each shard that contains the removed node
		for shardName, shard := range shardingStateCopy.Physical {
			if !slices.Contains(shard.BelongsToNodes, removedNode) {
				continue
			}

			if err := s.processShardReplicaRemoval(ctx, schemaReader, className, shardName, removedNode, desiredRF); err != nil {
				return err
			}
		}
	}

	return nil
}

// processShardReplicaRemoval handles the removal of a node from a specific shard's replicas.
func (s *Raft) processShardReplicaRemoval(ctx context.Context, schemaReader schema.SchemaReader, className, shardName, removedNode string, desiredRF int) error {
	currentReplicas, err := schemaReader.ShardReplicas(className, shardName)
	if err != nil {
		return fmt.Errorf("get replicas for shard %q in collection %q: %w", shardName, className, err)
	}

	currentCount := len(currentReplicas)

	// If we're at or below the desired RF, add replacement replicas first
	if currentCount <= desiredRF {
		if err := s.addReplacementReplicas(ctx, className, shardName, currentReplicas, removedNode, desiredRF); err != nil {
			return err
		}
	}

	// Re-read replicas to get the current state after potential additions
	finalReplicas, err := schemaReader.ShardReplicas(className, shardName)
	if err != nil {
		return fmt.Errorf("get replicas for shard %q in collection %q before deletion: %w", shardName, className, err)
	}

	// Verify the removed node is still present before attempting deletion
	if !slices.Contains(finalReplicas, removedNode) {
		return nil
	}

	finalCount := len(finalReplicas)
	if finalCount < desiredRF {
		s.log.WithFields(logrus.Fields{
			"collection":         className,
			"shard":              shardName,
			"removed_node":       removedNode,
			"replication_factor": desiredRF,
			"current_replicas":   finalCount,
		}).Warn("skipping replica removal after node departure: insufficient nodes to maintain replication factor")
		return nil
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

	return nil
}

// addReplacementReplicas adds replacement replicas to ensure we have at least desiredRF + 1
// replicas before removing the old one.
func (s *Raft) addReplacementReplicas(ctx context.Context, className, shardName string, currentReplicas []string, removedNode string, desiredRF int) error {
	availableNodes := s.StorageCandidates()

	// Build set of available nodes (excluding removed node and existing replicas)
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
	targetCount := desiredRF + 1
	currentCount := len(currentReplicas)

	for currentCount < targetCount && len(availableSet) > 0 {
		// Pick first available node
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

	return nil
}

func (s *Raft) Stats() map[string]any {
	s.log.Debug("membership.stats")
	return s.store.Stats()
}
