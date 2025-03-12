//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replication

import (
	"errors"
	"slices"

	"github.com/weaviate/weaviate/cluster/proto/api"
)

var ErrShardAlreadyReplicating = errors.New("target shard is already being replicated")
var ErrReplicationOpNotFound = errors.New("could not find the replication op")

func (s *ShardReplicationFSM) Replicate(id uint64, c *api.ReplicationReplicateShardRequest) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	srcFQDN := newShardFQDN(c.SourceNode, c.SourceCollection, c.SourceShard)
	targetFQDN := newShardFQDN(c.TargetNode, c.SourceCollection, c.SourceShard)
	if _, ok := s.opsByTargetFQDN[targetFQDN]; ok {
		return ErrShardAlreadyReplicating
	}

	op := shardReplicationOp{
		id:          id,
		sourceShard: srcFQDN,
		targetShard: targetFQDN,
	}
	s.opsByNode[c.TargetNode] = append(s.opsByNode[c.TargetNode], op)
	s.opsByShard[c.SourceShard] = append(s.opsByShard[c.SourceShard], op)
	s.opsByCollection[c.SourceCollection] = append(s.opsByShard[c.SourceCollection], op)
	s.opsByTargetFQDN[targetFQDN] = op
	s.opsById[op.id] = op
	s.opsStatus[op] = shardReplicationOpStatus{state: api.REGISTERED}
	return nil
}

func (s *ShardReplicationFSM) UpdateReplicationOpStatus(c *api.ReplicationUpdateOpStateRequest) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	op, ok := s.opsById[c.Id]
	if !ok {
		return ErrReplicationOpNotFound
	}
	s.opsStatus[op] = shardReplicationOpStatus{state: c.State}

	return nil
}

func (s *ShardReplicationFSM) DeleteReplicationOp(c *api.ReplicationDeleteOpRequest) error {
	return s.deleteShardReplicationOp(c.Id)
}

// TODO: Improve the error handling in that function
func (s *ShardReplicationFSM) deleteShardReplicationOp(id uint64) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	op, ok := s.opsById[id]
	if !ok {
		return ErrReplicationOpNotFound
	}

	ops, ok := s.opsByNode[op.sourceShard.nodeId]
	if !ok {
		//TODO should not happen
	}
	opsReplace, ok := findAndDeleteOp(op.id, ops)
	if ok {
		s.opsByNode[op.sourceShard.nodeId] = opsReplace
	}

	ops, ok = s.opsByCollection[op.sourceShard.collectionId]
	if !ok {
		//TODO should not happen
	}
	opsReplace, ok = findAndDeleteOp(op.id, ops)
	if ok {
		s.opsByCollection[op.sourceShard.collectionId] = opsReplace
	}

	ops, ok = s.opsByShard[op.sourceShard.shardId]
	if !ok {
		//TODO should not happen
	}
	opsReplace, ok = findAndDeleteOp(op.id, ops)
	if ok {
		s.opsByShard[op.sourceShard.shardId] = opsReplace
	}

	delete(s.opsByTargetFQDN, op.targetShard)
	delete(s.opsById, op.id)
	delete(s.opsStatus, op)

	return nil

}

func findAndDeleteOp(id uint64, ops []shardReplicationOp) ([]shardReplicationOp, bool) {
	indexToDelete := 0
	ok := false
	// Iterate by hand as the slices should be kept small enough & we can't use the `slices` package binary search as we have a custom type
	// in the slice and the Comparable constraint only works on primitive type
	for i, op := range ops {
		if op.id == id {
			ok = true
			indexToDelete = i
		}
	}
	if ok {
		slices.Delete(ops, indexToDelete, indexToDelete)
	}
	return ops, ok
}
