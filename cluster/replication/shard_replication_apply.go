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
	"fmt"
	"slices"

	"github.com/hashicorp/go-multierror"
	"github.com/weaviate/weaviate/cluster/proto/api"
)

var (
	ErrShardAlreadyReplicating = errors.New("target shard is already being replicated")
	ErrReplicationOpNotFound   = errors.New("could not find the replication op")
)

func (s *ShardReplicationFSM) Replicate(id uint64, c *api.ReplicationReplicateShardRequest) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	op := ShardReplicationOp{
		ID:          id,
		UUID:        c.Uuid,
		SourceShard: newShardFQDN(c.SourceNode, c.SourceCollection, c.SourceShard),
		TargetShard: newShardFQDN(c.TargetNode, c.SourceCollection, c.SourceShard),
	}
	return s.writeOpIntoFSM(op, NewShardReplicationStatus(api.REGISTERED))
}

func (s *ShardReplicationFSM) RegisterError(id uint64, c *api.ReplicationRegisterErrorRequest) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	op, ok := s.opsById[id]
	if !ok {
		return ErrReplicationOpNotFound
	}
	status, ok := s.opsStatus[op]
	if !ok {
		return fmt.Errorf("could not find op status for op %d", id)
	}
	if err := status.AddError(c.Error); err != nil {
		return err
	}
	s.opsStatus[op] = status

	return nil
}

// writeOpIntoFSM writes the op with status into the FSM. It *does* not holds the lock onto the maps so the callee must make sure the lock
// is held
func (s *ShardReplicationFSM) writeOpIntoFSM(op ShardReplicationOp, status ShardReplicationOpStatus) error {
	if _, ok := s.opsByTargetFQDN[op.TargetShard]; ok {
		return ErrShardAlreadyReplicating
	}

	s.idsByUuid[op.UUID] = op.ID
	s.opsBySource[op.SourceShard.NodeId] = append(s.opsBySource[op.SourceShard.NodeId], op)
	s.opsByTarget[op.TargetShard.NodeId] = append(s.opsByTarget[op.TargetShard.NodeId], op)
	s.opsByShard[op.SourceShard.CollectionId] = append(s.opsByShard[op.SourceShard.ShardId], op)
	s.opsByCollection[op.SourceShard.CollectionId] = append(s.opsByCollection[op.SourceShard.CollectionId], op)
	s.opsByTargetFQDN[op.TargetShard] = op
	s.opsById[op.ID] = op
	s.opsStatus[op] = status

	s.opsByStateGauge.WithLabelValues(status.GetCurrentState().String()).Inc()

	return nil
}

func (s *ShardReplicationFSM) UpdateReplicationOpStatus(c *api.ReplicationUpdateOpStateRequest) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	op, ok := s.opsById[c.Id]
	if !ok {
		return ErrReplicationOpNotFound
	}
	status, ok := s.opsStatus[op]
	if !ok {
		return fmt.Errorf("could not find op status for op %d", c.Id)
	}
	s.opsByStateGauge.WithLabelValues(status.GetCurrentState().String()).Dec()
	status.ChangeState(c.State)
	s.opsStatus[op] = status
	s.opsByStateGauge.WithLabelValues(status.GetCurrentState().String()).Inc()

	return nil
}

func (s *ShardReplicationFSM) CancelReplicationOp(c *api.ReplicationCancelOpRequest) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	id, ok := s.idsByUuid[c.Uuid]
	if !ok {
		return ErrReplicationOpNotFound
	}
	op, ok := s.opsById[id]
	if !ok {
		return ErrReplicationOpNotFound
	}
	status, ok := s.opsStatus[op]
	if !ok {
		return fmt.Errorf("could not find op status for op %d", id)
	}
	s.opsByStateGauge.WithLabelValues(status.GetCurrentState().String()).Dec()
	status.ChangeState(api.ABORTED)
	s.opsStatus[op] = status
	s.opsByStateGauge.WithLabelValues(status.GetCurrentState().String()).Inc()

	return nil
}

func (s *ShardReplicationFSM) DeleteReplicationOp(c *api.ReplicationDeleteOpRequest) error {
	return s.deleteShardReplicationOp(c.Id)
}

// TODO: Improve the error handling in that function
func (s *ShardReplicationFSM) deleteShardReplicationOp(id uint64) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	var err error
	op, ok := s.opsById[id]
	if !ok {
		return ErrReplicationOpNotFound
	}

	ops, ok := s.opsByTarget[op.SourceShard.NodeId]
	if !ok {
		err = multierror.Append(err, fmt.Errorf("could not find op in ops by target, this should not happen"))
	}
	opsReplace, ok := findAndDeleteOp(op.ID, ops)
	if ok {
		s.opsByTarget[op.SourceShard.NodeId] = opsReplace
	}

	ops, ok = s.opsBySource[op.TargetShard.NodeId]
	if !ok {
		err = multierror.Append(err, fmt.Errorf("could not find op in ops by source, this should not happen"))
	}
	opsReplace, ok = findAndDeleteOp(op.ID, ops)
	if ok {
		s.opsBySource[op.SourceShard.NodeId] = opsReplace
	}

	ops, ok = s.opsByCollection[op.SourceShard.CollectionId]
	if !ok {
		err = multierror.Append(err, fmt.Errorf("could not find op in ops by collection, this should not happen"))
	}
	opsReplace, ok = findAndDeleteOp(op.ID, ops)
	if ok {
		s.opsByCollection[op.SourceShard.CollectionId] = opsReplace
	}

	ops, ok = s.opsByShard[op.SourceShard.ShardId]
	if !ok {
		err = multierror.Append(err, fmt.Errorf("could not find op in ops by shard, this should not happen"))
	}
	opsReplace, ok = findAndDeleteOp(op.ID, ops)
	if ok {
		s.opsByShard[op.SourceShard.ShardId] = opsReplace
	}

	status, ok := s.opsStatus[op]
	if !ok {
		err = multierror.Append(err, fmt.Errorf("could not find op status for op %d", id))
	} else {
		s.opsByStateGauge.WithLabelValues(status.GetCurrentState().String()).Dec()
	}

	delete(s.idsByUuid, op.UUID)
	delete(s.opsByTargetFQDN, op.TargetShard)
	delete(s.opsById, op.ID)
	delete(s.opsStatus, op)

	return err
}

func findAndDeleteOp(id uint64, ops []ShardReplicationOp) ([]ShardReplicationOp, bool) {
	indexToDelete := 0
	ok := false
	// Iterate by hand as the slices should be kept small enough & we can't use the `slices` package binary search as we have a custom type
	// in the slice and the Comparable constraint only works on primitive type
	for i, op := range ops {
		if op.ID == id {
			ok = true
			indexToDelete = i
		}
	}
	if ok {
		ops = slices.Delete(ops, indexToDelete, indexToDelete)
	}
	return ops, ok
}
