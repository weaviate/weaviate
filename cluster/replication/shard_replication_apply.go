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
	"github.com/weaviate/weaviate/cluster/replication/types"
)

var ErrShardAlreadyReplicating = errors.New("replica is already being replicated")

func (s *ShardReplicationFSM) Replicate(id uint64, c *api.ReplicationReplicateShardRequest) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	op := ShardReplicationOp{
		ID:           id,
		UUID:         c.Uuid,
		SourceShard:  newShardFQDN(c.SourceNode, c.SourceCollection, c.SourceShard),
		TargetShard:  newShardFQDN(c.TargetNode, c.SourceCollection, c.SourceShard),
		TransferType: api.ShardReplicationTransferType(c.TransferType),
	}
	return s.writeOpIntoFSM(op, NewShardReplicationStatus(api.REGISTERED))
}

func (s *ShardReplicationFSM) RegisterError(c *api.ReplicationRegisterErrorRequest) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	op, ok := s.opsById[c.Id]
	if !ok {
		return fmt.Errorf("could not find op %d: %w", c.Id, types.ErrReplicationOperationNotFound)
	}
	status, ok := s.opsStatus[op]
	if !ok {
		return fmt.Errorf("could not find op status for op %d", c.Id)
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

	if existingOps, ok := s.opsBySourceFQDN[op.SourceShard]; ok {
		for _, existingOp := range existingOps {
			// First check the status of the existing op. If it's READY or CANCELLED we can accept a new op
			// If it's ongoing we need to check if it's a move, in which case we can't accept any new op
			// Otherwise we can accept a copy if the existing op is also a copy
			if existingOpStatus, ok := s.opsStatus[existingOp]; !ok {
				// This should never happen
				return fmt.Errorf("could not find op status for op %d", existingOp.ID)
			} else if existingOpStatus.GetCurrentState() == api.CANCELLED {
				continue
			} else if existingOpStatus.GetCurrentState() == api.READY && existingOp.TransferType == api.COPY {
				continue
			}

			// If any of the ops we're handling is a move we can't accept any new op
			if existingOp.TransferType == api.MOVE {
				return ErrShardAlreadyReplicating
			}

			// At this point we know the existing op is a copy, if our new op is a move we can't accept it
			if op.TransferType == api.MOVE {
				return ErrShardAlreadyReplicating
			}

			// Existing op is an ongoing copy, our new op is also a copy, we can accept it
		}
	}

	s.idsByUuid[op.UUID] = op.ID
	s.opsBySource[op.SourceShard.NodeId] = append(s.opsBySource[op.SourceShard.NodeId], op)
	s.opsByTarget[op.TargetShard.NodeId] = append(s.opsByTarget[op.TargetShard.NodeId], op)
	// Make sure the nested map exists and is initialized
	if _, ok := s.opsByCollectionAndShard[op.SourceShard.CollectionId]; !ok {
		s.opsByCollectionAndShard[op.SourceShard.CollectionId] = make(map[string][]ShardReplicationOp)
	}
	s.opsByCollectionAndShard[op.SourceShard.CollectionId][op.SourceShard.ShardId] = append(s.opsByCollectionAndShard[op.SourceShard.CollectionId][op.SourceShard.ShardId], op)
	s.opsByCollection[op.SourceShard.CollectionId] = append(s.opsByCollection[op.SourceShard.CollectionId], op)
	s.opsByTargetFQDN[op.TargetShard] = op
	s.opsBySourceFQDN[op.SourceShard] = append(s.opsBySourceFQDN[op.SourceShard], op)
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
		return fmt.Errorf("could not find op %d: %w", c.Id, types.ErrReplicationOperationNotFound)
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

func (s *ShardReplicationFSM) CancelReplication(c *api.ReplicationCancelRequest) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	id, ok := s.idsByUuid[c.Uuid]
	if !ok {
		return types.ErrReplicationOperationNotFound
	}
	op, ok := s.opsById[id]
	if !ok {
		return fmt.Errorf("could not find op %d: %w", id, types.ErrReplicationOperationNotFound)
	}
	status, ok := s.opsStatus[op]
	if !ok {
		return fmt.Errorf("could not find op status for op %d", id)
	}
	status.TriggerCancellation()
	s.opsStatus[op] = status

	return nil
}

func (s *ShardReplicationFSM) DeleteReplication(c *api.ReplicationDeleteRequest) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	id, ok := s.idsByUuid[c.Uuid]
	if !ok {
		return types.ErrReplicationOperationNotFound
	}
	op, ok := s.opsById[id]
	if !ok {
		return fmt.Errorf("could not find op %d: %w", id, types.ErrReplicationOperationNotFound)
	}
	status, ok := s.opsStatus[op]
	if !ok {
		return fmt.Errorf("could not find op status for op %d", id)
	}
	status.TriggerDeletion()
	s.opsStatus[op] = status

	return nil
}

func (s *ShardReplicationFSM) RemoveReplicationOp(c *api.ReplicationRemoveOpRequest) error {
	return s.removeReplicationOp(c.Id)
}

func (s *ShardReplicationFSM) CancellationComplete(c *api.ReplicationCancellationCompleteRequest) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	op, ok := s.opsById[c.Id]
	if !ok {
		return fmt.Errorf("could not find op %d: %w", c.Id, types.ErrReplicationOperationNotFound)
	}
	status, ok := s.opsStatus[op]
	if !ok {
		return fmt.Errorf("could not find op status for op %d", c.Id)
	}
	status.CompleteCancellation()
	s.opsStatus[op] = status

	return nil
}

// TODO: Improve the error handling in that function
func (s *ShardReplicationFSM) removeReplicationOp(id uint64) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	var err error
	op, ok := s.opsById[id]
	if !ok {
		return types.ErrReplicationOperationNotFound
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

	ops, ok = s.opsBySourceFQDN[op.SourceShard]
	if !ok {
		err = multierror.Append(err, fmt.Errorf("could not find op in ops by source fqdn, this should not happen"))
	}
	opsReplace, ok = findAndDeleteOp(op.ID, ops)
	if ok {
		s.opsBySourceFQDN[op.SourceShard] = opsReplace
	}

	shardOps, ok := s.opsByCollectionAndShard[op.SourceShard.CollectionId]
	if !ok {
		err = multierror.Append(err, fmt.Errorf("could not find op in ops by collection and shard, this should not happen"))
	} else {
		ops, ok = shardOps[op.SourceShard.ShardId]
		if !ok {
			err = multierror.Append(err, fmt.Errorf("could not find op in ops by shard, this should not happen"))
		}
		opsReplace, ok = findAndDeleteOp(op.ID, ops)
		if ok {
			s.opsByCollectionAndShard[op.SourceShard.CollectionId][op.SourceShard.ShardId] = opsReplace
		}
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
