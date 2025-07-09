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

	"github.com/go-openapi/strfmt"
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
	status, ok := s.statusById[op.ID]
	if !ok {
		return fmt.Errorf("could not find op status for op %d", c.Id)
	}
	if err := status.AddError(c.Error); err != nil {
		return err
	}
	s.statusById[op.ID] = status

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
			if existingOpStatus, ok := s.statusById[existingOp.ID]; !ok {
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
	s.statusById[op.ID] = status

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
	status, ok := s.statusById[op.ID]
	if !ok {
		return fmt.Errorf("could not find op status for op %d", c.Id)
	}

	if status.GetCurrentState() == api.CANCELLED {
		return fmt.Errorf("cannot update op %d state, it is already cancelled", c.Id)
	}

	s.opsByStateGauge.WithLabelValues(status.GetCurrentState().String()).Dec()
	status.ChangeState(c.State)
	s.statusById[op.ID] = status
	s.opsByStateGauge.WithLabelValues(status.GetCurrentState().String()).Inc()

	return nil
}

func (s *ShardReplicationFSM) StoreSchemaVersion(c *api.ReplicationStoreSchemaVersionRequest) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	status, ok := s.statusById[c.Id]
	if !ok {
		return fmt.Errorf("could not find op status for op %d: %w", c.Id, types.ErrReplicationOperationNotFound)
	}
	status.SchemaVersion = c.SchemaVersion
	s.statusById[c.Id] = status

	return nil
}

func (s *ShardReplicationFSM) SetUnCancellable(id uint64) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	status, ok := s.statusById[id]
	if !ok {
		return fmt.Errorf("could not find op status for op %d: %w", id, types.ErrReplicationOperationNotFound)
	}
	status.UnCancellable = true
	s.statusById[id] = status

	return nil
}

func (s *ShardReplicationFSM) GetReplicationOpUUIDFromId(id uint64) (strfmt.UUID, error) {
	s.opsLock.RLock()
	defer s.opsLock.RUnlock()

	op, ok := s.opsById[id]
	if !ok {
		return "", fmt.Errorf("%w: %d", types.ErrReplicationOperationNotFound, id)
	}
	return op.UUID, nil
}

func (s *ShardReplicationFSM) CancelReplication(c *api.ReplicationCancelRequest) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	id, ok := s.idsByUuid[c.Uuid]
	if !ok {
		return fmt.Errorf("%w: %s", types.ErrReplicationOperationNotFound, c.Uuid)
	}
	op, ok := s.opsById[id]
	if !ok {
		return fmt.Errorf("could not find op %d: %w", id, types.ErrReplicationOperationNotFound)
	}
	status, ok := s.statusById[op.ID]
	if !ok {
		return fmt.Errorf("could not find op status for op %d", id)
	}

	// Only allow to cancel ops if they are cancellable (before being added to sharding state)
	if status.UnCancellable {
		return types.ErrCancellationImpossible
	}

	status.TriggerCancellation()
	s.statusById[op.ID] = status

	return nil
}

func (s *ShardReplicationFSM) DeleteReplication(c *api.ReplicationDeleteRequest) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	id, ok := s.idsByUuid[c.Uuid]
	if !ok {
		return fmt.Errorf("could not find op %s: %w", c.Uuid, types.ErrReplicationOperationNotFound)
	}
	op, ok := s.opsById[id]
	if !ok {
		return fmt.Errorf("could not find op %d: %w", id, types.ErrReplicationOperationNotFound)
	}
	status, ok := s.statusById[op.ID]
	if !ok {
		return fmt.Errorf("could not find op status for op %d", id)
	}

	// Only allow to delete ops if they are cancellable (before being added to sharding state) and not READY
	if status.UnCancellable && status.GetCurrentState() != api.READY {
		return types.ErrDeletionImpossible
	}

	status.TriggerDeletion()
	s.statusById[op.ID] = status

	return nil
}

func (s *ShardReplicationFSM) DeleteAllReplications(c *api.ReplicationDeleteAllRequest) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	for id, status := range s.statusById {
		if status.UnCancellable && status.GetCurrentState() != api.READY {
			continue
		}
		status.TriggerDeletion()
		s.statusById[id] = status
	}
	return nil
}

func (s *ShardReplicationFSM) RemoveReplicationOp(c *api.ReplicationRemoveOpRequest) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	return s.removeReplicationOp(c.Id)
}

func (s *ShardReplicationFSM) CancellationComplete(c *api.ReplicationCancellationCompleteRequest) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	op, ok := s.opsById[c.Id]
	if !ok {
		return fmt.Errorf("could not find op %d: %w", c.Id, types.ErrReplicationOperationNotFound)
	}
	status, ok := s.statusById[op.ID]
	if !ok {
		return fmt.Errorf("could not find op status for op %d", c.Id)
	}
	status.CompleteCancellation()
	s.statusById[op.ID] = status

	return nil
}

func (s *ShardReplicationFSM) DeleteReplicationsByCollection(collection string) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	ops, ok := s.opsByCollection[collection]
	if !ok {
		return nil // nothing to do
	}

	for _, op := range ops {
		status, ok := s.statusById[op.ID]
		if !ok {
			return fmt.Errorf("could not find op status for op %d: %w", op.ID, types.ErrReplicationOperationNotFound)
		}
		status.TriggerDeletion()
		s.statusById[op.ID] = status
	}

	return nil
}

func (s *ShardReplicationFSM) DeleteReplicationsByTenants(collection string, tenants []string) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	ops := make([]ShardReplicationOp, 0)
	for _, tenant := range tenants {
		opsPerTenant, ok := s.opsByCollectionAndShard[collection][tenant]
		if !ok {
			continue
		}
		ops = append(ops, opsPerTenant...)
	}
	if len(ops) == 0 {
		return nil // nothing to do
	}

	for _, op := range ops {
		status, ok := s.statusById[op.ID]
		if !ok {
			return fmt.Errorf("could not find op status for op %d: %w", op.ID, types.ErrReplicationOperationNotFound)
		}
		status.TriggerDeletion()
		s.statusById[op.ID] = status
	}

	return nil
}

func (s *ShardReplicationFSM) ForceDeleteAll() error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	for id := range s.opsById {
		err := s.removeReplicationOp(id)
		if err != nil {
			return fmt.Errorf("could not remove op %d: %w", id, err)
		}
	}

	return nil
}

func (s *ShardReplicationFSM) ForceDeleteByCollection(collection string) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	ops, ok := s.opsByCollection[collection]
	if !ok {
		return nil // nothing to do
	}

	for _, op := range ops {
		err := s.removeReplicationOp(op.ID)
		if err != nil {
			return fmt.Errorf("could not remove op %d: %w", op.ID, err)
		}
	}

	return nil
}

func (s *ShardReplicationFSM) ForceDeleteByCollectionAndShard(collection, shard string) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	collectionOps, ok := s.opsByCollectionAndShard[collection]
	if !ok {
		return nil // nothing to do
	}

	shardOps, ok := collectionOps[shard]
	if !ok {
		return nil // nothing to do
	}

	for _, op := range shardOps {
		err := s.removeReplicationOp(op.ID)
		if err != nil {
			return fmt.Errorf("could not remove op %d: %w", op.ID, err)
		}
	}

	return nil
}

func (s *ShardReplicationFSM) ForceDeleteByTargetNode(node string) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	ops, ok := s.opsByTarget[node]
	if !ok {
		return nil // nothing to do
	}

	for _, op := range ops {
		err := s.removeReplicationOp(op.ID)
		if err != nil {
			return fmt.Errorf("could not remove op %d: %w", op.ID, err)
		}
	}

	return nil
}

func (s *ShardReplicationFSM) ForceDeleteByUuid(uuid strfmt.UUID) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	id, ok := s.idsByUuid[uuid]
	if !ok {
		return fmt.Errorf("could not find op with uuid %s: %w", uuid, types.ErrReplicationOperationNotFound)
	}

	if err := s.removeReplicationOp(id); err != nil {
		return fmt.Errorf("could not remove op %d: %w", id, err)
	}

	return nil
}

func (s *ShardReplicationFSM) hasOngoingSourceReplication(sourceFQDN shardFQDN) bool {
	ops, ok := s.opsBySourceFQDN[sourceFQDN]
	if !ok {
		return false
	}

	for _, op := range ops {
		status, ok := s.statusById[op.ID]
		if !ok {
			continue
		}

		if status.ShouldConsumeOps() {
			return true
		} else {
			continue
		}
	}
	return false
}

func (s *ShardReplicationFSM) hasOngoingTargetReplication(targetFQDN shardFQDN) bool {
	op, ok := s.opsByTargetFQDN[targetFQDN]
	if !ok {
		return false
	}
	status, ok := s.statusById[op.ID]
	if !ok {
		return false
	}
	return status.ShouldConsumeOps()
}

func (s *ShardReplicationFSM) HasOngoingReplication(collection string, shard string, replica string) bool {
	s.opsLock.RLock()
	defer s.opsLock.RUnlock()

	FQDN := newShardFQDN(replica, collection, shard)
	return s.hasOngoingSourceReplication(FQDN) || s.hasOngoingTargetReplication(FQDN)
}

// TODO: Improve the error handling in that function
func (s *ShardReplicationFSM) removeReplicationOp(id uint64) error {
	var err error
	op, ok := s.opsById[id]
	if !ok {
		return fmt.Errorf("could not find op %d: %w", id, types.ErrReplicationOperationNotFound)
	}

	ops, ok := s.opsByTarget[op.TargetShard.NodeId]
	if !ok {
		err = multierror.Append(err, fmt.Errorf("could not find op %d in ops by target %s, this should not happen", op.ID, op.SourceShard.NodeId))
	}
	opsReplace, ok := findAndDeleteOp(op.ID, ops)
	if ok {
		s.opsByTarget[op.TargetShard.NodeId] = opsReplace
	}

	ops, ok = s.opsBySource[op.SourceShard.NodeId]
	if !ok {
		err = multierror.Append(err, fmt.Errorf("could not find op %d in ops by source %s, this should not happen", op.ID, op.TargetShard.NodeId))
	}
	opsReplace, ok = findAndDeleteOp(op.ID, ops)
	if ok {
		s.opsBySource[op.SourceShard.NodeId] = opsReplace
	}

	ops, ok = s.opsByCollection[op.SourceShard.CollectionId]
	if !ok {
		err = multierror.Append(err, fmt.Errorf("could not find op %d in ops by collection %s, this should not happen", op.ID, op.SourceShard.CollectionId))
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

	status, ok := s.statusById[op.ID]
	if !ok {
		err = multierror.Append(err, fmt.Errorf("could not find op status for op %d", id))
	} else {
		s.opsByStateGauge.WithLabelValues(status.GetCurrentState().String()).Dec()
	}

	delete(s.idsByUuid, op.UUID)
	delete(s.opsByTargetFQDN, op.TargetShard)
	delete(s.opsById, op.ID)
	delete(s.statusById, op.ID)

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
		ops = slices.Delete(ops, indexToDelete, indexToDelete+1)
	}
	return ops, ok
}
