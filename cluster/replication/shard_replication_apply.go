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

package replication

import (
	"errors"
	"fmt"
	"time"

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
		ID:              id,
		UUID:            c.Uuid,
		SourceShard:     newShardFQDN(c.SourceNode, c.SourceCollection, c.SourceShard),
		TargetShard:     newShardFQDN(c.TargetNode, c.SourceCollection, c.SourceShard),
		TransferType:    api.ShardReplicationTransferType(c.TransferType),
		StartTimeUnixMs: time.Now().UnixMilli(),
	}
	if err := s.validateReplicationAdmission(op); err != nil {
		return err
	}
	s.insertOpIntoFSM(op, NewShardReplicationStatus(api.REGISTERED))
	return nil
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
	if err := status.AddError(c.Error, c.TimeUnixMs); err != nil {
		return err
	}
	s.statusById[op.ID] = status

	return nil
}

// validateReplicationAdmission runs only on the write path (Restore must not re-validate
// already-admitted state). Callers must hold s.opsLock.
func (s *ShardReplicationFSM) validateReplicationAdmission(op ShardReplicationOp) error {
	if err := s.checkNoConflictingOp(op, s.opsByTargetFQDN[op.TargetShard], "target"); err != nil {
		return err
	}
	if err := s.checkNoConflictingOp(op, s.opsBySourceFQDN[op.SourceShard], "source"); err != nil {
		return err
	}
	return s.checkSourceNotInFlightAsTarget(op)
}

func (s *ShardReplicationFSM) checkSourceNotInFlightAsTarget(op ShardReplicationOp) error {
	for _, existingOp := range s.opsByTargetFQDN[op.SourceShard] {
		status, ok := s.statusById[existingOp.ID]
		if !ok {
			return fmt.Errorf("could not find op status for op %d", existingOp.ID)
		}
		switch status.GetCurrentState() {
		case api.READY, api.CANCELLED:
			// settled target, safe to source from (DEHYDRATING is complete too, but we
			// err closed and wait for READY rather than reason about the drain boundary)
		default:
			return fmt.Errorf("op %s sources replica %s, the in-flight target of op %d: %w", op.UUID, op.SourceShard, existingOp.ID, ErrShardAlreadyReplicating)
		}
	}
	return nil
}

func (s *ShardReplicationFSM) checkNoConflictingOp(op ShardReplicationOp, existingOps []ShardReplicationOp, scope string) error {
	for _, existingOp := range existingOps {
		status, ok := s.statusById[existingOp.ID]
		if !ok {
			return fmt.Errorf("could not find op status for op %d", existingOp.ID)
		}
		switch {
		case status.GetCurrentState() == api.CANCELLED:
			continue
		case status.GetCurrentState() == api.READY && existingOp.TransferType == api.COPY:
			continue
		case existingOp.TransferType == api.MOVE:
			return fmt.Errorf("existing op %s shares a %s replica and is a MOVE: %w", op.UUID, scope, ErrShardAlreadyReplicating)
		case op.TransferType == api.MOVE:
			return fmt.Errorf("existing op %s shares a %s replica (COPY), but new op is a MOVE: %w", op.UUID, scope, ErrShardAlreadyReplicating)
		}
	}
	return nil
}

// insertOpIntoFSM inserts op into every in-memory index. Callers must hold s.opsLock.
func (s *ShardReplicationFSM) insertOpIntoFSM(op ShardReplicationOp, status ShardReplicationOpStatus) {
	s.idsByUuid[op.UUID] = op.ID
	s.opsBySource[op.SourceShard.NodeId] = append(s.opsBySource[op.SourceShard.NodeId], op)
	s.opsByTarget[op.TargetShard.NodeId] = append(s.opsByTarget[op.TargetShard.NodeId], op)
	// Make sure the nested map exists and is initialized
	if _, ok := s.opsByCollectionAndShard[op.SourceShard.CollectionId]; !ok {
		s.opsByCollectionAndShard[op.SourceShard.CollectionId] = make(map[string][]ShardReplicationOp)
	}
	s.opsByCollectionAndShard[op.SourceShard.CollectionId][op.SourceShard.ShardId] = append(s.opsByCollectionAndShard[op.SourceShard.CollectionId][op.SourceShard.ShardId], op)
	s.opsByCollection[op.SourceShard.CollectionId] = append(s.opsByCollection[op.SourceShard.CollectionId], op)
	s.opsByTargetFQDN[op.TargetShard] = append(s.opsByTargetFQDN[op.TargetShard], op)
	s.opsBySourceFQDN[op.SourceShard] = append(s.opsBySourceFQDN[op.SourceShard], op)
	s.opsById[op.ID] = op
	s.statusById[op.ID] = status

	s.opsByStateGauge.WithLabelValues(status.GetCurrentState().String()).Inc()
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

// NodeReachedState records (c.NodeId → c.State) for op c.Id. Monotonic per
// peer via StateRank — safe against re-broadcasts from log replay.
func (s *ShardReplicationFSM) NodeReachedState(c *api.ReplicationNodeReachedStateRequest) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	status, ok := s.statusById[c.Id]
	if !ok {
		// Stale broadcast for an op that's been pruned — silent no-op.
		return nil
	}
	if status.PerNodeState == nil {
		status.PerNodeState = make(map[string]api.ShardReplicationState)
	}
	if api.StateRank(c.State) > api.StateRank(status.PerNodeState[c.NodeId]) {
		status.PerNodeState[c.NodeId] = c.State
		s.statusById[c.Id] = status
	}

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
	// If the op is already cancelled or in the process of being cancelled, we cannot make it uncancellable
	if status.ShouldCancel || status.GetCurrentState() == api.CANCELLED {
		return fmt.Errorf("op %d: %w", id, types.ErrOpCancellationInFlight)
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
	s.opsByStateGauge.WithLabelValues(status.GetCurrentState().String()).Dec()
	status.CompleteCancellation()
	s.statusById[op.ID] = status
	s.opsByStateGauge.WithLabelValues(status.GetCurrentState().String()).Inc()

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

	ids := make([]uint64, 0, len(s.opsById))
	for id := range s.opsById {
		ids = append(ids, id)
	}

	return s.removeReplicationOps(ids)
}

func (s *ShardReplicationFSM) ForceDeleteByCollection(collection string) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	ops, ok := s.opsByCollection[collection]
	if !ok {
		return nil // nothing to do
	}

	return s.removeReplicationOps(idsOf(ops))
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

	return s.removeReplicationOps(idsOf(shardOps))
}

func (s *ShardReplicationFSM) ForceDeleteByTargetNode(node string) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	ops, ok := s.opsByTarget[node]
	if !ok {
		return nil // nothing to do
	}

	return s.removeReplicationOps(idsOf(ops))
}

// idsOf snapshots the ids before any removal runs: removeReplicationOps
// rewrites the backing arrays these slices are headers over.
func idsOf(ops []ShardReplicationOp) []uint64 {
	ids := make([]uint64, 0, len(ops))
	for _, op := range ops {
		ids = append(ids, op.ID)
	}
	return ids
}

// ForceDeleteByIds removes the listed ops with no teardown and no state checks,
// like the rest of the force-delete family. Ids not present are skipped, so a
// batch re-proposed after leader churn is a no-op.
//
// It must stay a pure function of ids: no clock, config, node identity or op
// state may be read here, because every node applies it independently.
func (s *ShardReplicationFSM) ForceDeleteByIds(ids []uint64) error {
	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	return s.removeReplicationOps(ids)
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

// removeReplicationOps removes every op in ids in one filtering pass per touched
// index bucket. Unknown ids are skipped. Callers must hold s.opsLock.
func (s *ShardReplicationFSM) removeReplicationOps(ids []uint64) error {
	idSet := make(map[uint64]struct{}, len(ids))
	ops := make([]ShardReplicationOp, 0, len(ids))

	// Keys mirror insertOpIntoFSM: the collection, collection-and-shard and
	// source-FQDN indices key off SourceShard, opsByTarget/opsByTargetFQDN off
	// TargetShard.
	targetNodes := make(map[string]struct{})
	sourceNodes := make(map[string]struct{})
	collections := make(map[string]struct{})
	shardsByCollection := make(map[string]map[string]struct{})
	targetFQDNs := make(map[shardFQDN]struct{})
	sourceFQDNs := make(map[shardFQDN]struct{})

	for _, id := range ids {
		if _, seen := idSet[id]; seen {
			continue
		}
		op, ok := s.opsById[id]
		if !ok {
			continue
		}
		idSet[id] = struct{}{}
		ops = append(ops, op)

		targetNodes[op.TargetShard.NodeId] = struct{}{}
		sourceNodes[op.SourceShard.NodeId] = struct{}{}
		collections[op.SourceShard.CollectionId] = struct{}{}
		if _, ok := shardsByCollection[op.SourceShard.CollectionId]; !ok {
			shardsByCollection[op.SourceShard.CollectionId] = make(map[string]struct{})
		}
		shardsByCollection[op.SourceShard.CollectionId][op.SourceShard.ShardId] = struct{}{}
		targetFQDNs[op.TargetShard] = struct{}{}
		sourceFQDNs[op.SourceShard] = struct{}{}
	}
	if len(ops) == 0 {
		return nil
	}

	for node := range targetNodes {
		filterOpsFromBucket(s.opsByTarget, node, idSet)
	}
	for node := range sourceNodes {
		filterOpsFromBucket(s.opsBySource, node, idSet)
	}
	for collection := range collections {
		filterOpsFromBucket(s.opsByCollection, collection, idSet)
	}
	for fqdn := range targetFQDNs {
		filterOpsFromBucket(s.opsByTargetFQDN, fqdn, idSet)
	}
	for fqdn := range sourceFQDNs {
		filterOpsFromBucket(s.opsBySourceFQDN, fqdn, idSet)
	}
	for collection, shards := range shardsByCollection {
		byShard, ok := s.opsByCollectionAndShard[collection]
		if !ok {
			continue
		}
		for shard := range shards {
			filterOpsFromBucket(byShard, shard, idSet)
		}
		if len(byShard) == 0 {
			delete(s.opsByCollectionAndShard, collection)
		}
	}

	for _, op := range ops {
		if status, ok := s.statusById[op.ID]; ok {
			s.opsByStateGauge.WithLabelValues(status.GetCurrentState().String()).Dec()
		}
		delete(s.idsByUuid, op.UUID)
		delete(s.opsById, op.ID)
		delete(s.statusById, op.ID)
	}

	return nil
}

// filterOpsFromBucket rewrites m[key] without the ops in idSet, deleting the key
// once the bucket drains. Deleting matters for opsByTargetFQDN: a present key is
// OR-folded by filterOneReplicaReadWrite, so an empty slice reads (false,false)
// and drops a live replica from routing instead of falling through to the source
// check. It also keeps the (slice, ok) getters from reporting a drained bucket
// as present.
func filterOpsFromBucket[K comparable](m map[K][]ShardReplicationOp, key K, idSet map[uint64]struct{}) {
	ops, ok := m[key]
	if !ok {
		return
	}
	kept := ops[:0]
	for _, op := range ops {
		if _, remove := idSet[op.ID]; !remove {
			kept = append(kept, op)
		}
	}
	if len(kept) == 0 {
		delete(m, key)
		return
	}
	m[key] = kept
}

// removeReplicationOp is the single-op path. Unlike removeReplicationOps it
// reports a missing op as ErrReplicationOperationNotFound, which the consumer
// path relies on.
func (s *ShardReplicationFSM) removeReplicationOp(id uint64) error {
	var err error
	op, ok := s.opsById[id]
	if !ok {
		return fmt.Errorf("could not find op %d: %w", id, types.ErrReplicationOperationNotFound)
	}

	// insertOpIntoFSM writes every index below together, so a missing bucket is
	// a torn FSM. Reported but not fatal.
	if _, ok := s.opsByTarget[op.TargetShard.NodeId]; !ok {
		err = multierror.Append(err, fmt.Errorf("could not find op %d in ops by target %s, this should not happen", op.ID, op.TargetShard.NodeId))
	}
	if _, ok := s.opsBySource[op.SourceShard.NodeId]; !ok {
		err = multierror.Append(err, fmt.Errorf("could not find op %d in ops by source %s, this should not happen", op.ID, op.SourceShard.NodeId))
	}
	if _, ok := s.opsByCollection[op.SourceShard.CollectionId]; !ok {
		err = multierror.Append(err, fmt.Errorf("could not find op %d in ops by collection %s, this should not happen", op.ID, op.SourceShard.CollectionId))
	}
	if _, ok := s.opsBySourceFQDN[op.SourceShard]; !ok {
		err = multierror.Append(err, errors.New("could not find op in ops by source fqdn, this should not happen"))
	}
	if _, ok := s.opsByTargetFQDN[op.TargetShard]; !ok {
		err = multierror.Append(err, errors.New("could not find op in ops by target fqdn, this should not happen"))
	}
	if byShard, ok := s.opsByCollectionAndShard[op.SourceShard.CollectionId]; !ok {
		err = multierror.Append(err, errors.New("could not find op in ops by collection and shard, this should not happen"))
	} else if _, ok := byShard[op.SourceShard.ShardId]; !ok {
		err = multierror.Append(err, errors.New("could not find op in ops by shard, this should not happen"))
	}
	if _, ok := s.statusById[op.ID]; !ok {
		err = multierror.Append(err, fmt.Errorf("could not find op status for op %d", id))
	}

	if removeErr := s.removeReplicationOps([]uint64{id}); removeErr != nil {
		err = multierror.Append(err, removeErr)
	}

	return err
}
