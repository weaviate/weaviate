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

package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
)

func (s *Raft) ScaleApply(ctx context.Context, desiredState api.ShardingState) (opsUUIDs []strfmt.UUID, err error) {
	defer func() {
		if err != nil {
			s.ForceDeleteReplicationsByCollection(ctx, desiredState.Collection)
		}
	}()

	ops, err := s.GetReplicationDetailsByCollection(ctx, desiredState.Collection)
	if err != nil {
		return nil, fmt.Errorf("get replication details for %q: %w", desiredState.Collection, err)
	}
	for _, op := range ops {
		if api.ShardReplicationState(op.Status.State) != api.CANCELLED && api.ShardReplicationState(op.Status.State) != api.READY {
			return nil, fmt.Errorf("cannot scale while there are ongoing replications for collection %q", desiredState.Collection)
		}
	}

	shardingState, _, err := s.QueryShardingState(desiredState.Collection)
	if err != nil {
		return nil, fmt.Errorf("query sharding state for %q: %w", desiredState.Collection, err)
	}

	currentShards := make(map[string][]string)

	for shardName, shard := range shardingState.Physical {
		currentShards[shardName] = shard.BelongsToNodes
	}

	diff := diffNodesPerShard(currentShards, desiredState.Shards)

	for shardName, nodes := range diff {
		if len(nodes.Added) == 0 && len(nodes.Removed) == 0 {
			continue
		}

		if len(nodes.Added) > 0 && len(nodes.Remaining) == 0 {
			return nil, fmt.Errorf("cannot determine source node for shard %q: no remaining nodes", shardName)
		}

		for _, node := range nodes.Added {
			id, err := uuid.NewRandom()
			if err != nil {
				return nil, fmt.Errorf("create uuid for new replica: %w", err)
			}
			uuid := strfmt.UUID(id.String())

			sourceNode := nodes.Remaining[rand.Intn(len(nodes.Remaining))]

			err = s.ReplicationReplicateReplica(ctx, uuid, sourceNode, desiredState.Collection, shardName, node, api.COPY.String())
			if err != nil {
				return nil, fmt.Errorf("replication add replica to shard for %q: %w", desiredState.Collection, err)
			}

			opsUUIDs = append(opsUUIDs, uuid)
		}

		for _, node := range nodes.Removed {
			if _, err := s.DeleteReplicaFromShard(ctx, desiredState.Collection, shardName, node); err != nil {
				return nil, fmt.Errorf("delete replica %s from shard %s: %w", node, shardName, err)
			}
		}
	}

	return opsUUIDs, nil
}

func diffNodesPerShard(before, after map[string][]string) map[string]struct {
	Remaining []string
	Removed   []string
	Added     []string
} {
	result := make(map[string]struct {
		Remaining []string
		Removed   []string
		Added     []string
	}, len(before))

	for shardName, beforeShard := range before {
		afterShard, ok := after[shardName]
		if !ok {
			// Shard missing entirely in the new state — all previous nodes are removed
			result[shardName] = struct {
				Remaining []string
				Removed   []string
				Added     []string
			}{
				Remaining: nil,
				Removed:   beforeShard,
				Added:     nil,
			}
			continue
		}

		beforeSet := make(map[string]struct{}, len(beforeShard))
		for _, n := range beforeShard {
			beforeSet[n] = struct{}{}
		}

		afterSet := make(map[string]struct{}, len(afterShard))
		for _, n := range afterShard {
			afterSet[n] = struct{}{}
		}

		var remaining, removed, added []string
		for _, n := range beforeShard {
			if _, ok := afterSet[n]; ok {
				remaining = append(remaining, n)
			} else {
				removed = append(removed, n)
			}
		}
		for _, n := range afterShard {
			if _, ok := beforeSet[n]; !ok {
				added = append(added, n)
			}
		}

		result[shardName] = struct {
			Remaining []string
			Removed   []string
			Added     []string
		}{
			Remaining: remaining,
			Removed:   removed,
			Added:     added,
		}
	}

	return result
}

func (s *Raft) ReplicationReplicateReplica(ctx context.Context, uuid strfmt.UUID, sourceNode string, sourceCollection string, sourceShard string, targetNode string, transferType string) error {
	req := &api.ReplicationReplicateShardRequest{
		Version:          api.ReplicationCommandVersionV0,
		SourceNode:       sourceNode,
		SourceCollection: sourceCollection,
		SourceShard:      sourceShard,
		TargetNode:       targetNode,
		Uuid:             uuid,
		TransferType:     transferType,
	}

	if err := replication.ValidateReplicationReplicateShard(s.SchemaReader(), req); err != nil {
		return fmt.Errorf("%w: %w", replicationTypes.ErrInvalidRequest, err)
	}

	subCommand, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &api.ApplyRequest{
		Type:       api.ApplyRequest_TYPE_REPLICATION_REPLICATE,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(ctx, command); err != nil {
		return err
	}
	return nil
}

func (s *Raft) ReplicationUpdateReplicaOpStatus(ctx context.Context, id uint64, state api.ShardReplicationState) error {
	req := &api.ReplicationUpdateOpStateRequest{
		Version: api.ReplicationCommandVersionV0,
		Id:      id,
		State:   state,
	}

	subCommand, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &api.ApplyRequest{
		Type:       api.ApplyRequest_TYPE_REPLICATION_REPLICATE_UPDATE_STATE,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(ctx, command); err != nil {
		return err
	}
	return nil
}

func (s *Raft) ReplicationRegisterError(ctx context.Context, id uint64, errorToRegister string) error {
	req := &api.ReplicationRegisterErrorRequest{
		Version: api.ReplicationCommandVersionV0,
		Id:      id,
		Error:   errorToRegister,
	}

	subCommand, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &api.ApplyRequest{
		Type:       api.ApplyRequest_TYPE_REPLICATION_REPLICATE_REGISTER_ERROR,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(ctx, command); err != nil {
		return err
	}
	return nil
}

func (s *Raft) ReplicationCancellationComplete(ctx context.Context, id uint64) error {
	req := &api.ReplicationCancellationCompleteRequest{
		Version: api.ReplicationCommandVersionV0,
		Id:      id,
	}

	subCommand, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &api.ApplyRequest{
		Type:       api.ApplyRequest_TYPE_REPLICATION_REPLICATE_CANCELLATION_COMPLETE,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(ctx, command); err != nil {
		return err
	}
	return nil
}

func (s *Raft) CancelReplication(ctx context.Context, uuid strfmt.UUID) error {
	req := &api.ReplicationCancelRequest{
		Version: api.ReplicationCommandVersionV0,
		Uuid:    uuid,
	}

	subCommand, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &api.ApplyRequest{
		Type:       api.ApplyRequest_TYPE_REPLICATION_REPLICATE_CANCEL,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(ctx, command); err != nil {
		if strings.Contains(err.Error(), replicationTypes.ErrReplicationOperationNotFound.Error()) {
			return fmt.Errorf("execute cancel replication: %w", replicationTypes.ErrReplicationOperationNotFound)
		}
		if strings.Contains(err.Error(), replicationTypes.ErrCancellationImpossible.Error()) {
			return fmt.Errorf("execute cancel replication: %w", replicationTypes.ErrCancellationImpossible)
		}
		return err
	}
	return nil
}

func (s *Raft) DeleteReplication(ctx context.Context, uuid strfmt.UUID) error {
	req := &api.ReplicationDeleteRequest{
		Version: api.ReplicationCommandVersionV0,
		Uuid:    uuid,
	}

	subCommand, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &api.ApplyRequest{
		Type:       api.ApplyRequest_TYPE_REPLICATION_REPLICATE_DELETE,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(ctx, command); err != nil {
		if strings.Contains(err.Error(), replicationTypes.ErrReplicationOperationNotFound.Error()) {
			return fmt.Errorf("execute delete replication: %w", replicationTypes.ErrReplicationOperationNotFound)
		}
		if strings.Contains(err.Error(), replicationTypes.ErrDeletionImpossible.Error()) {
			return fmt.Errorf("execute delete replication: %w", replicationTypes.ErrDeletionImpossible)
		}
		return err
	}
	return nil
}

func (s *Raft) ReplicationRemoveReplicaOp(ctx context.Context, id uint64) error {
	req := &api.ReplicationRemoveOpRequest{
		Version: api.ReplicationCommandVersionV0,
		Id:      id,
	}

	subCommand, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &api.ApplyRequest{
		Type:       api.ApplyRequest_TYPE_REPLICATION_REPLICATE_REMOVE,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(ctx, command); err != nil {
		return err
	}
	return nil
}

func (s *Raft) ForceDeleteAllReplications(ctx context.Context) error {
	req := &api.ReplicationForceDeleteAllRequest{}

	subCommand, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &api.ApplyRequest{
		Type:       api.ApplyRequest_TYPE_REPLICATION_REPLICATE_FORCE_DELETE_ALL,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(ctx, command); err != nil {
		return err
	}
	return nil
}

func (s *Raft) ForceDeleteReplicationsByCollection(ctx context.Context, collection string) error {
	req := &api.ReplicationForceDeleteByCollectionRequest{
		Collection: collection,
	}

	subCommand, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &api.ApplyRequest{
		Type:       api.ApplyRequest_TYPE_REPLICATION_REPLICATE_FORCE_DELETE_BY_COLLECTION,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(ctx, command); err != nil {
		return err
	}
	return nil
}

func (s *Raft) ForceDeleteReplicationsByCollectionAndShard(ctx context.Context, collection, shard string) error {
	req := &api.ReplicationForceDeleteByCollectionAndShardRequest{
		Collection: collection,
		Shard:      shard,
	}

	subCommand, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &api.ApplyRequest{
		Type:       api.ApplyRequest_TYPE_REPLICATION_REPLICATE_FORCE_DELETE_BY_COLLECTION_AND_SHARD,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(ctx, command); err != nil {
		return err
	}
	return nil
}

func (s *Raft) ForceDeleteReplicationsByTargetNode(ctx context.Context, node string) error {
	req := &api.ReplicationForceDeleteByTargetNodeRequest{
		Node: node,
	}

	subCommand, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &api.ApplyRequest{
		Type:       api.ApplyRequest_TYPE_REPLICATION_REPLICATE_FORCE_DELETE_BY_TARGET_NODE,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(ctx, command); err != nil {
		return err
	}
	return nil
}

func (s *Raft) ForceDeleteReplicationByUuid(ctx context.Context, uuid strfmt.UUID) error {
	req := &api.ReplicationForceDeleteByUuidRequest{
		Uuid: uuid,
	}

	subCommand, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &api.ApplyRequest{
		Type:       api.ApplyRequest_TYPE_REPLICATION_REPLICATE_FORCE_DELETE_BY_UUID,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(ctx, command); err != nil {
		return err
	}
	return nil
}

func (s *Raft) DeleteAllReplications(ctx context.Context) error {
	req := &api.ReplicationDeleteAllRequest{
		Version: api.ReplicationCommandVersionV0,
	}

	subCommand, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &api.ApplyRequest{
		Type:       api.ApplyRequest_TYPE_REPLICATION_REPLICATE_DELETE_ALL,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(ctx, command); err != nil {
		return err
	}
	return nil
}

func (s *Raft) DeleteReplicationsByCollection(ctx context.Context, collection string) error {
	req := &api.ReplicationsDeleteByCollectionRequest{
		Version:    api.ReplicationCommandVersionV0,
		Collection: collection,
	}

	subCommand, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &api.ApplyRequest{
		Type:       api.ApplyRequest_TYPE_REPLICATION_REPLICATE_DELETE_BY_COLLECTION,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(ctx, command); err != nil {
		return err
	}
	return nil
}

func (s *Raft) DeleteReplicationsByTenants(ctx context.Context, collection string, tenants []string) error {
	req := &api.ReplicationsDeleteByTenantsRequest{
		Version:    api.ReplicationCommandVersionV0,
		Collection: collection,
		Tenants:    tenants,
	}

	subCommand, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &api.ApplyRequest{
		Type:       api.ApplyRequest_TYPE_REPLICATION_REPLICATE_DELETE_BY_TENANTS,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(ctx, command); err != nil {
		return err
	}
	return nil
}

func (s *Raft) ReplicationStoreSchemaVersion(ctx context.Context, id uint64, schemaVersion uint64) error {
	req := &api.ReplicationStoreSchemaVersionRequest{
		Version:       api.ReplicationCommandVersionV0,
		SchemaVersion: schemaVersion,
		Id:            id,
	}

	subCommand, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &api.ApplyRequest{
		Type:       api.ApplyRequest_TYPE_REPLICATION_REGISTER_SCHEMA_VERSION,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(ctx, command); err != nil {
		return err
	}
	return nil
}
