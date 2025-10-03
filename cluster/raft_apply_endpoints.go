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
	"errors"
	"fmt"
	"math/rand"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func (s *Raft) AddClass(ctx context.Context, cls *models.Class, ss *sharding.State) (uint64, error) {
	if cls == nil || cls.Class == "" {
		return 0, fmt.Errorf("nil class or empty class name: %w", schema.ErrBadRequest)
	}

	req := cmd.AddClassRequest{Class: cls, State: ss}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_ADD_CLASS,
		Class:      cls.Class,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

func (s *Raft) UpdateClass(ctx context.Context, cls *models.Class, ss *sharding.State) (uint64, error) {
	if cls == nil || cls.Class == "" {
		return 0, fmt.Errorf("nil class or empty class name: %w", schema.ErrBadRequest)
	}

	if ss != nil {
		err := s.ForceDeleteReplicationsByCollection(ctx, cls.Class)
		if err != nil {
			return 0, fmt.Errorf("force delete replications for %q: %w", cls.Class, err)
		}

		ssBefore, _, err := s.QueryShardingState(cls.Class)
		if err != nil {
			return 0, fmt.Errorf("query sharding state for %q: %w", cls.Class, err)
		}

		diff := diffNodesPerShard(ssBefore, ss)

		for shardName, nodes := range diff {
			if len(nodes.Added) == 0 && len(nodes.Removed) == 0 {
				continue
			}

			if len(nodes.Added) > 0 && len(nodes.Remaining) == 0 {
				return 0, fmt.Errorf("cannot determine source node for shard %q: no remaining nodes", shardName)
			}

			for _, node := range nodes.Added {
				id, err := uuid.NewRandom()
				if err != nil {
					return 0, fmt.Errorf("create uuid for new replica: %w", err)
				}
				uuid := strfmt.UUID(id.String())

				sourceNode := nodes.Remaining[rand.Intn(len(nodes.Remaining))]

				err = s.ReplicationReplicateReplica(ctx, uuid, sourceNode, cls.Class, shardName, node, cmd.COPY.String())
				if err != nil {
					return 0, fmt.Errorf("replication add replica to shard for %q: %w", cls.Class, err)
				}
			}

			for _, node := range nodes.Removed {
				if _, err := s.DeleteReplicaFromShard(ctx, cls.Class, shardName, node); err != nil {
					return 0, fmt.Errorf("delete replica %s from shard %s: %w", node, shardName, err)
				}
			}
		}
	}

	req := cmd.UpdateClassRequest{Class: cls}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_UPDATE_CLASS,
		Class:      cls.Class,
		SubCommand: subCommand,
	}

	return s.Execute(ctx, command)
}

func diffNodesPerShard(before, after *sharding.State) map[string]struct {
	Remaining []string
	Removed   []string
	Added     []string
} {
	result := make(map[string]struct {
		Remaining []string
		Removed   []string
		Added     []string
	}, len(before.Physical))

	for shardName, beforeShard := range before.Physical {
		afterShard, ok := after.Physical[shardName]
		if !ok {
			// Shard missing entirely in the new state — all previous nodes are removed
			result[shardName] = struct {
				Remaining []string
				Removed   []string
				Added     []string
			}{
				Remaining: nil,
				Removed:   beforeShard.BelongsToNodes,
				Added:     nil,
			}
			continue
		}

		beforeSet := make(map[string]struct{}, len(beforeShard.BelongsToNodes))
		for _, n := range beforeShard.BelongsToNodes {
			beforeSet[n] = struct{}{}
		}

		afterSet := make(map[string]struct{}, len(afterShard.BelongsToNodes))
		for _, n := range afterShard.BelongsToNodes {
			afterSet[n] = struct{}{}
		}

		var remaining, removed, added []string
		for _, n := range beforeShard.BelongsToNodes {
			if _, ok := afterSet[n]; ok {
				remaining = append(remaining, n)
			} else {
				removed = append(removed, n)
			}
		}
		for _, n := range afterShard.BelongsToNodes {
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

func (s *Raft) DeleteClass(ctx context.Context, name string) (uint64, error) {
	command := &cmd.ApplyRequest{
		Type:  cmd.ApplyRequest_TYPE_DELETE_CLASS,
		Class: name,
	}
	return s.Execute(ctx, command)
}

func (s *Raft) RestoreClass(ctx context.Context, cls *models.Class, ss *sharding.State) (uint64, error) {
	if cls == nil || cls.Class == "" {
		return 0, fmt.Errorf("nil class or empty class name: %w", schema.ErrBadRequest)
	}
	req := cmd.AddClassRequest{Class: cls, State: ss}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_RESTORE_CLASS,
		Class:      cls.Class,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

func (s *Raft) AddProperty(ctx context.Context, class string, props ...*models.Property) (uint64, error) {
	for _, p := range props {
		if p == nil || p.Name == "" || class == "" {
			return 0, fmt.Errorf("empty property or empty class name: %w", schema.ErrBadRequest)
		}
	}
	req := cmd.AddPropertyRequest{Properties: props}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_ADD_PROPERTY,
		Class:      class,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

func (s *Raft) AddReplicaToShard(ctx context.Context, class, shard, targetNode string) (uint64, error) {
	if class == "" || shard == "" || targetNode == "" {
		return 0, fmt.Errorf("empty class or shard or sourceNode or targetNode: %w", schema.ErrBadRequest)
	}
	req := cmd.AddReplicaToShard{
		Class:      class,
		Shard:      shard,
		TargetNode: targetNode,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_ADD_REPLICA_TO_SHARD,
		Class:      req.Class,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

func (s *Raft) DeleteReplicaFromShard(ctx context.Context, class, shard, targetNode string) (uint64, error) {
	if class == "" || shard == "" || targetNode == "" {
		return 0, fmt.Errorf("empty class or shard or sourceNode or targetNode: %w", schema.ErrBadRequest)
	}
	req := cmd.DeleteReplicaFromShard{
		Class:      class,
		Shard:      shard,
		TargetNode: targetNode,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_DELETE_REPLICA_FROM_SHARD,
		Class:      req.Class,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

func (s *Raft) ReplicationAddReplicaToShard(ctx context.Context, class, shard, targetNode string, opId uint64) (uint64, error) {
	if class == "" || shard == "" || targetNode == "" {
		return 0, fmt.Errorf("empty class or shard or sourceNode or targetNode: %w", schema.ErrBadRequest)
	}
	req := cmd.ReplicationAddReplicaToShard{
		Class:      class,
		Shard:      shard,
		TargetNode: targetNode,
		OpId:       opId,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_REPLICATION_REPLICATE_ADD_REPLICA_TO_SHARD,
		Class:      req.Class,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

func (s *Raft) SyncShard(ctx context.Context, collection, shard, nodeId string) (uint64, error) {
	if collection == "" || shard == "" || nodeId == "" {
		return 0, fmt.Errorf("empty class or shard or sourceNode or targetNode: %w", schema.ErrBadRequest)
	}
	req := cmd.SyncShardRequest{
		Collection: collection,
		Shard:      shard,
		NodeId:     nodeId,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_REPLICATION_REPLICATE_SYNC_SHARD,
		Class:      req.Collection,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

func (s *Raft) UpdateShardStatus(ctx context.Context, class, shard, status string) (uint64, error) {
	if class == "" || shard == "" {
		return 0, fmt.Errorf("empty class or shard: %w", schema.ErrBadRequest)
	}
	req := cmd.UpdateShardStatusRequest{Class: class, Shard: shard, Status: status}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_UPDATE_SHARD_STATUS,
		Class:      req.Class,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

func (s *Raft) AddTenants(ctx context.Context, class string, req *cmd.AddTenantsRequest) (uint64, error) {
	if class == "" || req == nil {
		return 0, fmt.Errorf("empty class name or nil request: %w", schema.ErrBadRequest)
	}
	subCommand, err := proto.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_ADD_TENANT,
		Class:      class,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

func (s *Raft) UpdateTenants(ctx context.Context, class string, req *cmd.UpdateTenantsRequest) (uint64, error) {
	if class == "" || req == nil {
		return 0, fmt.Errorf("empty class name or nil request: %w", schema.ErrBadRequest)
	}
	subCommand, err := proto.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_UPDATE_TENANT,
		Class:      class,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

func (s *Raft) DeleteTenants(ctx context.Context, class string, req *cmd.DeleteTenantsRequest) (uint64, error) {
	if class == "" || req == nil {
		return 0, fmt.Errorf("empty class name or nil request: %w", schema.ErrBadRequest)
	}
	subCommand, err := proto.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_DELETE_TENANT,
		Class:      class,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

func (s *Raft) UpdateTenantsProcess(ctx context.Context, class string, req *cmd.TenantProcessRequest) (uint64, error) {
	if class == "" || req == nil {
		return 0, fmt.Errorf("empty class name or nil request: %w", schema.ErrBadRequest)
	}
	subCommand, err := proto.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_TENANT_PROCESS,
		Class:      class,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

func (s *Raft) StoreSchemaV1() error {
	command := &cmd.ApplyRequest{
		Type: cmd.ApplyRequest_TYPE_STORE_SCHEMA_V1,
	}
	_, err := s.Execute(context.Background(), command)
	return err
}

func (s *Raft) Execute(ctx context.Context, req *cmd.ApplyRequest) (uint64, error) {
	t := prometheus.NewTimer(
		monitoring.GetMetrics().SchemaWrites.WithLabelValues(
			req.Type.String(),
		))
	defer t.ObserveDuration()

	var schemaVersion uint64
	err := backoff.Retry(func() error {
		var err error

		// Validate the apply first
		if _, ok := cmd.ApplyRequest_Type_name[int32(req.Type.Number())]; !ok {
			err = types.ErrUnknownCommand
			// This is an invalid apply command, don't retry
			return backoff.Permanent(err)
		}

		// We are the leader, let's apply
		if s.store.IsLeader() {
			schemaVersion, err = s.store.Execute(req)
			// We might fail due to leader not found as we are losing or transferring leadership, retry
			if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
				return err
			}
			return backoff.Permanent(err)
		}

		leader := s.store.Leader()
		if leader == "" {
			err = s.leaderErr()
			s.log.Warnf("apply: could not find leader: %s", err)
			return err
		}

		var resp *cmd.ApplyResponse
		resp, err = s.cl.Apply(ctx, leader, req)
		if err != nil {
			// Don't retry if the actual apply to the leader failed, we have retry at the network layer already
			return backoff.Permanent(err)
		}
		schemaVersion = resp.Version
		return nil
		// pass in the election timeout after applying multiplier
	}, backoffConfig(ctx, s.store.raftConfig().ElectionTimeout))

	return schemaVersion, err
}
