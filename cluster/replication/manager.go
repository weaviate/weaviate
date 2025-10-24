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
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/usecases/cluster"
)

var ErrBadRequest = errors.New("bad request")

type Manager struct {
	replicationFSM *ShardReplicationFSM
	schemaReader   schema.SchemaReader
	nodeSelector   cluster.NodeSelector
}

func NewManager(schemaReader schema.SchemaReader, nodeSelector cluster.NodeSelector, reg prometheus.Registerer) *Manager {
	replicationFSM := NewShardReplicationFSM(reg)
	return &Manager{
		replicationFSM: replicationFSM,
		schemaReader:   schemaReader,
		nodeSelector:   nodeSelector,
	}
}

func (m *Manager) GetReplicationFSM() *ShardReplicationFSM {
	return m.replicationFSM
}

func (m *Manager) Snapshot() ([]byte, error) {
	return m.replicationFSM.Snapshot()
}

func (m *Manager) Restore(bytes []byte) error {
	return m.replicationFSM.Restore(bytes)
}

func (m *Manager) Replicate(logId uint64, c *cmd.ApplyRequest) error {
	req := &cmd.ReplicationReplicateShardRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	// Validate that the command is valid and can be applied with the current schema
	if err := ValidateReplicationReplicateShard(m.schemaReader, req); err != nil {
		return err
	}

	// Store the shard replication op in the FSM
	return m.replicationFSM.Replicate(logId, req)
}

func (m *Manager) RegisterError(c *cmd.ApplyRequest) error {
	req := &cmd.ReplicationRegisterErrorRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	// Store an op's error emitted by the consumer in the FSM
	if err := m.replicationFSM.RegisterError(req); err != nil {
		if errors.Is(err, ErrMaxErrorsReached) {
			uuid, err := m.GetReplicationOpUUIDFromId(req.Id)
			if err != nil {
				return fmt.Errorf("failed to get op uuid from id %d: %w", req.Id, err)
			}
			return m.replicationFSM.CancelReplication(&cmd.ReplicationCancelRequest{
				Uuid: uuid,
			})
		}
		return err
	}
	return nil
}

func (m *Manager) GetReplicationOpUUIDFromId(id uint64) (strfmt.UUID, error) {
	return m.replicationFSM.GetReplicationOpUUIDFromId(id)
}

func (m *Manager) UpdateReplicateOpState(c *cmd.ApplyRequest) error {
	req := &cmd.ReplicationUpdateOpStateRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	// Store the updated shard replication op in the FSM
	return m.replicationFSM.UpdateReplicationOpStatus(req)
}

func (m *Manager) StoreSchemaVersion(c *cmd.ApplyRequest) error {
	req := &cmd.ReplicationStoreSchemaVersionRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return m.replicationFSM.StoreSchemaVersion(req)
}

func (m *Manager) GetReplicationDetailsByReplicationId(c *cmd.QueryRequest) ([]byte, error) {
	subCommand := cmd.ReplicationDetailsRequest{}
	if err := json.Unmarshal(c.SubCommand, &subCommand); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	op, ok := m.replicationFSM.GetOpByUuid(subCommand.Uuid)
	if !ok {
		return nil, fmt.Errorf("%w: %s", types.ErrReplicationOperationNotFound, subCommand.Uuid)
	}

	response := makeReplicationDetailsResponse(&op.Op, &op.Status)
	payload, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("could not marshal query response for replication operation '%s': %w", op.Op.UUID, err)
	}

	return payload, nil
}

func (m *Manager) GetReplicationOperationState(c *cmd.QueryRequest) ([]byte, error) {
	subCommand := cmd.ReplicationOperationStateRequest{}
	if err := json.Unmarshal(c.SubCommand, &subCommand); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	op, ok := m.replicationFSM.GetOpById(subCommand.Id)
	if !ok {
		return nil, fmt.Errorf("unable to retrieve replication operation '%d' status", subCommand.Id)
	}

	response := cmd.ReplicationOperationStateResponse{
		State: op.Status.GetCurrent().State,
	}
	payload, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("could not marshal query response for replication operation '%d': %w", subCommand.Id, err)
	}

	return payload, nil
}

func (m *Manager) GetReplicationDetailsByCollection(c *cmd.QueryRequest) ([]byte, error) {
	subCommand := cmd.ReplicationDetailsRequestByCollection{}
	if err := json.Unmarshal(c.SubCommand, &subCommand); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	responses := []cmd.ReplicationDetailsResponse{}
	ops, ok := m.replicationFSM.GetOpsForCollection(subCommand.Collection)
	if !ok {
		return nil, fmt.Errorf("%w: %s", types.ErrReplicationOperationNotFound, subCommand.Collection)
	}

	for _, op := range ops {
		responses = append(responses, makeReplicationDetailsResponse(&op.Op, &op.Status))
	}

	payload, err := json.Marshal(responses)
	if err != nil {
		return nil, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}

func (m *Manager) GetReplicationDetailsByCollectionAndShard(c *cmd.QueryRequest) ([]byte, error) {
	subCommand := cmd.ReplicationDetailsRequestByCollectionAndShard{}
	if err := json.Unmarshal(c.SubCommand, &subCommand); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	responses := []cmd.ReplicationDetailsResponse{}
	ops, ok := m.replicationFSM.GetOpsForCollectionAndShard(subCommand.Collection, subCommand.Shard)
	if !ok {
		return nil, fmt.Errorf("%w: %s", types.ErrReplicationOperationNotFound, subCommand.Collection)
	}

	for _, op := range ops {
		responses = append(responses, makeReplicationDetailsResponse(&op.Op, &op.Status))
	}

	payload, err := json.Marshal(responses)
	if err != nil {
		return nil, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}

func (m *Manager) GetReplicationDetailsByTargetNode(c *cmd.QueryRequest) ([]byte, error) {
	subCommand := cmd.ReplicationDetailsRequestByTargetNode{}
	if err := json.Unmarshal(c.SubCommand, &subCommand); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	responses := []cmd.ReplicationDetailsResponse{}
	ops, ok := m.replicationFSM.GetOpsForTargetNode(subCommand.Node)
	if !ok {
		return nil, fmt.Errorf("%w: %s", types.ErrReplicationOperationNotFound, subCommand.Node)
	}

	for _, op := range ops {
		responses = append(responses, makeReplicationDetailsResponse(&op.Op, &op.Status))
	}

	payload, err := json.Marshal(responses)
	if err != nil {
		return nil, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}

func (m *Manager) GetAllReplicationDetails(c *cmd.QueryRequest) ([]byte, error) {
	statusByOps := m.replicationFSM.GetStatusByOps()
	responses := make([]cmd.ReplicationDetailsResponse, 0, len(statusByOps))
	for op, status := range statusByOps {
		responses = append(responses, makeReplicationDetailsResponse(&op, &status))
	}

	payload, err := json.Marshal(responses)
	if err != nil {
		return nil, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}

func (m *Manager) QueryShardingStateByCollection(c *cmd.QueryRequest) ([]byte, error) {
	subCommand := cmd.ReplicationQueryShardingStateByCollectionRequest{}
	if err := json.Unmarshal(c.SubCommand, &subCommand); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	shardingState := m.schemaReader.CopyShardingState(subCommand.Collection)
	if shardingState == nil {
		return nil, fmt.Errorf("%w: %s", types.ErrNotFound, subCommand.Collection)
	}

	shards := make(map[string][]string)
	for _, shard := range shardingState.Physical {
		shards[shard.Name] = shard.BelongsToNodes
	}

	response := cmd.ShardingState{
		Collection: subCommand.Collection,
		Shards:     shards,
	}

	payload, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}

func (m *Manager) QueryShardingStateByCollectionAndShard(c *cmd.QueryRequest) ([]byte, error) {
	subCommand := cmd.ReplicationQueryShardingStateByCollectionAndShardRequest{}
	if err := json.Unmarshal(c.SubCommand, &subCommand); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	shardingState := m.schemaReader.CopyShardingState(subCommand.Collection)
	if shardingState == nil {
		return nil, fmt.Errorf("%w: %s", types.ErrNotFound, subCommand.Collection)
	}

	shards := make(map[string][]string)
	for _, shard := range shardingState.Physical {
		if shard.Name == subCommand.Shard {
			shards[shard.Name] = shard.BelongsToNodes
		}
	}

	if len(shards) == 0 {
		return nil, fmt.Errorf("%w: %s", types.ErrNotFound, subCommand.Shard)
	}

	response := cmd.ShardingState{
		Collection: subCommand.Collection,
		Shards:     shards,
	}

	payload, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}

func (m *Manager) QueryReplicationScalePlan(c *cmd.QueryRequest) ([]byte, error) {
	subCommand := cmd.ReplicationScalePlanRequest{}
	if err := json.Unmarshal(c.SubCommand, &subCommand); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	shardingState := m.schemaReader.CopyShardingState(subCommand.Collection)
	if shardingState == nil {
		return nil, fmt.Errorf("%w: %s", ErrClassNotFound, subCommand.Collection)
	}

	currentShards := make(map[string][]string, len(shardingState.Physical))
	desiredShards := make(map[string][]string, len(shardingState.Physical))

	for name, shard := range shardingState.Physical {
		currentShards[name] = make([]string, len(shard.BelongsToNodes))
		copy(currentShards[name], shard.BelongsToNodes)

		if err := shard.AdjustReplicas(int(subCommand.ReplicationFactor), m.nodeSelector); err != nil {
			return nil, err
		}

		desiredShards[name] = make([]string, len(shard.BelongsToNodes))
		copy(desiredShards[name], shard.BelongsToNodes)
	}

	scalePlan := cmd.ReplicationScalePlan{
		PlanID:                       strfmt.UUID(uuid.New().String()),
		Collection:                   subCommand.Collection,
		ShardReplicationScaleActions: make(map[string]cmd.ShardReplicationScaleActions),
	}

	diff := diffNodesPerShard(currentShards, desiredShards)

	for shardName, nodes := range diff {
		if len(nodes.Added) == 0 && len(nodes.Removed) == 0 {
			// No changes for this shard
			continue
		}

		if len(nodes.Added) > 0 && len(nodes.Remaining) == 0 {
			return nil, fmt.Errorf("cannot determine source node for shard %q: no remaining nodes", shardName)
		}

		scalePlan.ShardReplicationScaleActions[shardName] = cmd.ShardReplicationScaleActions{
			RemoveNodes: make(map[string]struct{}, len(nodes.Removed)),
			AddNodes:    make(map[string]string, len(nodes.Added)),
		}

		for node := range nodes.Removed {
			if _, ok := nodes.Remaining[node]; ok {
				return nil, fmt.Errorf("unexpected removed node %q is also in remaining for shard %q", node, shardName)
			}
			if _, ok := nodes.Added[node]; ok {
				return nil, fmt.Errorf("unexpected removed node %q is also in added for shard %q", node, shardName)
			}

			scalePlan.ShardReplicationScaleActions[shardName].RemoveNodes[node] = struct{}{}
		}

		for node := range nodes.Added {
			if _, ok := nodes.Remaining[node]; ok {
				return nil, fmt.Errorf("unexpected added node %q is also in remaining for shard %q", node, shardName)
			}
			if _, ok := nodes.Removed[node]; ok {
				return nil, fmt.Errorf("unexpected added node %q is also in removed for shard %q", node, shardName)
			}

			// pick a random source node from the remaining nodes
			var sourceNode string
			i := rand.Intn(len(nodes.Remaining))
			j := 0
			for n := range nodes.Remaining {
				if j == i {
					sourceNode = n
					break
				}
				j++
			}

			scalePlan.ShardReplicationScaleActions[shardName].AddNodes[node] = sourceNode
		}
	}

	response := cmd.ReplicationScalePlanResponse{
		ReplicationScalePlan: scalePlan,
	}

	payload, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}

func diffNodesPerShard(before, after map[string][]string) map[string]struct {
	Remaining map[string]struct{}
	Removed   map[string]struct{}
	Added     map[string]struct{}
} {
	result := make(map[string]struct {
		Remaining map[string]struct{}
		Removed   map[string]struct{}
		Added     map[string]struct{}
	}, len(before))

	for shardName, beforeShard := range before {
		beforeSet := make(map[string]struct{}, len(beforeShard))
		for _, n := range beforeShard {
			beforeSet[n] = struct{}{}
		}

		afterShard, ok := after[shardName]
		if !ok {
			// Shard missing entirely in the new state — all previous nodes are removed
			result[shardName] = struct {
				Remaining map[string]struct{}
				Removed   map[string]struct{}
				Added     map[string]struct{}
			}{
				Remaining: make(map[string]struct{}),
				Removed:   beforeSet,
				Added:     make(map[string]struct{}),
			}
			continue
		}

		afterSet := make(map[string]struct{}, len(afterShard))
		for _, n := range afterShard {
			afterSet[n] = struct{}{}
		}

		remaining := make(map[string]struct{})
		removed := make(map[string]struct{})
		added := make(map[string]struct{})

		for _, n := range beforeShard {
			if _, ok := afterSet[n]; ok {
				remaining[n] = struct{}{}
			} else {
				removed[n] = struct{}{}
			}
		}
		for _, n := range afterShard {
			if _, ok := beforeSet[n]; !ok {
				added[n] = struct{}{}
			}
		}

		result[shardName] = struct {
			Remaining map[string]struct{}
			Removed   map[string]struct{}
			Added     map[string]struct{}
		}{
			Remaining: remaining,
			Removed:   removed,
			Added:     added,
		}
	}

	return result
}

func makeReplicationDetailsResponse(op *ShardReplicationOp, status *ShardReplicationOpStatus) cmd.ReplicationDetailsResponse {
	return cmd.ReplicationDetailsResponse{
		Uuid:               op.UUID,
		Id:                 op.ID,
		ShardId:            op.SourceShard.ShardId,
		Collection:         op.SourceShard.CollectionId,
		SourceNodeId:       op.SourceShard.NodeId,
		TargetNodeId:       op.TargetShard.NodeId,
		TransferType:       op.TransferType.String(),
		Uncancelable:       status.UnCancellable,
		ScheduledForCancel: status.ShouldCancel,
		ScheduledForDelete: status.ShouldDelete,
		Status:             status.GetCurrent().ToAPIFormat(),
		StatusHistory:      status.GetHistory().ToAPIFormat(),
	}
}

func (m *Manager) CancelReplication(c *cmd.ApplyRequest) error {
	req := &cmd.ReplicationCancelRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	// Trigger cancellation of the replication operation in the FSM
	return m.replicationFSM.CancelReplication(req)
}

func (m *Manager) DeleteReplication(c *cmd.ApplyRequest) error {
	req := &cmd.ReplicationDeleteRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	// Trigger deletion of the replication operation in the FSM
	return m.replicationFSM.DeleteReplication(req)
}

func (m *Manager) DeleteAllReplications(c *cmd.ApplyRequest) error {
	req := &cmd.ReplicationDeleteAllRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	// Trigger deletion of all replication operation in the FSM
	return m.replicationFSM.DeleteAllReplications(req)
}

func (m *Manager) RemoveReplicaOp(c *cmd.ApplyRequest) error {
	req := &cmd.ReplicationRemoveOpRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	// Remove the replication operation itself from the FSM
	return m.replicationFSM.RemoveReplicationOp(req)
}

func (m *Manager) ReplicationCancellationComplete(c *cmd.ApplyRequest) error {
	req := &cmd.ReplicationCancellationCompleteRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	// Mark the replication operation as cancelled in the FSM
	return m.replicationFSM.CancellationComplete(req)
}

func (m *Manager) DeleteReplicationsByCollection(c *cmd.ApplyRequest) error {
	req := &cmd.ReplicationsDeleteByCollectionRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	// Trigger deletion of all replication operations for the specified class in the FSM
	return m.replicationFSM.DeleteReplicationsByCollection(req.Collection)
}

func (m *Manager) DeleteReplicationsByTenants(c *cmd.ApplyRequest) error {
	req := &cmd.ReplicationsDeleteByTenantsRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	// Trigger deletion of all replication operations for the specified class in the FSM
	return m.replicationFSM.DeleteReplicationsByTenants(req.Collection, req.Tenants)
}

func (m *Manager) ForceDeleteAll(c *cmd.ApplyRequest) error {
	req := &cmd.ReplicationForceDeleteAllRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return m.replicationFSM.ForceDeleteAll()
}

func (m *Manager) ForceDeleteByCollection(c *cmd.ApplyRequest) error {
	req := &cmd.ReplicationForceDeleteByCollectionRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return m.replicationFSM.ForceDeleteByCollection(req.Collection)
}

func (m *Manager) ForceDeleteByCollectionAndShard(c *cmd.ApplyRequest) error {
	req := &cmd.ReplicationForceDeleteByCollectionAndShardRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return m.replicationFSM.ForceDeleteByCollectionAndShard(req.Collection, req.Shard)
}

func (m *Manager) ForceDeleteByTargetNode(c *cmd.ApplyRequest) error {
	req := &cmd.ReplicationForceDeleteByTargetNodeRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return m.replicationFSM.ForceDeleteByTargetNode(req.Node)
}

func (m *Manager) ForceDeleteByUuid(c *cmd.ApplyRequest) error {
	req := &cmd.ReplicationForceDeleteByUuidRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return m.replicationFSM.ForceDeleteByUuid(req.Uuid)
}
