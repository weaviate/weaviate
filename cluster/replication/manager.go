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

	"github.com/prometheus/client_golang/prometheus"

	"github.com/sirupsen/logrus"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/schema"
)

var ErrBadRequest = errors.New("bad request")

type Manager struct {
	replicationFSM *ShardReplicationFSM
	schemaReader   schema.SchemaReader
}

func NewManager(logger *logrus.Logger, schemaReader schema.SchemaReader, replicaCopier types.ReplicaCopier, reg prometheus.Registerer) *Manager {
	replicationFSM := newShardReplicationFSM(reg)
	return &Manager{
		replicationFSM: replicationFSM,
		schemaReader:   schemaReader,
	}
}

func (m *Manager) GetReplicationFSM() *ShardReplicationFSM {
	return m.replicationFSM
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
	// Store in the FSM the shard replication op
	return m.replicationFSM.Replicate(logId, req)
}

func (m *Manager) UpdateReplicateOpState(c *cmd.ApplyRequest) error {
	req := &cmd.ReplicationUpdateOpStateRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	// Store in the FSM the shard replication op
	return m.replicationFSM.UpdateReplicationOpStatus(req)
}

func (m *Manager) GetReplicationDetailsByReplicationId(c *cmd.QueryRequest) ([]byte, error) {
	subCommand := cmd.ReplicationDetailsRequest{}
	if err := json.Unmarshal(c.SubCommand, &subCommand); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	op, ok := m.replicationFSM.opsById[subCommand.Id]
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrReplicationOperationNotFound, subCommand.Id)
	}

	status, ok := m.replicationFSM.opsStatus[op]
	if !ok {
		return nil, fmt.Errorf("unable to retrieve replication operation '%d' status", op.id)
	}

	response := cmd.ReplicationDetailsResponse{
		Id:           op.id,
		ShardId:      op.sourceShard.shardId,
		Collection:   op.sourceShard.collectionId,
		SourceNodeId: op.sourceShard.nodeId,
		TargetNodeId: op.targetShard.nodeId,
		Status:       status.state.String(),
	}

	payload, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("could not marshal query response for replication operation '%d': %w", op.id, err)
	}

	return payload, nil
}
