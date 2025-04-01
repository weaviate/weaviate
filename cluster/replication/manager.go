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

func NewManager(logger *logrus.Logger, schemaReader schema.SchemaReader, replicaCopier types.ReplicaCopier) *Manager {
	replicationFSM := newShardReplicationFSM()
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
