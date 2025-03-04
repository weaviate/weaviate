package replication

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/sirupsen/logrus"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/schema"
)

var ErrBadRequest = errors.New("bad request")

type Manager struct {
	replicationFSM    *ShardReplicationFSM
	replicationEngine *shardReplicationEngine
	schemaReader      schema.SchemaReader
}

func NewManager(logger *logrus.Logger, schemaReader schema.SchemaReader) *Manager {
	replicationFSM := newShardReplicationFSM()
	return &Manager{
		replicationFSM:    replicationFSM,
		replicationEngine: newShardReplicationEngine(logger, replicationFSM),
		schemaReader:      schemaReader,
	}
}

func (m *Manager) GetReplicationFSM() *ShardReplicationFSM {
	return m.replicationFSM
}

func (m *Manager) StartReplicationEngine() {
	return
	m.replicationEngine.Start()
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
