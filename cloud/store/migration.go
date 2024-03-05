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

package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	schemaTypes "github.com/weaviate/weaviate/adapters/repos/schema/types"
	cmd "github.com/weaviate/weaviate/cloud/proto/cluster"
)

// MigrateToRaft will perform the migration from schemaRepo to the RAFT based representation stored in st.
// If the current node is not the leader, the function will return early without error.
// If the current node is the leader, it will read the schema from schemaRepo and import it in the RAFT based
// representation using st.Execute to ensure RAFT has log entries for the schema.
// It returns an error if any happens during the migration process.
func (st *Store) MigrateToRaft(schemaRepo schemaTypes.SchemaRepo) error {
	log := st.log.With("action", "raft_migration")

	// Fist let's ensure either a raft node is a leader, or this node is the leader.
	ticker := time.NewTicker(500 * time.Millisecond)
	// Using a for loop with ticker.C instead of select allows us to have an immediate "tick" at 0 instead of waiting
	// the delay for the first tick
	for ; true; <-ticker.C {
		// Raft is shutting down, no leader found so far let's exit
		if !st.open.Load() {
			return nil
		}

		if st.IsLeader() || st.Leader() != "" {
			log.Info("leader found", "leader", st.Leader())
			break
		}
	}

	// If we are not leader we abort the process, this ensure only one node will try to migrate from non raft to raft
	// representation at the same time
	if !st.IsLeader() {
		log.Info("Node is not raft leader, skipping migration")
		return nil
	}

	// Get the schema from the non-raft representation and load it from disk.
	// If no schema is present then exit early as there is nothing to migration from
	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()
	stateGetter, err := schemaRepo.Load(ctx)
	if err != nil {
		return fmt.Errorf("could not load non-raft schema to memory for migration: %w", err)
	}
	shardingState := stateGetter.GetShardingState()
	schema := stateGetter.GetSchema()

	// Migrate the class from schemaRepo to RAFT
	marshalledSubCmd, err := json.Marshal(&cmd.SetSchemaRequest{
		Classes:       schema.Classes,
		ShardingState: shardingState,
	})
	if err != nil {
		return fmt.Errorf("could not marshall add class request to bytes: %w", err)
	}
	err = st.Execute(&cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_SET_SCHEMA,
		SubCommand: marshalledSubCmd,
		SchemaOnly: true,
	})
	if err != nil {
		return fmt.Errorf("could not migrate to raft: %w", err)
	}

	return nil
}
