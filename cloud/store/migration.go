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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	schemaTypes "github.com/weaviate/weaviate/adapters/repos/schema/types"
	cmd "github.com/weaviate/weaviate/cloud/proto/cluster"
)

const (
	raftMigrationFileName        = "raft_migration_progress.json"
	raftMigrationFilePermissions = 0o644
)

type migrationProgress struct {
	MigratedClasses map[string]interface{} `json:"migrated_classes"`
}

func NewMigrationProgress() *migrationProgress {
	return &migrationProgress{MigratedClasses: make(map[string]interface{})}
}

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
	if schema == nil || len(schema.Classes) <= 0 {
		log.Info("no schema available or schema is empty in non-raft representation, nothing to migrate")
		return nil
	}

	// Load current migration progress from the disk.
	// If no progress was made so far, ensure the migrationProgress struct is initialized empty
	migrationProgressFile := filepath.Join(st.raftDir, raftMigrationFileName)
	migrationProgress := NewMigrationProgress()
	migrationProgressFromDisk, err := os.ReadFile(migrationProgressFile)
	recoveringMigration := false
	// If an error happens here we don't want to abort the migration to have a smoother experience. Instead wipe the
	// file and consider to migration took place.
	if err != nil && !os.IsNotExist(err) {
		log.Warn(fmt.Sprintf("could not read raft migration progress file: %s", err))
	} else {
		recoveringMigration = true
		err = json.Unmarshal(migrationProgressFromDisk, migrationProgress)
		if err != nil {
			log.Warn("could not unmarshall raft migration progress file contents, migration will proceed and not consider any progress made")
			// Ensure we reset the progress struct to a pristine state
			migrationProgress = NewMigrationProgress()
		}
	}

	// Migrate only if there is no data in the current RAFT schema and we are not recovering a migration
	if st.db.Schema.Len() != 0 && !recoveringMigration {
		log.Info("current RAFT based schema has data and no migration has been recovered, skipping migration")
		return nil
	}

	log.Info(fmt.Sprintf("Migrating %d classes from non-raft to raft based representation", len(schema.Classes)))
	// Before starting the migration, write the progress file at least once to ensure that we will still recover the
	// migration if we crash between the first import into RAFT and the write of that update to disk.
	err = os.WriteFile(migrationProgressFile, []byte{}, raftMigrationFilePermissions)
	if err != nil {
		return fmt.Errorf("could not write migration progress to %s: %w", migrationProgressFile, err)
	}
	// Apply the change from non-raft to raft based schema, applying change with "schema only" to true to avoid
	// re-updating the underlying representation
	for _, class := range schema.Classes {
		if class == nil {
			log.Warn("found nil class in non-raft based schema, continuing migration")
			continue
		}

		// Skip class if it has been already migrated
		if _, ok := migrationProgress.MigratedClasses[class.Class]; ok {
			log.Info(fmt.Sprintf("skipping migrating class %s as it is marked as already migrated", class.Class))
			continue
		}

		log.Debug(fmt.Sprintf("migrating class %s to raft based schema", class.Class))

		// Migrate the class from schemaRepo to RAFT
		marshalledSubCmd, err := json.Marshal(&cmd.AddClassRequest{
			Class: class,
			State: shardingState[class.Class],
		})
		if err != nil {
			return fmt.Errorf("could not marshall add class request to bytes: %w", err)
		}
		err = st.Execute(&cmd.ApplyRequest{
			Type:       cmd.ApplyRequest_TYPE_ADD_CLASS,
			Class:      class.Class,
			SubCommand: marshalledSubCmd,
		})
		// That check if necessary if we are recovering from a migration where the migration progress file got corrupted
		// and we are re-importing class that already exists in the raft schema
		if err != nil && errors.Is(err, errClassExists) {
			log.Info(fmt.Sprintf("class %s already exists in schema: skipping migration", class.Class))
		} else if err != nil {
			return fmt.Errorf("could not add class %s to raft based schema during migration: %w", class.Class, err)
		}

		// Update migrationProgress with the newly migrated class and write to disk
		migrationProgress.MigratedClasses[class.Class] = true
		jsonData, err := json.Marshal(migrationProgress)
		if err != nil {
			return fmt.Errorf("could not marshall migration progress: %w", err)
		}
		err = os.WriteFile(migrationProgressFile, jsonData, raftMigrationFilePermissions)
		if err != nil {
			return fmt.Errorf("could not write migration progress to %s: %w", migrationProgressFile, err)
		}
		log.Debug(fmt.Sprintf("successfully migrated class %s to raft based schema", class.Class))
	}

	// Migration complete successfully, ensure we remove the migration file so that we don't try any further migration
	err = os.Remove(migrationProgressFile)
	if err != nil {
		return fmt.Errorf("could not delete migration progress file after completing migration: %w", err)
	}

	log.Info("migration from non raft to raft based schema completed successfully")

	return nil
}
