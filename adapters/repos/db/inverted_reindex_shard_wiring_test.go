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

package db

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// installTestMigrationTaskSources installs the pair reconciliation consults,
// both answering from the same list: the ordinary case where this node has
// caught up with the leader.
func installTestMigrationTaskSources(ctx context.Context, database *DB, tasks ...*distributedtask.Task) {
	database.SetMigrationLocalTaskSource(ctx,
		func() ([]*distributedtask.Task, bool) { return tasks, true },
		func(context.Context) ([]*distributedtask.Task, error) { return tasks, nil })
}

// TestReconcileAfterTaskMapSkipsAShardShuttingDown pins the guard the walk was
// missing. It holds a shard pointer across a pass that removes directories,
// and a tenant deactivation, a tenant delete or a collection delete can take
// that shard down underneath it. The teardown then fails a memtable flush into
// a directory this pass removed, and the failure latches on the shard: every
// later activation of that tenant returns the teardown error rather than
// serving it.
func TestReconcileAfterTaskMapSkipsAShardShuttingDown(t *testing.T) {
	const propName = "title"

	tests := []struct {
		name         string
		shuttingDown bool
		wantSurvives bool
	}{
		{
			name: "a shard that is staying decides the migration the cluster abandoned",
		},
		{
			name:         "a shard on its way out is left to its next activation",
			shuttingDown: true,
			wantSurvives: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			className := "WiringShutdownGuard_" + uuid.NewString()[:8]
			shd, idx := testShardWithSettings(t, ctx, newTestClassWithProps(className, []string{propName}),
				enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(context.Background())

			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, propName)
			require.NoError(t, shard.migrationRecords.Put(NewMigrationRecordMerged(subject)))
			for _, dir := range migrationOwnedDirs(subject) {
				require.NoError(t, os.MkdirAll(filepath.Join(shard.pathLSM(), dir), 0o777))
			}
			staged := filepath.Join(shard.pathLSM(), subject.StagedDirs[propName])

			if tt.shuttingDown {
				shard.shutdownRequested.Store(true)
				// Before the deferred Shutdown, which the flag would refuse.
				defer shard.shutdownRequested.Store(false)
			}

			// The pass walks db.indices, which the shard fixture does not
			// populate.
			require.NotNil(t, idx.db, "the test shard fixture has to wire idx.db")
			idx.db.indices[indexID(idx.Config.ClassName)] = idx

			installTestMigrationTaskSources(ctx, idx.db, &distributedtask.Task{
				Namespace: ReindexNamespace,
				TaskDescriptor: distributedtask.TaskDescriptor{
					ID: subject.TaskID, Version: subject.Key.TaskVersion,
				},
				Status: distributedtask.TaskStatusCancelled,
			})

			assert.Equal(t, tt.wantSurvives, dirExists(t, staged), "the staged directory")
			_, present := shard.migrationRecords.Get(subject.Key)
			assert.Equal(t, tt.wantSurvives, present, "the migration record")
		})
	}
}

// TestReconcileWithoutADatabaseHandle pins the startup window. An index is
// given its database handle only after its constructor returns, and an eagerly
// loaded shard reconciles inside that call — so a record whose disposition
// needs the task map reconciles with no handle to read it from. The shard
// constructor's recover swallows the resulting panic, the index never
// registers, and every later submit fails against the whole collection.
func TestReconcileWithoutADatabaseHandle(t *testing.T) {
	const propName = "title"

	// Only the three in-flight states consult the task map; the two flipped
	// ones are decided by probing handles alone.
	tests := []struct {
		name string
		rec  func(MigrationSubject) MigrationRecord
	}{
		{
			name: "iterating",
			rec:  func(s MigrationSubject) MigrationRecord { return NewMigrationRecordIterating(s, MigrationCheckpoint{}) },
		},
		{
			name: "iterated",
			rec:  func(s MigrationSubject) MigrationRecord { return NewMigrationRecordIterated(s) },
		},
		{
			name: "merged",
			rec:  func(s MigrationSubject) MigrationRecord { return NewMigrationRecordMerged(s) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			className := "WiringNoDBHandle_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{propName})
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(context.Background())

			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, propName)
			require.NoError(t, shard.migrationRecords.Put(tt.rec(subject)))
			for _, dir := range migrationOwnedDirs(subject) {
				require.NoError(t, os.MkdirAll(filepath.Join(shard.pathLSM(), dir), 0o777))
			}
			staged := filepath.Join(shard.pathLSM(), subject.StagedDirs[propName])

			handle := idx.db
			idx.db = nil
			// Restored before the deferred Shutdown, which needs the handle.
			defer func() { idx.db = handle }()

			require.NotPanics(t, func() { shard.reconcileMigrationRecords(ctx, class) })

			assert.True(t, dirExists(t, staged), "the staged directory")
			_, present := shard.migrationRecords.Get(subject.Key)
			assert.True(t, present, "the migration record")
		})
	}
}
