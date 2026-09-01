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
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// newRebuildSearchableTaskAtGen is newRebuildSearchableTask with the
// generation left to the caller, so one test can hold two generations of the
// same task at once.
func newRebuildSearchableTaskAtGen(t *testing.T, idx *Index, className, propName string,
	generation int,
) *ShardReindexTaskGeneric {
	t.Helper()
	task := NewShardReindexTaskGeneric(
		"RebuildSearchable", idx.logger,
		&RebuildSearchableStrategy{propNames: []string{propName}, generation: generation},
		reindexTaskConfig{
			concurrency:                   2,
			memtableOptFactor:             4,
			processingDuration:            10 * time.Minute,
			pauseDuration:                 1 * time.Second,
			checkProcessingEveryNoObjects: 1000,

			selectionEnabled: true,
			selectedPropsByCollection: map[string]map[string]struct{}{
				className: {propName: {}},
			},
			selectedShardsByCollection: map[string]map[string]struct{}{
				className: nil,
			},
		},
		&UuidKeyParser{}, uuidObjectsIteratorAsync,
		defaultIndexClosingGuard,
	)
	task.setMigrationIdentity(
		distributedtask.TaskDescriptor{ID: "test-rebuild-searchable", Version: 1},
		"shard-1__node-0",
		&ReindexTaskPayload{MigrationType: ReindexTypeRebuildSearchable},
	)
	return task
}

// The record key carries the task version, the strategy and the unit, but not
// the generation. A retried unit mints the next generation and writes to
// directories named after it, so the record the previous generation left under
// that same key describes directories this task never touches: resuming from
// its checkpoint would skip every object the first pass already wrote, into a
// bucket that never received them.
func TestARetriedGenerationDoesNotAdoptTheAbandonedGenerationsRecord(t *testing.T) {
	t.Run("the lookup refuses it", func(t *testing.T) { runRetriedGenerationCase(t, true) })
	t.Run("shard load records the generation it will write", func(t *testing.T) {
		runRetriedGenerationCase(t, false)
	})
}

func runRetriedGenerationCase(t *testing.T, seamOnly bool) {
	const propName = "title"

	ctx := testCtx()
	className := "RebuildGen" + uuid.NewString()[:8]
	shd, idx := testShardWithSettings(t, ctx,
		newRebuildSearchableTestClass(className, []string{propName}),
		enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(context.Background())

	gen1 := newRebuildSearchableTaskAtGen(t, idx, className, propName, 1)
	gen2 := newRebuildSearchableTaskAtGen(t, idx, className, propName, 2)
	require.Equal(t, gen1.migrationRecordKey(), gen2.migrationRecordKey(),
		"the two generations share one record key, which is what makes this reachable")

	subject := gen1.migrationSubject(shard, []string{propName}, time.Now())
	require.Equal(t, "rebuild_searchable_title_1", subject.TrackerDir)
	require.Equal(t, "property_title_searchable__rebuild_searchable_ingest_1",
		subject.StagedDirs[propName])
	require.NoError(t, gen1.putMigrationRecord(shard, NewMigrationRecordIterating(subject,
		MigrationCheckpoint{LastProcessedKey: []byte("halfway"), UpdatedAt: time.Now()})))

	if seamOnly {
		_, adopted := gen2.migrationRecord(shard)
		require.False(t, adopted, "generation 2 must not read generation 1's record as its own")
		kept, ok := gen1.migrationRecord(shard)
		require.True(t, ok, "generation 1 still owns the record it wrote")
		require.Equal(t, "rebuild_searchable_title_1", kept.Subject().TrackerDir)
		return
	}

	require.NoError(t, gen2.OnAfterLsmInit(ctx, shard))

	rec, ok := shard.migrationRecordStore().Get(gen2.migrationRecordKey())
	require.True(t, ok)
	require.Equal(t, MigrationStateIterating, rec.State())
	require.Equal(t, "rebuild_searchable_title_2", rec.Subject().TrackerDir,
		"the record now names the directories this generation writes")
	require.Equal(t, "property_title_searchable__rebuild_searchable_ingest_2",
		rec.Subject().StagedDirs[propName])
	iterating, isIterating := rec.(MigrationRecordIterating)
	require.True(t, isIterating)
	require.Empty(t, iterating.Checkpoint().LastProcessedKey,
		"a generation that wrote nothing yet has no object to resume past")
}
