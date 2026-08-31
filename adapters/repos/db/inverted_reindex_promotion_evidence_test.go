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
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// The flip decision is recorded before the first pointer moves, so a Swapped
// record proves only that the flip was decided. The tests here pin the readers
// that need the post-condition instead: promotion, which renames one directory
// over another, and the completion path, which commits the schema effect
// cluster-wide.

// disableSearchableIndexOnProp is the sequence [Shard.updatePropertyBuckets]
// runs inside the RAFT apply of a property update that turns indexSearchable
// off.
func disableSearchableIndexOnProp(t *testing.T, ctx context.Context, shard *Shard, propName string) {
	t.Helper()
	main := helpers.BucketSearchableFromPropNameLSM(propName)
	require.NoError(t, shard.removeBucket(ctx, main))
	sweep := migrationSweepStateFor(shard.pathLSM(), shard.index.logger)
	shard.cleanStaleMigrationDirs(ctx, propName, "searchable", sweep)
	shard.cleanStaleSidecarDirs(ctx, main, sweep.committed)
}

// TestPromotionNeverRenamesAStagedDirTheLoadCreated pins that a load leaves a
// promoted property's staged directory absent. Re-creating it hands the next
// load's promotion an empty directory to rename over the live canonical one,
// which empties the property's index for good.
func TestPromotionNeverRenamesAStagedDirTheLoadCreated(t *testing.T) {
	ctx := testCtx()
	propNames := []string{"title", "subtitle"}
	className := "PromoEvidence_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, propNames)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	for _, obj := range makeMultiPropConvergenceObjects(t, 25, className, propNames) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task := newTestTask(idx.logger,
		&testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}})
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err)
		if rerunAt.IsZero() {
			break
		}
	}

	// The trigger: the searchable index goes off on one of the migration's two
	// properties while the flip is decided but not yet promoted. That property
	// can never settle, so the record stays Swapped past the other one's
	// promotion — and every later load still reads it as a promotion subject.
	disableSearchableIndexOnProp(t, ctx, shard, "subtitle")

	shardName, lsmPath := shard.Name(), shard.pathLSM()
	stagedTitle := filepath.Join(lsmPath, task.ingestBucketName("title"))
	canonicalTitle := helpers.BucketSearchableFromPropNameLSM("title")

	reload := func(prev *Shard) *Shard {
		t.Helper()
		require.NoError(t, prev.Shutdown(ctx))
		simulateProcessRestartBucketCleanup(t, lsmPath)
		next := newTestTask(idx.logger,
			&testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}})
		idx.shardReindexer = &testShardReindexer{task: next}
		loaded, err := idx.initShard(ctx, shardName, class, nil, true, true)
		require.NoError(t, err)
		idx.shards.Store(shardName, loaded)
		return loaded.(*Shard)
	}

	shard1 := reload(shard)
	assert.NoDirExists(t, stagedTitle,
		"the load re-created a staged directory promotion had already renamed onto the canonical name")
	before := fingerprintInvertedBucket(t, shard1.store.Bucket(canonicalTitle))
	require.NotEmpty(t, before, "fixture: the promoted canonical bucket must hold the migrated terms")

	shard2 := reload(shard1)
	defer shard2.Shutdown(ctx)
	after := fingerprintInvertedBucket(t, shard2.store.Bucket(canonicalTitle))
	assert.Equal(t, before, after,
		"the second load's promotion renamed a directory over the property's live index")
}

// TestRunSwapRefusesAPropertyItNeverFlipped pins that the completion path only
// reports success for a property whose canonical name really serves the
// migration's data. An absent staged bucket is not evidence of a flip: it looks
// the same whether the flip ran or the bucket was never loaded.
func TestRunSwapRefusesAPropertyItNeverFlipped(t *testing.T) {
	tests := []struct {
		name         string
		disableIndex bool
		wantErr      bool
	}{
		{name: "staged bucket cold, its dir still there", wantErr: false},
		{name: "staged dir removed by an index DELETE", disableIndex: true, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := testCtx()
			const propName = "title"
			className := "SwapEvidence_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{propName})

			idx, cold, preStrategy := aColdShardWithMergedStagedData(t, ctx, class, propName)

			if test.disableIndex {
				disableSearchableIndexOnProp(t, ctx, cold, propName)
			}

			swap, wrapper := newSearchableRetokenizeTask(t, idx, className, propName,
				models.PropertyTokenizationField, preStrategy)
			err := swap.RunSwapOnShard(ctx, cold)

			if !test.wantErr {
				require.NoError(t, err)
				assert.True(t, wrapper.migrationCompleted)
				return
			}
			require.Errorf(t, err,
				"RunSwapOnShard reported a migration complete over a property it never flipped")
			assert.Falsef(t, wrapper.migrationCompleted,
				"the schema effect was committed for a property whose index never moved")
		})
	}
}

// TestCompletionRefusesARecordNoPromotionSettled pins the two re-entry paths
// into OnMigrationComplete on the same answer. A DELETE takes both the staged
// and the canonical directory away, promotion refuses the property, and shard
// init re-creates the canonical bucket empty — so "the staged directory is
// gone" is true here without any promotion having run, and the record's own
// state is what has to be asked instead.
func TestCompletionRefusesARecordNoPromotionSettled(t *testing.T) {
	tests := []struct {
		name    string
		reenter func(t *testing.T, ctx context.Context, task *ShardReindexTaskGeneric, shard *Shard) error
	}{
		{
			name: "the swap phase runs again on a shard that restarted",
			reenter: func(_ *testing.T, ctx context.Context, task *ShardReindexTaskGeneric, shard *Shard) error {
				return task.RunSwapOnShard(ctx, shard)
			},
		},
		{
			name: "the load path picks the migration up again",
			reenter: func(_ *testing.T, ctx context.Context, task *ShardReindexTaskGeneric, shard *Shard) error {
				_, _, err := task.OnAfterLsmInitAsync(ctx, shard)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := testCtx()
			const propName = "title"
			className := "CompletionEvidence_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{propName})

			idx, cold, preStrategy := aColdShardWithMergedStagedData(t, ctx, class, propName)
			shardName, lsmPath := cold.Name(), cold.pathLSM()
			disableSearchableIndexOnProp(t, ctx, cold, propName)

			swap, _ := newSearchableRetokenizeTask(t, idx, className, propName,
				models.PropertyTokenizationField, preStrategy)
			require.Error(t, swap.RunSwapOnShard(ctx, cold),
				"fixture: the flip cannot run, but its decision is recorded write-ahead")

			require.NoError(t, cold.Shutdown(ctx))
			simulateProcessRestartBucketCleanup(t, lsmPath)
			idx.shardReindexer = &noRecoveryTaskReindexer{}
			loaded, err := idx.initShard(ctx, shardName, class, nil, true, true)
			require.NoError(t, err)
			post := loaded.(*Shard)
			idx.shards.Store(shardName, loaded)
			defer post.Shutdown(ctx)

			canonical := helpers.BucketSearchableFromPropNameLSM(propName)
			require.NoDirExists(t, filepath.Join(lsmPath, swap.ingestBucketName(propName)),
				"fixture: the DELETE must leave no staged directory")
			require.Empty(t, fingerprintInvertedBucket(t, post.store.Bucket(canonical)),
				"fixture: shard init must have re-created the canonical bucket empty")

			reenter, wrapper := newSearchableRetokenizeTask(t, idx, className, propName,
				models.PropertyTokenizationField, preStrategy)
			err = test.reenter(t, ctx, reenter, post)
			require.Error(t, err,
				"the migration was reported complete over an index nothing ever promoted")
			assert.Contains(t, err.Error(), "not promoted")
			assert.False(t, wrapper.migrationCompleted,
				"the schema effect was committed over an empty canonical bucket")
		})
	}
}

// aColdShardWithMergedStagedData drives one property to Merged, then reloads
// the shard with no recovery task registered: the staged directory is on disk
// and nothing has opened it, which is the state an absent ingest bucket cannot
// be told apart from a completed flip.
func aColdShardWithMergedStagedData(t *testing.T, ctx context.Context, class *models.Class,
	propName string,
) (*Index, *Shard, string) {
	t.Helper()
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	for _, obj := range makeConvergenceTestObjects(t, 25, class.Class) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	canonical := helpers.BucketSearchableFromPropNameLSM(propName)
	preStrategy := shard.store.Bucket(canonical).Strategy()
	task, _ := newSearchableRetokenizeTask(t, idx, class.Class, propName,
		models.PropertyTokenizationField, preStrategy)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))

	shardName, lsmPath := shard.Name(), shard.pathLSM()
	require.NoError(t, shard.Shutdown(ctx))
	simulateProcessRestartBucketCleanup(t, lsmPath)
	idx.shardReindexer = &noRecoveryTaskReindexer{}
	loaded, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err)
	cold := loaded.(*Shard)
	idx.shards.Store(shardName, loaded)
	t.Cleanup(func() { _ = cold.Shutdown(ctx) })
	require.Nil(t, cold.store.Bucket(task.ingestBucketName(propName)),
		"fixture: the staged bucket must not be loaded")
	return idx, cold, preStrategy
}

// noRecoveryTaskReindexer is a shard load that registers no reindex task, so
// nothing opens the migration's staged buckets.
type noRecoveryTaskReindexer struct{}

func (r *noRecoveryTaskReindexer) RunAfterLsmInit(context.Context, *Shard) error { return nil }
