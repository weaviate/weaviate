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

func disableSearchableIndexOnProp(t *testing.T, ctx context.Context, shard *Shard, propName string) {
	t.Helper()
	main := helpers.BucketSearchableFromPropNameLSM(propName)
	require.NoError(t, shard.removeBucket(ctx, main))
	sweep := migrationSweepStateFor(shard.pathLSM(), shard.index.logger)
	shard.cleanStaleMigrationDirs(ctx, propName, "searchable", sweep)
	shard.cleanStaleSidecarDirs(ctx, main, sweep.committed)
}

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

type noRecoveryTaskReindexer struct{}

func (r *noRecoveryTaskReindexer) RunAfterLsmInit(context.Context, *Shard) error { return nil }
