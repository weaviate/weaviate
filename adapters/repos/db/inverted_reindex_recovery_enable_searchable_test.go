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

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// EnableSearchable backfills a from-scratch searchable bucket that
// PreReindexHook creates as StrategyInverted, via forced tokenization.

// newEnableSearchableTestClass builds a class with IndexSearchable=false
// (so PreReindexHook actually creates the bucket).
// newTestClassWithProps can't be reused — it leaves IndexSearchable=nil
// which defaults to true, and the bucket gets created at shard init.
func newEnableSearchableTestClass(className string, propNames []string) *models.Class {
	vFalse := false
	props := make([]*models.Property, len(propNames))
	for i, name := range propNames {
		props[i] = &models.Property{
			Name:            name,
			DataType:        schema.DataTypeText.PropString(),
			Tokenization:    models.PropertyTokenizationWord,
			IndexSearchable: &vFalse,
		}
	}
	return &models.Class{
		Class:             className,
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords:              &models.StopwordConfig{Preset: "none"},
			IndexNullState:         true,
			IndexPropertyLength:    true,
			UsingBlockMaxWAND:      false,
		},
		Properties: props,
	}
}

// newEnableSearchableTask wraps an EnableSearchableStrategy in the
// shared test infrastructure. Mirrors newFilterableRetokenizeTask
// (`inverted_reindex_recovery_filterable_retokenize_test.go:106`) and
// newSearchableRetokenizeTask (`convergence_test.go:125`); only the
// strategy struct's field set differs (EnableSearchable takes a slice
// of prop names + tokenization, not a single prop + targetTokenization).
//
// The reindexTaskConfig mirrors production's blockmaxSearchableTaskConfig
// (`inverted_reindex_blockmax_searchable_task.go:20`) — same
// concurrency, memtable factors, processing/pause durations, and
// selectionEnabled with the prop list. Drift from production here
// would let the test pass while production fails the same convergence
// invariant.
func newEnableSearchableTask(
	t *testing.T, idx *Index, className, propName, tokenization string,
) (*ShardReindexTaskGeneric, *testEnableSearchableStrategyWrapper) {
	t.Helper()
	wrapped := &testEnableSearchableStrategyWrapper{
		EnableSearchableStrategy: EnableSearchableStrategy{
			propNames:    []string{propName},
			tokenization: tokenization,
			generation:   1,
		},
	}
	selected := map[string]struct{}{propName: {}}
	task := NewShardReindexTaskGeneric(
		"EnableSearchable", idx.logger, wrapped,
		reindexTaskConfig{
			concurrency:                   2,
			memtableOptFactor:             4,
			processingDuration:            10 * time.Minute,
			pauseDuration:                 1 * time.Second,
			checkProcessingEveryNoObjects: 1000,

			selectionEnabled: true,
			selectedPropsByCollection: map[string]map[string]struct{}{
				className: selected,
			},
			selectedShardsByCollection: map[string]map[string]struct{}{
				className: nil,
			},
		},
		&UuidKeyParser{}, uuidObjectsIteratorAsync,
	)
	return task, wrapped
}

// testEnableSearchableStrategyWrapper overrides OnMigrationComplete
// with a flag-setter so the test can assert the callback fires. The
// production strategy's OnMigrationComplete is a documented no-op (the
// schema flip lives in ReindexProvider.OnTaskCompleted cluster-wide),
// so this wrapper is purely observational — same shape as
// testFilterableRetokenizeStrategyWrapper.
type testEnableSearchableStrategyWrapper struct {
	EnableSearchableStrategy
	migrationCompleted bool
}

func (s *testEnableSearchableStrategyWrapper) OnMigrationComplete(_ context.Context, _ ShardLike) error {
	s.migrationCompleted = true
	return nil
}

// TestRecoveryConvergence_EnableSearchable_Baseline drives a class from
// no searchable bucket to a fully populated blockmax searchable bucket.
func TestRecoveryConvergence_EnableSearchable_Baseline(t *testing.T) {
	const propName = "title"
	const numObjects = 25

	ctx := testCtx()
	className := "EnableSearchableBaseline_" + uuid.NewString()[:8]
	class := newEnableSearchableTestClass(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	searchBucketName := helpers.BucketSearchableFromPropNameLSM(propName)
	// Precondition: searchable bucket does NOT exist (IndexSearchable=false
	// on the prop → shard init skipped the bucket creation in
	// shard_init_properties.go:493). EnableSearchable.PreReindexHook is
	// what creates it.
	require.Nil(t, shard.store.Bucket(searchBucketName),
		"pre-migration searchable bucket must NOT exist (IndexSearchable=false)")

	task, wrapped := newEnableSearchableTask(t, idx, className, propName,
		models.PropertyTokenizationWord)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	require.True(t, wrapped.migrationCompleted,
		"OnMigrationComplete must fire post-migration")

	postBucket := shard.store.Bucket(searchBucketName)
	require.NotNil(t, postBucket,
		"post-migration searchable bucket must exist (created by PreReindexHook)")
	require.Equal(t, lsmkv.StrategyInverted, postBucket.Strategy(),
		"post-migration searchable bucket must be StrategyInverted (blockmax)")

	postFP := fingerprintInvertedBucket(t, postBucket)
	require.NotEmpty(t, postFP,
		"post-migration searchable fingerprint must be non-empty (word tokenization)")

	// Under word tokenization our 25-token cycling dictionary produces
	// 25 distinct terms (same shape as the MapToBlockmax baseline's
	// expectedTokens block at convergence_test.go:264-274).
	expectedTokens := []string{
		"alpha", "bravo", "charlie", "delta", "echo",
		"foxtrot", "golf", "hotel", "india", "juliett",
		"kilo", "lima", "mike", "november", "oscar",
		"papa", "quebec", "romeo", "sierra", "tango",
		"uniform", "victor", "whiskey", "xray", "yankee",
	}
	for _, tok := range expectedTokens {
		docIDs, ok := postFP[tok]
		require.Truef(t, ok,
			"baseline fingerprint missing token %q (post-migration bucket should contain every dictionary word)", tok)
		require.NotEmptyf(t, docIDs,
			"baseline fingerprint token %q has no docIDs (posting list is empty)", tok)
	}

	rt, err := task.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	require.True(t, rt.IsReindexed())
	require.True(t, rt.IsPrepended())
	require.True(t, rt.IsMerged())
	require.True(t, rt.IsSwapped())
	require.True(t, rt.IsTidied())
}
