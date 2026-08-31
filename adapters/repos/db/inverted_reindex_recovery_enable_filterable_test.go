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
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// EnableFilterable backfills a from-scratch filterable bucket while
// IndexFilterable is still false; ForceFilterable drives it.

// newEnableFilterableTask wraps EnableFilterableStrategy. Selection is
// mandatory: the strategy can't discover targets via the schema-flag
// scan because that flag is still false at migration time.
func newEnableFilterableTask(t *testing.T, idx *Index, className string, propNames ...string) (*ShardReindexTaskGeneric, *testEnableFilterableStrategyWrapper) {
	t.Helper()
	return newEnableFilterableTaskAtGeneration(t, idx, className, 1, propNames...)
}

// newEnableFilterableTaskAtGeneration is newEnableFilterableTask for the
// back-to-back case, where a second migration on the same property carries a
// higher generation and a higher task version — the pair the supersession
// relation orders by.
func newEnableFilterableTaskAtGeneration(t *testing.T, idx *Index, className string,
	generation int, propNames ...string,
) (*ShardReindexTaskGeneric, *testEnableFilterableStrategyWrapper) {
	t.Helper()
	wrapped := &testEnableFilterableStrategyWrapper{
		EnableFilterableStrategy: EnableFilterableStrategy{
			propNames:  propNames,
			generation: generation,
		},
	}
	selectedProps := map[string]struct{}{}
	for _, propName := range propNames {
		selectedProps[propName] = struct{}{}
	}
	task := NewShardReindexTaskGeneric(
		"EnableFilterable", idx.logger, wrapped,
		reindexTaskConfig{
			concurrency:                   2,
			memtableOptFactor:             4,
			processingDuration:            10 * time.Minute,
			pauseDuration:                 1 * time.Second,
			checkProcessingEveryNoObjects: 1000,

			selectionEnabled: true,
			selectedPropsByCollection: map[string]map[string]struct{}{
				className: selectedProps,
			},
			selectedShardsByCollection: map[string]map[string]struct{}{
				className: nil, // nil = all shards
			},
		},
		&UuidKeyParser{}, uuidObjectsIteratorAsync,
	)
	// Without an identity the task's record key is incomplete and every
	// transition would refuse to write itself.
	task.setMigrationIdentity(
		distributedtask.TaskDescriptor{ID: "test-enable-filterable", Version: uint64(generation)},
		"shard-1__node-0",
		&ReindexTaskPayload{MigrationType: ReindexTypeEnableFilterable},
	)
	return task, wrapped
}

// testEnableFilterableStrategyWrapper overrides OnMigrationComplete with a
// flag-setter so the test can assert completion. The real strategy's
// OnMigrationComplete is already a no-op (cluster-wide schema flip lives
// in OnTaskCompleted), so this wrapper is essentially an observer.
// Mirrors testFilterableRetokenizeStrategyWrapper.
type testEnableFilterableStrategyWrapper struct {
	EnableFilterableStrategy
	migrationCompleted bool
}

func (s *testEnableFilterableStrategyWrapper) OnMigrationComplete(_ context.Context, _ ShardLike) error {
	s.migrationCompleted = true
	return nil
}

// newEnableFilterableTestClass builds a class fixture for the
// EnableFilterable matrix: one Word-tokenized text property with
// IndexFilterable=false (so the filterable bucket genuinely does not
// exist pre-migration). The default newTestClassWithProps leaves
// IndexFilterable nil (defaults to true) — not what we want here.
func newEnableFilterableTestClass(className string, propNames ...string) *models.Class {
	class := newTestClassWithProps(className, propNames)
	for _, prop := range class.Properties {
		prop.IndexFilterable = boolPtr(false)
	}
	return class
}

// TestRecoveryConvergence_EnableFilterable_Baseline drives a class from
// no filterable bucket to a fully-populated RoaringSet bucket.
func TestRecoveryConvergence_EnableFilterable_Baseline(t *testing.T) {
	const propName = "title"
	const numObjects = 25

	ctx := testCtx()
	className := "EnableFilterableBaseline_" + uuid.NewString()[:8]
	class := newEnableFilterableTestClass(className, propName)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	// Pre-migration: the filterable bucket must NOT exist. With
	// IndexFilterable=false on the class, createPropertyValueIndex
	// (`shard_init_properties.go:471`) skips creating the bucket.
	filtBucketName := helpers.BucketFromPropNameLSM(propName)
	preBucket := shard.store.Bucket(filtBucketName)
	require.Nilf(t, preBucket,
		"pre-migration filterable bucket must be absent (IndexFilterable=false on class)")

	task, wrapped := newEnableFilterableTask(t, idx, className, propName)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	require.True(t, wrapped.migrationCompleted,
		"OnMigrationComplete must fire post-migration")

	postBucket := shard.store.Bucket(filtBucketName)
	require.NotNil(t, postBucket, "post-migration filterable bucket must exist")
	require.Equal(t, lsmkv.StrategyRoaringSet, postBucket.Strategy(),
		"post-migration filterable bucket must be StrategyRoaringSet")
	postFP := fingerprintRoaringSetBucket(t, postBucket)
	require.NotEmpty(t, postFP,
		"post-migration filterable fingerprint must be non-empty (analyzer-overlay backfill)")

	// Every word-tokenized dictionary token should be present given
	// numObjects=25 and the 3-word cycling pattern (each token appears
	// as one of the 3 words for some doc).
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
			"post-migration filterable fingerprint missing token %q (every dictionary word should appear)", tok)
		require.NotEmptyf(t, docIDs,
			"post-migration filterable token %q has no docIDs (posting list is empty)", tok)
	}
}
