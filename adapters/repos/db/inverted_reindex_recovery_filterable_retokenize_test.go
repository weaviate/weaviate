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
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// FilterableRetokenize migrates a filterable bucket's tokenization
// (StrategyRoaringSet) independently of the searchable half.

// fingerprintRoaringSetBucket returns a deterministic (term → sorted
// []docID) snapshot. RoaringSet-aware sibling of
// fingerprintInvertedBucket.
func fingerprintRoaringSetBucket(t *testing.T, b *lsmkv.Bucket) map[string][]uint64 {
	t.Helper()
	out := map[string][]uint64{}
	if b == nil {
		return out
	}
	c := b.CursorRoaringSet()
	defer c.Close()
	for k, bm := c.First(); k != nil; k, bm = c.Next() {
		term := string(append([]byte(nil), k...))
		var ids []uint64
		if bm != nil {
			// sroar.Bitmap.ToArray returns docIDs in ascending order
			// already; we still sort defensively in case the API
			// contract changes (cheap on the sizes the tests use).
			ids = bm.ToArray()
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		out[term] = ids
	}
	return out
}

// newFilterableRetokenizeTask wraps a FilterableRetokenizeStrategy in
// the test infrastructure. Pattern mirrors newSearchableRetokenizeTask
// (`inverted_reindex_recovery_convergence_test.go:125`) but for the
// filterable half of a change-tokenization migration.
//
// `targetTokenization` is the post-migration tokenization (e.g.
// `models.PropertyTokenizationField` for word→field, the exact change
// the production e2e tests exercise).
func newFilterableRetokenizeTask(t *testing.T, idx *Index, className, propName, targetTokenization string) (*ShardReindexTaskGeneric, *testFilterableRetokenizeStrategyWrapper) {
	t.Helper()
	wrapped := &testFilterableRetokenizeStrategyWrapper{
		FilterableRetokenizeStrategy: FilterableRetokenizeStrategy{
			propName:           propName,
			targetTokenization: targetTokenization,
			className:          className,
			generation:         1,
		},
	}
	task := NewShardReindexTaskGeneric(
		"FilterableRetokenize", idx.logger, wrapped,
		reindexTaskConfig{
			concurrency:                   2,
			memtableOptFactor:             4,
			backupMemtableOptFactor:       1,
			processingDuration:            10 * time.Minute,
			pauseDuration:                 1 * time.Second,
			checkProcessingEveryNoObjects: 1000,
		},
		&UuidKeyParser{}, uuidObjectsIteratorAsync,
	)
	return task, wrapped
}

// testFilterableRetokenizeStrategyWrapper overrides OnMigrationComplete
// with a flag-setter so the test can assert completion. The real
// strategy's OnMigrationComplete is already a no-op (schema flip lives
// in OnTaskCompleted), so this wrapper is essentially an observer.
// Mirrors testSearchableRetokenizeStrategyWrapper for the searchable
// half.
type testFilterableRetokenizeStrategyWrapper struct {
	FilterableRetokenizeStrategy
	migrationCompleted bool
}

func (s *testFilterableRetokenizeStrategyWrapper) OnMigrationComplete(_ context.Context, _ ShardLike) error {
	s.migrationCompleted = true
	return nil
}

// TestRecoveryConvergence_FilterableRetokenize_Baseline drives a
// filterable bucket from word to field tokenization.
func TestRecoveryConvergence_FilterableRetokenize_Baseline(t *testing.T) {
	const propName = "title"
	const numObjects = 25

	ctx := testCtx()
	className := "FilterRetokenizeBaseline_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	filtBucketName := helpers.BucketFromPropNameLSM(propName)
	preBucket := shard.store.Bucket(filtBucketName)
	require.NotNil(t, preBucket, "pre-migration filterable bucket must exist")
	require.Equal(t, lsmkv.StrategyRoaringSet, preBucket.Strategy(),
		"pre-migration filterable bucket must be StrategyRoaringSet")
	// Pre-migration: under word tokenization every word in our 25-word
	// dictionary appears as a term.
	preFP := fingerprintRoaringSetBucket(t, preBucket)
	require.NotEmpty(t, preFP,
		"pre-migration filterable fingerprint must be non-empty (word tokenization)")

	task, wrapped := newFilterableRetokenizeTask(t, idx, className, propName,
		models.PropertyTokenizationField)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	require.True(t, wrapped.migrationCompleted,
		"OnMigrationComplete must fire post-migration")

	postBucket := shard.store.Bucket(filtBucketName)
	require.NotNil(t, postBucket, "post-migration filterable bucket must exist")
	require.Equal(t, lsmkv.StrategyRoaringSet, postBucket.Strategy(),
		"post-migration filterable bucket must remain StrategyRoaringSet")
	postFP := fingerprintRoaringSetBucket(t, postBucket)
	require.NotEmpty(t, postFP,
		"post-migration filterable fingerprint must be non-empty (field tokenization)")
	// Under field tokenization the entire field value is one term per
	// document. With our 3-token cycling, every doc has a distinct
	// 3-word value so the post-migration bucket has numObjects distinct
	// terms.
	require.Lenf(t, postFP, numObjects,
		"post-migration field-tokenized bucket should have %d terms (one per object), got %d",
		numObjects, len(postFP))
	for term, ids := range postFP {
		require.Lenf(t, ids, 1,
			"post-migration field-tokenized term %q should have exactly 1 docID, got %d", term, len(ids))
	}

	rt, err := task.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	require.True(t, rt.IsReindexed())
	require.True(t, rt.IsPrepended())
	require.True(t, rt.IsMerged())
	require.True(t, rt.IsSwapped())
	require.True(t, rt.IsTidied())
}
