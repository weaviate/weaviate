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
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Exhaustive single-process recovery-convergence tests for the v2
// inverted-index reindex pipeline. See weaviate/0-weaviate-issues#240.

// fingerprintInvertedBucket returns a (term → sorted []docID) snapshot
// of an inverted/MapCollection searchable bucket. Frequency is dropped;
// per-doc inclusion is enough to catch posting-list divergence.
func fingerprintInvertedBucket(t *testing.T, b *lsmkv.Bucket) map[string][]uint64 {
	t.Helper()
	out := map[string][]uint64{}
	if b == nil {
		return out
	}
	c, err := b.MapCursor()
	require.NoError(t, err)
	defer c.Close()
	for k, pairs := c.First(context.Background()); k != nil; k, pairs = c.Next(context.Background()) {
		term := string(append([]byte(nil), k...))
		ids := make([]uint64, 0, len(pairs))
		for _, p := range pairs {
			require.Lenf(t, p.Key, 8,
				"unexpected pair key length on term %q: want 8 bytes (big-endian docID), got %d",
				term, len(p.Key))
			id := uint64(p.Key[0])<<56 |
				uint64(p.Key[1])<<48 |
				uint64(p.Key[2])<<40 |
				uint64(p.Key[3])<<32 |
				uint64(p.Key[4])<<24 |
				uint64(p.Key[5])<<16 |
				uint64(p.Key[6])<<8 |
				uint64(p.Key[7])
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		out[term] = ids
	}
	return out
}

// newSearchableRetokenizeTask wraps the production
// SearchableRetokenizeStrategy in test scaffolding. Semantic
// migration: swap is driven via RunReindexOnly/RunPrepare/RunSwap on
// each shard, not the inline runtimeSwap used by MapToBlockmax.
func newSearchableRetokenizeTask(t *testing.T, idx *Index, className, propName, targetTokenization, bucketStrategy, unitID string) (*ShardReindexTaskGeneric, *testSearchableRetokenizeStrategyWrapper) {
	t.Helper()
	return newSearchableRetokenizeTaskAtGeneration(t, idx, className, propName, targetTokenization, bucketStrategy, 1, unitID)
}

func newSearchableRetokenizeTaskAtGeneration(t *testing.T, idx *Index, className, propName,
	targetTokenization, bucketStrategy string, generation int, unitID string,
) (*ShardReindexTaskGeneric, *testSearchableRetokenizeStrategyWrapper) {
	t.Helper()
	wrapped := &testSearchableRetokenizeStrategyWrapper{
		SearchableRetokenizeStrategy: SearchableRetokenizeStrategy{
			propName:           propName,
			targetTokenization: targetTokenization,
			className:          className,
			bucketStrategy:     bucketStrategy,
			generation:         generation,
		},
	}
	task := NewShardReindexTaskGeneric(
		"SearchableRetokenize", idx.logger, wrapped,
		reindexTaskConfig{
			concurrency:                   2,
			memtableOptFactor:             4,
			processingDuration:            10 * time.Minute,
			pauseDuration:                 1 * time.Second,
			checkProcessingEveryNoObjects: 1000,
		},
		&UuidKeyParser{}, uuidObjectsIteratorAsync,
		defaultIndexClosingGuard,
	)
	task.setMigrationIdentity(
		distributedtask.TaskDescriptor{ID: "test-searchable-retokenize", Version: uint64(generation)},
		unitID,
		&ReindexTaskPayload{
			MigrationType:      ReindexTypeChangeTokenization,
			TargetTokenization: targetTokenization,
		},
	)
	return task, wrapped
}

// testSearchableRetokenizeStrategyWrapper stubs OnMigrationComplete
// (the real strategy is a no-op for searchable — the schema flip is
// done by FilterableRetokenize when it runs second; we don't run that
// here). Same pattern as testMigrationStrategy for MapToBlockmax.
type testSearchableRetokenizeStrategyWrapper struct {
	SearchableRetokenizeStrategy
	migrationCompleted bool
}

func (s *testSearchableRetokenizeStrategyWrapper) OnMigrationComplete(_ context.Context, _ ShardLike) error {
	s.migrationCompleted = true
	return nil
}

// The dictionary [makeConvergenceTestObjects] cycles through. The baseline is
// checked against it, so a defect the rows all share cannot pass unnoticed.
var convergenceTokens = []string{
	"alpha", "bravo", "charlie", "delta", "echo",
	"foxtrot", "golf", "hotel", "india", "juliett",
	"kilo", "lima", "mike", "november", "oscar",
	"papa", "quebec", "romeo", "sierra", "tango",
	"uniform", "victor", "whiskey", "xray", "yankee",
}

// makeConvergenceTestObjects builds n objects whose `title` cycles through the
// dictionary so each token appears in multiple docs.
func makeConvergenceTestObjects(t *testing.T, n int, className string) []*storobj.Object {
	t.Helper()
	tokens := convergenceTokens
	out := make([]*storobj.Object, n)
	for i := 0; i < n; i++ {
		text := tokens[i%len(tokens)] + " " + tokens[(i+1)%len(tokens)] + " " + tokens[(i+2)%len(tokens)]
		out[i] = createTestObjectWithText(className, text)
	}
	return out
}

// TestRecoveryConvergence_Baseline fingerprints a clean MapToBlockmax
// migration; other recovery-convergence tests compare against it.
func TestRecoveryConvergence_Baseline(t *testing.T) {
	ctx := testCtx()
	const propName = "title"
	const numObjects = 25

	className := "ConvergenceBaseline_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	objects := makeConvergenceTestObjects(t, numObjects, className)
	for _, obj := range objects {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
	preBucket := shard.store.Bucket(bucketName)
	require.NotNil(t, preBucket, "pre-migration searchable bucket must exist")
	require.Equal(t, lsmkv.StrategyMapCollection, preBucket.Strategy())

	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy, shard.migrationUnit())
	require.NoError(t, task.RunOnShard(ctx, shard))
	require.True(t, strategy.migrationCompleted)

	postBucket := shard.store.Bucket(bucketName)
	require.NotNil(t, postBucket, "post-migration searchable bucket must exist")
	require.Equal(t, lsmkv.StrategyInverted, postBucket.Strategy())

	fp := fingerprintInvertedBucket(t, postBucket)
	require.NotEmpty(t, fp, "baseline fingerprint must have at least one term")

	for _, tok := range convergenceTokens {
		docIDs, ok := fp[tok]
		require.Truef(t, ok, "baseline fingerprint missing token %q", tok)
		require.NotEmptyf(t, docIDs, "baseline fingerprint token %q has empty posting list", tok)
	}
}

// computeBaselineFingerprint runs a clean migration on a throw-away
// shard and returns its post-migration fingerprint. Recovery-from-state
// cases compare against this as ground truth.
func computeBaselineFingerprint(t *testing.T, propName string, numObjects int) map[string][]uint64 {
	t.Helper()
	ctx := testCtx()
	className := "ConvergenceBaselineRef_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy, shard.migrationUnit())
	require.NoError(t, task.RunOnShard(ctx, shard))
	require.True(t, strategy.migrationCompleted)

	bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
	return fingerprintInvertedBucket(t, shard.store.Bucket(bucketName))
}

// On a relaunch over an already-finished rebuild the iteration has nothing to
// do, so a RunOnShard that does not sequence the swap after it reports success
// on an index that was never swapped.
func TestRunOnShardSwapsARebuildThatIsAlreadyComplete(t *testing.T) {
	const propName = "title"
	const numObjects = 25

	ctx := testCtx()
	className := "RelaunchAfterRebuild_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy, shard.migrationUnit())

	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	rec, ok := task.migrationRecord(shard)
	require.True(t, ok, "the rebuild must leave a record")
	require.Equal(t, MigrationStateIterated, rec.State(),
		"fixture: the rebuild must be complete and the swap still pending")
	require.False(t, strategy.migrationCompleted)

	require.NoError(t, task.RunOnShard(ctx, shard))

	rec, ok = task.migrationRecord(shard)
	require.True(t, ok)
	require.Equal(t, MigrationStateSwapped, rec.State(),
		"a relaunch on a complete rebuild must reach the swap, not report success without it")
	require.True(t, strategy.migrationCompleted)
	require.Equal(t, lsmkv.StrategyInverted,
		shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName)).Strategy(),
		"the canonical bucket must serve the rebuilt data after the relaunch")
}
