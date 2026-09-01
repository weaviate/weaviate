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

// Recovery-convergence matrix for RebuildSearchable — rebuild an
// existing BlockMax bucket from objects. Source and target are both
// StrategyInverted, so the test class needs UsingBlockMaxWAND=true.
// Driven via the trio Run*OnShard methods even though production
// dispatches via RunOnShard; recovery dispatches off the on-disk
// sentinel, indifferent to invocation route.

// newRebuildSearchableTestClass mirrors newTestClassWithProps but flips
// UsingBlockMaxWAND to true so the searchable bucket for each property
// starts at StrategyInverted (BlockMax) — RebuildSearchable's source
// strategy is StrategyInverted, so without this flip the test would
// instead set up a MapCollection searchable bucket and the strategy
// would find no properties to rebuild.
func newRebuildSearchableTestClass(className string, propNames []string) *models.Class {
	props := make([]*models.Property, len(propNames))
	for i, name := range propNames {
		props[i] = &models.Property{
			Name:         name,
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWord,
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
			UsingBlockMaxWAND:      true, // start at StrategyInverted searchable buckets
		},
		Properties: props,
	}
}

// newRebuildSearchableTask wraps a RebuildSearchableStrategy in the
// test infrastructure. Mirrors newSearchableRetokenizeTask /
// newFilterableRetokenizeTask but the strategy only carries propNames +
// generation (no targetTokenization, no bucketStrategy — rebuild is
// schema-stable). Config mirrors blockmaxSearchableTaskConfig with
// selection enabled so getPropsToReindex picks up the requested
// property even though discovery-by-strategy would also find it.
func newRebuildSearchableTask(t *testing.T, idx *Index, className, propName string) (*ShardReindexTaskGeneric, *testRebuildSearchableStrategyWrapper) {
	t.Helper()
	wrapped := &testRebuildSearchableStrategyWrapper{
		RebuildSearchableStrategy: RebuildSearchableStrategy{
			propNames:  []string{propName},
			generation: 1,
		},
	}
	task := NewShardReindexTaskGeneric(
		"RebuildSearchable", idx.logger, wrapped,
		reindexTaskConfig{
			concurrency:                   2,
			memtableOptFactor:             4,
			backupMemtableOptFactor:       1,
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
	)
	return task, wrapped
}

// testRebuildSearchableStrategyWrapper overrides OnMigrationComplete
// with a flag-setter so the test can assert completion. The real
// strategy's OnMigrationComplete is already a no-op (rebuild preserves
// the property's existing schema flags), so this wrapper is purely an
// observer. Mirrors testSearchableRetokenizeStrategyWrapper /
// testFilterableRetokenizeStrategyWrapper.
type testRebuildSearchableStrategyWrapper struct {
	RebuildSearchableStrategy
	migrationCompleted bool
}

func (s *testRebuildSearchableStrategyWrapper) OnMigrationComplete(_ context.Context, _ ShardLike) error {
	s.migrationCompleted = true
	return nil
}

// TestRecoveryConvergence_RebuildSearchable_Baseline establishes that
// the production migration code path can drive a fully-clean rebuild
// of an existing BlockMax searchable bucket on this fixture. Sanity
// check before the matrix: if this fails, every cell in the matrix
// would fail for the same root cause and the failure output here is
// far more actionable than five matrix rows failing in parallel.
func TestRecoveryConvergence_RebuildSearchable_Baseline(t *testing.T) {
	const propName = "title"
	const numObjects = 25

	ctx := testCtx()
	className := "RebuildSearchableBaseline_" + uuid.NewString()[:8]
	class := newRebuildSearchableTestClass(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	searchBucketName := helpers.BucketSearchableFromPropNameLSM(propName)
	preBucket := shard.store.Bucket(searchBucketName)
	require.NotNil(t, preBucket, "pre-migration searchable bucket must exist")
	require.Equal(t, lsmkv.StrategyInverted, preBucket.Strategy(),
		"pre-migration searchable bucket must be StrategyInverted (UsingBlockMaxWAND=true)")
	preFP := fingerprintInvertedBucket(t, preBucket)
	require.NotEmpty(t, preFP,
		"pre-migration searchable fingerprint must be non-empty (objects already inserted)")

	task, wrapped := newRebuildSearchableTask(t, idx, className, propName)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	require.True(t, wrapped.migrationCompleted,
		"OnMigrationComplete must fire post-migration")

	postBucket := shard.store.Bucket(searchBucketName)
	require.NotNil(t, postBucket, "post-migration searchable bucket must exist")
	require.Equal(t, lsmkv.StrategyInverted, postBucket.Strategy(),
		"post-migration searchable bucket must remain StrategyInverted")
	postFP := fingerprintInvertedBucket(t, postBucket)
	require.NotEmpty(t, postFP,
		"post-migration searchable fingerprint must be non-empty (objects re-tokenized)")

	// RebuildSearchable preserves tokenization (the dedicated retokenize
	// strategies handle that case). With word tokenization unchanged,
	// the pre- and post-migration term sets must match exactly.
	require.Equalf(t, len(preFP), len(postFP),
		"rebuild must preserve term count: pre=%d post=%d", len(preFP), len(postFP))
	for term, preIDs := range preFP {
		postIDs, ok := postFP[term]
		require.Truef(t, ok, "term %q present pre-migration but missing post-migration", term)
		require.Equalf(t, preIDs, postIDs,
			"term %q post-migration doc-id list diverges from pre-migration\n  pre  (%d): %v\n  post (%d): %v",
			term, len(preIDs), preIDs, len(postIDs), postIDs)
	}

	rt, err := task.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	require.True(t, rt.IsReindexed())
	require.True(t, rt.IsPrepended())
	require.True(t, rt.IsMerged())
	require.True(t, rt.IsSwapped())
	require.True(t, rt.IsTidied())
}

// TestRecoveryConvergence_RebuildSearchable_FromEachState pins the
// #240 Symptom B invariant for the RebuildSearchable strategy: from
// any on-disk state a replica could land in after a mid-migration
// restart, the recovery code path converges on bucket content
// bit-equivalent to the clean baseline run.
//
// Five sentinel states, all reached via either production code (the
// Run*OnShard trio) or — for the two atomic-method-internal states
// (IsPrepended, IsSwapped) — synthetic removal of the later sentinel
// file. Same scheme PR #11415 used for SearchableRetokenize.
func TestRecoveryConvergence_RebuildSearchable_FromEachState(t *testing.T) {
	const propName = "title"
	const numObjects = 25

	recoveryConvergenceMatrix[string]{
		namePrefix: "RebuildSearchable",
		buildClass: func(className string) *models.Class {
			return newRebuildSearchableTestClass(className, []string{propName})
		},
		seedObjects: func(t *testing.T, ctx context.Context, shard *Shard, className string) {
			for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}
		},
		buildTask: func(t *testing.T, idx *Index, className string) (*ShardReindexTaskGeneric, func() bool) {
			task, wrapped := newRebuildSearchableTask(t, idx, className, propName)
			return task, func() bool { return wrapped.migrationCompleted }
		},
		bucketName:   helpers.BucketSearchableFromPropNameLSM(propName),
		wantStrategy: lsmkv.StrategyInverted,
		fingerprint:  fingerprintInvertedBucket,
	}.run(t)
}
