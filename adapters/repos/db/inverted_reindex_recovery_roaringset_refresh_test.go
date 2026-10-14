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
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Recovery-convergence matrix for RoaringSetRefresh — the same-strategy
// refresh of a filterable RoaringSet bucket (production code path
// behind `repair-filterable`). Format-only, so one RunOnShard call
// sequences the whole lifecycle.

// newRoaringSetRefreshTask wraps RoaringSetRefreshStrategy.
func newRoaringSetRefreshTask(t *testing.T, idx *Index) (*ShardReindexTaskGeneric, *roaringSetRefreshStrategyWrapper) {
	t.Helper()
	wrapped := &roaringSetRefreshStrategyWrapper{
		RoaringSetRefreshStrategy: RoaringSetRefreshStrategy{
			generation: 1,
		},
	}
	task := NewShardReindexTaskGeneric(
		"RoaringSetRefresh", idx.logger, wrapped,
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

// roaringSetRefreshStrategyWrapper overrides OnMigrationComplete with a
// flag-setter so the test can assert completion. The real strategy's
// OnMigrationComplete is already a no-op (same-strategy refresh needs
// no schema update); this wrapper is essentially an observer.
type roaringSetRefreshStrategyWrapper struct {
	RoaringSetRefreshStrategy
	migrationCompleted bool
}

func (s *roaringSetRefreshStrategyWrapper) OnMigrationComplete(_ context.Context, _ ShardLike) error {
	s.migrationCompleted = true
	return nil
}

// TestRecoveryConvergence_RoaringSetRefresh_Baseline establishes that the
// production migration code path runs end-to-end on the test fixture and
// produces a non-empty filterable-bucket fingerprint. Sanity check
// before the matrix: if this fails, every cell in the matrix would fail
// for the same root cause.
func TestRecoveryConvergence_RoaringSetRefresh_Baseline(t *testing.T) {
	const propName = "title"
	const numObjects = 25

	ctx := testCtx()
	className := "RoaringSetRefreshBaseline_" + uuid.NewString()[:8]
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
	preFP := fingerprintRoaringSetBucket(t, preBucket)
	require.NotEmpty(t, preFP,
		"pre-migration filterable fingerprint must be non-empty")

	task, wrapped := newRoaringSetRefreshTask(t, idx)
	require.NoError(t, task.RunOnShard(ctx, shard))
	require.True(t, wrapped.migrationCompleted,
		"OnMigrationComplete must fire post-migration")

	postBucket := shard.store.Bucket(filtBucketName)
	require.NotNil(t, postBucket, "post-migration filterable bucket must exist")
	require.Equal(t, lsmkv.StrategyRoaringSet, postBucket.Strategy(),
		"post-migration filterable bucket must remain StrategyRoaringSet")
	postFP := fingerprintRoaringSetBucket(t, postBucket)
	require.NotEmpty(t, postFP,
		"post-migration filterable fingerprint must be non-empty")
	// Same-strategy refresh: a clean rebuild must produce exactly the
	// same per-term posting list set as the pre-migration bucket (the
	// objects didn't change). This is the definitional invariant for a
	// "repair" strategy.
	require.Equalf(t, len(preFP), len(postFP),
		"refresh changed term count: pre=%d post=%d", len(preFP), len(postFP))
	for term, preIDs := range preFP {
		postIDs, ok := postFP[term]
		require.Truef(t, ok, "term %q present pre-migration but missing post-migration", term)
		require.Equalf(t, preIDs, postIDs,
			"term %q posting list changed across same-strategy refresh", term)
	}

	rt, err := task.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	require.True(t, rt.IsReindexed())
	require.True(t, rt.IsPrepended())
	require.True(t, rt.IsMerged())
	require.True(t, rt.IsSwapped())
	require.True(t, rt.IsTidied())
}

// TestRecoveryConvergence_RoaringSetRefresh_FromEachState pins the
// #240 Symptom B invariant for the RoaringSetRefresh strategy: from any
// on-disk state a replica could land in after a mid-migration restart,
// the recovery code path converges on filterable-bucket content
// bit-equivalent to the clean baseline run.
func TestRecoveryConvergence_RoaringSetRefresh_FromEachState(t *testing.T) {
	const propName = "title"
	const numObjects = 25

	recoveryConvergenceMatrix[string]{
		namePrefix: "RoaringSetRefresh",
		buildClass: func(className string) *models.Class {
			return newTestClassWithProps(className, []string{propName})
		},
		seedObjects: func(t *testing.T, ctx context.Context, shard *Shard, className string) {
			for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}
		},
		buildTask: func(t *testing.T, idx *Index, _ string) (*ShardReindexTaskGeneric, func() bool) {
			task, wrapped := newRoaringSetRefreshTask(t, idx)
			return task, func() bool { return wrapped.migrationCompleted }
		},
		bucketName:   helpers.BucketFromPropNameLSM(propName),
		wantStrategy: lsmkv.StrategyRoaringSet,
		fingerprint:  fingerprintRoaringSetBucket,
	}.run(t)
}
