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

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Donated by QA at the weaviate/weaviate#12221 base-sync carry check
// (stable/v1.38 pulled in weaviate/weaviate#11985's swapFallbackNamer,
// which EnableSearchableStrategy.MakeAddCallback/MakeDeleteCallback now
// compose with this PR's tally wrap - see that strategy's godoc). No
// existing test drove a write through the COMPOSED callback's fallback
// branch with a tally call chained after the postings mirror:
//   - inverted_reindex_write_during_swap_window_test.go exercises the
//     fallback branch (resolveDoubleWriteBucket falling back to the
//     canonical name SwapBucketPointer just repointed), but
//     FilterableToRangeableStrategy has no tally call at all.
//   - inverted_reindex_sidecar_backfill_test.go exercises the tally, but
//     only ever through the DIRECT (sidecar-still-registered) resolution
//     branch - nothing in that suite runs Phase 2a's SwapBucketPointer
//     first.
//
// These three tests are the only coverage of the composed behavior the
// merge resolution created.

// enableSearchableSwapWindowMarkerWord is outside sidecarBackfillTextObjects'
// 5-word dictionary, so an injected object's posting is unambiguous.
const enableSearchableSwapWindowMarkerWord = "zulu"

// enableSearchableSwapWindowScenario drives EnableSearchableStrategy's swap
// phase with an injection hook firing immediately after SwapBucketPointer
// for the migrating prop - the ingest name is already unregistered, but the
// double-write callbacks stay armed until runtimeSwap's deferred
// disableCallbacks at function exit, well after this hook returns. Mirrors
// swapWindowScenario (inverted_reindex_write_during_swap_window_test.go),
// specialized for EnableSearchable's tally-carrying callback.
type enableSearchableSwapWindowScenario struct {
	inject func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric, objects []*storobj.Object)
}

func (sc enableSearchableSwapWindowScenario) run(t *testing.T, numObjects int,
) (shard *Shard, task *ShardReindexTaskGeneric, objects []*storobj.Object) {
	t.Helper()
	ctx := testCtx()
	className := "EnableSearchableSwapWindow_" + uuid.NewString()[:8]
	vFalse, vTrue := false, true
	// Filterable=true (live index) so AnalyzeObject doesn't gate the prop
	// out of the double-write callback machinery for from-scratch writes -
	// same shape as newSidecarBackfillSearchableCallbackFixture.
	class := newSidecarBackfillTextClass(className, &vTrue, &vFalse)
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	shard = shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(ctx) })

	objects = sidecarBackfillTextObjects(className, numObjects, 0)
	for _, obj := range objects {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	_, preCount, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)
	require.Zero(t, preCount, "sanity: unindexed prop must have no tally before the migration starts")

	task, _ = newEnableSearchableTask(t, idx, className, sidecarBackfillTextProp, models.PropertyTokenizationWord)

	injected := false
	origSwap := task.processOneSwapPropFn
	task.processOneSwapPropFn = func(ctx context.Context, store *lsmkv.Store, rt reindexTracker, propIdx int, prop string) (*lsmkv.Bucket, error) {
		oldMain, err := origSwap(ctx, store, rt, propIdx, prop)
		if err != nil {
			return oldMain, err
		}
		sc.inject(t, ctx, shard, task, objects)
		injected = true
		return oldMain, nil
	}

	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	require.True(t, injected, "swap-window injection hook must have fired")

	return shard, task, objects
}

// (1) composed-callback fallback-branch tally independence: a write landing
// immediately after SwapBucketPointer resolves postings via the FALLBACK
// branch (ingest name gone, resolveDoubleWriteBucket falls back to the
// canonical name); the tally call chained after postings in MakeAddCallback
// must fire identically regardless of which physical bucket postings
// resolved to.
func TestSidecarBackfill_EnableSearchable_SwapWindowFallbackCallbackTallyIndependence(t *testing.T) {
	const numObjects = 20

	var preInjectSum, preInjectCount, postInjectSum, postInjectCount int

	sc := enableSearchableSwapWindowScenario{
		inject: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric, objects []*storobj.Object) {
			require.Nil(t, shard.store.Bucket(task.ingestBucketName(sidecarBackfillTextProp)),
				"sanity: ingest bucket name must already be unregistered at the injection point (fallback branch, "+
					"not the direct one)")

			var err error
			preInjectSum, preInjectCount, _, err = shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
			require.NoError(t, err)

			marker := &storobj.Object{
				MarshallerVersion: 1,
				Object: models.Object{
					ID:    strfmt.UUID(uuid.NewString()),
					Class: shard.Index().Config.ClassName.String(),
					Properties: map[string]interface{}{
						sidecarBackfillTextProp: enableSearchableSwapWindowMarkerWord,
					},
				},
			}
			require.NoError(t, shard.PutObject(ctx, marker),
				"a write resolved through the composed callback's fallback branch must not fail")

			postInjectSum, postInjectCount, _, err = shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
			require.NoError(t, err)
		},
	}

	shard, _, _ := sc.run(t, numObjects)

	assert.Equalf(t, preInjectCount+1, postInjectCount,
		"regression (weaviate/weaviate#12211 x #12221 composition): the tally COUNT must increment by exactly 1 "+
			"for a write resolved through the composed callback's FALLBACK branch, independent of which physical "+
			"bucket the postings leg landed in - got %d, want %d", postInjectCount, preInjectCount+1)
	assert.Equalf(t, preInjectSum+1, postInjectSum,
		"regression (weaviate/weaviate#12211 x #12221 composition): the tally SUM must increment by exactly the "+
			"marker object's 1-word contribution for a write resolved through the FALLBACK branch - got %d, want %d",
		postInjectSum, preInjectSum+1)

	bucket := shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM(sidecarBackfillTextProp))
	require.NotNil(t, bucket)
	fp := fingerprintInvertedBucket(t, bucket)
	assert.Lenf(t, fp[enableSearchableSwapWindowMarkerWord], 1,
		"the marker object's posting must be present exactly once, resolved via the fallback branch")
}

// (2) write-during-swap-window convergence (postings once + tally n+1): the
// FULL post-migration state (after completeMigrationOnShard's recompute has
// also run) must show the swap-window write's posting exactly once and the
// tally converged to n+1 - the recompute's ResetProperty wipes whatever the
// callback tallied mid-flight and the fresh rescan re-derives it from disk,
// so the object is not double-counted despite being visible to both.
func TestSidecarBackfill_EnableSearchable_SwapWindowWriteConvergesPostingsOnceAndTallyNPlusOne(t *testing.T) {
	const numObjects = 20

	sc := enableSearchableSwapWindowScenario{
		inject: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric, objects []*storobj.Object) {
			require.Nil(t, shard.store.Bucket(task.ingestBucketName(sidecarBackfillTextProp)),
				"sanity: ingest bucket name must already be unregistered at the injection point (fallback branch)")

			marker := &storobj.Object{
				MarshallerVersion: 1,
				Object: models.Object{
					ID:    strfmt.UUID(uuid.NewString()),
					Class: shard.Index().Config.ClassName.String(),
					Properties: map[string]interface{}{
						sidecarBackfillTextProp: enableSearchableSwapWindowMarkerWord,
					},
				},
			}
			require.NoError(t, shard.PutObject(ctx, marker))
		},
	}

	shard, _, _ := sc.run(t, numObjects)

	bucket := shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM(sidecarBackfillTextProp))
	require.NotNil(t, bucket)
	fp := fingerprintInvertedBucket(t, bucket)
	assert.Lenf(t, fp[enableSearchableSwapWindowMarkerWord], 1,
		"regression (weaviate/weaviate#12211 x #12221 composition): a write landing in the swap window must "+
			"produce exactly one posting after the migration completes - not lost, not duplicated by the "+
			"post-swap recompute re-discovering it")

	_, count, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)
	assert.Equalf(t, numObjects+1, count,
		"regression (weaviate/weaviate#12211 x #12221 composition): BM25 tally COUNT after a full migration with "+
			"a swap-window write must converge to n+1 - the post-swap recompute's ResetProperty+rescan must "+
			"supersede the callback's own mid-flight increment for this object exactly once, not double it - "+
			"got %d, want %d", count, numObjects+1)
}

// (3) the delete counterpart: a delete landing in the same fallback window
// must converge to n-1, not stuck at n (leaked postings) and not
// over-corrected to n-2 (double-decrement).
func TestSidecarBackfill_EnableSearchable_SwapWindowDeleteConvergesTallyNMinusOne(t *testing.T) {
	const numObjects = 20

	sc := enableSearchableSwapWindowScenario{
		inject: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric, objects []*storobj.Object) {
			require.Nil(t, shard.store.Bucket(task.ingestBucketName(sidecarBackfillTextProp)),
				"sanity: ingest bucket name must already be unregistered at the injection point (fallback branch)")

			// objects[0] is the sole 1-word object ("alpha") - every other
			// object's text also STARTS with "alpha" (sidecarBackfillTextObjects
			// cycles word-counts 1..5 by prefix), so "alpha"'s posting-list
			// LENGTH, not membership, is the deletion signal.
			require.NoError(t, shard.DeleteObject(ctx, objects[0].ID(), time.Now()),
				"a delete resolved through the composed callback's fallback branch must not fail")
		},
	}

	shard, _, _ := sc.run(t, numObjects)

	bucket := shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM(sidecarBackfillTextProp))
	require.NotNil(t, bucket)
	fp := fingerprintInvertedBucket(t, bucket)
	assert.Lenf(t, fp["alpha"], numObjects-1,
		"regression (weaviate/weaviate#12211 x #12221 composition): a delete landing in the swap window must "+
			"remove exactly one posting after the migration completes - every object's text contains \"alpha\", "+
			"so its posting-list length is the deletion signal - got %d entries, want %d",
		len(fp["alpha"]), numObjects-1)

	_, count, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)
	assert.Equalf(t, numObjects-1, count,
		"regression (weaviate/weaviate#12211 x #12221 composition): BM25 tally COUNT after a full migration with "+
			"a swap-window delete must converge to n-1 - got %d, want %d", count, numObjects-1)
}
