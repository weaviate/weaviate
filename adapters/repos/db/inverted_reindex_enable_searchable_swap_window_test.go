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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/storobj"
)

// This suite pins the composition of weaviate/weaviate#11985's
// swapFallbackNamer with this PR's tally wrap in
// EnableSearchableStrategy.MakeAddCallback/MakeDeleteCallback (see that
// strategy's godoc): a write resolved through the callback's FALLBACK
// branch (resolveDoubleWriteBucket falling back to the canonical name once
// SwapBucketPointer has unregistered the ingest name) must tally
// identically to one resolved through the DIRECT branch, and postings +
// tally must converge correctly once the swap-window write's migration
// finishes. No other suite drives this composed path:
//   - inverted_reindex_write_during_swap_window_test.go exercises the
//     fallback branch, but FilterableToRangeableStrategy has no tally call
//     at all.
//   - inverted_reindex_sidecar_backfill_test.go exercises the tally, but
//     only ever through the DIRECT (sidecar-still-registered) resolution
//     branch - nothing in that suite runs Phase 2a's SwapBucketPointer
//     first.

// enableSearchableSwapWindowMarkerWord is outside sidecarBackfillTextObjects'
// 5-word dictionary, so an injected object's posting is unambiguous.
const enableSearchableSwapWindowMarkerWord = "zulu"

// sidecarBackfillTextObjectsWordSum returns the total word count across n
// objects built by sidecarBackfillTextObjects(_, n, 0) - same wordCount :=
// (i%5)+1 cycle, summed. Used to compute the exact BM25 SUM a full
// migration must converge to instead of a hardcoded literal.
func sidecarBackfillTextObjectsWordSum(n int) int {
	sum := 0
	for i := 0; i < n; i++ {
		sum += (i % 5) + 1
	}
	return sum
}

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
	shard, _, task, _, objects = newBackfilledEnableSearchableFixture(t, ctx, "EnableSearchableSwapWindow", numObjects)

	_, preCount, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)
	require.Zero(t, preCount, "sanity: unindexed prop must have no tally before the migration starts")

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

			marker := newSidecarMarkerObject(shard.Index().Config.ClassName.String(), sidecarBackfillTextProp, enableSearchableSwapWindowMarkerWord)
			require.NoError(t, shard.PutObject(ctx, marker),
				"a write resolved through the composed callback's fallback branch must not fail")

			postInjectSum, postInjectCount, _, err = shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
			require.NoError(t, err)
		},
	}

	shard, _, _ := sc.run(t, numObjects)

	assert.Equalf(t, preInjectCount+1, postInjectCount,
		"regression (weaviate/weaviate#11985 x #12221 composition): the tally COUNT must increment by exactly 1 "+
			"for a write resolved through the composed callback's FALLBACK branch, independent of which physical "+
			"bucket the postings leg landed in - got %d, want %d", postInjectCount, preInjectCount+1)
	assert.Equalf(t, preInjectSum+1, postInjectSum,
		"regression (weaviate/weaviate#11985 x #12221 composition): the tally SUM must increment by exactly the "+
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

			marker := newSidecarMarkerObject(shard.Index().Config.ClassName.String(), sidecarBackfillTextProp, enableSearchableSwapWindowMarkerWord)
			require.NoError(t, shard.PutObject(ctx, marker))
		},
	}

	shard, _, _ := sc.run(t, numObjects)

	bucket := shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM(sidecarBackfillTextProp))
	require.NotNil(t, bucket)
	fp := fingerprintInvertedBucket(t, bucket)
	assert.Lenf(t, fp[enableSearchableSwapWindowMarkerWord], 1,
		"regression (weaviate/weaviate#11985 x #12221 composition): a write landing in the swap window must "+
			"produce exactly one posting after the migration completes - not lost, not duplicated by the "+
			"post-swap recompute re-discovering it")

	sum, count, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)
	assert.Equalf(t, numObjects+1, count,
		"regression (weaviate/weaviate#11985 x #12221 composition): BM25 tally COUNT after a full migration with "+
			"a swap-window write must converge to n+1 - the post-swap recompute's ResetProperty+rescan must "+
			"supersede the callback's own mid-flight increment for this object exactly once, not double it - "+
			"got %d, want %d", count, numObjects+1)
	wantSum := sidecarBackfillTextObjectsWordSum(numObjects) + 1 // marker is 1 word ("zulu")
	assert.Equalf(t, wantSum, sum,
		"regression (weaviate/weaviate#11985 x #12221 composition): BM25 tally SUM after a full migration with a "+
			"swap-window write must converge to the pre-existing corpus' word-count sum plus the marker's 1-word "+
			"contribution, not double-counted or dropped by the post-swap recompute - got %d, want %d", sum, wantSum)
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
		"regression (weaviate/weaviate#11985 x #12221 composition): a delete landing in the swap window must "+
			"remove exactly one posting after the migration completes - every object's text contains \"alpha\", "+
			"so its posting-list length is the deletion signal - got %d entries, want %d",
		len(fp["alpha"]), numObjects-1)

	sum, count, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)
	assert.Equalf(t, numObjects-1, count,
		"regression (weaviate/weaviate#11985 x #12221 composition): BM25 tally COUNT after a full migration with "+
			"a swap-window delete must converge to n-1 - got %d, want %d", count, numObjects-1)
	wantSum := sidecarBackfillTextObjectsWordSum(numObjects) - 1 // objects[0] ("alpha") is 1 word
	assert.Equalf(t, wantSum, sum,
		"regression (weaviate/weaviate#11985 x #12221 composition): BM25 tally SUM after a full migration with a "+
			"swap-window delete must converge to the pre-existing corpus' word-count sum minus the deleted "+
			"object's 1-word contribution - got %d, want %d", sum, wantSum)
}

// directResolutionCallbackTallyDelta drives EnableSearchableStrategy through
// the reindex phase only (ingest bucket loaded, double-write callbacks
// armed, no swap yet) and returns the tally delta (sum, count) a single
// marker write produces via the callback's DIRECT resolution branch
// (resolveDoubleWriteBucket finds the ingest bucket by name - no fallback
// needed).
func directResolutionCallbackTallyDelta(t *testing.T, numObjects int, markerWord string) (deltaSum, deltaCount int) {
	t.Helper()
	ctx := testCtx()
	shard, _, task, _, _ := newBackfilledEnableSearchableFixture(t, ctx, "EnableSearchableSwapWindowDirect", numObjects)
	className := shard.Index().Config.ClassName.String()

	require.NotNilf(t, shard.store.Bucket(task.ingestBucketName(sidecarBackfillTextProp)),
		"sanity: ingest bucket must still be registered - this is the DIRECT resolution branch, not the fallback one")

	preSum, preCount, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)

	marker := newSidecarMarkerObject(className, sidecarBackfillTextProp, markerWord)
	require.NoError(t, shard.PutObject(ctx, marker),
		"a write resolved through the callback's DIRECT branch must not fail")

	postSum, postCount, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)

	// Drive the migration to completion so the fixture converges cleanly;
	// the delta this helper returns was already captured above, strictly
	// inside the reindex window.
	require.NoError(t, task.RunSwapOnShard(ctx, shard))

	return postSum - preSum, postCount - preCount
}

// (4) direct-vs-fallback tally-delta comparison: the SAME single-object
// write must produce an IDENTICAL tally delta (sum, count) whether the
// callback resolves the bucket via the DIRECT branch (ingest bucket still
// registered, pre-swap) or the FALLBACK branch (ingest name unregistered
// post-SwapBucketPointer, resolves to the canonical name instead) - the
// tally leg must not depend on which physical bucket the postings leg
// happened to land in.
func TestSidecarBackfill_EnableSearchable_DirectVsFallbackCallbackTallyDeltaIdentical(t *testing.T) {
	const numObjects = 20
	// Multi-word, unlike this file's other 1-word markers - a SUM/COUNT
	// mixup in either resolution branch would inflate SUM by more than
	// COUNT, so this test doesn't rely solely on the mid-tidy-tally
	// suite's marker elsewhere for that discrimination.
	const markerWord = "golf hotel india"

	directDeltaSum, directDeltaCount := directResolutionCallbackTallyDelta(t, numObjects, markerWord)

	var fallbackDeltaSum, fallbackDeltaCount int
	sc := enableSearchableSwapWindowScenario{
		inject: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric, objects []*storobj.Object) {
			require.Nil(t, shard.store.Bucket(task.ingestBucketName(sidecarBackfillTextProp)),
				"sanity: ingest bucket name must already be unregistered at the injection point (fallback branch)")

			preSum, preCount, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
			require.NoError(t, err)

			marker := newSidecarMarkerObject(shard.Index().Config.ClassName.String(), sidecarBackfillTextProp, markerWord)
			require.NoError(t, shard.PutObject(ctx, marker),
				"a write resolved through the composed callback's fallback branch must not fail")

			postSum, postCount, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
			require.NoError(t, err)

			fallbackDeltaSum = postSum - preSum
			fallbackDeltaCount = postCount - preCount
		},
	}
	sc.run(t, numObjects)

	assert.Equalf(t, directDeltaSum, fallbackDeltaSum,
		"regression (weaviate/weaviate#11985 x #12221 composition): the tally SUM delta from a single-object "+
			"write must be identical whether the callback resolved the bucket via the DIRECT branch (delta=%d) or "+
			"the FALLBACK branch (delta=%d) - the tally leg must not depend on which physical bucket the "+
			"postings leg landed in", directDeltaSum, fallbackDeltaSum)
	assert.Equalf(t, directDeltaCount, fallbackDeltaCount,
		"regression (weaviate/weaviate#11985 x #12221 composition): the tally COUNT delta from a single-object "+
			"write must be identical whether the callback resolved the bucket via the DIRECT branch (delta=%d) or "+
			"the FALLBACK branch (delta=%d)", directDeltaCount, fallbackDeltaCount)
}

// (5) delete-side untally through the fallback branch, observed BEFORE
// completeMigrationOnShard's post-swap recompute has a chance to run. Every
// other delete assertion in this file (test 3) checks the FINAL,
// post-recompute state - but the recompute's ResetProperty+full-rescan
// re-derives the tally straight from the objects bucket regardless of
// whether the mid-flight untrack worked, because the deleted object is
// genuinely gone from the store by the time the rescan runs. That final
// state therefore cannot distinguish a working untrackMigratingPropLength
// from a broken one; a broken untrack would be silently masked by the
// later recompute. This test captures the tally strictly inside the
// swap-window injection hook - before runtimeSwap's completeMigrationOnShard
// call has had a chance to run - so a broken untrack shows up here even
// though it would converge to the correct value eventually anyway.
func TestSidecarBackfill_EnableSearchable_DeleteUntallyThroughFallbackObservedBeforeRecompute(t *testing.T) {
	const numObjects = 20
	ctx := testCtx()

	shard, _, task, _, _ := newBackfilledEnableSearchableFixture(t, ctx, "EnableSearchableSwapWindowDeleteUntally", numObjects)
	className := shard.Index().Config.ClassName.String()

	// DIRECT-branch add: track a marker object via the callback machinery
	// while the ingest bucket is still registered, so its contribution is
	// already reflected in the tracker's incremental state before the
	// swap-window delete below.
	require.NotNilf(t, shard.store.Bucket(task.ingestBucketName(sidecarBackfillTextProp)),
		"sanity: ingest bucket must be registered for the DIRECT-branch add")
	marker := newSidecarMarkerObject(className, sidecarBackfillTextProp, enableSearchableSwapWindowMarkerWord) // 1 word
	markerID := marker.ID()

	preAddSum, preAddCount, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)

	require.NoError(t, shard.PutObject(ctx, marker))

	afterAddSum, afterAddCount, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)
	require.Equalf(t, preAddCount+1, afterAddCount,
		"sanity: DIRECT-branch add must tally the marker object exactly once before the delete-side assertion "+
			"below means anything")
	require.Equalf(t, preAddSum+1, afterAddSum,
		"sanity: DIRECT-branch add must tally the marker's 1-word contribution")

	var deleteObservedSum, deleteObservedCount int
	deleteObserved := false
	origSwap := task.processOneSwapProp
	task.processOneSwapPropFn = func(swapCtx context.Context, store *lsmkv.Store, rt reindexTracker, propIdx int, propName string) (*lsmkv.Bucket, error) {
		oldMain, err := origSwap(swapCtx, store, rt, propIdx, propName)
		if err != nil {
			return oldMain, err
		}
		require.Nilf(t, shard.store.Bucket(task.ingestBucketName(sidecarBackfillTextProp)),
			"sanity: ingest bucket name must already be unregistered at the injection point (fallback branch)")
		require.NoErrorf(t, shard.DeleteObject(ctx, markerID, time.Now()),
			"a delete resolved through the callback's FALLBACK branch must not fail")

		deleteObservedSum, deleteObservedCount, _, err = shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
		require.NoError(t, err)
		deleteObserved = true
		return oldMain, nil
	}

	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	require.True(t, deleteObserved, "swap-window delete injection hook must have fired")

	assert.Equalf(t, preAddCount, deleteObservedCount,
		"regression (weaviate/weaviate#11985 x #12221 composition): untrackMigratingPropLength must revert the "+
			"COUNT to its pre-add value IMMEDIATELY on a fallback-resolved delete, observed strictly BEFORE "+
			"completeMigrationOnShard's post-swap recompute runs - a broken untrack would only be masked by that "+
			"LATER full rescan, not caught here - got %d, want %d", deleteObservedCount, preAddCount)
	assert.Equalf(t, preAddSum, deleteObservedSum,
		"regression (weaviate/weaviate#11985 x #12221 composition): untrackMigratingPropLength must revert the "+
			"SUM to its pre-add value IMMEDIATELY on a fallback-resolved delete, observed strictly BEFORE the "+
			"post-swap recompute - got %d, want %d", deleteObservedSum, preAddSum)

	// Full post-migration convergence, matching the other tests in this
	// file: the deleted marker must not resurface, and every pre-existing
	// object must still be backfilled exactly once.
	_, finalCount, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)
	assert.Equalf(t, numObjects, finalCount,
		"post-migration convergence: the deleted marker must not appear in the final tally, and every "+
			"pre-existing object must be backfilled exactly once - got %d, want %d", finalCount, numObjects)
}
