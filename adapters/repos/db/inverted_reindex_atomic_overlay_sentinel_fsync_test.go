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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
)

// sentinelFsyncFailTracker wraps a real *fileReindexTracker and injects a
// markSwappedProp failure for one prop, so the test exercises the actual
// on-disk sentinel-write code path (diskio.WriteFileSync et al.) for every
// OTHER call and only fails the one write under test.
type sentinelFsyncFailTracker struct {
	*fileReindexTracker
	failProp string
	failErr  error
	failed   bool
}

func (f *sentinelFsyncFailTracker) markSwappedProp(propName string) error {
	if propName == f.failProp {
		f.failed = true
		return f.failErr
	}
	return f.fileReindexTracker.markSwappedProp(propName)
}

// TestAtomicOverlaySwap_SentinelFsyncFailure_OverlayStaysConsistentWithBucket
// pins weaviate/0-weaviate-issues#323: on the LIVE (swapPropAtomic) Phase 2a
// path, a markSwappedProp sentinel-fsync failure that happens AFTER
// SwapBucketPointer has already succeeded must NOT leave the bucket pointer
// on NEW content while the tokenization overlay still reflects OLD - that
// mismatch is the silent-misroute bug: a live query tokenizes with the OLD
// analyzer against the NEW bucket's content and returns wrong (frequently
// zero) results until a restart lets crash recovery re-derive a consistent
// pair.
//
// Drives the PRODUCTION swapPropAtomic closure installed by
// [maybeWirePerPropOverlaySet] against a real on-disk reindexTracker whose
// markSwappedProp is made to fail for exactly this prop, and asserts the
// error still propagates (so the migration is marked failed and retried)
// while the shard's (bucket, overlay) pair for the prop stays CONSISTENT.
// TestAtomicOverlaySwap_SentinelFsyncFailure_PreFixOrderingWouldMisroute
// proves the fixture can actually expose the bug under the old ordering.
func TestAtomicOverlaySwap_SentinelFsyncFailure_OverlayStaysConsistentWithBucket(t *testing.T) {
	ctx := testCtx()
	fx := setupTwoTokenizationShard(t, ctx, "SentinelFsyncFailShard")
	shard := fx.shard
	fieldBucket, wordBucket := fx.fieldBucket, fx.wordBucket
	className, fieldProp := fx.className, fx.fieldProp
	phrase, validCount := fx.phrase, fx.matchDocs

	// Baseline: FIELD tokenization + FIELD bucket is the only consistent
	// pre-swap pair that finds the phrase docs.
	require.Equal(t, validCount,
		lookupCount(ctx, models.PropertyTokenizationField, fieldBucket, className, phrase))

	task := wireSimplifiedFieldToWordSwapTask(t, fx)

	rtIface, err := task.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	realRT, ok := rtIface.(*fileReindexTracker)
	require.True(t, ok, "test assumes the production file-backed tracker")
	injectedErr := errors.New("injected: sentinel fsync failure (simulated disk-full)")
	failingRT := &sentinelFsyncFailTracker{
		fileReindexTracker: realRT,
		failProp:           fieldProp,
		failErr:            injectedErr,
	}

	require.False(t, failingRT.IsSwappedProp(fieldProp),
		"sentinel must be absent before the swap attempt")

	oldBucket, swapErr := task.swapPropAtomic(ctx, shard.store, failingRT, 0, fieldProp)

	// The fsync failure must still surface: it's what marks the migration
	// failed and routes it to restart-based recovery.
	require.Error(t, swapErr, "the injected sentinel fsync failure must propagate to the caller")
	assert.ErrorIs(t, swapErr, injectedErr)
	assert.True(t, failingRT.failed, "markSwappedProp must have been invoked and failed")
	assert.NotNil(t, oldBucket, "the displaced old FIELD bucket must still be returned for Phase 2b bookkeeping")

	// THE ASSERTION: despite the error, bucket pointer and overlay must be
	// CONSISTENT - both already moved to the NEW (WORD) side, together,
	// because SwapBucketAndSetOverlay armed them as one critical section
	// BEFORE the (now-failed) sentinel write ran.
	tokPost, bktPost, releasePost := shard.PinTokenizationAndSearchableBucket(fieldProp, models.PropertyTokenizationField)
	defer releasePost()
	assert.Equal(t, models.PropertyTokenizationWord, tokPost,
		"BUG #323 regression: overlay must be armed for the FIELD prop even though markSwappedProp failed - "+
			"an unset overlay here means live queries tokenize with the OLD (FIELD) analyzer against the "+
			"NEW (WORD) bucket content")
	assert.Same(t, wordBucket, bktPost,
		"the FIELD prop's searchable bucket must already resolve to the WORD bucket "+
			"(SwapBucketPointer succeeded and is irreversible in-process)")
	assert.Equal(t, validCount, lookupCount(ctx, tokPost, bktPost, className, phrase),
		"BUG #323 regression: a live query using the shard's actual (tokenization, bucket) pair must still "+
			"find the phrase docs post-failure - a mismatched pair would tokenize wrong and miss them "+
			"(typically returning 0)")

	// The sentinel genuinely never landed (the injected fsync failed) -
	// that's the durability gap crash recovery is responsible for closing
	// on next restart, not something this call can paper over.
	assert.False(t, failingRT.IsSwappedProp(fieldProp),
		"sentinel must still be absent on disk - the fsync failure was not silently treated as success")
}

// TestAtomicOverlaySwap_SentinelFsyncFailure_PreFixOrderingWouldMisroute is
// the sensitivity check: it reproduces the PRE-FIX ordering test-side
// (markSwappedProp called INSIDE the flip callback, so its failure aborts
// SwapBucketAndSetOverlay before the overlay is armed) against the same
// fixture and injected failure, and asserts that ordering DOES produce the
// inconsistent (bucket, overlay) pair - proving the previous test isn't
// vacuously green because the fixture can never expose the bug.
func TestAtomicOverlaySwap_SentinelFsyncFailure_PreFixOrderingWouldMisroute(t *testing.T) {
	ctx := testCtx()
	fx := setupTwoTokenizationShard(t, ctx, "SentinelFsyncFailPreFixShard")
	shard := fx.shard
	wordBucket := fx.wordBucket
	className, fieldProp := fx.className, fx.fieldProp
	phrase := fx.phrase

	fieldBucketName := helpers.BucketSearchableFromPropNameLSM(fieldProp)
	wordBucketName := helpers.BucketSearchableFromPropNameLSM(fx.wordProp)
	injectedErr := errors.New("injected: sentinel fsync failure (simulated disk-full)")

	// Pre-fix shape: markSwappedProp runs INSIDE the flip callback passed to
	// SwapBucketAndSetOverlay, so its failure makes flip() return an error
	// and SwapBucketAndSetOverlay bails out BEFORE arming the overlay -
	// even though SwapBucketPointer, called first inside the same flip
	// callback, already succeeded and is irreversible in-process.
	preFixFlip := func() (*lsmkv.Bucket, error) {
		oldBucket, err := shard.store.SwapBucketPointer(ctx, fieldBucketName, wordBucketName)
		if err != nil {
			return nil, err
		}
		return oldBucket, injectedErr // markSwappedProp-equivalent failure
	}

	oldBucket, err := shard.SwapBucketAndSetOverlay(fieldProp, models.PropertyTokenizationWord, preFixFlip)
	require.Error(t, err)
	assert.ErrorIs(t, err, injectedErr)
	assert.Nil(t, oldBucket, "SwapBucketAndSetOverlay returns nil on a flip() error")

	// The pointer already flipped (SwapBucketPointer ran first, inside
	// flip(), before the injected failure) - irreversible in-process.
	require.Same(t, wordBucket, shard.store.Bucket(fieldBucketName),
		"SwapBucketPointer's effect is not rolled back by a later failure")

	tokPost, bktPost, releasePost := shard.PinTokenizationAndSearchableBucket(fieldProp, models.PropertyTokenizationField)
	defer releasePost()
	assert.NotEqual(t, models.PropertyTokenizationWord, tokPost,
		"pre-fix ordering: the overlay must NOT be armed (flip() errored before the overlay-set code ran)")
	assert.Same(t, wordBucket, bktPost,
		"pre-fix ordering: the bucket pointer is already flipped to WORD regardless")
	assert.Equal(t, 0, lookupCount(ctx, tokPost, bktPost, className, phrase),
		"pre-fix ordering: the mismatched (OLD tokenization, NEW bucket) pair must miss the phrase docs - "+
			"this is the silent-misroute bug weaviate/0-weaviate-issues#323 describes")
}

// TestProcessOneSwapProp_AtomicPath_LeavesSentinelAndOverlayToCaller pins
// the OTHER half of the #323 fix: processOneSwapProp itself (the DEFAULT
// processOneSwapPropFn, used unmodified in production) must NOT call
// rt.markSwappedProp or onPropSwapped when swapPropAtomic is wired (the
// live/atomic path) - both become the CALLER's responsibility, run AFTER
// SwapBucketAndSetOverlay's critical section (see
// [maybeWirePerPropOverlaySet]). The other two tests in this file
// substitute a hand-rolled processOneSwapPropFn and so don't exercise this
// DEFAULT-wiring guard.
//
// Drives processOneSwapProp directly (not through runtimeSwap) against
// REAL ingest/main buckets produced by a real runtimePrepare, with
// swapPropAtomic set to a non-nil sentinel value (only its nilness matters
// to the guard) and a tracker that panics if markSwappedProp is ever
// called - proving the guard fires, not just that no side effect
// happened to occur.
func TestProcessOneSwapProp_AtomicPath_LeavesSentinelAndOverlayToCaller(t *testing.T) {
	ctx := testCtx()
	className := "ProcessOneSwapPropAtomicGuard"
	propNames := []string{"title", "description"}

	// setupPreparedReindexFixture drives iteration + runtimePrepare exactly
	// like TestRuntimeSwap_Phase2a_OverlayArmedBeforeSentinel: this produces
	// REAL ingest buckets under the production-computed names, so
	// processOneSwapProp's internal store.SwapBucketPointer call (unmodified
	// production code) succeeds for real.
	fx := setupPreparedReindexFixture(t, ctx, className, propNames, 5)
	shard, task, rtIface, props := fx.shard, fx.task, fx.rt, fx.props
	targetProp := props[0]

	realRT, ok := rtIface.(*fileReindexTracker)
	require.True(t, ok, "test assumes the production file-backed tracker")
	panicIfMarkedRT := &panicOnMarkSwappedPropTracker{fileReindexTracker: realRT}

	onPropSwappedFired := false
	task.onPropSwapped = func(string) { onPropSwappedFired = true }
	// Only nilness matters to processOneSwapProp's guard - this sentinel
	// closure is never invoked directly in this test (processOneSwapProp is
	// called directly below, not through swapPropAtomic).
	task.swapPropAtomic = func(context.Context, *lsmkv.Store, reindexTracker, int, string) (*lsmkv.Bucket, error) {
		t.Fatal("swapPropAtomic itself must not be invoked by this test")
		return nil, nil
	}

	require.False(t, panicIfMarkedRT.IsSwappedProp(targetProp),
		"sentinel must be absent before the flip")

	oldBucket, err := task.processOneSwapProp(ctx, shard.store, panicIfMarkedRT, 0, targetProp)

	require.NoError(t, err, "the pointer flip itself must succeed against the real prepared buckets")
	assert.NotNil(t, oldBucket, "a fresh flip must return the displaced old main bucket")
	assert.False(t, onPropSwappedFired,
		"onPropSwapped must NOT fire on the atomic path - SwapBucketAndSetOverlay arms the overlay itself "+
			"under tokenizationOverlayMu; re-entering that lock here would deadlock in production")
	assert.False(t, panicIfMarkedRT.IsSwappedProp(targetProp),
		"BUG #323 regression: processOneSwapProp must NOT write the sentinel on the atomic path - that is "+
			"now the swapPropAtomic caller's job, run AFTER the overlay is armed (see "+
			"maybeWirePerPropOverlaySet); writing it here would put the fsync back inside "+
			"SwapBucketAndSetOverlay's critical section and reintroduce the bug")
}

// panicOnMarkSwappedPropTracker wraps a real *fileReindexTracker and panics
// if markSwappedProp is ever called - used to prove a code path does NOT
// call it, which a silently-never-called spy field could mask if the
// caller under test has an early return before the flip even runs.
type panicOnMarkSwappedPropTracker struct {
	*fileReindexTracker
}

func (f *panicOnMarkSwappedPropTracker) markSwappedProp(propName string) error {
	panic("markSwappedProp must not be called on the atomic path (propName=" + propName + ")")
}
