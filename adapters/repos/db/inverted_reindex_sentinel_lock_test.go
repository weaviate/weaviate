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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
)

// sentinelLockProbeTracker wraps a real reindexTracker so the test can
// observe whether tokenizationOverlayMu was still held at the moment the
// per-prop sentinel fsync (markSwappedProp) ran. It is a plain test wrapper
// over the tracker interface — no production seam.
type sentinelLockProbeTracker struct {
	reindexTracker
	shard          *Shard
	markRan        atomic.Bool
	lockHeldAtMark atomic.Bool
}

func (w *sentinelLockProbeTracker) markSwappedProp(propName string) error {
	// F1 contract: the sentinel fsync must run with tokenizationOverlayMu
	// released, so it never blocks a query pinning the (bucket, tokenization)
	// pair under the read side of that lock. Probe it: if a read lock cannot
	// be acquired here, a writer still holds it — which would be the pre-fix
	// regression (fsync inside the critical section).
	if w.shard.tokenizationOverlayMu.TryRLock() {
		w.shard.tokenizationOverlayMu.RUnlock()
	} else {
		w.lockHeldAtMark.Store(true)
	}
	w.markRan.Store(true)
	return w.reindexTracker.markSwappedProp(propName)
}

// TestSwapBucketAndSetOverlay_SentinelFsyncOutsideLock pins F1: on the live
// atomic swap path, the per-prop sentinel fsync (markSwappedProp) MUST run
// AFTER tokenizationOverlayMu is released, while the bucket-pointer flip and
// the overlay set MUST run under it. The durability upgrade (fsync every
// sentinel) made markSwappedProp expensive; leaving it inside the overlay
// lock stalls every query on the migrating prop for the fsync duration
// (tens–hundreds of ms on a degraded disk), because queries pin the pair via
// Shard.PinTokenizationAndSearchableBucket's RLock.
//
// The test drives the real production wiring: maybeWirePerPropOverlaySet
// builds task.swapPropAtomic, which routes through
// Shard.SwapBucketAndSetOverlay (flip callback + post-unlock afterOverlay).
// It observes the lock state at both boundaries through the production code
// path — no test-only seam — using TryRLock as a non-blocking lock-state
// probe:
//
//   - inside the flip callback the write lock is held → TryRLock must FAIL
//   - inside markSwappedProp (the afterOverlay step) the lock is released →
//     TryRLock must SUCCEED
//
// A regression that moves the fsync back inside the critical section flips
// lockHeldAtMark to true and fails the assertion.
func TestSwapBucketAndSetOverlay_SentinelFsyncOutsideLock(t *testing.T) {
	ctx := testCtx()

	fx := setupTwoTokenizationShard(t, ctx, "SentinelFsyncOutsideLock")
	shard, idx := fx.shard, fx.idx
	className, fieldProp := fx.className, fx.fieldProp

	task := NewRuntimeSearchableRetokenizeTask(
		idx.logger, fieldProp, models.PropertyTokenizationWord,
		className, lsmkv.StrategyInverted, className, 1,
	)
	fieldBucketName := helpers.BucketSearchableFromPropNameLSM(fieldProp)
	wordBucketName := helpers.BucketSearchableFromPropNameLSM(fx.wordProp)

	// The flip callback runs inside SwapBucketAndSetOverlay's critical
	// section: a read lock must NOT be acquirable here. This override mirrors
	// the shape of the atomic-overlay proof — it performs only the pointer
	// flip (the production flip's IsSwappedProp gate is not under test).
	var flipSawLockFree atomic.Bool
	task.processOneSwapPropFn = func(ctx context.Context, store *lsmkv.Store,
		_ reindexTracker, _ int, _ string,
	) (*lsmkv.Bucket, error) {
		if shard.tokenizationOverlayMu.TryRLock() {
			shard.tokenizationOverlayMu.RUnlock()
			flipSawLockFree.Store(true)
		}
		return store.SwapBucketPointer(ctx, fieldBucketName, wordBucketName)
	}

	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		Collection:         className,
		Properties:         []string{fieldProp},
		TargetTokenization: models.PropertyTokenizationWord,
	}
	require.True(t, maybeWirePerPropOverlaySet(shard, payload, []*ShardReindexTaskGeneric{task}),
		"overlay wiring must be active for a tokenization-changing migration")

	// Real tracker (its init() creates .migrations/<name>), wrapped so the
	// deferred markSwappedProp can probe the lock state.
	realRT, err := task.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	rt := &sentinelLockProbeTracker{reindexTracker: realRT, shard: shard}
	require.False(t, rt.IsSwappedProp(fieldProp), "prop must start unswapped")

	oldBucket, err := task.swapPropAtomic(ctx, shard.store, rt, 0, fieldProp)
	require.NoError(t, err)
	require.NotNil(t, oldBucket, "swap must return the displaced old FIELD bucket")

	require.True(t, rt.markRan.Load(),
		"the per-prop sentinel fsync must run on the atomic path")
	require.False(t, rt.lockHeldAtMark.Load(),
		"F1: the sentinel fsync must run with tokenizationOverlayMu RELEASED, not inside the query-blocking critical section")
	require.False(t, flipSawLockFree.Load(),
		"the bucket-pointer flip must run UNDER tokenizationOverlayMu (atomic with the overlay set)")

	// The durable sentinel still lands — deferring the fsync must not drop it.
	require.True(t, rt.IsSwappedProp(fieldProp),
		"the per-prop sentinel must be on disk after the atomic swap")

	// And the overlay routes the FIELD prop to the WORD bucket post-swap.
	tok, bkt, release := shard.PinTokenizationAndSearchableBucket(fieldProp, models.PropertyTokenizationField)
	defer release()
	require.Equal(t, models.PropertyTokenizationWord, tok,
		"overlay must route the FIELD prop to WORD post-swap")
	require.Same(t, fx.wordBucket, bkt,
		"post-swap the FIELD prop's searchable bucket must resolve to the WORD bucket")
}
