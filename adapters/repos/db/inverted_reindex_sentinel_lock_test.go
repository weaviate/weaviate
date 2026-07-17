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

// sentinelLockProbeTracker wraps a real reindexTracker to observe whether
// tokenizationOverlayMu is held when the per-prop sentinel fsync
// (markSwappedProp) runs.
type sentinelLockProbeTracker struct {
	reindexTracker
	shard          *Shard
	markRan        atomic.Bool
	lockHeldAtMark atomic.Bool
}

func (w *sentinelLockProbeTracker) markSwappedProp(propName string) error {
	// F1: the sentinel fsync must run with tokenizationOverlayMu released.
	// TryRLock fails here if a writer still holds it.
	if w.shard.tokenizationOverlayMu.TryRLock() {
		w.shard.tokenizationOverlayMu.RUnlock()
	} else {
		w.lockHeldAtMark.Store(true)
	}
	w.markRan.Store(true)
	return w.reindexTracker.markSwappedProp(propName)
}

// Pins F1: on the atomic swap path, the per-prop sentinel fsync must run
// after tokenizationOverlayMu is released; the bucket-pointer flip and
// overlay set must run under it.
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

	// Runs inside SwapBucketAndSetOverlay's critical section, so a read lock
	// must NOT be acquirable here. Only the pointer flip is under test.
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

	tok, bkt, release := shard.PinTokenizationAndSearchableBucket(fieldProp, models.PropertyTokenizationField)
	defer release()
	require.Equal(t, models.PropertyTokenizationWord, tok,
		"overlay must route the FIELD prop to WORD post-swap")
	require.Same(t, fx.wordBucket, bkt,
		"post-swap the FIELD prop's searchable bucket must resolve to the WORD bucket")
}
