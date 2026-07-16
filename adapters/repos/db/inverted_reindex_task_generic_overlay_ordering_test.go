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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestRuntimeSwap_Phase2a_OverlayArmedBeforeSentinel pins the Phase 2a
// per-prop ordering that keeps live queries correct across a runtime
// swap. Inside runtimeSwap's tight loop, for each property the order MUST
// be:
//
//  1. SwapBucketPointer  — in-memory pointer flip (bucket now resolves to
//     the NEW, post-swap content)
//  2. onPropSwapped      — arm the per-shard tokenization overlay so the
//     query analyzer matches the NEW bucket
//  3. markSwappedProp    — the durable per-prop sentinel fsync
//
// The overlay (step 2) MUST sit BETWEEN the flip and the fsync. If the
// fsync (step 3, single-digit ms on slow disks) runs before the overlay,
// a live query landing in that window tokenizes its input with the OLD
// analyzer against the NEW bucket and returns a transient 0-result
// answer. That is the mechanism behind the CI flake in
// TestLiveQueriesDuringChangeTokenization/reverse_field_to_word_searchable:
// the sentinel fsync sat inside the query-visible window for no reason.
//
// The test observes the ordering exclusively through the production
// onPropSwapped hook — there is no test-only production seam. At hook-fire
// time it records, per prop, (a) that the pointer flip already happened
// (the canonical bucket pointer now equals the ingest bucket, not the
// pre-swap main bucket) and (b) that the per-prop sentinel is still absent
// on disk. After the swap it asserts the sentinel landed. Together these
// pin the full observable order: pointer flip → overlay set → sentinel on
// disk.
//
// A regression that moves the overlay arm back to AFTER markSwappedProp
// (the pre-fix order) fails assertion (b): the sentinel would already be
// on disk when the overlay is armed.
func TestRuntimeSwap_Phase2a_OverlayArmedBeforeSentinel(t *testing.T) {
	ctx := testCtx()
	const numObjects = 5
	className := "TestPhase2aOverlayOrder"
	propNames := []string{"title", "description", "summary"}
	class := newTestClassWithProps(className, propNames)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeMultiPropConvergenceObjects(t, numObjects, className, propNames) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy)

	// Drive iteration + runtimePrepare only; stop before Phase 2a so we can
	// capture pre-flip bucket pointers and wire the observation hook.
	task.skipSwapOnFinish.Store(true)
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err)
		if rerunAt.IsZero() {
			break
		}
	}

	rt, err := task.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	props, err := task.readPropsToReindex(rt)
	require.NoError(t, err)
	require.NoError(t, task.runtimePrepare(ctx, task.logger, shard, rt, props))
	require.NotEmpty(t, props)

	// After prepare, every prop's ingest bucket exists and the main (source)
	// bucket is still live under its canonical name — the flip has not run.
	origMain := make(map[string]*lsmkv.Bucket, len(props))
	origIngest := make(map[string]*lsmkv.Bucket, len(props))
	for _, p := range props {
		origMain[p] = shard.store.Bucket(strategy.SourceBucketName(p))
		require.NotNilf(t, origMain[p], "main bucket for %q must be live before the swap", p)
		origIngest[p] = shard.store.Bucket(task.ingestBucketName(p))
		require.NotNilf(t, origIngest[p], "ingest bucket for %q must exist after prepare", p)
	}

	type observation struct {
		prop           string
		flipHappened   bool
		sentinelAbsent bool
	}
	// The Phase 2a loop is sequential (no goroutines), so the hook fires on
	// the calling goroutine, once per prop, in order — no synchronization
	// needed around observed.
	var observed []observation
	task.onPropSwapped = func(propName string) {
		cur := shard.store.Bucket(strategy.SourceBucketName(propName))
		observed = append(observed, observation{
			prop:           propName,
			flipHappened:   cur == origIngest[propName] && cur != origMain[propName],
			sentinelAbsent: !rt.IsSwappedProp(propName),
		})
	}

	require.NoError(t, task.runtimeSwap(ctx, task.logger, shard, rt, props))

	require.Lenf(t, observed, len(props),
		"onPropSwapped must fire exactly once per prop (%d), got %d", len(props), len(observed))
	for i, obs := range observed {
		assert.Equalf(t, props[i], obs.prop,
			"hook fired out of prop order at index %d", i)
		assert.Truef(t, obs.flipHappened,
			"prop %q: SwapBucketPointer must run BEFORE the overlay hook — the "+
				"canonical bucket pointer should already resolve to the ingest bucket "+
				"when the overlay is armed", obs.prop)
		assert.Truef(t, obs.sentinelAbsent,
			"prop %q: markSwappedProp must run AFTER the overlay hook — the per-prop "+
				"sentinel must still be absent when the overlay is armed. If this fails, "+
				"the sentinel fsync sits inside the query-visible [flip … overlay] window "+
				"(the reverse_field_to_word_searchable flake mechanism).", obs.prop)
	}
	for _, p := range props {
		assert.Truef(t, rt.IsSwappedProp(p),
			"prop %q sentinel must be on disk after runtimeSwap completes", p)
	}
}
