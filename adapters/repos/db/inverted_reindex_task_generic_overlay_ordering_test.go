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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// preparedReindexFixture is a shard + task advanced through Phase 1
// (OnAfterLsmInit/Async) and runtimePrepare, so every prop's ingest bucket
// exists and the main (source) bucket is still live under its canonical
// name - the flip has not run. Shared by
// TestRuntimeSwap_Phase2a_OverlayArmedBeforeSentinel and
// TestProcessOneSwapProp_AtomicPath_LeavesSentinelAndOverlayToCaller, which
// both drive Phase 2a from this identical starting point.
type preparedReindexFixture struct {
	shard *Shard
	idx   *Index
	task  *ShardReindexTaskGeneric
	rt    reindexTracker
	props []string
}

// setupPreparedReindexFixture builds a shard for className/propNames, writes
// numObjects docs via makeMultiPropConvergenceObjects, then drives the task
// through OnAfterLsmInit/OnAfterLsmInitAsync and runtimePrepare so Phase 2a
// can run against real ingest buckets.
func setupPreparedReindexFixture(t *testing.T, ctx context.Context, className string, propNames []string, numObjects int) *preparedReindexFixture {
	t.Helper()
	class := newTestClassWithProps(className, propNames)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { _ = shard.Shutdown(ctx) })

	for _, obj := range makeMultiPropConvergenceObjects(t, numObjects, className, propNames) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy)

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

	return &preparedReindexFixture{
		shard: shard,
		idx:   idx,
		task:  task,
		rt:    rt,
		props: props,
	}
}

// TestRuntimeSwap_Phase2a_OverlayArmedBeforeSentinel pins that
// onPropSwapped arms the overlay BETWEEN the bucket-pointer flip and the
// markSwappedProp fsync - reordering it lets a live query tokenize the
// new bucket with the old analyzer for a transient 0-result answer. It
// observes ordering through the production onPropSwapped hook, not a
// test-only seam.
func TestRuntimeSwap_Phase2a_OverlayArmedBeforeSentinel(t *testing.T) {
	ctx := testCtx()
	className := "TestPhase2aOverlayOrder"
	propNames := []string{"title", "description", "summary"}

	fx := setupPreparedReindexFixture(t, ctx, className, propNames, 5)
	shard, task, rt, props := fx.shard, fx.task, fx.rt, fx.props

	// After prepare, every prop's ingest bucket exists and the main (source)
	// bucket is still live under its canonical name — the flip has not run.
	origMain := make(map[string]*lsmkv.Bucket, len(props))
	origIngest := make(map[string]*lsmkv.Bucket, len(props))
	for _, p := range props {
		origMain[p] = shard.store.Bucket(task.strategy.SourceBucketName(p))
		require.NotNilf(t, origMain[p], "main bucket for %q must be live before the swap", p)
		origIngest[p] = shard.store.Bucket(task.ingestBucketName(p))
		require.NotNilf(t, origIngest[p], "ingest bucket for %q must exist after prepare", p)
	}

	type observation struct {
		prop           string
		flipHappened   bool
		sentinelAbsent bool
	}
	// Phase 2a runs sequentially (no goroutines): the hook fires once per
	// prop, in order, on this goroutine — no lock needed around observed.
	var observed []observation
	task.onPropSwapped = func(propName string) {
		cur := shard.store.Bucket(task.strategy.SourceBucketName(propName))
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
