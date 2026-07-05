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
	"encoding/binary"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filters"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// rangeableDocIDsAtLeast returns every docID whose value is >= v in a
// RoaringSetRange bucket, used to count the whole index (v=0 with all-positive
// data) without per-value cursor iteration.
func rangeableDocIDsAtLeast(t *testing.T, b *lsmkv.Bucket, v int64) []uint64 {
	t.Helper()
	reader := b.ReaderRoaringSetRange()
	defer reader.Close()
	lex, err := entinverted.LexicographicallySortableInt64(v)
	require.NoError(t, err)
	key := binary.BigEndian.Uint64(lex)
	bm, release, err := reader.Read(context.Background(), key, filters.OperatorGreaterThanEqual)
	require.NoError(t, err)
	if release != nil {
		defer release()
	}
	if bm == nil {
		return nil
	}
	return bm.ToArray()
}

const (
	swapWindowNumObjects = 25
	swapWindowPerValue   = swapWindowNumObjects / filterableToRangeableNumDistinctValues
)

// swapWindowScenario drives the shared lifecycle for a concurrent op landing in
// the swap window of weaviate/weaviate#11688: import 25 rangeable objects with
// NO live index (so the write depends entirely on the double-write callback),
// start the FilterableToRangeable migration, inject one op right after the
// per-prop pointer flip (ingest name unregistered, callbacks still armed), then
// swap. inject must not fail — pre-fix it paniced on the nil ingest bucket.
// run returns the post-swap rangeable bucket for the caller's assertions.
type swapWindowScenario struct {
	inject func(t *testing.T, ctx context.Context, shard *Shard, objs []*storobj.Object)
}

func (sc swapWindowScenario) run(t *testing.T) *lsmkv.Bucket {
	t.Helper()
	ctx := testCtx()
	className := "SwapWindow_" + uuid.NewString()[:8]
	class := newNoLiveIndexRangeableTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(ctx) })

	objs := makeFilterableToRangeableTestObjects(t, swapWindowNumObjects, className)
	for _, obj := range objs {
		require.NoError(t, shard.PutObject(ctx, obj))
	}
	// objs[0] carries value 0 (0 % numDistinct); the injected op targets it.

	task, _ := newFilterableToRangeableTask(t, idx, className, filterableToRangeablePropName)

	injected := false
	origSwap := task.processOneSwapPropFn
	task.processOneSwapPropFn = func(ctx context.Context, store *lsmkv.Store, rt reindexTracker, propIdx int, prop string) (*lsmkv.Bucket, error) {
		oldMain, err := origSwap(ctx, store, rt, propIdx, prop)
		if err != nil {
			return oldMain, err
		}
		sc.inject(t, ctx, shard, objs)
		injected = true
		return oldMain, nil
	}

	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	require.True(t, injected, "swap-window injection hook must have fired")

	bucket := shard.store.Bucket(helpers.BucketRangeableFromPropNameLSM(filterableToRangeablePropName))
	require.NotNil(t, bucket, "post-swap rangeable bucket must exist")
	return bucket
}

// TestReindex_ConcurrentWriteDuringSwapWindow_NotLost pins
// weaviate/weaviate#11688: a write landing between SwapBucketPointer
// (unregisters the ingest name) and disableCallbacks must not be lost —
// pre-fix the double-write callback dereferenced a nil bucket. The write is
// an UPDATE, exercising both the delete-old and add-new callback legs.
func TestReindex_ConcurrentWriteDuringSwapWindow_NotLost(t *testing.T) {
	// Outside the corpus so its posting list is unambiguously this write.
	const mark = int64(999999)
	bucket := swapWindowScenario{
		inject: func(t *testing.T, ctx context.Context, shard *Shard, objs []*storobj.Object) {
			require.NoError(t, shard.PutObject(ctx, &storobj.Object{
				MarshallerVersion: 1,
				Object: models.Object{
					ID:                 objs[0].ID(),
					Class:              string(objs[0].Class()),
					Properties:         map[string]interface{}{filterableToRangeablePropName: mark},
					CreationTimeUnix:   time.Now().UnixMilli(),
					LastUpdateTimeUnix: time.Now().UnixMilli(),
				},
			}), "live write during the swap window must not fail (pre-fix it paniced in the double-write callback)")
		},
	}.run(t)

	// add-new leg: the updated value is range-queryable at exactly its docID.
	assert.Lenf(t, readRangeableIDs(t, bucket, mark), 1,
		"#11688 swap-window: the update written during the swap window is NOT under the "+
			"target value %d — its double-write add leg was lost or paniced", mark)

	// delete-old leg: objs[0] no longer contributes to value 0.
	assert.Lenf(t, readRangeableIDs(t, bucket, 0), swapWindowPerValue-1,
		"#11688 swap-window: the update's delete-old leg did not remove the pre-value "+
			"from the index — value 0 still has the stale docID")

	// Every object present exactly once — objs[0] moved from 0 to mark.
	assert.Lenf(t, rangeableDocIDsAtLeast(t, bucket, 0), swapWindowNumObjects,
		"every object must be in the rangeable index exactly once after the swap")
}

// TestReindex_ConcurrentDeleteDuringSwapWindow_NotLost is the delete-only
// counterpart of TestReindex_ConcurrentWriteDuringSwapWindow_NotLost
// (weaviate/weaviate#11688): a mid-swap DELETE hits the same nil-bucket deref.
func TestReindex_ConcurrentDeleteDuringSwapWindow_NotLost(t *testing.T) {
	bucket := swapWindowScenario{
		inject: func(t *testing.T, ctx context.Context, shard *Shard, objs []*storobj.Object) {
			require.NoError(t, shard.DeleteObject(ctx, objs[0].ID(), time.Now()),
				"live delete during the swap window must not fail (pre-fix it paniced in the double-write delete callback)")
		},
	}.run(t)

	// The deleted object's contribution to value 0 is gone.
	assert.Lenf(t, readRangeableIDs(t, bucket, 0), swapWindowPerValue-1,
		"#11688 swap-window: the delete written during the swap window did not remove "+
			"the object from value 0 — its double-write delete leg was lost or paniced")

	// One fewer object in the whole index.
	assert.Lenf(t, rangeableDocIDsAtLeast(t, bucket, 0), swapWindowNumObjects-1,
		"exactly one object must be removed from the rangeable index after the swap-window delete")
}

// TestResolveDoubleWriteBucket_FallsBackAfterSwap is the hook-free unit test
// for resolveDoubleWriteBucket against the exact store state SwapBucketPointer
// produces (weaviate/weaviate#11688) — see that function's doc for the
// resolution rules.
func TestResolveDoubleWriteBucket_FallsBackAfterSwap(t *testing.T) {
	ctx := testCtx()
	className := "ResolveDoubleWrite_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"category"})
	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	// Synthetic names decoupled from any shard-managed bucket, so the resolver
	// (a pure store map lookup) is exercised in isolation.
	const sidecarName = "dw_resolver_ingest_sidecar"
	const canonicalName = "dw_resolver_canonical"
	require.NoError(t, shard.store.CreateOrLoadBucket(ctx, sidecarName,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet)))
	require.NoError(t, shard.store.CreateOrLoadBucket(ctx, canonicalName,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet)))

	// Pre-swap: sidecar resolves directly.
	require.Same(t, shard.store.Bucket(sidecarName),
		resolveDoubleWriteBucket(shard, sidecarName, canonicalName),
		"pre-swap the sidecar bucket must be resolved directly")

	// The production swap: canonical takes over the sidecar's physical bucket
	// and the sidecar NAME is unregistered.
	_, err := shard.store.SwapBucketPointer(ctx, canonicalName, sidecarName)
	require.NoError(t, err)
	require.Nil(t, shard.store.Bucket(sidecarName),
		"sanity: SwapBucketPointer must unregister the sidecar name")

	// Ingest-phase: sidecar gone, falls back to canonical (not nil).
	require.Same(t, shard.store.Bucket(canonicalName),
		resolveDoubleWriteBucket(shard, sidecarName, canonicalName),
		"#11688: post-swap the resolver must fall back to the canonical bucket, "+
			"not return nil (the pre-fix nil deref that paniced the write)")

	// Backup-phase: no fallback, nil is correct (no-op, not loss).
	require.Nil(t, resolveDoubleWriteBucket(shard, sidecarName, ""),
		"with no swap-fallback name the resolver must return nil (backup phase)")
}
