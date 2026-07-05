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

// TestReindex_ConcurrentWriteDuringSwapWindow_NotLost pins the third loss
// mechanism of weaviate/weaviate#11688: a live write landing between
// [lsmkv.Store.SwapBucketPointer] (which UNREGISTERS the ingest name) and the
// deferred disableCallbacks at the end of runtimeSwap. The property has no
// live index (IndexFilterable=false), so the write depends ENTIRELY on the
// double-write callback; pre-fix that callback resolved store.Bucket(ingestName)
// to nil and dereferenced it (lsmkv.MustBeExpectedStrategy on b.Strategy()) —
// a nil-pointer panic in the write goroutine that, through the REST stack,
// becomes an empty 200 with every inverted write of the call lost.
//
// The write is an UPDATE of an existing object, so it exercises BOTH callback
// legs in the window: the delete-old leg (removing the pre-value from the
// index) and the add-new leg. Post-fix (resolveDoubleWriteBucket) both fall
// back to the canonical bucket name — the same physical bucket the ingest name
// used to denote — and the update must be range-queryable after the migration.
func TestReindex_ConcurrentWriteDuringSwapWindow_NotLost(t *testing.T) {
	ctx := testCtx()
	const propName = filterableToRangeablePropName
	const numObjects = 25
	const perValue = numObjects / filterableToRangeableNumDistinctValues
	// Outside the corpus so its posting list is unambiguously this write.
	const mark = int64(999999)

	className := "SwapWindowUpdate_" + uuid.NewString()[:8]
	class := newNoLiveIndexRangeableTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	objs := makeFilterableToRangeableTestObjects(t, numObjects, className)
	for _, obj := range objs {
		require.NoError(t, shard.PutObject(ctx, obj))
	}
	// objs[0] carries value 0 (0 % numDistinct); we update it to mark below.

	task, _ := newFilterableToRangeableTask(t, idx, className, propName)

	// Inject the update right after the production per-prop pointer flip: the
	// ingest name is unregistered, the callbacks are still armed (their disable
	// is deferred to the end of runtimeSwap), and the migration scope is active.
	swapWindowWriteDone := false
	origSwap := task.processOneSwapPropFn
	task.processOneSwapPropFn = func(ctx context.Context, store *lsmkv.Store, rt reindexTracker, propIdx int, prop string) (*lsmkv.Bucket, error) {
		oldMain, err := origSwap(ctx, store, rt, propIdx, prop)
		if err != nil {
			return oldMain, err
		}
		require.NoError(t, shard.PutObject(ctx, &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:                 objs[0].ID(),
				Class:              className,
				Properties:         map[string]interface{}{propName: mark},
				CreationTimeUnix:   time.Now().UnixMilli(),
				LastUpdateTimeUnix: time.Now().UnixMilli(),
			},
		}), "live write during the swap window must not fail (pre-fix it paniced in the double-write callback)")
		swapWindowWriteDone = true
		return oldMain, nil
	}

	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	require.True(t, swapWindowWriteDone, "swap-window write hook must have fired")

	bucket := shard.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName))
	require.NotNil(t, bucket, "post-swap rangeable bucket must exist")

	// add-new leg: the updated value is range-queryable at exactly its docID.
	assert.Lenf(t, readRangeableIDs(t, bucket, mark), 1,
		"#11688 swap-window: the update written during the swap window is NOT under the "+
			"target value %d — its double-write add leg was lost or paniced", mark)

	// delete-old leg: objs[0] no longer contributes to value 0.
	assert.Lenf(t, readRangeableIDs(t, bucket, 0), perValue-1,
		"#11688 swap-window: the update's delete-old leg did not remove the pre-value "+
			"from the index — value 0 still has the stale docID")

	// Every object present exactly once — objs[0] moved from 0 to mark.
	assert.Lenf(t, rangeableDocIDsAtLeast(t, bucket, 0), numObjects,
		"every object must be in the rangeable index exactly once after the swap")
}

// TestReindex_ConcurrentDeleteDuringSwapWindow_NotLost is the delete-only
// counterpart of TestReindex_ConcurrentWriteDuringSwapWindow_NotLost: a mid-swap
// object DELETE routes through the same double-write callback, which had the
// same unchecked store.Bucket(ingestName) deref. Pre-fix the delete callback
// paniced on the nil bucket (deleteFromPropertyRangeBucket → b.Strategy());
// post-fix it resolves the canonical bucket and the delete's inverted removal
// lands, so the deleted object is gone from the post-migration index.
func TestReindex_ConcurrentDeleteDuringSwapWindow_NotLost(t *testing.T) {
	ctx := testCtx()
	const propName = filterableToRangeablePropName
	const numObjects = 25
	const perValue = numObjects / filterableToRangeableNumDistinctValues

	className := "SwapWindowDelete_" + uuid.NewString()[:8]
	class := newNoLiveIndexRangeableTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	objs := makeFilterableToRangeableTestObjects(t, numObjects, className)
	for _, obj := range objs {
		require.NoError(t, shard.PutObject(ctx, obj))
	}
	// objs[0] carries value 0; the backfill indexes it, then we delete it
	// inside the swap window.

	task, _ := newFilterableToRangeableTask(t, idx, className, propName)

	swapWindowDeleteDone := false
	origSwap := task.processOneSwapPropFn
	task.processOneSwapPropFn = func(ctx context.Context, store *lsmkv.Store, rt reindexTracker, propIdx int, prop string) (*lsmkv.Bucket, error) {
		oldMain, err := origSwap(ctx, store, rt, propIdx, prop)
		if err != nil {
			return oldMain, err
		}
		require.NoError(t, shard.DeleteObject(ctx, objs[0].ID(), time.Now()),
			"live delete during the swap window must not fail (pre-fix it paniced in the double-write delete callback)")
		swapWindowDeleteDone = true
		return oldMain, nil
	}

	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	require.True(t, swapWindowDeleteDone, "swap-window delete hook must have fired")

	bucket := shard.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName))
	require.NotNil(t, bucket, "post-swap rangeable bucket must exist")

	// The deleted object's contribution to value 0 is gone.
	assert.Lenf(t, readRangeableIDs(t, bucket, 0), perValue-1,
		"#11688 swap-window: the delete written during the swap window did not remove "+
			"the object from value 0 — its double-write delete leg was lost or paniced")

	// One fewer object in the whole index.
	assert.Lenf(t, rangeableDocIDsAtLeast(t, bucket, 0), numObjects-1,
		"exactly one object must be removed from the rangeable index after the swap-window delete")
}
