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

//go:build integrationTest

package inverted

import (
	"context"
	"testing"
	"unsafe"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	invnested "github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// newIdxBucket creates a temporary lsmkv store and an empty RoaringSet bucket
// for use as the _idx meta bucket in executeResolutionPlan tests.
func newIdxBucket(t *testing.T) *lsmkv.Bucket {
	t.Helper()
	logger, _ := test.NewNullLogger()
	store, err := lsmkv.New(t.TempDir(), t.TempDir(), logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { store.Shutdown(context.Background()) })

	require.NoError(t, store.CreateOrLoadBucket(context.Background(),
		"testmeta", lsmkv.WithStrategy(lsmkv.StrategyRoaringSet)))
	return store.Bucket("testmeta")
}

// writeIdx writes positions for a single array element into the meta bucket.
// positions should already have the real docID encoded (not docID=0 templates).
func writeIdx(t *testing.T, bucket *lsmkv.Bucket, path string, elemIdx int, positions []uint64) {
	t.Helper()
	require.NoError(t, bucket.RoaringSetAddList(invnested.IdxKey(path, elemIdx), positions))
}

// ---- executeResolutionPlan with idxLoopAnd integration tests ----------------

func TestExecuteResolutionPlanIdxLoopIntegration(t *testing.T) {
	const (
		doc5 = uint64(5)
		doc7 = uint64(7)
		doc9 = uint64(9)
	)

	// -------------------------------------------------------------------------
	// Preconditions requiring a real (non-nil) bucket
	// -------------------------------------------------------------------------

	t.Run("one bitmap returns MaskRootLeaf fast-path without cursor scan", func(t *testing.T) {
		ops := newLifecycleOps(t)
		bucket := newIdxBucket(t)
		// Write an idx entry that would produce a different result if scanned.
		writeIdx(t, bucket, "cars", 0, []uint64{invnested.Encode(1, 1, doc5), invnested.Encode(1, 2, doc5)})

		plan, bitmapsByPath := idxLoopPlan("cars", roaringset.NewBitmap(invnested.Encode(1, 1, doc5)))
		result, release, err := newPlanExecutor(plan, bitmapsByPath, bucket, ops, concurrency.SROAR_MERGE).execute(context.Background())
		require.NoError(t, err)
		defer release()
		// Fast-path: returns MaskRootLeaf(bitmap[0]), ignoring the bucket entirely.
		assert.Equal(t, []uint64{doc5}, result.ToArray())
	})

	t.Run("context already cancelled returns error", func(t *testing.T) {
		ops := newLifecycleOps(t)
		bucket := newIdxBucket(t)
		writeIdx(t, bucket, "cars", 0, []uint64{invnested.Encode(1, 1, doc5)})

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		plan, bitmapsByPath := idxLoopPlan("cars",
			roaringset.NewBitmap(invnested.Encode(1, 1, doc5)),
			roaringset.NewBitmap(invnested.Encode(1, 2, doc5)),
		)
		_, _, err := newPlanExecutor(plan, bitmapsByPath, bucket, ops, concurrency.SROAR_MERGE).execute(ctx)
		require.Error(t, err)
	})

	// -------------------------------------------------------------------------
	// Core same-element semantics
	// -------------------------------------------------------------------------

	t.Run("two conditions both in element 0 — doc returned", func(t *testing.T) {
		// cars = [{tires:[{width:205}], accessories:[{type:"spoiler"}]}]
		// condA = tires.width matches at root=1,leaf=1
		// condB = accessories.type matches at root=1,leaf=2
		// Both are within cars[0] → should return doc5.
		ops := newLifecycleOps(t)
		bucket := newIdxBucket(t)
		writeIdx(t, bucket, "cars", 0, []uint64{
			invnested.Encode(1, 1, doc5), // leaf=1 (tires[0].width)
			invnested.Encode(1, 2, doc5), // leaf=2 (accessories[0].type)
		})

		condA := roaringset.NewBitmap(invnested.Encode(1, 1, doc5))
		condB := roaringset.NewBitmap(invnested.Encode(1, 2, doc5))
		plan, bitmapsByPath := idxLoopPlan("cars", condA, condB)
		result, release, err := newPlanExecutor(plan, bitmapsByPath, bucket, ops, concurrency.SROAR_MERGE).execute(context.Background())
		require.NoError(t, err)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{doc5}, result.ToArray())
	})

	t.Run("two conditions in different elements — empty result", func(t *testing.T) {
		// condA matches cars[0], condB matches cars[1] — different elements.
		ops := newLifecycleOps(t)
		bucket := newIdxBucket(t)
		writeIdx(t, bucket, "cars", 0, []uint64{invnested.Encode(1, 1, doc5)})
		writeIdx(t, bucket, "cars", 1, []uint64{invnested.Encode(2, 1, doc5)})

		condA := roaringset.NewBitmap(invnested.Encode(1, 1, doc5)) // in cars[0]
		condB := roaringset.NewBitmap(invnested.Encode(2, 1, doc5)) // in cars[1]
		plan, bitmapsByPath := idxLoopPlan("cars", condA, condB)
		result, release, err := newPlanExecutor(plan, bitmapsByPath, bucket, ops, concurrency.SROAR_MERGE).execute(context.Background())
		require.NoError(t, err)
		defer release()
		requireBitmapValid(t, result)
		assert.True(t, result.IsEmpty())
	})

	t.Run("two conditions both in element 1 (not element 0) — doc returned", func(t *testing.T) {
		// Verifies that the cursor scans past element 0 to find the match in element 1.
		ops := newLifecycleOps(t)
		bucket := newIdxBucket(t)
		writeIdx(t, bucket, "cars", 0, []uint64{invnested.Encode(1, 1, doc5)}) // condA only
		writeIdx(t, bucket, "cars", 1, []uint64{
			invnested.Encode(2, 1, doc5),
			invnested.Encode(2, 2, doc5),
		})

		condA := roaringset.NewBitmap(invnested.Encode(2, 1, doc5)) // in cars[1]
		condB := roaringset.NewBitmap(invnested.Encode(2, 2, doc5)) // in cars[1]
		plan, bitmapsByPath := idxLoopPlan("cars", condA, condB)
		result, release, err := newPlanExecutor(plan, bitmapsByPath, bucket, ops, concurrency.SROAR_MERGE).execute(context.Background())
		require.NoError(t, err)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{doc5}, result.ToArray())
	})

	t.Run("three conditions all in same element — doc returned", func(t *testing.T) {
		ops := newLifecycleOps(t)
		bucket := newIdxBucket(t)
		writeIdx(t, bucket, "cars", 0, []uint64{
			invnested.Encode(1, 1, doc5),
			invnested.Encode(1, 2, doc5),
			invnested.Encode(1, 3, doc5),
		})

		condA := roaringset.NewBitmap(invnested.Encode(1, 1, doc5))
		condB := roaringset.NewBitmap(invnested.Encode(1, 2, doc5))
		condC := roaringset.NewBitmap(invnested.Encode(1, 3, doc5))
		plan, bitmapsByPath := idxLoopPlan("cars", condA, condB, condC)
		result, release, err := newPlanExecutor(plan, bitmapsByPath, bucket, ops, concurrency.SROAR_MERGE).execute(context.Background())
		require.NoError(t, err)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{doc5}, result.ToArray())
	})

	t.Run("three conditions — two in element 0, third only in element 1 — empty", func(t *testing.T) {
		ops := newLifecycleOps(t)
		bucket := newIdxBucket(t)
		writeIdx(t, bucket, "cars", 0, []uint64{
			invnested.Encode(1, 1, doc5),
			invnested.Encode(1, 2, doc5),
		})
		writeIdx(t, bucket, "cars", 1, []uint64{invnested.Encode(2, 1, doc5)})

		condA := roaringset.NewBitmap(invnested.Encode(1, 1, doc5)) // cars[0]
		condB := roaringset.NewBitmap(invnested.Encode(1, 2, doc5)) // cars[0]
		condC := roaringset.NewBitmap(invnested.Encode(2, 1, doc5)) // cars[1] only
		plan, bitmapsByPath := idxLoopPlan("cars", condA, condB, condC)
		result, release, err := newPlanExecutor(plan, bitmapsByPath, bucket, ops, concurrency.SROAR_MERGE).execute(context.Background())
		require.NoError(t, err)
		defer release()
		requireBitmapValid(t, result)
		assert.True(t, result.IsEmpty())
	})

	// -------------------------------------------------------------------------
	// Multiple documents
	// -------------------------------------------------------------------------

	t.Run("two docs — one matches same element, one has split conditions", func(t *testing.T) {
		// doc5: condA and condB both in cars[0] → match
		// doc7: condA in cars[0], condB in cars[1] → no match
		ops := newLifecycleOps(t)
		bucket := newIdxBucket(t)
		writeIdx(t, bucket, "cars", 0, []uint64{
			invnested.Encode(1, 1, doc5),
			invnested.Encode(1, 2, doc5),
			invnested.Encode(1, 1, doc7), // doc7's condA
		})
		writeIdx(t, bucket, "cars", 1, []uint64{
			invnested.Encode(2, 1, doc7), // doc7's condB (wrong element)
		})

		condA := roaringset.NewBitmap(invnested.Encode(1, 1, doc5), invnested.Encode(1, 1, doc7))
		condB := roaringset.NewBitmap(invnested.Encode(1, 2, doc5), invnested.Encode(2, 1, doc7))
		plan, bitmapsByPath := idxLoopPlan("cars", condA, condB)
		result, release, err := newPlanExecutor(plan, bitmapsByPath, bucket, ops, concurrency.SROAR_MERGE).execute(context.Background())
		require.NoError(t, err)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{doc5}, result.ToArray())
	})

	t.Run("two docs both satisfy conditions in their respective elements", func(t *testing.T) {
		ops := newLifecycleOps(t)
		bucket := newIdxBucket(t)
		writeIdx(t, bucket, "cars", 0, []uint64{
			invnested.Encode(1, 1, doc5),
			invnested.Encode(1, 2, doc5),
			invnested.Encode(1, 1, doc7),
			invnested.Encode(1, 2, doc7),
		})

		condA := roaringset.NewBitmap(invnested.Encode(1, 1, doc5), invnested.Encode(1, 1, doc7))
		condB := roaringset.NewBitmap(invnested.Encode(1, 2, doc5), invnested.Encode(1, 2, doc7))
		plan, bitmapsByPath := idxLoopPlan("cars", condA, condB)
		result, release, err := newPlanExecutor(plan, bitmapsByPath, bucket, ops, concurrency.SROAR_MERGE).execute(context.Background())
		require.NoError(t, err)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{doc5, doc7}, result.ToArray())
	})

	t.Run("three docs — only middle one satisfies same-element constraint", func(t *testing.T) {
		// doc5: condA in cars[0], condB in cars[1] → no match
		// doc7: condA and condB both in cars[0]     → match
		// doc9: condA in cars[0], condB in cars[1] → no match
		ops := newLifecycleOps(t)
		bucket := newIdxBucket(t)
		writeIdx(t, bucket, "cars", 0, []uint64{
			invnested.Encode(1, 1, doc5),
			invnested.Encode(1, 1, doc7),
			invnested.Encode(1, 2, doc7),
			invnested.Encode(1, 1, doc9),
		})
		writeIdx(t, bucket, "cars", 1, []uint64{
			invnested.Encode(2, 1, doc5),
			invnested.Encode(2, 1, doc9),
		})

		condA := roaringset.NewBitmap(invnested.Encode(1, 1, doc5), invnested.Encode(1, 1, doc7), invnested.Encode(1, 1, doc9))
		condB := roaringset.NewBitmap(invnested.Encode(2, 1, doc5), invnested.Encode(1, 2, doc7), invnested.Encode(2, 1, doc9))
		plan, bitmapsByPath := idxLoopPlan("cars", condA, condB)
		result, release, err := newPlanExecutor(plan, bitmapsByPath, bucket, ops, concurrency.SROAR_MERGE).execute(context.Background())
		require.NoError(t, err)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{doc7}, result.ToArray())
	})

	// -------------------------------------------------------------------------
	// Edge cases
	// -------------------------------------------------------------------------

	t.Run("preFilter empty — cursor never opened", func(t *testing.T) {
		// condA matches doc5, condB matches doc7 — no overlap at root+docID level,
		// so preFilter is empty and the function returns early.
		ops := newLifecycleOps(t)
		bucket := newIdxBucket(t)
		writeIdx(t, bucket, "cars", 0, []uint64{
			invnested.Encode(1, 1, doc5),
			invnested.Encode(1, 2, doc7),
		})

		condA := roaringset.NewBitmap(invnested.Encode(1, 1, doc5)) // only doc5
		condB := roaringset.NewBitmap(invnested.Encode(1, 2, doc7)) // only doc7
		plan, bitmapsByPath := idxLoopPlan("cars", condA, condB)
		result, release, err := newPlanExecutor(plan, bitmapsByPath, bucket, ops, concurrency.SROAR_MERGE).execute(context.Background())
		require.NoError(t, err)
		defer release()
		requireBitmapValid(t, result)
		assert.True(t, result.IsEmpty())
	})

	t.Run("no idx entries for path — empty result", func(t *testing.T) {
		// Bucket exists but has no _idx.cars entries.
		ops := newLifecycleOps(t)
		bucket := newIdxBucket(t)

		condA := roaringset.NewBitmap(invnested.Encode(1, 1, doc5))
		condB := roaringset.NewBitmap(invnested.Encode(1, 2, doc5))
		plan, bitmapsByPath := idxLoopPlan("cars", condA, condB)
		result, release, err := newPlanExecutor(plan, bitmapsByPath, bucket, ops, concurrency.SROAR_MERGE).execute(context.Background())
		require.NoError(t, err)
		defer release()
		requireBitmapValid(t, result)
		assert.True(t, result.IsEmpty())
	})

	t.Run("conditions match but no idx entry covers both positions — empty", func(t *testing.T) {
		// Both conditions match doc5, but the idx entry for element 0 only covers
		// condA's position — condB's position is absent from that element.
		ops := newLifecycleOps(t)
		bucket := newIdxBucket(t)
		writeIdx(t, bucket, "cars", 0, []uint64{invnested.Encode(1, 1, doc5)}) // only leaf=1

		condA := roaringset.NewBitmap(invnested.Encode(1, 1, doc5))
		condB := roaringset.NewBitmap(invnested.Encode(1, 2, doc5)) // leaf=2 not in element 0
		plan, bitmapsByPath := idxLoopPlan("cars", condA, condB)
		result, release, err := newPlanExecutor(plan, bitmapsByPath, bucket, ops, concurrency.SROAR_MERGE).execute(context.Background())
		require.NoError(t, err)
		defer release()
		requireBitmapValid(t, result)
		assert.True(t, result.IsEmpty())
	})

	t.Run("multiple elements only one contains both conditions", func(t *testing.T) {
		// Five elements, conditions only co-occur in element 3.
		ops := newLifecycleOps(t)
		bucket := newIdxBucket(t)
		for i := 0; i < 5; i++ {
			root := uint16(i + 1)
			if i == 3 {
				writeIdx(t, bucket, "cars", i, []uint64{
					invnested.Encode(root, 1, doc5),
					invnested.Encode(root, 2, doc5),
				})
			} else {
				writeIdx(t, bucket, "cars", i, []uint64{invnested.Encode(root, 1, doc5)})
			}
		}

		condA := roaringset.NewBitmap(invnested.Encode(4, 1, doc5)) // only in element 3 (root=4)
		condB := roaringset.NewBitmap(invnested.Encode(4, 2, doc5)) // only in element 3 (root=4)
		plan, bitmapsByPath := idxLoopPlan("cars", condA, condB)
		result, release, err := newPlanExecutor(plan, bitmapsByPath, bucket, ops, concurrency.SROAR_MERGE).execute(context.Background())
		require.NoError(t, err)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{doc5}, result.ToArray())
	})
}

// newTrackingPool creates a BitmapBufPoolTracking and registers a t.Cleanup
// that fails the test if any buffer remains unreleased when the (sub)test
// completes. The pool zeroes backing buffers on release so that any bitmap
// read after its release returns zeros, making premature releases visible as
// wrong values in assertions.
func newTrackingPool(t *testing.T) *roaringset.BitmapBufPoolTracking {
	t.Helper()
	pool := roaringset.NewBitmapBufPoolTracking()
	t.Cleanup(func() {
		if n := pool.Outstanding(); n != 0 {
			t.Errorf("pool: %d bitmap buffer(s) not released", n)
		}
	})
	return pool
}

// requireBitmapValid asserts that bm's backing buffer has not been zeroed by
// the tracking pool's release function. It reads n[indexNumKeys] — the second
// uint64 in bm.data — which sroar always initialises to 1 (sentinel key=0x00
// container). A zeroed buffer has 0, indicating premature release.
//
// TODO aliszka:nested_filtering replace unsafe read with bm.NumKeys() once
// that method is exposed by the sroar library.
func requireBitmapValid(t *testing.T, bm *sroar.Bitmap) {
	t.Helper()
	require.NotNil(t, bm)
	// TODO aliszka:nested_filtering remove unsafe once sroar exposes NumKeys().
	// bm.data []uint16 is the first field of sroar.Bitmap. Its backing array
	// pointer is at offset 0 of the struct. n[indexNumKeys] is at byte offset 8
	// (second uint64) within that array.
	dataPtr := *(*unsafe.Pointer)(unsafe.Pointer(bm))
	numKeys := *(*uint64)(unsafe.Pointer(uintptr(dataPtr) + 8))
	require.Positive(t, numKeys, "bitmap backing buffer is zeroed — premature release?")
}

// newLifecycleOps returns a BitmapOps backed by a tracking pool.
func newLifecycleOps(t *testing.T) *invnested.BitmapOps {
	t.Helper()
	return invnested.NewBitmapOps(newTrackingPool(t))
}
