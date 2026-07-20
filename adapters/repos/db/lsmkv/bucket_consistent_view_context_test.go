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

package lsmkv

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestConsistentViewFromCtx pins that a view attached to ctx is only ever
// handed back to the same bucket that acquired it (GH 12242).
func TestConsistentViewFromCtx(t *testing.T) {
	bucketA := &Bucket{strategy: StrategyRoaringSet}
	bucketB := &Bucket{strategy: StrategyRoaringSet}
	viewA := BucketConsistentView{Bucket: bucketA}

	t.Run("no view attached", func(t *testing.T) {
		_, ok := bucketA.consistentViewFromCtx(context.Background())
		require.False(t, ok, "a plain context must never be mistaken for carrying a view")
	})

	t.Run("view attached for the same bucket is returned", func(t *testing.T) {
		ctx := ContextWithConsistentView(context.Background(), viewA)
		got, ok := bucketA.consistentViewFromCtx(ctx)
		require.True(t, ok)
		require.Same(t, bucketA, got.Bucket)
	})

	t.Run("view attached for a different bucket is ignored, not misapplied", func(t *testing.T) {
		ctx := ContextWithConsistentView(context.Background(), viewA)
		_, ok := bucketB.consistentViewFromCtx(ctx)
		require.False(t, ok,
			"bucketB must never receive bucketA's view -- using it would apply the wrong bucket's "+
				"segment snapshot to bucketB's reads")
	})
}

// TestRoaringSetGet_ReusesContextConsistentView proves RoaringSetGet, given a
// ctx-supplied view, returns the same result as the default per-call
// GetConsistentView path (GH 12242).
func TestRoaringSetGet_ReusesContextConsistentView(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyRoaringSet),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

	require.NoError(t, b.RoaringSetAddList([]byte("k1"), []uint64{1, 2, 3}))
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.RoaringSetAddList([]byte("k1"), []uint64{4}))

	// default path: RoaringSetGet acquires its own view internally
	wantBM, wantRelease, err := b.RoaringSetGet(ctx, []byte("k1"))
	require.NoError(t, err)
	want := wantBM.ToArray()
	wantRelease()

	// ctx-supplied view path: acquire once, attach, reuse
	view := b.GetConsistentView()
	viewCtx := ContextWithConsistentView(ctx, view)
	gotBM, gotRelease, err := b.RoaringSetGet(viewCtx, []byte("k1"))
	require.NoError(t, err)
	got := gotBM.ToArray()
	gotRelease()
	view.ReleaseView()

	require.Equal(t, want, got,
		"RoaringSetGet via a caller-supplied ContextWithConsistentView must return the exact "+
			"same result as the default per-call GetConsistentView path")
}

// TestRoaringSetGet_MismatchedBucketViewFallsBackSafely proves RoaringSetGet
// falls back to its own view, not another bucket's, when ctx carries a
// mismatched one.
func TestRoaringSetGet_MismatchedBucketViewFallsBackSafely(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	bA, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyRoaringSet),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bA.Shutdown(ctx)) })
	require.NoError(t, bA.RoaringSetAddList([]byte("shared-key"), []uint64{100, 200}))
	require.NoError(t, bA.FlushAndSwitch())

	bB, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyRoaringSet),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bB.Shutdown(ctx)) })
	require.NoError(t, bB.RoaringSetAddList([]byte("shared-key"), []uint64{7, 8, 9}))
	require.NoError(t, bB.FlushAndSwitch())

	viewA := bA.GetConsistentView()
	defer viewA.ReleaseView()
	ctxWithAsView := ContextWithConsistentView(ctx, viewA)

	bm, release, err := bB.RoaringSetGet(ctxWithAsView, []byte("shared-key"))
	require.NoError(t, err)
	defer release()

	require.Equal(t, []uint64{7, 8, 9}, bm.ToArray(),
		"bucket B must return its own data, not bucket A's, when handed a context carrying "+
			"bucket A's consistent view")
}
