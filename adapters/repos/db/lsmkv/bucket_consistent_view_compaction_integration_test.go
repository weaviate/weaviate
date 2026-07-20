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

package lsmkv

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestBatchViewAcquisition_CompactionDuringHeldView guards the
// compaction/held-view lifetime contract (GH 12242): compactOnce swaps the
// merged segment in immediately regardless of a held view, while physical
// removal of the old segments is deferred to dropSegmentsAwaiting() until
// the view's refcount reaches zero.
func TestBatchViewAcquisition_CompactionDuringHeldView(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	dirName := t.TempDir()

	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyRoaringSet),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

	// never auto-flush; two explicit flushes give exactly one compactable pair
	b.SetMemtableThreshold(1e9)

	require.NoError(t, b.RoaringSetAddList([]byte("k1"), []uint64{1, 2}))
	require.NoError(t, b.FlushAndSwitch()) // segment 1

	require.NoError(t, b.RoaringSetAddList([]byte("k1"), []uint64{3}))
	require.NoError(t, b.RoaringSetAddList([]byte("k2"), []uint64{4, 5}))
	require.NoError(t, b.FlushAndSwitch()) // segment 2

	require.Len(t, b.disk.segments, 2, "test setup must produce exactly one compactable pair")

	assertReads := func(t *testing.T, ctx context.Context) {
		t.Helper()
		bm1, rel1, err := b.RoaringSetGet(ctx, []byte("k1"))
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 3}, bm1.ToArray())
		rel1()

		bm2, rel2, err := b.RoaringSetGet(ctx, []byte("k2"))
		require.NoError(t, err)
		require.Equal(t, []uint64{4, 5}, bm2.ToArray())
		rel2()
	}

	// acquire once, reuse across reads (mirrors resolveFlatEqualSlab)
	view := b.GetConsistentView()
	viewCtx := ContextWithConsistentView(ctx, view)

	// reads through the held view, before compaction runs
	assertReads(t, viewCtx)

	compacted, err := b.disk.compactOnce(ctx)
	require.NoError(t, err)
	require.True(t, compacted, "the two flushed segments must have been a valid compaction pair")

	require.Len(t, b.disk.segments, 1,
		"compaction must swap the new merged segment into the live segment list immediately, "+
			"regardless of the held view -- it must never stall waiting for a reader's refcount")
	require.Len(t, b.disk.segmentsAwaitingDrop, 2,
		"the two pre-compaction segments must be pushed to the deferred-drop queue -- "+
			"they cannot be physically removed while the held view's refcount is nonzero")

	// still-held view must keep reading the frozen pre-compaction segments
	assertReads(t, viewCtx)

	droppedWhileHeld, err := b.disk.dropSegmentsAwaiting()
	require.NoError(t, err)
	require.Zero(t, droppedWhileHeld,
		"dropSegmentsAwaiting must refuse to remove a segment with nonzero refcount "+
			"while the batch view is still held")
	require.Len(t, b.disk.segmentsAwaitingDrop, 2, "both old segments must still be waiting")

	view.ReleaseView()

	droppedAfterRelease, err := b.disk.dropSegmentsAwaiting()
	require.NoError(t, err)
	require.Equal(t, 2, droppedAfterRelease,
		"releasing the batch view must unblock the deferred drop on the next cleanup pass")
	require.Empty(t, b.disk.segmentsAwaitingDrop)

	// non-batch read path must still be correct post-cleanup
	assertReads(t, ctx)
}
