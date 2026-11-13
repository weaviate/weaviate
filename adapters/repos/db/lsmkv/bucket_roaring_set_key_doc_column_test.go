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
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// newFlushedKeyDocColumnBucket opens a bucket whose column holds n flushed keys,
// key%02d -> doc i, shut down on cleanup.
func newFlushedKeyDocColumnBucket(t *testing.T, ctx context.Context, n int, docIDCount uint64) (*Bucket, []string) {
	t.Helper()
	b := createTestBucketRoaringSetWithKeyDocColumn(t, ctx, t.TempDir(), docIDCount)
	t.Cleanup(func() { assert.NoError(t, b.Shutdown(ctx)) })
	names := make([]string, 0, n)
	for i := range n {
		names = append(names, fmt.Sprintf("key%02d", i))
		require.NoError(t, b.RoaringSetAddOne([]byte(names[i]), uint64(i)))
	}
	require.NoError(t, b.FlushAndSwitch())
	require.NotNil(t, b.KeyDocColumn(), "the column is what Resolve answers from")
	return b, names
}

// TestMemtableMatchesReadsFlushingBeforeActive pins the flushing memtable's layer before the active one's, absent keys nil.
func TestMemtableMatchesReadsFlushingBeforeActive(t *testing.T) {
	// the older memtable adds the document; the newer one deletes it
	flushing := memtableWith(t, nil)
	require.NoError(t, flushing.roaringSetAddOne([]byte("value"), 7))
	active := memtableWith(t, nil)
	require.NoError(t, active.roaringSetRemoveOne([]byte("value"), 7))

	// two keys, so the window is wider than one and the absent key has a slot
	keys := sortedKeysOf(t, []string{"absent", "value"})
	windowsOf := func(t *testing.T, cv BucketConsistentView) [][]roaringset.BitmapLayer {
		t.Helper()
		batch, err := newRoaringSetBatch(cv.WithoutEmptyActiveMemtable(), keys, BatchReaderWindowKeys, BatchReaderWindowBytes)
		require.NoError(t, err)
		r := &RoaringSetKeyDocColumnReader{roaringSetBatch: batch}
		from, windows, err := r.nextWindow()
		require.NoError(t, err)
		require.Zero(t, from, "the batch fits one window")
		return windows
	}

	windows := windowsOf(t, BucketConsistentView{Flushing: flushing, Active: active})
	require.Len(t, windows, 2, "both memtables hold the key")

	for i, w := range windows {
		require.Len(t, w, keys.Len(), "the window is dense over the batch")
		assert.Nil(t, w[0].Additions, "memtable %d says nothing about the absent key", i)
		assert.Nil(t, w[0].Deletions, "memtable %d says nothing about the absent key", i)
	}
	assert.True(t, windows[0][1].Additions.Contains(7),
		"first is the flushing memtable, which added it")
	assert.True(t, windows[1][1].Deletions.Contains(7),
		"second is the active memtable, whose deletion must be the one that lands")

	// an empty memtable is omitted, so position in the result is order, not identity
	windows = windowsOf(t, BucketConsistentView{Flushing: flushing, Active: memtableWith(t, nil)})
	require.Len(t, windows, 1)
	assert.True(t, windows[0][1].Additions.Contains(7))
}

// TestKeyDocColumnReaderWalksTheBatch pins nextWindow covering each key once, in order, then returning nil.
func TestKeyDocColumnReaderWalksTheBatch(t *testing.T) {
	mt := memtableWith(t, nil)
	names := make([]string, 0, 10)
	for i := range 10 {
		names = append(names, fmt.Sprintf("key%02d", i))
	}
	require.NoError(t, mt.roaringSetAddOne([]byte(names[7]), 7))
	keys := sortedKeysOf(t, names)

	t.Run("every key is covered once, in order", func(t *testing.T) {
		// a window of 4 over 10 keys: 0..3, 4..7, 8..9
		batch, err := newRoaringSetBatch(BucketConsistentView{Active: mt}.WithoutEmptyActiveMemtable(), keys, 4, BatchReaderWindowBytes)
		require.NoError(t, err)
		r := &RoaringSetKeyDocColumnReader{roaringSetBatch: batch}

		var starts, widths []int
		// bounded, so a reader that stops advancing fails instead of hanging
		for range keys.Len() + 1 {
			from, windows, err := r.nextWindow()
			require.NoError(t, err)
			if windows == nil {
				break
			}
			starts = append(starts, from)
			widths = append(widths, len(windows[0]))
		}
		assert.Equal(t, []int{0, 4, 8}, starts, "each window starts where the last ended")
		assert.Equal(t, []int{4, 4, 2}, widths, "the last window is short, not padded")

		_, windows, err := r.nextWindow()
		require.NoError(t, err)
		assert.Nil(t, windows, "a spent batch stays spent")
	})

	t.Run("a narrower window does not keep the last one's rows alive", func(t *testing.T) {
		// the window buffer must not keep a wider window's cloned rows past a narrower one's length
		require.NoError(t, mt.roaringSetAddOne([]byte(names[6]), 6))
		batch, err := newRoaringSetBatch(BucketConsistentView{Active: mt}.WithoutEmptyActiveMemtable(), keys, 4, BatchReaderWindowBytes)
		require.NoError(t, err)
		r := &RoaringSetKeyDocColumnReader{roaringSetBatch: batch}

		// windows are [0,4), [4,8), [8,10) — keys 6 and 7 sit at offsets 2 and 3
		// of the middle one, past the end of the last
		for range 2 {
			_, windows, err := r.nextWindow()
			require.NoError(t, err)
			require.NotNil(t, windows)
		}
		require.NotZero(t, r.windows[0].layer[2], "the middle window holds key 6")
		require.NotZero(t, r.windows[0].layer[3], "and key 7")

		_, windows, err := r.nextWindow()
		require.NoError(t, err)
		require.Len(t, windows[0], 2, "the last window is two keys wide")

		layer := r.windows[0].layer
		tail := layer[:cap(layer)][2:]
		for i, layer := range tail {
			assert.Zero(t, layer, "slot %d past the window still holds a row", i+2)
		}
	})

	t.Run("a bucket with nothing unflushed is done before it starts", func(t *testing.T) {
		// an untouched active memtable, as a fully flushed bucket has, is dropped from the batch
		batch, err := newRoaringSetBatch(BucketConsistentView{Active: memtableWith(t, nil)}.WithoutEmptyActiveMemtable(), keys, 4, BatchReaderWindowBytes)
		require.NoError(t, err)
		require.Zero(t, batch.mtCount, "the empty active memtable is dropped")
		r := &RoaringSetKeyDocColumnReader{roaringSetBatch: batch}

		_, windows, err := r.nextWindow()
		require.NoError(t, err)
		assert.Nil(t, windows, "no memtable contributes, so there is no window to walk")
	})
}

// cancelAfter is live for the first n Err checks and cancelled after, so a test can cancel between windows.
type cancelAfter struct {
	context.Context
	left int
}

func (c *cancelAfter) Err() error {
	if c.left <= 0 {
		return context.Canceled
	}
	c.left--
	return nil
}

// TestKeyDocColumnResolveStopsWhenCancelled pins Resolve stopping at the next window once the context is cancelled.
func TestKeyDocColumnResolveStopsWhenCancelled(t *testing.T) {
	ctx := context.Background()
	b, names := newFlushedKeyDocColumnBucket(t, ctx, 10, 100)
	// something unflushed, so there are windows to walk at all
	require.NoError(t, b.RoaringSetAddOne([]byte(names[3]), 99))

	view := b.GetConsistentRoaringSetView()
	defer view.ReleaseView()
	r, err := newRoaringSetKeyDocColumnReaderWithBounds(view.WithoutEmptyActiveMemtable(), sortedKeysOf(t, names), 4, BatchReaderWindowBytes)
	require.NoError(t, err)

	// live for the column's base and its one layer and for the first window,
	// cancelled for the second
	res, err := r.Resolve(&cancelAfter{Context: ctx, left: 3})
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, res, "a cancelled walk answers nothing")
	assert.Equal(t, 4, r.pos, "it stopped after the first window rather than walking all three")
}

// TestKeyDocColumnResolveFailsOnADocumentPastItsWidth pins the reader failing the query rather than dropping the error.
func TestKeyDocColumnResolveFailsOnADocumentPastItsWidth(t *testing.T) {
	ctx := context.Background()
	b := createTestBucketRoaringSetWithKeyDocColumn(t, ctx, t.TempDir(), 100)
	defer b.Shutdown(ctx)

	require.NoError(t, b.RoaringSetAddOne([]byte("a"), 1))
	require.NoError(t, b.FlushAndSwitch())
	require.NotNil(t, b.KeyDocColumn())
	require.NoError(t, b.RoaringSetAddOne([]byte("a"), 1<<32))

	view := b.GetConsistentRoaringSetView()
	defer view.ReleaseView()
	r, err := view.WithoutEmptyActiveMemtable().NewRoaringSetKeyDocColumnReader(sortedKeysOf(t, []string{"a"}))
	require.NoError(t, err)

	res, err := r.Resolve(ctx)
	require.ErrorContains(t, err, "docID 4294967296")
	assert.Nil(t, res)
}

// TestKeyDocColumnResolveAnswersTwice pins a second Resolve on the same reader applying the memtables again.
func TestKeyDocColumnResolveAnswersTwice(t *testing.T) {
	ctx := context.Background()
	b, names := newFlushedKeyDocColumnBucket(t, ctx, 10, 100)

	// unflushed on top: one key gains a document, another loses its only one
	require.NoError(t, b.RoaringSetAddOne([]byte(names[3]), 99))
	require.NoError(t, b.RoaringSetRemoveOne([]byte(names[6]), 6))

	view := b.GetConsistentRoaringSetView()
	defer view.ReleaseView()
	// a window narrower than the batch, so the walk has somewhere to be spent
	r, err := newRoaringSetKeyDocColumnReaderWithBounds(view.WithoutEmptyActiveMemtable(), sortedKeysOf(t, names), 4, BatchReaderWindowBytes)
	require.NoError(t, err)

	answer := func(t *testing.T) []uint64 {
		t.Helper()
		res, err := r.Resolve(ctx)
		require.NoError(t, err)
		require.NotNil(t, res)
		bm, put := res.Materialize(roaringset.NewBitmapBufPoolNoop())
		defer put()
		return bm.ToArray()
	}

	first := answer(t)
	assert.Contains(t, first, uint64(99), "the unflushed addition")
	assert.NotContains(t, first, uint64(6), "the unflushed deletion")

	assert.Equal(t, first, answer(t),
		"the same batch over the same view answers the same way")
}

// TestKeyDocColumnResolveAnswersTheView pins Resolve answering the view's column state, whatever the bucket does after.
func TestKeyDocColumnResolveAnswersTheView(t *testing.T) {
	ctx := context.Background()

	for _, tt := range []struct {
		name  string
		after func(t *testing.T, b *Bucket)
	}{
		{
			// the deletion lands after the view's memtable is flushed, so only the live column sees it
			name: "a document is deleted and flushed in between",
			after: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.FlushAndSwitch())
				require.NoError(t, b.RoaringSetRemoveOne([]byte("key03"), 3))
				require.NoError(t, b.FlushAndSwitch())
			},
		},
		{
			name: "the column is detached in between",
			after: func(t *testing.T, b *Bucket) {
				b.disk.dropKeyDocColumn()
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b, names := newFlushedKeyDocColumnBucket(t, ctx, 6, 100)

			// unflushed when the view is taken: an addition and a deletion the column does not hold
			require.NoError(t, b.RoaringSetAddOne([]byte(names[1]), 91))
			require.NoError(t, b.RoaringSetRemoveOne([]byte(names[4]), 4))

			view := b.GetConsistentRoaringSetView()
			defer view.ReleaseView()

			// whatever happens now, the view already holds what it holds
			tt.after(t, b)

			r, err := newRoaringSetKeyDocColumnReaderWithBounds(view.WithoutEmptyActiveMemtable(), sortedKeysOf(t, names), 4, BatchReaderWindowBytes)
			require.NoError(t, err)
			res, err := r.Resolve(ctx)
			require.NoError(t, err)
			require.NotNil(t, res, "a view that took a column answers from it")

			bm, put := res.Materialize(roaringset.NewBitmapBufPoolNoop())
			defer put()
			assert.Equal(t, []uint64{0, 1, 2, 3, 5, 91}, bm.ToArray(),
				"the view's documents: the unflushed addition present, the "+
					"unflushed deletion applied, and neither applied twice")
		})
	}
}

// TestKeyDocColumnResolveAcrossNarrowedWindows pins keys a window's byte budget cut off read in the next window, not skipped.
func TestKeyDocColumnResolveAcrossNarrowedWindows(t *testing.T) {
	ctx := context.Background()
	b, names := newFlushedKeyDocColumnBucket(t, ctx, 12, 100_000)

	// 4096 unflushed documents on every other key, so a budget can end a window between rows
	for i := 0; i < len(names); i += 2 {
		docs := make([]uint64, 4096)
		for d := range docs {
			docs[d] = uint64(i+1)*1_000_000 + uint64(d)*64
		}
		require.NoError(t, b.RoaringSetAddList([]byte(names[i]), docs))
	}

	keys := sortedKeysOf(t, names)
	view := b.GetConsistentRoaringSetView()
	defer view.ReleaseView()

	answer := func(t *testing.T, window, budget int) ([]uint64, RoaringSetBatchReaderStats) {
		t.Helper()
		r, err := newRoaringSetKeyDocColumnReaderWithBounds(view.WithoutEmptyActiveMemtable(), keys, window, budget)
		require.NoError(t, err)
		res, err := r.Resolve(ctx)
		require.NoError(t, err)
		require.NotNil(t, res)
		bm, put := res.Materialize(roaringset.NewBitmapBufPoolNoop())
		defer put()
		return bm.ToArray(), r.Stats()
	}

	// what one of those rows costs, so a budget can be set just above it
	oneRow := func() int {
		dst := make([]roaringset.BitmapLayer, 1)
		fill, err := view.Active.roaringSetGetWindow(keys, 0, 1, dst, math.MaxInt)
		require.NoError(t, err)
		require.Positive(t, fill.Bytes, "key00 carries an unflushed row")
		return fill.Bytes
	}()

	whole, wholeStats := answer(t, len(names), math.MaxInt)
	require.Zero(t, wholeStats.NarrowedFills, "read whole, nothing was cut short")
	require.Equal(t, 1, wholeStats.Fills, "one window covered the batch")

	for _, tt := range []struct {
		name           string
		window, budget int
	}{
		{name: "the budget ends every window after one row", window: len(names), budget: oneRow + 1},
		{name: "the budget ends a window inside a narrower one", window: 4, budget: oneRow + 1},
		{name: "a row wider than the whole budget is still answered", window: len(names), budget: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, stats := answer(t, tt.window, tt.budget)
			require.Positive(t, stats.NarrowedFills,
				"the budget must be what ended a window here, or this proves nothing")
			assert.Equal(t, whole, got,
				"a batch read in narrowed windows answers what the whole one does")
		})
	}
}
