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
	"path"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// TestKeyDocColumnDetachedWhenAFlushCannotBeAbsorbed pins the column being
// dropped when it declines a flush.
//
// The index refuses a flush whose data is not what it is for — here, values
// shared by several documents, past the point where holding them costs more than
// the structure saves. Those documents are in the new segment either way, and
// the index never reads segments, so a column left attached would answer
// ContainsAny without them from then on: results that look right with rows
// missing. Dropping it costs the property its acceleration and gives back the
// right answer.
func TestKeyDocColumnDetachedWhenAFlushCannotBeAbsorbed(t *testing.T) {
	ctx := context.Background()

	// past overflowFloor, so the flush carries more keys holding a second
	// document than the index will maintain
	const sharedValueKeys = 1025

	b := createTestBucketRoaringSetWithKeyDocColumn(t, ctx, t.TempDir(), 10_000)
	defer b.Shutdown(ctx)

	require.NoError(t, b.RoaringSetAddOne([]byte("unique"), 1))
	require.NoError(t, b.FlushAndSwitch())
	require.NotNil(t, b.KeyDocColumn(), "a flush it can absorb leaves it attached")

	for i := range sharedValueKeys {
		key := fmt.Appendf(nil, "shared%04d", i)
		require.NoError(t, b.RoaringSetAddList(key, []uint64{uint64(2 * i), uint64(2*i + 1)}))
	}
	require.NoError(t, b.FlushAndSwitch())

	assert.Nil(t, b.KeyDocColumn(),
		"a flush it could not absorb leaves it holding less than the segments do")

	// every document is still there to be read; only the shortcut to them is gone
	bm, release, err := b.RoaringSetGet(ctx, []byte("shared0007"))
	require.NoError(t, err)
	defer release()
	assert.True(t, bm.Contains(14), "both documents under a shared value must resolve")
	assert.True(t, bm.Contains(15))
}

// TestBucketOpensWithoutAColumnItCannotBuild pins that a column that cannot be
// built costs the property its acceleration and nothing else.
//
// This is the whole of the degradation contract, and it is deliberately unlike
// the rangeable representation next to it, which fails the bucket when its own
// build fails. ContainsAny returns the same documents through the standard fold
// without a column, so refusing to open would turn a missed optimisation into an
// unavailable collection. What it must not do is fall back in silence.
//
// Reached here through the one invalid combination the options can express — a
// column asked for with no bound on the document IDs to size it — which is also
// every other build failure's path out.
func TestBucketOpensWithoutAColumnItCannotBuild(t *testing.T) {
	ctx := context.Background()

	logger, hook := test.NewNullLogger()
	b := createTestBucketWithOptionsAndLogger(t, ctx, t.TempDir(), logger,
		WithStrategy(StrategyRoaringSet),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
		WithKeyDocColumn(true),
		// and no WithDocIDCountGetter
	)
	defer b.Shutdown(ctx)

	assert.Nil(t, b.KeyDocColumn(), "nothing was built")

	var warned *logrus.Entry
	for _, e := range hook.AllEntries() {
		if e.Data["action"] == "keydoccolumn_build" {
			warned = e
		}
	}
	require.NotNil(t, warned, "a property that quietly fell back is what this must not be")
	assert.Equal(t, logrus.WarnLevel, warned.Level)
	assert.Contains(t, warned.Message, "standard path")
	assert.NotEmpty(t, warned.Data["bucket"], "which property it is about")

	// and the bucket is a working roaringset bucket, before and after a flush
	require.NoError(t, b.RoaringSetAddOne([]byte("value"), 7))
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.RoaringSetAddOne([]byte("value"), 8))

	bm, release, err := b.RoaringSetGet(ctx, []byte("value"))
	require.NoError(t, err)
	defer release()
	assert.True(t, bm.Contains(7), "the flushed document")
	assert.True(t, bm.Contains(8), "and the unflushed one")
	assert.Nil(t, b.KeyDocColumn(), "a flush does not build what open could not")
}

// TestMemtableMatchesReadsFlushingBeforeActive pins the order the unflushed
// layers come back in, and that only the keys a memtable held come back at all.
//
// The caller replays them in order over an index that holds only flushed data,
// so the last one to speak about a key wins. Inverted, a document deleted in the
// active memtable would be resurrected by the older flushing memtable that still
// holds it — the right count of documents, the wrong ones, and nothing anywhere
// reporting a problem.
//
// Built from a view assembled by hand rather than by racing a real flush: the
// order is a property of the read, and a test that has to catch a bucket
// mid-flush to check it would be pinning the timing instead.
func TestMemtableMatchesReadsFlushingBeforeActive(t *testing.T) {
	logger, _ := test.NewNullLogger()
	newMT := func(t *testing.T) *Memtable {
		t.Helper()
		dir := path.Join(t.TempDir(), "mt")
		cl, err := newCommitLogger(dir, StrategyRoaringSet, 0)
		require.NoError(t, err)
		mt, err := newMemtable(cl, nil, logger, nil, memtableConfig{
			path: dir, strategy: StrategyRoaringSet,
		})
		require.NoError(t, err)
		t.Cleanup(func() { cl.close() })
		return mt
	}

	// the older memtable adds the document; the newer one deletes it
	flushing := newMT(t)
	require.NoError(t, flushing.roaringSetAddOne([]byte("value"), 7))
	active := newMT(t)
	require.NoError(t, active.roaringSetRemoveOne([]byte("value"), 7))

	// two keys, so a window wider than one is exercised and the absent key has
	// somewhere to be absent from
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

	// an empty memtable is left out entirely, so a caller cannot read position as
	// identity — only the order among those returned
	windows = windowsOf(t, BucketConsistentView{Flushing: flushing, Active: newMT(t)})
	require.Len(t, windows, 1)
	assert.True(t, windows[0][1].Additions.Contains(7))
}

// TestKeyDocColumnReaderWalksTheBatch pins the reader's own walk: each call
// covers the next window, and the batch ends by handing back nothing.
//
// The caller applies what it is given at the offset it is given, so a walk that
// repeated an offset, skipped one, or never ended would put a key's documents
// on another key or loop — none of which the window's contents would reveal.
func TestKeyDocColumnReaderWalksTheBatch(t *testing.T) {
	logger, _ := test.NewNullLogger()
	dir := path.Join(t.TempDir(), "mt")
	cl, err := newCommitLogger(dir, StrategyRoaringSet, 0)
	require.NoError(t, err)
	t.Cleanup(func() { cl.close() })
	mt, err := newMemtable(cl, nil, logger, nil, memtableConfig{
		path: dir, strategy: StrategyRoaringSet,
	})
	require.NoError(t, err)

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
		// bounded so a reader that stops advancing fails here rather than
		// looping until the suite is killed
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
		// Covers the fill both readers share. The rows a window clones are the
		// reader's whole memory cost, so a buffer that keeps a wide window's
		// clones behind a narrow one's len holds them for as long as the reader
		// lives, invisibly: every result is still right.
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
		// an active memtable nobody has written to, which is what a bucket whose
		// writes have all been flushed has; the batch drops it and is left with
		// no memtable to read
		empty := path.Join(t.TempDir(), "mt")
		emptyCL, err := newCommitLogger(empty, StrategyRoaringSet, 0)
		require.NoError(t, err)
		t.Cleanup(func() { emptyCL.close() })
		emptyMT, err := newMemtable(emptyCL, nil, logger, nil, memtableConfig{
			path: empty, strategy: StrategyRoaringSet,
		})
		require.NoError(t, err)

		batch, err := newRoaringSetBatch(BucketConsistentView{Active: emptyMT}.WithoutEmptyActiveMemtable(), keys, 4, BatchReaderWindowBytes)
		require.NoError(t, err)
		require.Zero(t, batch.mtCount, "the empty active memtable is dropped")
		r := &RoaringSetKeyDocColumnReader{roaringSetBatch: batch}

		_, windows, err := r.nextWindow()
		require.NoError(t, err)
		assert.Nil(t, windows, "no memtable contributes, so there is no window to walk")
	})
}

// cancelAfter reports a live context for the first n checks and a cancelled one
// after that, so a test can land the cancellation between two windows without
// racing a timer.
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

// TestKeyDocColumnResolveStopsWhenCancelled pins the walk giving up partway.
//
// A batch wide enough to want this reader is many windows, each taking a
// memtable's lock. Checking only before the walk would leave a cancelled query
// taking every one of them and resolving a batch nobody is waiting for — no
// wrong answer, just work and contention after the point of it.
func TestKeyDocColumnResolveStopsWhenCancelled(t *testing.T) {
	ctx := context.Background()
	b := createTestBucketRoaringSetWithKeyDocColumn(t, ctx, t.TempDir(), 100)
	defer b.Shutdown(ctx)

	names := make([]string, 0, 10)
	for i := range 10 {
		names = append(names, fmt.Sprintf("key%02d", i))
		require.NoError(t, b.RoaringSetAddOne([]byte(names[i]), uint64(i)))
	}
	require.NoError(t, b.FlushAndSwitch())
	require.NotNil(t, b.KeyDocColumn(), "the column is what Resolve answers from")
	// something unflushed, so there are windows to walk at all
	require.NoError(t, b.RoaringSetAddOne([]byte(names[3]), 99))

	view := b.GetConsistentRoaringSetView()
	defer view.ReleaseView()
	r, err := newRoaringSetKeyDocColumnReader(view.WithoutEmptyActiveMemtable(), sortedKeysOf(t, names), 4, BatchReaderWindowBytes)
	require.NoError(t, err)

	// live for the check before the walk and for the first window's, cancelled
	// for the second
	res, err := r.Resolve(&cancelAfter{Context: ctx, left: 2})
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, res, "a cancelled walk answers nothing")
	assert.Equal(t, 4, r.pos, "it stopped after the first window rather than walking all three")
}

// TestKeyDocColumnResolveAnswersTwice pins a reader answering the same batch
// again rather than answering it once.
//
// A spent cursor would resolve the column and apply no memtable over it, so
// every unflushed addition would be missing and every unflushed deletion
// unapplied — in a result that looks like any other, since nothing about it says
// which layers reached it.
func TestKeyDocColumnResolveAnswersTwice(t *testing.T) {
	ctx := context.Background()
	b := createTestBucketRoaringSetWithKeyDocColumn(t, ctx, t.TempDir(), 100)
	defer b.Shutdown(ctx)

	names := make([]string, 0, 10)
	for i := range 10 {
		names = append(names, fmt.Sprintf("key%02d", i))
		require.NoError(t, b.RoaringSetAddOne([]byte(names[i]), uint64(i)))
	}
	require.NoError(t, b.FlushAndSwitch())
	require.NotNil(t, b.KeyDocColumn())

	// unflushed on top: one key gains a document, another loses its only one
	require.NoError(t, b.RoaringSetAddOne([]byte(names[3]), 99))
	require.NoError(t, b.RoaringSetRemoveOne([]byte(names[6]), 6))

	view := b.GetConsistentRoaringSetView()
	defer view.ReleaseView()
	// a window narrower than the batch, so the walk has somewhere to be spent
	r, err := newRoaringSetKeyDocColumnReader(view.WithoutEmptyActiveMemtable(), sortedKeysOf(t, names), 4, BatchReaderWindowBytes)
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

// TestKeyDocColumnResolveAnswersTheView pins the column answering the view it
// was opened on, whatever the index does afterwards.
//
// The column is not read when the view is taken unless the view takes it: read
// later instead, it would have absorbed flushes the view's memtables no longer
// hold, and a deletion those memtables still carry would be re-imposed on a base
// that had moved past it. The answer would then be the view for the keys the
// memtables mention and something newer for the rest — no single moment, and not
// what the fold under the same view returns.
func TestKeyDocColumnResolveAnswersTheView(t *testing.T) {
	ctx := context.Background()

	for _, tt := range []struct {
		name  string
		after func(t *testing.T, b *Bucket)
	}{
		{
			// The deletion goes into a memtable created after the view, so the
			// view cannot see it and the column can: flushing first seals the
			// one the view holds. Read live, the column would apply a deletion
			// this view never knew about and answer with fewer documents than
			// the fold does under it.
			name: "a document is deleted and flushed in between",
			after: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.FlushAndSwitch())
				require.NoError(t, b.RoaringSetRemoveOne([]byte("key03"), 3))
				require.NoError(t, b.FlushAndSwitch())
			},
		},
		{
			// past the threshold at which the column folds its layers into a
			// fresh base, so the state the view holds is one the index has
			// replaced rather than merely added to
			name: "enough flushes to rebuild the column land in between",
			after: func(t *testing.T, b *Bucket) {
				for i := range 10 {
					require.NoError(t, b.RoaringSetAddOne(
						fmt.Appendf(nil, "later%02d", i), uint64(200+i)))
					require.NoError(t, b.FlushAndSwitch())
				}
			},
		},
		{
			name: "the column is detached in between",
			after: func(t *testing.T, b *Bucket) {
				b.DetachKeyDocColumn()
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := createTestBucketRoaringSetWithKeyDocColumn(t, ctx, t.TempDir(), 100)
			defer b.Shutdown(ctx)

			names := make([]string, 0, 6)
			for i := range 6 {
				names = append(names, fmt.Sprintf("key%02d", i))
				require.NoError(t, b.RoaringSetAddOne([]byte(names[i]), uint64(i)))
			}
			require.NoError(t, b.FlushAndSwitch())
			require.NotNil(t, b.KeyDocColumn())

			// unflushed when the view is taken: an addition and a deletion the
			// view's memtable carries and the column does not
			require.NoError(t, b.RoaringSetAddOne([]byte(names[1]), 91))
			require.NoError(t, b.RoaringSetRemoveOne([]byte(names[4]), 4))

			view := b.GetConsistentRoaringSetView()
			defer view.ReleaseView()

			// whatever happens now, the view already holds what it holds
			tt.after(t, b)

			r, err := newRoaringSetKeyDocColumnReader(view.WithoutEmptyActiveMemtable(), sortedKeysOf(t, names), 4, BatchReaderWindowBytes)
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

// TestKeyDocColumnResolveAcrossNarrowedWindows pins the walk coming back for the
// keys a window's byte budget cut it short of.
//
// A window ends at whichever of its two limits it reaches first, and past that
// end no memtable has written anything. Advancing by the keys the window asked
// for rather than by where it ended would step over those keys with their slots
// still zero — which reads as "no memtable holds this key" rather than "nobody
// looked", so their unflushed writes would be missing from an answer that looks
// like any other.
//
// Asserted against the same batch read whole: a budget is a limit on how much is
// copied at once, never on what is answered.
func TestKeyDocColumnResolveAcrossNarrowedWindows(t *testing.T) {
	ctx := context.Background()
	b := createTestBucketRoaringSetWithKeyDocColumn(t, ctx, t.TempDir(), 100_000)
	defer b.Shutdown(ctx)

	names := make([]string, 0, 12)
	for i := range 12 {
		names = append(names, fmt.Sprintf("key%02d", i))
		require.NoError(t, b.RoaringSetAddOne([]byte(names[i]), uint64(i)))
	}
	require.NoError(t, b.FlushAndSwitch())
	require.NotNil(t, b.KeyDocColumn(), "the column answers the flushed half")

	// Unflushed rows wide enough that a couple of them spend a budget: four
	// thousand documents sixty-four apart stay within a handful of containers, so
	// it is the count rather than the spread that makes them expensive. Every
	// other key, so the walk has to come back for keys between them too.
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
		r, err := newRoaringSetKeyDocColumnReader(view.WithoutEmptyActiveMemtable(), keys, window, budget)
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

// TestShutdownGivesTheColumnBack pins a closing segment group letting go of its
// index and taking its bytes back out of the node's total.
//
// A column is resident memory attributed to a node rather than to a bucket, so
// nothing else would ever subtract it: a bucket dropped and reopened — which a
// tenant offload or a reindex does — would leave its bytes counted forever, and
// the figure operators size a node by would only climb.
func TestShutdownGivesTheColumnBack(t *testing.T) {
	ctx := context.Background()

	b := createTestBucketRoaringSetWithKeyDocColumn(t, ctx, t.TempDir(), 100)
	require.NoError(t, b.RoaringSetAddOne([]byte("value"), 1))
	require.NoError(t, b.FlushAndSwitch())

	require.NotNil(t, b.KeyDocColumn())
	require.Positive(t, b.disk.keyDocColumnSizeReported.SizeBytes,
		"the group has told the node about it")

	require.NoError(t, b.Shutdown(ctx))

	assert.Nil(t, b.KeyDocColumn(), "the group no longer holds it")
	assert.Zero(t, b.disk.keyDocColumnSizeReported.SizeBytes,
		"and has taken back everything it added")
	// shutdown also closes the index, so a flattening in flight gives up rather
	// than rebuilding a base for a group that is gone. Not asserted here: a closed
	// index answers exactly as it did, by design, and the only difference is
	// internal to the package that owns it — where TestCloseStopsFlattening pins
	// it. Reaching it from here would mean exporting state for a test to read.
}
