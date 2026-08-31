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

// The batch reader against synthetic layers and fake segments, so a case
// costs nothing to add. Coverage against a real bucket (per-key path
// parity, flushes, tombstones) is in bucket_roaring_set_test.go.

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/inverted"
)

// TestBatchReaderMatchesPerKeyReads is the differential the prefetch has to
// survive: a batch read, with each memtable consulted once per window, must
// return exactly what reading each key on its own returns. The fixtures cover
// a key held by both memtables, a key held by a memtable and not disk, and a
// document deleted while flushing and re-added in active (survives only if
// layers fold oldest first).
func TestBatchReaderMatchesPerKeyReads(t *testing.T) {
	t.Parallel()

	oneSegment := []Segment{
		newFakeRoaringSetSegment(map[string]*sroar.Bitmap{
			"aaa": bitmapFromSlice([]uint64{1, 2, 3}),
			"ccc": bitmapFromSlice([]uint64{4}),
			"eee": bitmapFromSlice([]uint64{5, 6}),
			"ggg": bitmapFromSlice([]uint64{7}),
		}),
	}

	oneSegmentFlushing := newTestMemtableRoaringSet(map[string][]uint64{
		"bbb": {10, 11},
		"ccc": {12},
		"fff": {13},
	})
	// "aaa" is deleted while flushing and re-added in active (order-sensitive);
	// "eee" is deleted and stays deleted.
	require.NoError(t, oneSegmentFlushing.roaringSetRemoveList([]byte("aaa"), []uint64{2}))
	require.NoError(t, oneSegmentFlushing.roaringSetRemoveList([]byte("eee"), []uint64{5}))
	// A write carrying no values (an empty Positions produces this upstream):
	// the per-key path sees a row, the window sees absence, and both must agree.
	require.NoError(t, oneSegmentFlushing.roaringSetAddList([]byte("ddd"), []uint64{}))

	// oldest first: bbb is added by one segment and deleted by a later one,
	// ccc is added by two, ddd only by the newest.
	manySegments := []Segment{
		newFakeRoaringSetSegment(map[string]*sroar.Bitmap{
			"aaa": bitmapFromSlice([]uint64{1, 2}),
			"bbb": bitmapFromSlice([]uint64{3}),
			"ccc": bitmapFromSlice([]uint64{4}),
		}),
		newFakeRoaringSetSegment(map[string]*sroar.Bitmap{
			"ccc": bitmapFromSlice([]uint64{5}),
			"ddd": bitmapFromSlice([]uint64{6}),
		}),
		newFakeRoaringSetSegment(map[string]*sroar.Bitmap{
			"aaa": bitmapFromSlice([]uint64{7}),
		}),
	}

	// Rows wide enough that a one-row budget ends a window, so whichever
	// memtable is read first settles where it ends.
	fatRow := func(seed uint64) []uint64 {
		docs := make([]uint64, 4096)
		for i := range docs {
			docs[i] = seed*1_000_000 + uint64(i)*64
		}
		return docs
	}
	narrowingFlushing := newTestMemtableRoaringSet(map[string][]uint64{
		"aaa": fatRow(1), "bbb": fatRow(2), "ccc": fatRow(3), "ddd": fatRow(4),
	})
	// Holds keys past where flushing's budget stops it: a window left too wide
	// would answer them from slots flushing never wrote.
	narrowingActive := newTestMemtableRoaringSet(map[string][]uint64{
		"bbb": {91}, "ddd": {92}, "eee": {93},
	})
	// Its own segment, since the fake counts reads without a lock and the
	// cases below run in parallel.
	narrowingDisk := []Segment{
		newFakeRoaringSetSegment(map[string]*sroar.Bitmap{
			"aaa": bitmapFromSlice([]uint64{1, 2, 3}),
			"ccc": bitmapFromSlice([]uint64{4}),
			"eee": bitmapFromSlice([]uint64{5, 6}),
		}),
	}
	oneFatRow := func() int {
		single := sortedKeysOf(t, []string{"aaa"})
		dst := make([]roaringset.BitmapLayer, 1)
		fill, err := narrowingFlushing.roaringSetGetWindow(single, 0, 1, dst, math.MaxInt)
		require.NoError(t, err)
		require.Positive(t, fill.Bytes)
		return fill.Bytes
	}()

	tests := []struct {
		name     string
		segments []Segment
		flushing *testMemtable
		active   *testMemtable
		batch    []string
		windows  []int
		budget   int // zero for no budget
		want     map[string][]uint64
	}{
		{
			name:     "one disk segment",
			segments: oneSegment,
			flushing: oneSegmentFlushing,
			active: newTestMemtableRoaringSet(map[string][]uint64{
				"aaa": {2},
				"ccc": {14},
				"hhh": {15},
			}),
			batch:   []string{"aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh", "iii"},
			windows: []int{1, 2, 3, 4096},
			// "aaa" is disk {1,2,3} minus the flushing deletion of 2 plus the
			// active re-add, which survives only folding oldest first.
			want: map[string][]uint64{
				"aaa": {1, 2, 3},
				"bbb": {10, 11},
				"ccc": {4, 12, 14},
				"ddd": nil,
				"eee": {6},
				"fff": {13},
				"ggg": {7},
				"hhh": {15},
				"iii": nil,
			},
		},
		{
			name:     "several disk segments",
			segments: manySegments,
			flushing: newTestMemtableRoaringSet(map[string][]uint64{"bbb": {30}, "eee": {31}}),
			active:   newTestMemtableRoaringSet(map[string][]uint64{"ccc": {40}, "fff": {41}}),
			batch:    []string{"aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg"},
			windows:  []int{1, 3, memtableWindowKeys},
			want: map[string][]uint64{
				"aaa": {1, 2, 7},
				"bbb": {3, 30},
				"ccc": {4, 5, 40},
				"ddd": {6},
				"eee": {31},
				"fff": {41},
				"ggg": nil,
			},
		},
		{
			// The narrowing memtable is read first, so what the second is asked
			// for rests on the bound it already settled. No want: the rows here
			// are thousands of documents wide; the per-key differential is what
			// this case is for.
			name:     "a narrowing memtable read before one holding keys past it",
			segments: narrowingDisk,
			flushing: narrowingFlushing,
			active:   narrowingActive,
			batch:    []string{"aaa", "bbb", "ccc", "ddd", "eee"},
			windows:  []int{5, memtableWindowKeys},
			budget:   oneFatRow + 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := &Bucket{
				strategy: StrategyRoaringSet,
				disk:     &SegmentGroup{segments: tt.segments},
				logger:   nullLogger(),
			}
			view := BucketConsistentView{
				Active: tt.active, Flushing: tt.flushing, Disk: tt.segments, Bucket: b,
			}

			for _, window := range tt.windows {
				t.Run(fmt.Sprintf("window %d", window), func(t *testing.T) {
					keys := sortedKeysOf(t, tt.batch)
					budget := tt.budget
					if budget == 0 {
						budget = math.MaxInt
					}
					r, err := newRoaringSetBatchReaderWithBounds(view, keys, window, budget)
					require.NoError(t, err)

					requireMatchesPerKey(t, b, view, r, keys, "")
					if tt.want == nil {
						return
					}
					// A second reader: one walks its batch once.
					r2, err := newRoaringSetBatchReaderWithBounds(view, keys, window, budget)
					require.NoError(t, err)
					requireRowsAre(t, r2, keys, tt.want)
				})
			}
		})
	}
}

// TestBatchReaderMatchesPerKeyReadsRandomized runs the same differential over
// random layer contents, where an off-by-one in the per-layer cursor would
// drop a match or carry one key's layer onto its neighbour. Every layer only
// adds, so each row's contents are the union of what the three hold for its
// key — an expectation independent of either read path.
func TestBatchReaderMatchesPerKeyReadsRandomized(t *testing.T) {
	t.Parallel()
	rnd := rand.New(rand.NewSource(3))

	unionOf := func(docs ...[]uint64) []uint64 {
		seen := map[uint64]struct{}{}
		for _, d := range docs {
			for _, v := range d {
				seen[v] = struct{}{}
			}
		}
		if len(seen) == 0 {
			return nil
		}
		out := make([]uint64, 0, len(seen))
		for v := range seen {
			out = append(out, v)
		}
		slices.Sort(out)
		return out
	}

	for round := 0; round < 60; round++ {
		universe := 25
		pick := func(n int) map[string][]uint64 {
			out := map[string][]uint64{}
			for _, k := range sampleDistinct(rnd, universe, n) {
				out[k] = []uint64{uint64(rnd.Intn(50))}
			}
			return out
		}
		diskDocs := pick(rnd.Intn(universe))
		flushingDocs := pick(rnd.Intn(universe))
		activeDocs := pick(rnd.Intn(universe))

		diskKeys := map[string]*sroar.Bitmap{}
		for k, v := range diskDocs {
			diskKeys[k] = bitmapFromSlice(v)
		}
		diskSeg := newFakeRoaringSetSegment(diskKeys)
		flushing := newTestMemtableRoaringSet(flushingDocs)
		active := newTestMemtableRoaringSet(activeDocs)

		b := &Bucket{
			strategy: StrategyRoaringSet,
			disk:     &SegmentGroup{segments: []Segment{diskSeg}},
			logger:   nullLogger(),
		}
		view := BucketConsistentView{Active: active, Flushing: flushing, Disk: []Segment{diskSeg}, Bucket: b}

		batch := sampleDistinct(rnd, universe, rnd.Intn(universe)+1)
		sort.Strings(batch)
		keys := sortedKeysOf(t, batch)
		window := 1 + rnd.Intn(5)

		r, err := newRoaringSetBatchReaderWithBounds(view, keys, window, math.MaxInt)
		require.NoError(t, err)
		requireMatchesPerKey(t, b, view, r, keys, fmt.Sprintf("round %d", round))

		want := map[string][]uint64{}
		for i := 0; i < keys.Len(); i++ {
			k := string(keys.At(i))
			want[k] = unionOf(diskDocs[k], flushingDocs[k], activeDocs[k])
		}
		// A second reader: the first has served every key already.
		abs, err := newRoaringSetBatchReaderWithBounds(view, keys, window, math.MaxInt)
		require.NoError(t, err)
		requireRowsAre(t, abs, keys, want)
	}
}

// TestBatchReaderSurvivesTheFoldMutatingItsRows covers what the fold does to
// every row: adopts the first as its accumulator and merges the rest into it
// in place. A window clones once for all the keys it covers, so a row the
// caller mutates must not be one the reader still needs.
func TestBatchReaderSurvivesTheFoldMutatingItsRows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		disk          map[string]*sroar.Bitmap
		flushing      map[string][]uint64
		active        map[string][]uint64
		activeDeletes map[string][]uint64
	}{
		{
			name:   "active alone",
			disk:   map[string]*sroar.Bitmap{},
			active: map[string][]uint64{"aaa": {1}, "bbb": {2}, "ccc": {3}},
		},
		{
			name:     "flushing under active",
			disk:     map[string]*sroar.Bitmap{},
			flushing: map[string][]uint64{"aaa": {7}, "ccc": {8}},
			active:   map[string][]uint64{"aaa": {1}, "bbb": {2}, "ccc": {3}},
		},
		{
			name:          "active deletions replayed over flushing",
			disk:          map[string]*sroar.Bitmap{},
			flushing:      map[string][]uint64{"aaa": {7, 8}, "ccc": {9}},
			active:        map[string][]uint64{"aaa": {1}, "bbb": {2}, "ccc": {3}},
			activeDeletes: map[string][]uint64{"aaa": {8}},
		},
		{
			// Every key has a disk row, deliberately: there the merger folds
			// onto the disk base and never hands the window's clone out.
			name: "every key has a disk row",
			disk: map[string]*sroar.Bitmap{
				"aaa": bitmapFromSlice([]uint64{5}),
				"bbb": bitmapFromSlice([]uint64{6}),
				"ccc": bitmapFromSlice([]uint64{4}),
			},
			flushing: map[string][]uint64{"aaa": {7}},
			active:   map[string][]uint64{"aaa": {1}, "bbb": {2}, "ccc": {3}},
		},
	}

	windows := []struct {
		size      int
		wantFills int // one window at the production size, two when it splits them
	}{
		{memtableWindowKeys, 1},
		{2, 2},
	}

	for _, tc := range tests {
		for _, w := range windows {
			t.Run(fmt.Sprintf("%s/window=%d", tc.name, w.size), func(t *testing.T) {
				t.Parallel()

				diskSeg := newFakeRoaringSetSegment(tc.disk)
				b := &Bucket{
					strategy: StrategyRoaringSet,
					disk:     &SegmentGroup{segments: []Segment{diskSeg}},
					logger:   nullLogger(),
				}
				view := BucketConsistentView{Disk: []Segment{diskSeg}, Bucket: b}
				mts := []*testMemtable{}
				if tc.flushing != nil {
					flushing := newTestMemtableRoaringSet(tc.flushing)
					view.Flushing = flushing
					mts = append(mts, flushing)
				}
				active := newTestMemtableRoaringSet(tc.active)
				for k, v := range tc.activeDeletes {
					require.NoError(t, active.roaringSetRemoveList([]byte(k), v))
				}
				view.Active = active
				mts = append(mts, active)

				keys := sortedKeysOf(t, []string{"aaa", "bbb", "ccc"})
				r, err := newRoaringSetBatchReaderWithBounds(view, keys, w.size, math.MaxInt)
				require.NoError(t, err)

				for i := 0; i < keys.Len(); i++ {
					want, wr, err := b.roaringSetGetFromConsistentView(view, keys.At(i), concurrency.SROAR_MERGE)
					require.NoError(t, err)
					got, gr, err := r.Next(concurrency.SROAR_MERGE)
					require.NoError(t, err)
					require.Equalf(t, docsOrNil(want), docsOrNil(got), "key %d", i)
					got.Set(9999) // what the fold does to every row it reads
					wr()
					gr()
				}

				for _, mt := range mts {
					require.Equal(t, w.wantFills, mt.roaringSetGetWindowCalls,
						"the walk must fill once per window, whatever the fold did to the rows")
				}
			})
		}
	}
}

// TestBatchReaderFillWindowError covers a memtable read that fails partway
// through a window. The window is filled one memtable at a time, so the
// failure leaves one loaded for the new window and the other still holding
// the last one — a mixture that must not be served.
func TestBatchReaderFillWindowError(t *testing.T) {
	t.Parallel()

	readErr := errors.New("memtable read failed")

	tests := []struct {
		name string
		// how far the walk gets before the failing read; window is 4 over 10
		// keys, so 0 fails the first fill and 4 fails a later one.
		readBefore int
	}{
		{name: "first window, nothing loaded yet", readBefore: 0},
		{name: "a later window, one already served", readBefore: 4},
		{name: "mid-window, no fill due", readBefore: 5},
	}

	// Failing flushing (index 0) aborts before active is read at all; failing
	// active instead leaves flushing loaded for the new window and active
	// holding the last one — the half-loaded state the invalidation exists for.
	for _, tc := range tests {
		for _, failFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/failFirst=%t", tc.name, failFirst), func(t *testing.T) {
				t.Parallel()

				diskSeg := newFakeRoaringSetSegment(map[string]*sroar.Bitmap{
					"k05": bitmapFromSlice([]uint64{55}),
				})
				b := &Bucket{
					strategy: StrategyRoaringSet,
					disk:     &SegmentGroup{segments: []Segment{diskSeg}},
					logger:   nullLogger(),
				}
				// k05 puts a row inside the second window, so a stale all-zero
				// window can't accidentally be the right answer.
				active := newTestMemtableRoaringSet(map[string][]uint64{
					"k00": {1}, "k01": {2}, "k03": {4}, "k05": {5}, "k08": {8}, "k09": {9},
				})
				view := BucketConsistentView{Active: active, Disk: []Segment{diskSeg}, Bucket: b}

				flushing := newTestMemtableRoaringSet(map[string][]uint64{"k00": {100}, "k08": {108}})
				view.Flushing = flushing
				failing := active
				if failFirst {
					failing = flushing
				}

				batch := make([]string, 10)
				for i := range batch {
					batch[i] = fmt.Sprintf("k%02d", i)
				}
				keys := sortedKeysOf(t, batch)
				r, err := newRoaringSetBatchReaderWithBounds(view, keys, 4, math.MaxInt)
				require.NoError(t, err)

				for i := 0; i < tc.readBefore; i++ {
					_, release, err := r.Next(concurrency.SROAR_MERGE)
					require.NoError(t, err)
					release()
				}

				// Fails only if this read is due a fill; mid-window it is served
				// from what is already loaded, which is equally worth pinning.
				failing.roaringSetGetWindowErr = readErr
				bm, release, err := r.Next(concurrency.SROAR_MERGE)
				if err != nil {
					require.ErrorIs(t, err, readErr)
					require.Nil(t, bm, "a failed read returns no row")
					require.Nil(t, release, "a failed read returns nothing to release")
				} else {
					require.NotNil(t, release) // served from the loaded window
					release()
				}
				failing.roaringSetGetWindowErr = nil

				// The walk resumes where it stopped; serving from the half-loaded
				// mixture would answer with a memtable's rows for the wrong keys.
				requireMatchesPerKey(t, b, view, r, keys, "after the failed fill")
			})
		}
	}
}

// TestBatchReaderFailedFillDropsCarriedRowsAndCountsItsWork covers the
// interleaving the other error tests cannot reach. They run an unlimited
// budget, so no window is ever narrowed and nothing is ever carried when the
// error arrives. Here the fat memtable fails while the thin one's rows from an
// earlier fill are still held, which is the only shape where dropping the
// window has anything to drop.
//
// The two rows differ in which memtable fails. Failing the fat one, read
// second, leaves the thin one's copy under its own lock inside the same fill,
// which that fill must still report; failing the thin one, read first, makes
// the bytes it reports the whole of what the fill copied.
func TestBatchReaderFailedFillDropsCarriedRowsAndCountsItsWork(t *testing.T) {
	t.Parallel()

	const (
		nKeys       = 24
		readBefore  = 3
		windowWidth = 8
	)
	// What the failing read reports having copied before it gave up.
	const partialCopy = 4096
	readErr := errors.New("injected memtable failure")

	tests := []struct {
		name string
		// failFirst picks the thin memtable, which the fold reads first, so no
		// other memtable copies anything in the failed fill and the bytes it
		// reports are the whole of what that fill copied.
		failFirst bool
	}{
		{name: "the memtable read first fails", failFirst: true},
		{name: "the memtable read second fails", failFirst: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			view, keys, budget, b := narrowingFixture(t, nKeys)
			failing := view.Active.(*testMemtable)
			if tc.failFirst {
				failing = view.Flushing.(*testMemtable)
			}

			r, err := newRoaringSetBatchReaderWithBounds(view, keys, windowWidth, budget)
			require.NoError(t, err)

			for i := 0; i < readBefore; i++ {
				_, release, err := r.Next(concurrency.SROAR_MERGE)
				require.NoError(t, err)
				release()
			}
			require.Positive(t, r.windows[0].carried+r.windows[1].carried,
				"the fixture must be carrying rows when the read fails, or this covers nothing")
			require.Equal(t, r.winEnd, r.pos, "the next read must be due a fill, or the error never fires")

			before := r.Stats()
			failing.roaringSetGetWindowErr = readErr
			failing.roaringSetGetWindowErrBytes = partialCopy
			bm, release, err := r.Next(concurrency.SROAR_MERGE)
			require.ErrorIs(t, err, readErr)
			require.Nil(t, bm, "a failed read returns no row")
			require.Nil(t, release, "a failed read returns nothing to release")
			failing.roaringSetGetWindowErr = nil
			after := r.Stats()

			requireNoClonesHeld(t, r)
			require.Zero(t, r.windows[0].carried+r.windows[1].carried,
				"rows dropped with the window must stop being charged against the next fill's share")

			require.Equal(t, before.Fills+1, after.Fills,
				"a fill that failed part way still acquired the locks it counted")
			require.LessOrEqual(t, after.MemtableReads, after.Fills*after.Memtables,
				"MemtableReads runs short of Fills times Memtables, never past it")

			copied := after.BytesCopied - before.BytesCopied
			if tc.failFirst {
				require.Equal(t, partialCopy, copied,
					"the rows copied before the read gave up are the whole of what this fill copied")
			} else {
				require.Greater(t, copied, partialCopy,
					"the memtable read before the failure copied under its lock too, and that is work the fill did")
			}

			// The walk resumes where it stopped; a kept row would answer a later
			// key with the row an abandoned window had loaded for it.
			requireMatchesPerKey(t, b, view, r, keys, "after the failed fill")
		})
	}
}

// TestBatchReaderFailedFillIsNotNarrowed pins that a fill which failed counts
// as no kind of ending.
//
// It needs a memtable that narrows the window and then a later one that fails:
// that is the only order in which reading the narrowed end and asking the
// caller give different answers.
func TestBatchReaderFailedFillIsNotNarrowed(t *testing.T) {
	t.Parallel()

	const (
		nKeys       = 8
		windowWidth = 8
		narrowsAt   = 4
	)
	readErr := errors.New("injected memtable failure")

	names := make([]string, nKeys)
	for i := range names {
		names[i] = fmt.Sprintf("k%02d", i)
	}
	keys := sortedKeysOf(t, names)

	// Read first, and given a share that covers exactly narrowsAt of its rows,
	// so it settles the window short of the width the fill opened at.
	narrowing := newTestMemtableRoaringSet(nil)
	for i, n := range names {
		require.NoError(t, narrowing.roaringSetAddList([]byte(n), []uint64{uint64(100 + i)}))
	}
	// Read second, and made to fail, so the window is already narrowed when the
	// fill gives up.
	failing := newTestMemtableRoaringSet(nil)
	for i, n := range names {
		require.NoError(t, failing.roaringSetAddList([]byte(n), []uint64{uint64(200 + i)}))
	}

	oneRow := make([]roaringset.BitmapLayer, 1)
	priced, err := narrowing.roaringSetGetWindow(keys, 0, 1, oneRow, math.MaxInt)
	require.NoError(t, err)
	narrowing.ranges = nil
	// Two memtables split the budget, so this leaves each a share of exactly
	// narrowsAt rows.
	budget := 2 * narrowsAt * priced.Bytes

	b := &Bucket{strategy: StrategyRoaringSet, disk: &SegmentGroup{}, logger: nullLogger()}
	view := BucketConsistentView{Active: failing, Flushing: narrowing, Bucket: b}

	r, err := newRoaringSetBatchReaderWithBounds(view, keys, windowWidth, budget)
	require.NoError(t, err)
	failing.roaringSetGetWindowErr = readErr

	_, _, err = r.Next(concurrency.SROAR_MERGE)
	require.ErrorIs(t, err, readErr)

	st := r.Stats()
	require.Equal(t, 1, st.Fills, "the failed fill is still a fill")
	// Read off the memtable rather than the reader: the failed fill dropped its
	// window, so what it settled at survives only in the range the memtable
	// recorded, which is [from, fill.To).
	require.Equal(t, [][2]int{{0, narrowsAt}}, narrowing.ranges,
		"the first memtable must have narrowed the window, or the two answers cannot differ")
	require.Zero(t, st.NarrowedFills,
		"a fill that failed ended on the failure, not on the byte budget")
}

// TestBatchReaderDiskErrorLeavesNothingToRetryWrong pins the other half of the
// error contract. A failed disk read happens after the key's slots have been
// emptied, so leaving the window intact would let a retry fold the disk row
// alone — a wrong row, with no error to say so. The retry must refill instead.
func TestBatchReaderDiskErrorLeavesNothingToRetryWrong(t *testing.T) {
	t.Parallel()

	readErr := errors.New("injected disk failure")
	diskSeg := newFakeRoaringSetSegment(map[string]*sroar.Bitmap{
		"k0": bitmapFromSlice([]uint64{1}),
		"k1": bitmapFromSlice([]uint64{2}),
	})
	b := &Bucket{
		strategy: StrategyRoaringSet,
		disk:     &SegmentGroup{segments: []Segment{diskSeg}},
		logger:   nullLogger(),
	}
	// The memtable deletes what disk holds, so folding without it differs from
	// the right answer, making the retry observable.
	active := newTestMemtableRoaringSet(nil)
	require.NoError(t, active.roaringSetRemoveList([]byte("k1"), []uint64{2}))
	require.NoError(t, active.roaringSetAddList([]byte("k1"), []uint64{9}))
	view := BucketConsistentView{Active: active, Disk: []Segment{diskSeg}, Bucket: b}

	keys := sortedKeysOf(t, []string{"k0", "k1"})
	r, err := newRoaringSetBatchReaderWithBounds(view, keys, memtableWindowKeys, math.MaxInt)
	require.NoError(t, err)

	_, release, err := r.Next(concurrency.SROAR_MERGE)
	require.NoError(t, err)
	release()

	diskSeg.roaringSetGetErr = readErr
	bm, release, err := r.Next(concurrency.SROAR_MERGE)
	require.ErrorIs(t, err, readErr)
	require.Nil(t, bm, "a failed read returns no row")
	require.Nil(t, release, "a failed read returns nothing to release")
	diskSeg.roaringSetGetErr = nil

	want, wr, err := b.roaringSetGetFromConsistentView(view, keys.At(1), concurrency.SROAR_MERGE)
	require.NoError(t, err)
	defer wr()

	got, gr, err := r.Next(concurrency.SROAR_MERGE)
	require.NoError(t, err, "the retry must succeed")
	defer gr()
	require.Equal(t, docsOrNil(want), docsOrNil(got),
		"the retry must refill: reading the emptied slots as absence would drop the memtable's row")
}

// TestBatchReaderReadsWhileMemtableIsWritten covers a window read racing a
// live memtable: it holds the read lock while it walks and copies, and the
// memtable keeps taking writes throughout. A write landing mid-batch may or
// may not be visible depending on timing, but a row must never be torn, a
// neighbour's, or half-copied.
//
// The flush-in-flight row adds what the first cannot reach: two memtables, a
// budget split between them, and windows narrowed by that split. Both rows are
// read while the active memtable takes writes.
func TestBatchReaderReadsWhileMemtableIsWritten(t *testing.T) {
	const (
		n      = memtableWindowKeys*2 + 3
		marker = uint64(999_999)
		// The document the flushing memtable holds for key i, apart enough from
		// the active one's that a swapped row is not a near miss.
		flushedBase = uint64(500_000)
	)

	tests := []struct {
		name string
		// flushed adds a second memtable the writer never touches.
		flushed bool
		// budget is the reader's byte allowance; the small one is what narrows
		// a window.
		budget int
	}{
		{name: "one memtable", budget: math.MaxInt},
		{name: "a flush in flight", flushed: true, budget: 8 << 10},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rows := map[string][]uint64{}
			flushedRows := map[string][]uint64{}
			batch := make([]string, n)
			for i := 0; i < n; i++ {
				batch[i] = fmt.Sprintf("k%06d", i)
				rows[batch[i]] = []uint64{uint64(i)}
				flushedRows[batch[i]] = []uint64{flushedBase + uint64(i)}
			}

			// The two values a key may read as, in the sorted order ToArray
			// returns: marker is above every other document here, and the
			// flushed one above every active one.
			rowBefore := func(i int) []uint64 {
				if tc.flushed {
					return []uint64{uint64(i), flushedBase + uint64(i)}
				}
				return []uint64{uint64(i)}
			}
			rowAfter := func(i int) []uint64 {
				return append(rowBefore(i), marker)
			}

			diskSeg := newFakeRoaringSetSegment(map[string]*sroar.Bitmap{})
			active := newTestMemtableRoaringSet(rows)
			b := &Bucket{
				strategy: StrategyRoaringSet,
				disk:     &SegmentGroup{segments: []Segment{diskSeg}},
				logger:   nullLogger(),
			}
			view := BucketConsistentView{Active: active, Disk: []Segment{diskSeg}, Bucket: b}
			if tc.flushed {
				view.Flushing = newTestMemtableRoaringSet(flushedRows)
			}
			keys := sortedKeysOf(t, batch)

			var written atomic.Bool
			var writeErr error
			var wg sync.WaitGroup
			start := make(chan struct{})
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer written.Store(true) // even on error, so the reader loop below can't spin forever
				<-start
				for _, k := range batch {
					if err := active.roaringSetAddOne([]byte(k), marker); err != nil {
						writeErr = err
						return
					}
				}
			}()

			// Fold repeatedly until the writer is done, so both pre- and post-write
			// states are actually observed rather than left to the scheduler. The
			// writer is held until one whole pass has been read.
			seenBefore, seenAfter := 0, 0
			for pass := 0; ; pass++ {
				done := written.Load()
				r, err := newRoaringSetBatchReaderWithBounds(view, keys, memtableWindowKeys, tc.budget)
				require.NoError(t, err)
				for i := 0; i < keys.Len(); i++ {
					got, release, err := r.Next(concurrency.SROAR_MERGE)
					require.NoError(t, err)
					switch {
					case slices.Equal(docsOrNil(got), rowBefore(i)):
						seenBefore++
					case slices.Equal(docsOrNil(got), rowAfter(i)):
						seenAfter++
					}
					require.Containsf(t, [][]uint64{rowBefore(i), rowAfter(i)}, docsOrNil(got),
						"pass %d index %d: row is neither the value before the write nor after it", pass, i)
					release()
				}
				if pass == 0 {
					close(start) // only now, so the pass above always ran at the pre-write value
				}
				if done {
					break
				}
			}
			wg.Wait()
			require.NoError(t, writeErr)
			require.Positive(t, seenBefore, "no row was read before the writer reached it; the two never overlapped")
			require.Positive(t, seenAfter, "no row was read after the writer reached it; the two never overlapped")

			// and once the writer has finished, every key must show the write.
			// The recorded reads are cleared first so what the stats below are
			// compared against is this reader's alone, not every pass above it.
			active.ranges = nil
			if tc.flushed {
				view.Flushing.(*testMemtable).ranges = nil
			}
			r, err := newRoaringSetBatchReaderWithBounds(view, keys, memtableWindowKeys, tc.budget)
			require.NoError(t, err)
			for i := 0; i < keys.Len(); i++ {
				got, release, err := r.Next(concurrency.SROAR_MERGE)
				require.NoError(t, err)
				require.Equal(t, rowAfter(i), docsOrNil(got), "index %d after the writer finished", i)
				release()
			}

			st := r.Stats()
			if !tc.flushed {
				require.Equal(t, 1, st.Memtables)
				return
			}
			// Without these the row runs the first row's code path and proves
			// nothing extra.
			require.Equal(t, 2, st.Memtables, "the flushing memtable must be read too")
			require.Positive(t, st.NarrowedFills, "the budget must be cutting windows short")
			require.Equal(t, len(view.Flushing.(*testMemtable).ranges)+len(active.ranges), st.MemtableReads,
				"every read a memtable served must be one the reader counted")
		})
	}
}

// TestBatchReaderFillsWindowsLazily covers ContainsAll abandoning its fold
// early: a reader that filled the whole batch up front would read windows
// nobody goes on to ask about.
func TestBatchReaderFillsWindowsLazily(t *testing.T) {
	t.Parallel()

	batch := make([]string, 20)
	for i := range batch {
		batch[i] = fmt.Sprintf("k%02d", i)
	}

	tests := []struct {
		name        string
		readThrough int // how many keys the fold reaches before giving up
		wantFills   int
	}{
		{name: "abandoned inside the first window", readThrough: 1, wantFills: 1},
		{name: "abandoned on a window boundary", readThrough: 4, wantFills: 1},
		{name: "abandoned one key into the second", readThrough: 5, wantFills: 2},
		{name: "read to the end", readThrough: 20, wantFills: 5},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			diskSeg := newFakeRoaringSetSegment(map[string]*sroar.Bitmap{})
			flushing := newTestMemtableRoaringSet(map[string][]uint64{"k03": {1}})
			active := newTestMemtableRoaringSet(map[string][]uint64{"k00": {1}, "k07": {2}})
			b := &Bucket{
				strategy: StrategyRoaringSet,
				disk:     &SegmentGroup{segments: []Segment{diskSeg}},
				logger:   nullLogger(),
			}
			view := BucketConsistentView{Active: active, Flushing: flushing, Disk: []Segment{diskSeg}, Bucket: b}

			keys := sortedKeysOf(t, batch)
			r, err := newRoaringSetBatchReaderWithBounds(view, keys, 4, math.MaxInt)
			require.NoError(t, err)
			for i := 0; i < tc.readThrough; i++ {
				_, release, err := r.Next(concurrency.SROAR_MERGE)
				require.NoError(t, err)
				release()
			}

			for _, mt := range []*testMemtable{flushing, active} {
				require.Equal(t, tc.wantFills, mt.roaringSetGetWindowCalls,
					"a window may not be read before a key inside it is asked for")
				require.Zero(t, mt.roaringSetGetCalls, "the per-key path must not be used")
			}
		})
	}
}

// TestBatchReaderStatsReportTheWork pins what the slow-query annotation reads:
// fills times memtables bounds the acquisitions batching paid for, and a fill
// count near the key count says the windows were narrow, not that the batch
// was large.
func TestBatchReaderStatsReportTheWork(t *testing.T) {
	t.Parallel()

	batch := make([]string, 10)
	for i := range batch {
		batch[i] = fmt.Sprintf("k%02d", i)
	}

	tests := []struct {
		name          string
		held          map[string][]uint64
		reads         int
		wantFills     int
		wantMemtables int
		wantCloned    bool
	}{
		{
			// 10 keys in windows of 4 is 3 windows, which is what a whole fold pays.
			name:          "the whole batch fills once per window",
			held:          map[string][]uint64{"k00": {1}, "k05": {2}},
			reads:         10,
			wantFills:     3,
			wantMemtables: 1,
			wantCloned:    true,
		},
		{
			// The annotation reads the reader after the fold: stopping inside the
			// second window reports the two windows entered, not the three spanned.
			name:          "a fold that stops early pays for fewer windows",
			held:          map[string][]uint64{"k00": {1}, "k05": {2}},
			reads:         5,
			wantFills:     2,
			wantMemtables: 1,
			wantCloned:    true,
		},
		{
			name:          "a memtable holding nothing clones nothing",
			held:          map[string][]uint64{},
			reads:         10,
			wantFills:     3,
			wantMemtables: 1,
			wantCloned:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			diskSeg := newFakeRoaringSetSegment(map[string]*sroar.Bitmap{})
			active := newTestMemtableRoaringSet(tc.held)
			// An empty memtable is skipped by the constructor; give it a write
			// the skip does not remove.
			if len(tc.held) == 0 {
				require.NoError(t, active.roaringSetAddList([]byte("zzz"), []uint64{1}))
			}
			b := &Bucket{
				strategy: StrategyRoaringSet,
				disk:     &SegmentGroup{segments: []Segment{diskSeg}},
				logger:   nullLogger(),
			}
			view := BucketConsistentView{Active: active, Disk: []Segment{diskSeg}, Bucket: b}

			keys := sortedKeysOf(t, batch)
			r, err := newRoaringSetBatchReaderWithBounds(view, keys, 4, math.MaxInt)
			require.NoError(t, err)
			for i := 0; i < tc.reads; i++ {
				_, release, err := r.Next(concurrency.SROAR_MERGE)
				require.NoError(t, err)
				release()
			}

			got := r.Stats()
			require.Equal(t, tc.wantFills, got.Fills)
			require.Equal(t, tc.wantMemtables, got.Memtables)
			require.Equal(t, tc.reads, got.KeysServed,
				"how far the fold got, which the fill count cannot say")
			require.Zero(t, got.NarrowedFills,
				"no budget here, so every window ended on its key count")
			if tc.wantCloned {
				require.Positive(t, got.BytesPeak, "rows the memtable holds must be counted")
				require.Positive(t, got.BytesCopied, "and counted again in the total")
			} else {
				require.Zero(t, got.BytesPeak, "a window matching nothing must hold nothing")
				require.Zero(t, got.BytesCopied, "and must copy nothing")
			}
		})
	}
}

// TestBatchReaderSkipsAnEmptyActiveMemtable pins the constructor's skip of an
// active memtable whose size is zero: such rows add and delete nothing, so
// the answer must match the per-key path, which does not skip it. A write
// carrying no values (or a commit-log replay with both slices empty) builds a
// node but leaves size at zero.
func TestBatchReaderSkipsAnEmptyActiveMemtable(t *testing.T) {
	t.Parallel()

	batch := []string{"aaa", "bbb", "ccc"}

	tests := []struct {
		name string
		// A flush in flight leaves one memtable to read; without one an empty
		// active leaves none, which is the shape a freshly flushed bucket serves
		// every query from.
		flushing  map[string][]uint64
		wantMts   int
		wantFills int
		want      map[string][]uint64
	}{
		{
			name:      "a flush in flight leaves one memtable",
			flushing:  map[string][]uint64{"bbb": {9}},
			wantMts:   1,
			wantFills: 1,
			want:      map[string][]uint64{"aaa": {1, 2}, "bbb": {9}, "ccc": {3}},
		},
		{
			name:      "nothing flushing leaves none",
			flushing:  nil,
			wantMts:   0,
			wantFills: 0,
			want:      map[string][]uint64{"aaa": {1, 2}, "bbb": nil, "ccc": {3}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Per case: the fake segment counts the reads it is asked for without
			// a lock, and the active memtable counts window calls.
			diskSeg := newFakeRoaringSetSegment(map[string]*sroar.Bitmap{
				"aaa": bitmapFromSlice([]uint64{1, 2}),
				"ccc": bitmapFromSlice([]uint64{3}),
			})

			// Written through the write path, so size and tree disagree exactly as
			// they would in a bucket: nodes for two keys, size still at zero.
			active := newTestMemtableRoaringSet(map[string][]uint64{})
			require.NoError(t, active.roaringSetAddList([]byte("aaa"), []uint64{}))
			require.NoError(t, active.roaringSetAddList([]byte("ccc"), []uint64{}))
			require.Zero(t, active.Size(), "the fixture must reach the state the skip is about")

			b := &Bucket{
				strategy: StrategyRoaringSet,
				disk:     &SegmentGroup{segments: []Segment{diskSeg}},
				logger:   nullLogger(),
			}
			view := BucketConsistentView{Active: active, Disk: []Segment{diskSeg}, Bucket: b}
			if tc.flushing != nil {
				view.Flushing = newTestMemtableRoaringSet(tc.flushing)
			}

			keys := sortedKeysOf(t, batch)
			r, err := newRoaringSetBatchReaderWithBounds(view, keys, memtableWindowKeys, math.MaxInt)
			require.NoError(t, err)
			require.Equal(t, tc.wantMts, r.mtCount, "the empty active memtable must be dropped from the view")
			require.Equal(t, tc.wantMts, r.Stats().Memtables,
				"Memtables reports what contributed, so it must drop the empty one too")

			requireMatchesPerKey(t, b, view, r, keys, tc.name)
			require.Zero(t, active.roaringSetGetWindowCalls, "a dropped memtable must not be read")
			require.Equal(t, tc.wantFills, r.Stats().Fills,
				"with nothing to read from there is no fill to count")

			abs, err := newRoaringSetBatchReaderWithBounds(view, keys, memtableWindowKeys, math.MaxInt)
			require.NoError(t, err)
			requireRowsAre(t, abs, keys, tc.want)
		})
	}
}

// TestBatchReaderReleasesWhatItHasServed pins that a served key's clone leaves
// the window immediately (only a fill drops clones otherwise, and a batch
// inside one window never reaches a second fill).
func TestBatchReaderReleasesWhatItHasServed(t *testing.T) {
	t.Parallel()

	batch := make([]string, 8)
	for i := range batch {
		batch[i] = fmt.Sprintf("k%02d", i)
	}

	diskSeg := newFakeRoaringSetSegment(map[string]*sroar.Bitmap{})
	held := map[string][]uint64{} // every key held, so a leftover slot means a leftover clone
	for i, k := range batch {
		held[k] = []uint64{uint64(i)}
	}
	active := newTestMemtableRoaringSet(held)
	b := &Bucket{
		strategy: StrategyRoaringSet,
		disk:     &SegmentGroup{segments: []Segment{diskSeg}},
		logger:   nullLogger(),
	}
	view := BucketConsistentView{Active: active, Disk: []Segment{diskSeg}, Bucket: b}

	keys := sortedKeysOf(t, batch)
	r, err := newRoaringSetBatchReaderWithBounds(view, keys, len(batch), math.MaxInt)
	require.NoError(t, err)

	const served = 3
	for i := 0; i < served; i++ {
		_, release, err := r.Next(concurrency.SROAR_MERGE)
		require.NoError(t, err)
		release()
	}
	require.Equal(t, 1, active.roaringSetGetWindowCalls, "the window must not have refilled")

	for mt := 0; mt < r.mtCount; mt++ {
		for i := 0; i < served; i++ {
			require.Nilf(t, r.windows[mt].layer[i].Additions, "layer %d slot %d still holds a served clone", mt, i)
			require.Nilf(t, r.windows[mt].layer[i].Deletions, "layer %d slot %d still holds a served clone", mt, i)
		}
		for i := served; i < len(batch); i++ {
			require.NotNilf(t, r.windows[mt].layer[i].Additions, "layer %d slot %d lost a row it has not served", mt, i)
		}
	}
}

// TestBatchReaderAtProductionWindow runs the differential over a batch
// several times the real window size, unlike every other test here which uses
// a tiny window to reach the boundaries cheaply.
func TestBatchReaderAtProductionWindow(t *testing.T) {
	t.Parallel()

	const n = memtableWindowKeys*3 + 7

	diskRows := map[string]*sroar.Bitmap{}
	flushingRows := map[string][]uint64{}
	activeRows := map[string][]uint64{}
	batch := make([]string, n)
	for i := 0; i < n; i++ {
		batch[i] = fmt.Sprintf("k%06d", i)
		switch {
		case i%7 == 0:
			diskRows[batch[i]] = bitmapFromSlice([]uint64{uint64(i)})
		case i%11 == 0:
			flushingRows[batch[i]] = []uint64{uint64(i)}
		case i%13 == 0:
			activeRows[batch[i]] = []uint64{uint64(i)}
		}
	}
	// the boundaries themselves, held by every layer at once
	for _, i := range []int{memtableWindowKeys - 1, memtableWindowKeys, memtableWindowKeys*2 - 1, memtableWindowKeys * 2, n - 1} {
		diskRows[batch[i]] = bitmapFromSlice([]uint64{uint64(i)})
		flushingRows[batch[i]] = []uint64{uint64(i) + 1}
		activeRows[batch[i]] = []uint64{uint64(i) + 2}
	}

	diskSeg := newFakeRoaringSetSegment(diskRows)
	flushing := newTestMemtableRoaringSet(flushingRows)
	active := newTestMemtableRoaringSet(activeRows)
	b := &Bucket{
		strategy: StrategyRoaringSet,
		disk:     &SegmentGroup{segments: []Segment{diskSeg}},
		logger:   nullLogger(),
		active:   active,
		flushing: flushing,
	}
	view := BucketConsistentView{Active: active, Flushing: flushing, Disk: []Segment{diskSeg}, Bucket: b}

	// Through the constructor, at the real key limit.
	keys := sortedKeysOf(t, batch)
	reader, err := NewRoaringSetBatchReader(view, keys)
	require.NoError(t, err)

	requireMatchesPerKey(t, b, view, reader, keys, "")

	windows := (n + memtableWindowKeys - 1) / memtableWindowKeys
	require.Equal(t, windows, flushing.roaringSetGetWindowCalls, "one read per window")
	require.Equal(t, windows, active.roaringSetGetWindowCalls, "one read per window")
}

// TestBatchReaderRejectsItsBounds covers the two constructor arguments that
// fail differently when wrong: a non-positive window panics deep in the first
// fill, while a non-positive budget silently degrades to a fill per key. Both
// are refused up front instead.
func TestBatchReaderRejectsItsBounds(t *testing.T) {
	t.Parallel()
	keys := sortedKeysOf(t, []string{"a", "b"})
	// A whole view, so the reader the last case builds is one Next could serve;
	// without the bucket it would be accepted and then nil-dereference.
	b := &Bucket{strategy: StrategyRoaringSet, disk: &SegmentGroup{}, logger: nullLogger()}
	view := BucketConsistentView{Active: newTestMemtableRoaringSet(nil), Bucket: b}

	for _, window := range []int{0, -1, -256} {
		_, err := newRoaringSetBatchReaderWithBounds(view, keys, window, math.MaxInt)
		require.Errorf(t, err, "window %d must be rejected", window)
	}
	for _, budget := range []int{0, -1, -256} {
		_, err := newRoaringSetBatchReaderWithBounds(view, keys, memtableWindowKeys, budget)
		require.Errorf(t, err, "budget %d must be rejected", budget)
	}
	// Both good, so the cases above fail on the bound named and not on the view.
	_, err := newRoaringSetBatchReaderWithBounds(view, keys, memtableWindowKeys, readerWindowBytes)
	require.NoError(t, err)
}

// TestBatchReaderReadsPastTheBatch pins the shape of asking for a row past
// Len: an error and neither a row nor a release (a release beside an error
// would go uncalled, since folds drop it).
func TestBatchReaderReadsPastTheBatch(t *testing.T) {
	t.Parallel()

	diskSeg := newFakeRoaringSetSegment(map[string]*sroar.Bitmap{
		"a": bitmapFromSlice([]uint64{1}),
	})
	b := &Bucket{
		strategy: StrategyRoaringSet,
		disk:     &SegmentGroup{segments: []Segment{diskSeg}},
		logger:   nullLogger(),
	}
	keys := sortedKeysOf(t, []string{"a", "b"})
	view := BucketConsistentView{
		Active: newTestMemtableRoaringSet(map[string][]uint64{"b": {2}}),
		Disk:   []Segment{diskSeg}, Bucket: b,
	}
	r, err := newRoaringSetBatchReaderWithBounds(view, keys, memtableWindowKeys, math.MaxInt)
	require.NoError(t, err)

	for i := 0; i < keys.Len(); i++ {
		_, release, err := r.Next(concurrency.SROAR_MERGE)
		require.NoErrorf(t, err, "read %d of %d", i, keys.Len())
		release()
	}
	// Twice: a reader that has refused once must not then answer.
	for i := 0; i < 2; i++ {
		bm, release, err := r.Next(concurrency.SROAR_MERGE)
		require.Errorf(t, err, "read %d past the batch", i)
		require.Nil(t, bm, "a failed read returns no row")
		require.Nil(t, release, "a failed read returns nothing to release")
	}
}

// requireRowsAre checks the reader against written-down rows rather than the
// per-key path — both sides of the differential fold through
// roaringSetGetWithLayers, so a bug there would agree with itself.
func requireRowsAre(t *testing.T, r *RoaringSetBatchReader, keys inverted.SortedKeys, want map[string][]uint64) {
	t.Helper()
	for i := 0; i < keys.Len(); i++ {
		got, release, err := r.Next(concurrency.SROAR_MERGE)
		require.NoErrorf(t, err, "read %d", i)
		require.Equalf(t, want[string(keys.At(i))], docsOrNil(got), "key %q", keys.At(i))
		release()
	}
}

// requireMatchesPerKey is the differential: whatever the window did, each key
// must read the same as it would on its own.
//
// Starts wherever the reader has reached, so a walk resumed after a failed read
// is the same assertion as a walk from the start; a reader fresh from the
// constructor is at zero.
func requireMatchesPerKey(t *testing.T, b *Bucket, view BucketConsistentView,
	r *RoaringSetBatchReader, keys inverted.SortedKeys, msg string,
) {
	t.Helper()
	for i := r.pos; i < keys.Len(); i++ {
		want, wr, err := b.roaringSetGetFromConsistentView(view, keys.At(i), concurrency.SROAR_MERGE)
		require.NoError(t, err)
		got, gr, err := r.Next(concurrency.SROAR_MERGE)
		require.NoErrorf(t, err, "read %d", i)
		require.Equalf(t, docsOrNil(want), docsOrNil(got), "%s: key %q (index %d)", msg, keys.At(i), i)
		wr()
		gr()
	}
}

// narrowingFixture builds the shape the carry tests need: a thin memtable read
// first, which fills whatever width it is asked for, and a fat one read second
// whose share stops it after the always-taken first key, so it is what narrows
// every window under the one already read. Key i of the thin memtable holds
// document 100+i. The budget returned is one byte past a single fat row.
func narrowingFixture(t *testing.T, nKeys int) (BucketConsistentView, inverted.SortedKeys, int, *Bucket) {
	t.Helper()

	names := make([]string, nKeys)
	for i := range names {
		names[i] = fmt.Sprintf("k%03d", i)
	}
	keys := sortedKeysOf(t, names)

	flushing := newTestMemtableRoaringSet(nil)
	for i, n := range names {
		require.NoError(t, flushing.roaringSetAddList([]byte(n), []uint64{uint64(100 + i)}))
	}

	// One document per container, so a row is expensive next to the thin ones.
	active := newTestMemtableRoaringSet(nil)
	wide := make([]uint64, 4096)
	for i := range wide {
		wide[i] = uint64(i) * 65536
	}
	for _, n := range names {
		require.NoError(t, active.roaringSetAddList([]byte(n), wide))
	}

	oneRow := make([]roaringset.BitmapLayer, 1)
	first, err := active.roaringSetGetWindow(keys, 0, 1, oneRow, math.MaxInt)
	require.NoError(t, err)
	// The pricing read above went through the same recorder a reader's reads do,
	// so a test walking ranges would find it before the reader's first one.
	active.ranges = nil

	b := &Bucket{strategy: StrategyRoaringSet, disk: &SegmentGroup{}, logger: nullLogger()}
	return BucketConsistentView{Active: active, Flushing: flushing, Bucket: b}, keys, first.Bytes + 1, b
}

// requireWindowInvariants asserts that a slot's rows and the indices
// describing them still agree. Holding them in one struct makes a slot one
// value; it does not make the three fields consistent, and fillWindow settles
// filled in one pass and recomputes carried in a second, so nothing but this
// says they ended up in step.
func requireWindowInvariants(t *testing.T, r *RoaringSetBatchReader) {
	t.Helper()
	for mt := 0; mt < r.mtCount; mt++ {
		w := r.windows[mt]
		require.GreaterOrEqualf(t, w.filled, r.winStart,
			"memtable %d holds rows from before the window they are laid out against", mt)
		require.GreaterOrEqualf(t, len(w.layer), r.winEnd-r.winStart,
			"memtable %d has no slot for every key of the window", mt)

		carried := 0
		for i := r.winEnd; i < w.filled; i++ {
			carried += w.layer[i-r.winStart].LenInBytes()
		}
		require.Equalf(t, carried, w.carried,
			"memtable %d is charged for something other than the rows it kept past the window", mt)

		for at := w.filled - r.winStart; at < len(w.layer); at++ {
			require.Nilf(t, w.layer[at].Additions,
				"memtable %d holds additions in slot %d, past what it filled", mt, at)
			require.Nilf(t, w.layer[at].Deletions,
				"memtable %d holds deletions in slot %d, past what it filled", mt, at)
		}
	}
}

// requireNoClonesHeld asserts the reader holds no bitmap anywhere in its
// buffers, over the whole capacity rather than the current window: a row left
// past winEnd is what a missed clear looks like.
func requireNoClonesHeld(t *testing.T, r *RoaringSetBatchReader) {
	t.Helper()
	for mt := range r.mtCount {
		for at, layer := range r.windows[mt].layer[:cap(r.windows[mt].layer)] {
			require.Nilf(t, layer.Additions, "memtable %d still holds additions in slot %d", mt, at)
			require.Nilf(t, layer.Deletions, "memtable %d still holds deletions in slot %d", mt, at)
		}
	}
}

// TestBatchReaderKeepsNoClonesOnceTheBatchIsWalked pins that a reader with two
// memtables ends a batch holding none of their rows. The fat memtable's share
// stops it after the key it always takes, so every window here is one key wide
// while the buffer stays at the width the first one opened at; a fill that
// cleared only the window's own width would leave that tail behind for the
// reader's whole lifetime.
//
// Only the thin memtable can hold such a tail: the fat one is what narrows the
// window, so its filled index never runs past winEnd. The check's other half
// therefore passes by construction on this fixture.
func TestBatchReaderKeepsNoClonesOnceTheBatchIsWalked(t *testing.T) {
	t.Parallel()

	const (
		nKeys  = 10
		window = 8
	)

	view, keys, budget, b := narrowingFixture(t, nKeys)

	r, err := newRoaringSetBatchReaderWithBounds(view, keys, window, budget)
	require.NoError(t, err)

	for i := range nKeys {
		_, release, err := r.Next(concurrency.SROAR_MERGE)
		require.NoError(t, err)
		// After a row is served, not before: windows fill lazily, so both ends
		// are zero until the first Next and any width would pass.
		require.Lessf(t, r.winEnd-r.winStart, window,
			"key %d: the budget must narrow the window, or nothing is left unserved", i)
		release()
	}

	requireNoClonesHeld(t, r)

	// And the rows are right, which is the direction the batched parity table
	// does not reach: there the memtable read first is the one whose budget
	// settles the window, and here it is the one read second. What each is asked
	// for depends on the other, so both orders have to answer as the per-key
	// path does.
	fresh, err := newRoaringSetBatchReaderWithBounds(view, keys, window, budget)
	require.NoError(t, err)
	requireMatchesPerKey(t, b, view, fresh, keys, "the second memtable narrowing")
}

// TestBatchReaderEndsWindowsOnEitherLimit covers a batch whose windows don't
// all end the same way: thin rows run to the key count, fat rows spend the
// byte budget first. Every other budget case here meets one limit throughout,
// so a reader that quietly stopped honouring the other would still pass them.
func TestBatchReaderEndsWindowsOnEitherLimit(t *testing.T) {
	t.Parallel()

	const window, thinKeys, fatKeys = 4, 6, 6

	batch := make([]string, 0, thinKeys+fatKeys)
	held := map[string][]uint64{}
	for i := range thinKeys + fatKeys {
		k := fmt.Sprintf("k%02d", i)
		batch = append(batch, k)
		if i < thinKeys {
			held[k] = []uint64{uint64(i)}
			continue
		}
		docs := make([]uint64, 4096)
		for j := range docs {
			docs[j] = uint64(i)*1_000_000 + uint64(j)*64
		}
		held[k] = docs
	}

	diskSeg := newFakeRoaringSetSegment(map[string]*sroar.Bitmap{})
	b := &Bucket{
		strategy: StrategyRoaringSet,
		disk:     &SegmentGroup{segments: []Segment{diskSeg}},
		logger:   nullLogger(),
	}
	active := newTestMemtableRoaringSet(held)
	view := BucketConsistentView{Active: active, Disk: []Segment{diskSeg}, Bucket: b}
	keys := sortedKeysOf(t, batch)

	// Room for two fat rows, so a window of four reaches the count among the
	// thin ones and the budget among the fat ones.
	oneFat := func() int {
		single := sortedKeysOf(t, []string{fmt.Sprintf("k%02d", thinKeys)})
		dst := make([]roaringset.BitmapLayer, 1)
		fill, err := active.roaringSetGetWindow(single, 0, 1, dst, math.MaxInt)
		require.NoError(t, err)
		require.Positive(t, fill.Bytes)
		return fill.Bytes
	}()

	r, err := newRoaringSetBatchReaderWithBounds(view, keys, window, 2*oneFat+1)
	require.NoError(t, err)

	widths := map[int]bool{}
	for range keys.Len() {
		_, release, err := r.Next(concurrency.SROAR_MERGE)
		require.NoError(t, err)
		release()
		widths[r.winEnd-r.winStart] = true
	}

	require.True(t, widths[window],
		"the thin rows must let a window run to the key count, got widths %v", widths)
	var narrowed bool
	for w := range widths {
		if w < window {
			narrowed = true
		}
	}
	require.True(t, narrowed,
		"the fat rows must end a window before the key count, got widths %v", widths)

	// The stats must say which limit did it, so an operator can tell "this
	// batch was large" from "raise the byte budget".
	st := r.Stats()
	require.Positive(t, st.NarrowedFills, "the budget ended some windows")
	require.Less(t, st.NarrowedFills, st.Fills, "and the key count ended others")
	require.Equal(t, keys.Len(), st.KeysServed)
	// Peak is one window; total is every window — a batch spanning several
	// windows must separate the two.
	require.Positive(t, st.BytesPeak)
	require.Greater(t, st.BytesCopied, st.BytesPeak,
		"a batch spanning several windows must copy more than the widest of them held")

	r2, err := newRoaringSetBatchReaderWithBounds(view, keys, window, 2*oneFat+1)
	require.NoError(t, err)
	requireMatchesPerKey(t, b, view, r2, keys, "")
}

// TestBatchReaderHoldsOneBudgetAcrossMemtables pins that the byte budget
// bounds what a reader holds, not what each of its memtables holds — handing
// each the whole budget would double the peak whenever a flush is in flight.
func TestBatchReaderHoldsOneBudgetAcrossMemtables(t *testing.T) {
	t.Parallel()

	const nKeys = 200

	names := make([]string, nKeys)
	for i := range names {
		names[i] = fmt.Sprintf("k%05d", i)
	}
	keys := sortedKeysOf(t, names)

	// One document per container, so a window reaches the budget well before
	// it reaches the key count.
	spread := func(seed uint64) []uint64 {
		docs := make([]uint64, 200)
		for i := range docs {
			docs[i] = seed*1_000_000_000 + uint64(i)*65536
		}
		return docs
	}
	build := func(seed uint64) *testMemtable {
		m := newTestMemtableRoaringSet(nil)
		for i, n := range names {
			require.NoError(t, m.roaringSetAddList([]byte(n), spread(seed+uint64(i))))
		}
		return m
	}

	b := &Bucket{strategy: StrategyRoaringSet, disk: &SegmentGroup{}, logger: nullLogger()}
	const budget = 4 << 20

	peakOf := func(t *testing.T, view BucketConsistentView) RoaringSetBatchReaderStats {
		r, err := newRoaringSetBatchReaderWithBounds(view, keys, memtableWindowKeys, budget)
		require.NoError(t, err)
		_, release, err := r.Next(concurrency.SROAR_MERGE)
		require.NoError(t, err)
		release()
		return r.Stats()
	}

	twoActive, twoFlushing := build(1), build(500)
	one := peakOf(t, BucketConsistentView{Active: build(1), Bucket: b})
	two := peakOf(t, BucketConsistentView{Active: twoActive, Flushing: twoFlushing, Bucket: b})

	require.Equal(t, 1, one.Memtables)
	require.Equal(t, 2, two.Memtables)
	require.Positive(t, one.BytesPeak, "the fixture must reach the budget, or this proves nothing")

	// A window's first key is taken whatever it costs, so the budget may be
	// exceeded by the widest row each memtable holds — an average row would be
	// a tighter bound than the reader promises.
	widestRow := func(m *testMemtable) int {
		widest := 0
		dst := make([]roaringset.BitmapLayer, 1)
		for i := 0; i < keys.Len(); i++ {
			fill, err := m.roaringSetGetWindow(keys, i, i+1, dst, math.MaxInt)
			require.NoError(t, err)
			widest = max(widest, fill.Bytes)
		}
		return widest
	}
	require.LessOrEqual(t, two.BytesPeak, budget+widestRow(twoActive)+widestRow(twoFlushing),
		"two memtables must hold one budget between them, not one each")
	require.LessOrEqual(t, two.BytesPeak, one.BytesPeak*3/2,
		"a flush in flight must not roughly double what a window holds")
}

// TestBatchReaderKeepsRowsNarrowedPast covers a memtable read before the one
// that narrows the window: it read to the wide end, and what it holds past the
// narrowed end is kept rather than dropped, so the next fill asks it only for
// what it does not already have.
//
// Both halves are asserted. The ranges pin that no key is read twice, which is
// the whole point — without it the reader is correct but re-clones the tail
// every fill. The values pin that a kept row is served at the key it belongs
// to: the buffer is re-based on each fill, and an offset slip there would serve
// a neighbour's row with nothing else noticing.
func TestBatchReaderKeepsRowsNarrowedPast(t *testing.T) {
	t.Parallel()

	const nKeys = 12

	view, keys, budget, _ := narrowingFixture(t, nKeys)
	r, err := newRoaringSetBatchReaderWithBounds(view, keys, 8, budget)
	require.NoError(t, err)

	// The thin memtable, which the fold reads first and so is the one that reads
	// past a window a later memtable narrows.
	counted := view.Flushing.(*testMemtable)

	for i := 0; i < nKeys; i++ {
		bm, release, err := r.Next(concurrency.SROAR_MERGE)
		require.NoError(t, err)
		require.Truef(t, bm.Contains(uint64(100+i)),
			"key %d must be served the row the kept buffer holds for it", i)
		release()
	}

	require.Greater(t, len(counted.ranges), 1, "the batch must span windows, or this proves nothing")
	at := 0
	for _, rng := range counted.ranges {
		require.Equalf(t, at, rng[0], "read %v starts before the last one ended: a kept row was re-read", rng)
		at = rng[1]
	}
	require.Equal(t, nKeys, at, "every key must be read exactly once")
}

// TestBatchReaderBytesPeakTracksWhatAWindowHolds walks a batch fill by fill and
// compares BytesPeak against the peak the walk computes itself. A bound checked
// only at the end is satisfied by a peak that is too small, which is what makes
// the per-fill comparison the assertion that matters.
//
// The two fixtures answer different questions and neither answers both. With
// rows of one size a memtable that carried n bytes copies n fewer, so every
// fill holds the same amount and the peak has to stay inside the budget — but
// the first fill carries nothing and already copied that whole amount, so
// dropping the carry term reports the same number and nothing can tell. Rows
// growing along the batch make a later window hold more than any single fill
// copied, which is what exposes the term.
func TestBatchReaderBytesPeakTracksWhatAWindowHolds(t *testing.T) {
	t.Parallel()

	const nKeys = 24

	tests := []struct {
		name string
		// activeDocs is the active memtable's row width at key i. The flushing
		// memtable is thin and uniform either way, so it fills whatever width it
		// is asked for and carries what the active one narrows away.
		activeDocs   func(i int) int
		flushingDocs int
		// budgetRows is the byte budget in whole first-active-rows. Several fit,
		// so the "first key is taken whatever it costs" allowance is not in play
		// and no row is ever oversized.
		budgetRows int
		check      func(t *testing.T, st RoaringSetBatchReaderStats, wantPeak, widestCopy, budget int)
	}{
		{
			// Fat enough that a whole window of them costs more than one share,
			// so the share rather than the key count is what stops a read. With
			// the width binding instead, a read would be capped before the budget
			// was consulted and this would pin nothing.
			name:         "rows of one size hold the budget and no more",
			activeDocs:   func(int) int { return 1024 },
			flushingDocs: 614,
			budgetRows:   5,
			check: func(t *testing.T, st RoaringSetBatchReaderStats, wantPeak, widestCopy, budget int) {
				require.Greater(t, st.Fills, 1, "the batch must span windows, or nothing is ever carried")
				require.LessOrEqual(t, st.BytesPeak, budget,
					"a window that carried rows in must hold the budget, not the budget plus what it carried")
			},
		},
		{
			name:         "rows growing along the batch expose the carry term",
			activeDocs:   func(i int) int { return 64 + i*64 },
			flushingDocs: 16,
			budgetRows:   6,
			check: func(t *testing.T, st RoaringSetBatchReaderStats, wantPeak, widestCopy, budget int) {
				require.Greater(t, wantPeak, widestCopy,
					"the fixture must hold more in some window than any one fill copied, or dropping the carry term reports the same peak")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			names := make([]string, nKeys)
			for i := range names {
				names[i] = fmt.Sprintf("k%03d", i)
			}
			keys := sortedKeysOf(t, names)

			// One document per container, so a row costs roughly its count.
			docs := func(n int) []uint64 {
				out := make([]uint64, n)
				for i := range out {
					out[i] = uint64(i) * 65536
				}
				return out
			}

			flushing := newTestMemtableRoaringSet(nil)
			for _, n := range names {
				require.NoError(t, flushing.roaringSetAddList([]byte(n), docs(tc.flushingDocs)))
			}
			active := newTestMemtableRoaringSet(nil)
			for i, n := range names {
				require.NoError(t, active.roaringSetAddList([]byte(n), docs(tc.activeDocs(i))))
			}

			oneRow := make([]roaringset.BitmapLayer, 1)
			first, err := active.roaringSetGetWindow(keys, 0, 1, oneRow, math.MaxInt)
			require.NoError(t, err)

			b := &Bucket{strategy: StrategyRoaringSet, disk: &SegmentGroup{}, logger: nullLogger()}
			view := BucketConsistentView{Active: active, Flushing: flushing, Bucket: b}

			budget := tc.budgetRows * first.Bytes
			r, err := newRoaringSetBatchReaderWithBounds(view, keys, 8, budget)
			require.NoError(t, err)

			wantPeak, widestCopy, carryingFills := 0, 0, 0
			for i := 0; i < nKeys; i++ {
				carriedIn := r.windows[0].carried + r.windows[1].carried
				copiedBefore, fillsBefore := r.bytesCopied, r.fills

				_, release, err := r.Next(concurrency.SROAR_MERGE)
				require.NoError(t, err)
				release()

				if r.fills == fillsBefore {
					continue
				}
				copied := r.bytesCopied - copiedBefore
				if carriedIn > 0 {
					carryingFills++
				}
				widestCopy = max(widestCopy, copied)
				wantPeak = max(wantPeak, carriedIn+copied)
				require.Equal(t, wantPeak, r.bytesPeak, "peak after the fill that served key %d", i)
				requireWindowInvariants(t, r)
			}

			require.Positive(t, carryingFills,
				"the fixture must carry rows into a fill, or nothing here tracks the peak across one")
			require.Equal(t, wantPeak, r.Stats().BytesPeak)
			tc.check(t, r.Stats(), wantPeak, widestCopy, budget)
		})
	}
}

// TestBatchReaderNarrowsAtTheProductionByteBudget runs the exported
// constructor, so the window is bounded by memtableWindowKeys and
// readerWindowBytes rather than by numbers a test picked. Every other budget
// assertion passes a hand-computed budget small enough to reason about, so
// nothing asserted what the shipped one does.
//
// The two bounds are absolute on purpose. A budget derived from the constant
// would move with it and pass whatever it was set to; these fail if it is
// scaled far enough in either direction — too large and no window is ever cut
// short, too small and a window cannot hold what it is asserted to hold.
func TestBatchReaderNarrowsAtTheProductionByteBudget(t *testing.T) {
	t.Parallel()

	// One document per container, so a row costs roughly its container count
	// and a window's worth of them runs past the shipped budget.
	const (
		nKeys       = 80
		docsPerRow  = 1024
		peakAtLeast = 4 << 20
		minFills    = 2 // the fixture must take more than one window
	)

	names := make([]string, nKeys)
	for i := range names {
		names[i] = fmt.Sprintf("k%03d", i)
	}
	keys := sortedKeysOf(t, names)

	wide := make([]uint64, docsPerRow)
	for i := range wide {
		wide[i] = uint64(i) * 65536
	}
	active := newTestMemtableRoaringSet(nil)
	for _, n := range names {
		require.NoError(t, active.roaringSetAddList([]byte(n), wide))
	}

	b := &Bucket{strategy: StrategyRoaringSet, disk: &SegmentGroup{}, logger: nullLogger()}
	view := BucketConsistentView{Active: active, Bucket: b}

	r, err := NewRoaringSetBatchReader(view, keys)
	require.NoError(t, err)
	require.Less(t, nKeys, memtableWindowKeys,
		"the key count must not be what ends a window, or the byte budget is never consulted")

	for range names {
		_, release, err := r.Next(concurrency.SROAR_MERGE)
		require.NoError(t, err)
		release()
	}

	st := r.Stats()
	require.GreaterOrEqual(t, st.Fills, minFills,
		"the shipped byte budget must cut a window short of memtableWindowKeys")
	require.Equal(t, st.Fills-1, st.NarrowedFills,
		"every window but the last ends on the budget; the last ends because the batch does")
	require.Greater(t, st.BytesPeak, peakAtLeast,
		"a window at the shipped budget holds megabytes; a budget scaled down by orders of magnitude could not")
}

// TestBatchReaderSkipsAMemtableItHasReadToTheEnd pins that a fill does not
// always read every memtable, which is why MemtableReads is counted rather
// than derived from Fills times Memtables.
//
// This covers one of the two ways a fill skips a memtable: a window can never
// be wider than the batch, so one that already reached the last key has nothing
// left to be asked for. The other is
// [TestBatchReaderSkipsAMemtableHoldingMoreThanItsShare].
func TestBatchReaderSkipsAMemtableItHasReadToTheEnd(t *testing.T) {
	t.Parallel()

	const nKeys = 5
	names := make([]string, nKeys)
	for i := range names {
		names[i] = fmt.Sprintf("k%02d", i)
	}
	keys := sortedKeysOf(t, names)

	// Cheap rows, so one read covers the whole batch within its share.
	thin := map[string][]uint64{}
	// Fat enough that its share stops it after the always-taken first key.
	fatRow := make([]uint64, 4096)
	for i := range fatRow {
		fatRow[i] = uint64(i) * 64
	}
	fat := map[string][]uint64{}
	for i, n := range names {
		thin[n] = []uint64{uint64(i)}
		fat[n] = fatRow
	}
	flushing, active := newTestMemtableRoaringSet(thin), newTestMemtableRoaringSet(fat)

	priceOne := func(mt *testMemtable) int {
		dst := make([]roaringset.BitmapLayer, 1)
		fill, err := mt.roaringSetGetWindow(keys, 0, 1, dst, math.MaxInt)
		require.NoError(t, err)
		return fill.Bytes
	}
	share := priceOne(active) - 1
	require.Greater(t, share, nKeys*priceOne(flushing),
		"the thin memtable must fit the whole batch in one share, or it never reaches the end")

	b := &Bucket{strategy: StrategyRoaringSet, disk: &SegmentGroup{}, logger: nullLogger()}
	view := BucketConsistentView{Active: active, Flushing: flushing, Bucket: b}

	r, err := newRoaringSetBatchReaderWithBounds(view, keys, nKeys+3, 2*share)
	require.NoError(t, err)
	requireMatchesPerKey(t, b, view, r, keys, "a memtable read to the end of the batch")

	st := r.Stats()
	require.Less(t, st.MemtableReads, st.Fills*st.Memtables,
		"a memtable already at the last key is skipped, so the product overcounts")
	require.Positive(t, st.MemtableReads)
}

// TestBatchReaderSkipsAMemtableHoldingMoreThanItsShare covers the second way a
// fill skips a memtable: what it kept from an earlier window already costs more
// than its whole share.
//
// The read schedule is asserted whole rather than counted — the gap where no
// read starts is the skip itself.
func TestBatchReaderSkipsAMemtableHoldingMoreThanItsShare(t *testing.T) {
	t.Parallel()

	const nKeys = 8
	names := make([]string, nKeys)
	for i := range names {
		names[i] = fmt.Sprintf("k%02d", i)
	}
	keys := sortedKeysOf(t, names)

	// One document per container, so a row costs orders of magnitude more.
	wide := make([]uint64, 4096)
	for i := range wide {
		wide[i] = uint64(i) * 65536
	}

	// Read first, so it fills a whole window and then has it narrowed under
	// itself. Its wide row is what it ends up carrying.
	const wideAt = 2
	thin := newTestMemtableRoaringSet(nil)
	for i, n := range names {
		row := []uint64{uint64(100 + i)}
		if i == wideAt {
			row = wide
		}
		require.NoError(t, thin.roaringSetAddList([]byte(n), row))
	}

	// Read second, and every row too wide for its share, so it narrows every
	// window to the one key it always takes.
	fat := newTestMemtableRoaringSet(nil)
	for _, n := range names {
		require.NoError(t, fat.roaringSetAddList([]byte(n), wide))
	}

	priceOne := func(m *testMemtable, at int) int {
		dst := make([]roaringset.BitmapLayer, 1)
		fill, err := m.roaringSetGetWindow(keys, at, at+1, dst, math.MaxInt)
		require.NoError(t, err)
		return fill.Bytes
	}
	thinRow, wideRow := priceOne(thin, 0), priceOne(thin, wideAt)
	// The pricing reads went through the same recorder the reader's do.
	thin.ranges, fat.ranges = nil, nil

	// Four thin rows per memtable, so the thin one fills several keys per read
	// and the wide row is far outside what any share can hold.
	budget := 8 * thinRow
	share := budget / 2
	require.Greater(t, wideRow, share,
		"the carried row must cost more than a whole share, or no fill ever skips for want of budget")

	b := &Bucket{strategy: StrategyRoaringSet, disk: &SegmentGroup{}, logger: nullLogger()}
	view := BucketConsistentView{Active: fat, Flushing: thin, Bucket: b}

	r, err := newRoaringSetBatchReaderWithBounds(view, keys, nKeys, budget)
	require.NoError(t, err)
	requireMatchesPerKey(t, b, view, r, keys, "a memtable holding more than its share")

	// No read starts at key 3: the fill before it took the wide row at key 2,
	// the window narrowed to key 2 alone, and the thin memtable entered the
	// next fill carrying the whole row. Without the guard it reads [3,4) there.
	require.Equal(t, [][2]int{{0, 2}, {2, 3}, {3, 7}, {7, 8}}, thin.ranges,
		"no read may start at key 3 while the thin memtable is over its share")

	st := r.Stats()
	require.Equal(t, nKeys, st.Fills, "the fat memtable narrows every window to one key")
	require.Equal(t, len(thin.ranges)+len(fat.ranges), st.MemtableReads,
		"every read a memtable served must be one the reader counted")
	require.Less(t, st.MemtableReads, st.Fills*st.Memtables,
		"a skipped memtable is what makes the product an overcount")
}
