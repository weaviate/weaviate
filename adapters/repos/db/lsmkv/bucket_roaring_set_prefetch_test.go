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

// The batch reader against synthetic layers: window boundaries, fill counts,
// error paths and the stats it reports, on fake segments and hand-built
// memtables so a case costs nothing to add. What the reader does to a real
// bucket — matching the per-key path, surviving a flush, folding tombstones —
// is in bucket_roaring_set_test.go, where a case costs a temp dir and a flush.

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
// survive: reading a batch by index, with each memtable consulted once per
// window, must return exactly what reading each key on its own returns.
//
// The memtables here hold data. Against empty ones any prefetch agrees, since
// there is nothing to find. The shapes that matter are a key held by both
// memtables, a key held by a memtable and not by disk, and a document deleted
// in flushing and re-added in active, which only survives if the layers fold
// oldest first.
//
// The disk is varied because it changes how the fold begins. With one segment
// the first one found becomes the base and nothing is merged onto it; with
// several, each later one replays its deletions and additions in turn.
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
	// Written through the write path so the memtable's size agrees with its
	// tree, as it would in a bucket. "aaa" is deleted while flushing and re-added
	// in active, which is order-sensitive; "eee" is deleted and stays deleted, so
	// the deletion has to survive the read on its own rather than being undone by
	// a later layer.
	require.NoError(t, oneSegmentFlushing.roaringSetRemoveList([]byte("aaa"), []uint64{2}))
	require.NoError(t, oneSegmentFlushing.roaringSetRemoveList([]byte("eee"), []uint64{5}))
	// A write carrying no values, which an empty Positions produces upstream. It
	// builds a row with both bitmaps allocated and empty, so the two read paths
	// describe it differently — the per-key one as a row, the window as absence
	// — and must still answer the same.
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

	// Rows wide enough that a budget of one of them ends a window, so the memtable
	// read first is the one that settles where it ends. Four thousand documents
	// sixty-four apart stay within a handful of containers, so it is the count
	// rather than the spread that makes these rows expensive.
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
	// Holds keys past where the flushing memtable's budget stops it, which is what
	// a window left wider than that turns into a wrong answer: those keys would be
	// answered out of slots the flushing memtable never wrote.
	narrowingActive := newTestMemtableRoaringSet(map[string][]uint64{
		"bbb": {91}, "ddd": {92}, "eee": {93},
	})
	// Its own segment rather than a shared one: the fake counts the reads it is
	// asked for without a lock, and the cases below run in parallel.
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
		// Zero for no budget, so only a case about narrowing has to think about
		// one.
		budget int
		// Written down rather than derived, where the fold's outcome is worth
		// pinning against something other than the path under test. Optional:
		// the differential above runs either way.
		want map[string][]uint64
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
			// active re-add, which survives only folding oldest first; "eee" is
			// disk {5,6} minus a deletion nothing re-adds.
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
			// The narrowing memtable is read first, so what the second is asked for
			// rests on the bound already settled. Asked for the window's full width
			// instead, it would fill slots past that bound, and the keys beyond it
			// would be answered from a flushing memtable that never wrote them.
			//
			// No want: the rows here are thousands of documents wide, and the
			// differential against the per-key path is what this case is for.
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
					r, err := newRoaringSetBatchReader(view, keys, window, budget)
					require.NoError(t, err)

					requireMatchesPerKey(t, b, view, r, keys, "")
					if tt.want == nil {
						return
					}
					// A second reader: one walks its batch once.
					r2, err := newRoaringSetBatchReader(view, keys, window, budget)
					require.NoError(t, err)
					requireRowsAre(t, r2, keys, tt.want)
				})
			}
		})
	}
}

// TestBatchReaderMatchesPerKeyReadsRandomized runs the same differential over random layer
// contents, which is where an off-by-one in the per-layer cursor shows: it
// would drop a match, or carry one key's layer onto its neighbour.
//
// The differential alone cannot see everything it looks like it can: both paths
// end in roaringSetGetWithLayers, so a bug in that tail agrees with itself on
// both sides. Every layer here only adds, which makes a row's contents the union
// of what the three hold for its key — an expectation that comes from the
// fixture rather than from either path, so the rounds also pin something no
// symmetry can satisfy.
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

		r, err := newRoaringSetBatchReader(view, keys, window, math.MaxInt)
		require.NoError(t, err)
		requireMatchesPerKey(t, b, view, r, keys, fmt.Sprintf("round %d", round))

		want := map[string][]uint64{}
		for i := 0; i < keys.Len(); i++ {
			k := string(keys.At(i))
			want[k] = unionOf(diskDocs[k], flushingDocs[k], activeDocs[k])
		}
		// A second reader, since the first has served every key and would spend
		// the pass refilling.
		abs, err := newRoaringSetBatchReader(view, keys, window, math.MaxInt)
		require.NoError(t, err)
		requireRowsAre(t, abs, keys, want)
	}
}

// TestBatchReaderSurvivesTheFoldMutatingItsRows covers what the fold does to
// every row it is handed: it adopts the first as its accumulator and merges the
// rest into it in place. A window clones once for all the keys it covers, so a
// row the caller mutates must not be one the reader still needs — and the fills
// must stay one per window while that happens.
//
// A key with a disk row is the boundary: there the merger folds onto the disk
// base and never hands the window's clone out. The last case gives every key
// one, deliberately — a single key the disk missed would hand the clone out
// again and the case would stop being a control.
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
		size int
		// wantFills is what the three keys cost: one window at the production
		// size, two when the size splits them.
		wantFills int
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
				r, err := newRoaringSetBatchReader(view, keys, w.size, math.MaxInt)
				require.NoError(t, err)

				for i := 0; i < keys.Len(); i++ {
					want, wr, err := b.roaringSetGetFromConsistentView(view, keys.At(i), concurrency.SROAR_MERGE)
					require.NoError(t, err)
					got, gr, err := r.Next(concurrency.SROAR_MERGE)
					require.NoError(t, err)
					require.Equalf(t, docsOrNil(want), docsOrNil(got), "key %d", i)
					// What the fold does to every row it reads.
					got.Set(9999)
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
// through a window. The window is filled one memtable at a time, so the failure
// leaves one loaded for the new window and the other still holding the last one
// at its width — a mixture that must not be served. Depending on what the
// previous window was, serving it drops a memtable's rows, returns another key's
// row, or indexes past the end.
func TestBatchReaderFillWindowError(t *testing.T) {
	t.Parallel()

	readErr := errors.New("memtable read failed")

	tests := []struct {
		name string
		// how far the walk gets before the read that fails; with a window of 4
		// over 10 keys, 0 fails the first fill and 4 fails a later one, after a
		// window has already been served.
		readBefore int
	}{
		{name: "first window, nothing loaded yet", readBefore: 0},
		{name: "a later window, one already served", readBefore: 4},
		{name: "mid-window, no fill due", readBefore: 5},
	}

	// Which memtable fails decides how far the fill got. Flushing is index 0, so
	// failing it aborts before the active one is read at all; failing active
	// instead leaves flushing loaded for the new window and active holding the
	// last one — the half-loaded state the invalidation exists for, which
	// failing the first memtable never produces.
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
				// k05 puts a row inside the second window. Without it that window
				// holds nothing, a stale all-zero window is accidentally the right
				// answer, and the invalidation below cannot be caught failing.
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
				r, err := newRoaringSetBatchReader(view, keys, 4, math.MaxInt)
				require.NoError(t, err)

				for i := 0; i < tc.readBefore; i++ {
					_, release, err := r.Next(concurrency.SROAR_MERGE)
					require.NoError(t, err)
					release()
				}

				// Fails only if this read is due a fill; mid-window it is served
				// from what is already loaded, which is equally worth pinning.
				failing.roaringSetGetWindowErr = readErr
				at := tc.readBefore
				bm, release, err := r.Next(concurrency.SROAR_MERGE)
				if err != nil {
					require.ErrorIs(t, err, readErr)
					require.Nil(t, bm, "a failed read returns no row")
					require.Nil(t, release, "a failed read returns nothing to release")
				} else {
					// served from the loaded window, so the walk moved on
					require.NotNil(t, release)
					release()
					at++
				}
				failing.roaringSetGetWindowErr = nil

				// The walk resumes where it stopped. A failed fill left one
				// memtable loaded for the new window and the other holding the
				// last one, so serving from that mixture would answer with a
				// memtable's rows for the wrong keys.
				for i := at; i < keys.Len(); i++ {
					want, wr, err := b.roaringSetGetFromConsistentView(view, keys.At(i), concurrency.SROAR_MERGE)
					require.NoError(t, err)
					got, gr, err := r.Next(concurrency.SROAR_MERGE)
					require.NoError(t, err)
					require.Equalf(t, docsOrNil(want), docsOrNil(got), "index %d after the failed fill", i)
					wr()
					gr()
				}
			})
		}
	}
}

// TestBatchReaderDiskErrorLeavesNothingToRetryWrong pins the other half of the
// error contract. A failed disk read happens after the key's slots have been
// emptied, so leaving the window intact would let a retry read them as absence
// and fold the disk row alone — a wrong row, with no error to say so. The retry
// must refill instead, and answer as the per-key path does.
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
	// The memtable deletes what disk holds, so a row folded without it differs
	// from the right answer — which is what makes the retry observable at all.
	active := newTestMemtableRoaringSet(nil)
	require.NoError(t, active.roaringSetRemoveList([]byte("k1"), []uint64{2}))
	require.NoError(t, active.roaringSetAddList([]byte("k1"), []uint64{9}))
	view := BucketConsistentView{Active: active, Disk: []Segment{diskSeg}, Bucket: b}

	keys := sortedKeysOf(t, []string{"k0", "k1"})
	r, err := newRoaringSetBatchReader(view, keys, memtableWindowKeys, math.MaxInt)
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

// TestBatchReaderReadsWhileMemtableIsWritten covers a window read running
// against a live memtable: it holds the read lock while it walks the tree and
// copies what it finds, and the memtable goes on taking writes throughout.
//
// A write landing mid-batch may or may not be visible — the windows are filled
// as the fold reaches them, so which keys see it depends on timing, and the
// reader's contract says as much. What may never happen is a row that is
// neither the value before the write nor the value after: a torn read, or a
// neighbour's row, or a bitmap caught half-copied.
func TestBatchReaderReadsWhileMemtableIsWritten(t *testing.T) {
	const (
		n      = memtableWindowKeys*2 + 3
		marker = uint64(999_999)
	)

	rows := map[string][]uint64{}
	batch := make([]string, n)
	for i := 0; i < n; i++ {
		batch[i] = fmt.Sprintf("k%06d", i)
		rows[batch[i]] = []uint64{uint64(i)}
	}

	diskSeg := newFakeRoaringSetSegment(map[string]*sroar.Bitmap{})
	active := newTestMemtableRoaringSet(rows)
	b := &Bucket{
		strategy: StrategyRoaringSet,
		disk:     &SegmentGroup{segments: []Segment{diskSeg}},
		logger:   nullLogger(),
	}
	view := BucketConsistentView{Active: active, Disk: []Segment{diskSeg}, Bucket: b}
	keys := sortedKeysOf(t, batch)

	var written atomic.Bool
	var writeErr error
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Even on the way out, so a failed write ends the reader's loop below
		// rather than leaving it spinning for a writer that has given up.
		defer written.Store(true)
		<-start
		for _, k := range batch {
			if err := active.roaringSetAddOne([]byte(k), marker); err != nil {
				writeErr = err
				return
			}
		}
	}()

	// Fold the batch repeatedly until the writer is done, so reads and writes
	// overlap rather than merely interleaving once. Both states have to be
	// observed: a run in which every row already carried the marker would
	// satisfy the assertion below without ever having raced anything.
	// Neither is left to the scheduler: the writer is held until one whole pass
	// has been read, and the pass that ends the loop runs after it finished.
	seenBefore, seenAfter := 0, 0
	for pass := 0; ; pass++ {
		done := written.Load()
		r, err := newRoaringSetBatchReader(view, keys, memtableWindowKeys, math.MaxInt)
		require.NoError(t, err)
		for i := 0; i < keys.Len(); i++ {
			got, release, err := r.Next(concurrency.SROAR_MERGE)
			require.NoError(t, err)
			if len(docsOrNil(got)) == 1 {
				seenBefore++
			} else {
				seenAfter++
			}
			require.Containsf(t, [][]uint64{{uint64(i)}, {uint64(i), marker}}, docsOrNil(got),
				"pass %d index %d: row is neither the value before the write nor after it", pass, i)
			release()
		}
		if pass == 0 {
			// Only now, so every row above was read at its pre-write value. done
			// cannot be true yet, so this always runs and the writer never hangs.
			close(start)
		}
		if done {
			break
		}
	}
	wg.Wait()
	require.NoError(t, writeErr)
	require.Positive(t, seenBefore, "no row was read before the writer reached it; the two never overlapped")
	require.Positive(t, seenAfter, "no row was read after the writer reached it; the two never overlapped")

	// and once the writer has finished, every key must show the write
	r, err := newRoaringSetBatchReader(view, keys, memtableWindowKeys, math.MaxInt)
	require.NoError(t, err)
	for i := 0; i < keys.Len(); i++ {
		got, release, err := r.Next(concurrency.SROAR_MERGE)
		require.NoError(t, err)
		require.Equal(t, []uint64{uint64(i), marker}, docsOrNil(got), "index %d after the writer finished", i)
		release()
	}
}

// TestBatchReaderFillsWindowsLazily pins the other half of the same contract.
// ContainsAll abandons its fold as soon as the intersection empties, often after
// a handful of keys, so a reader that filled the whole batch up front would read
// windows whose keys nobody goes on to ask about — and on a large batch that is
// most of them.
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
		// Reading everything is the other half of the same contract: one
		// acquisition per window rather than one per key, which is the whole
		// point of the window and which no differential would notice losing.
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
			r, err := newRoaringSetBatchReader(view, keys, 4, math.MaxInt)
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

// TestBatchReaderStatsReportTheWork pins what the slow-query annotation reads.
// A batched filter that has gone slow is otherwise indistinguishable from one
// that merely ran during a slow query: fills times memtables is the acquisitions
// the batching paid for, and a fill count approaching the key count says the
// windows were narrow rather than that the batch was large.
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
			// 10 keys in windows of 4 is 3 windows, which is what a whole fold
			// pays. Fills against Len is what says the batching, rather than the
			// rows, is where a slow filter went.
			name:          "the whole batch fills once per window",
			held:          map[string][]uint64{"k00": {1}, "k05": {2}},
			reads:         10,
			wantFills:     3,
			wantMemtables: 1,
			wantCloned:    true,
		},
		{
			// That the annotation reads the reader after the fold rather than
			// before: a fold stopping inside the second window has to report the
			// two windows it entered, not the three the batch spans. Lazy filling
			// itself is TestBatchReaderFillsWindowsLazily's.
			name:          "a fold that stops early pays for fewer windows",
			held:          map[string][]uint64{"k00": {1}, "k05": {2}},
			reads:         5,
			wantFills:     2,
			wantMemtables: 1,
			wantCloned:    true,
		},
		{
			// Nothing held, so the window costs acquisitions and no memory. This
			// is the case the window cannot be blamed for.
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
			// A memtable holding nothing is skipped by the constructor, so give it
			// a write the skip does not remove.
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
			r, err := newRoaringSetBatchReader(view, keys, 4, math.MaxInt)
			require.NoError(t, err)
			for i := 0; i < tc.reads; i++ {
				_, release, err := r.Next(concurrency.SROAR_MERGE)
				require.NoError(t, err)
				release()
			}

			got := r.Stats()
			require.Equal(t, tc.wantFills, got.Fills)
			require.Equal(t, tc.wantMemtables, got.Memtables)
			require.Equal(t, tc.reads, got.KeysRead,
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

// TestBatchReaderSkipsAnEmptyActiveMemtable pins the skip the reader's
// constructor argues for: an active memtable whose size is zero is dropped from
// the view, and the rows it holds are ones that add and delete nothing, so the
// answer is the same as the per-key path gives — and that path does not skip it.
//
// Size counts entries added or removed, so a write carrying no values builds a
// node and leaves the size at zero. Replaying a commit log record with both
// slices empty does the same. The argument is that such rows fold as if absent;
// this runs the differential the argument implies.
func TestBatchReaderSkipsAnEmptyActiveMemtable(t *testing.T) {
	t.Parallel()

	batch := []string{"aaa", "bbb", "ccc"}
	diskSeg := newFakeRoaringSetSegment(map[string]*sroar.Bitmap{
		"aaa": bitmapFromSlice([]uint64{1, 2}),
		"ccc": bitmapFromSlice([]uint64{3}),
	})
	flushing := newTestMemtableRoaringSet(map[string][]uint64{"bbb": {9}})

	// Written through the write path, so size and tree disagree exactly as they
	// would in a bucket: nodes for two keys, and a size still at zero.
	active := newTestMemtableRoaringSet(map[string][]uint64{})
	require.NoError(t, active.roaringSetAddList([]byte("aaa"), []uint64{}))
	require.NoError(t, active.roaringSetAddList([]byte("ccc"), []uint64{}))
	require.Zero(t, active.Size(), "the fixture must reach the state the skip is about")

	b := &Bucket{
		strategy: StrategyRoaringSet,
		disk:     &SegmentGroup{segments: []Segment{diskSeg}},
		logger:   nullLogger(),
	}
	view := BucketConsistentView{Active: active, Flushing: flushing, Disk: []Segment{diskSeg}, Bucket: b}

	keys := sortedKeysOf(t, batch)
	r, err := newRoaringSetBatchReader(view, keys, memtableWindowKeys, math.MaxInt)
	require.NoError(t, err)
	require.Equal(t, 1, r.mtCount, "the empty active memtable must be dropped from the view")

	requireMatchesPerKey(t, b, view, r, keys, "empty active memtable")
	require.Zero(t, active.roaringSetGetWindowCalls, "a dropped memtable must not be read")

	abs, err := newRoaringSetBatchReader(view, keys, memtableWindowKeys, math.MaxInt)
	require.NoError(t, err)
	requireRowsAre(t, abs, keys, map[string][]uint64{
		"aaa": {1, 2},
		"bbb": {9},
		"ccc": {3},
	})
}

// TestBatchReaderNarrowsTheWindowToTheBudget pins one thing, which is the reader's
// half of the memory bound: that a fill adopts where the walk stopped. The walk
// reporting the right stop is no use if the fill keeps the width it asked for, and
// that failure is invisible to every other test here — the rows still read
// correctly, they just cost a window's memory each time.
//
// Deliberately nothing else. The always-taken first key is pinned on the walk,
// which is where a budget under one row can be handed in cheaply. Which slots the
// walk wrote cannot be pinned from here: a lone memtable stops itself at its own
// budget, so the slots past winEnd are untouched whatever bound the fill passed.
// And that rows read correctly through a narrowed window belongs to
// TestBatchReaderMatchesPerKeyReads' budgeted case, over the two memtables where
// the bound is what decides it.
func TestBatchReaderNarrowsTheWindowToTheBudget(t *testing.T) {
	t.Parallel()

	// Rows spread across containers, so each costs real bytes.
	const rows, docsPerRow = 12, 4096
	batch := make([]string, rows)
	held := map[string][]uint64{}
	for i := range batch {
		batch[i] = fmt.Sprintf("k%02d", i)
		docs := make([]uint64, docsPerRow)
		for j := range docs {
			docs[j] = uint64(i + j*rows*8)
		}
		held[batch[i]] = docs
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
	// A window wide enough for the whole batch, so only the budget can narrow it.
	// One row's worth of budget, so the window ends on cost rather than on the
	// key count — the shape a low-cardinality property produces in production,
	// where rows are fat enough to reach the real constant.
	one := make([]roaringset.BitmapLayer, 1)
	first, err := active.roaringSetGetWindow(keys, 0, 1, one, math.MaxInt)
	require.NoError(t, err)
	require.Positive(t, first.Bytes)
	budget := 3 * first.Bytes

	r, err := newRoaringSetBatchReader(view, keys, rows, budget)
	require.NoError(t, err)
	require.NoError(t, r.fillWindow())
	require.Less(t, r.winEnd-r.winStart, rows,
		"the budget must have ended the window before the key count did")
}

// TestBatchReaderReleasesWhatItHasServed pins that a served key's clone leaves
// the window. Only a fill drops clones, and a batch inside one window never
// reaches a second fill, so without this every row the window matched would
// stay live for as long as the fold runs — the whole window at once rather than
// the part of it still to be read.
//
// It reads the front of the window and asserts on the back, so the assertion
// cannot pass by the window having been refilled underneath it.
func TestBatchReaderReleasesWhatItHasServed(t *testing.T) {
	t.Parallel()

	batch := make([]string, 8)
	for i := range batch {
		batch[i] = fmt.Sprintf("k%02d", i)
	}

	diskSeg := newFakeRoaringSetSegment(map[string]*sroar.Bitmap{})
	// Every key held, so a slot left behind is a slot holding a clone.
	held := map[string][]uint64{}
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
	r, err := newRoaringSetBatchReader(view, keys, len(batch), math.MaxInt)
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
			require.Nilf(t, r.layers[mt][i].Additions, "layer %d slot %d still holds a served clone", mt, i)
			require.Nilf(t, r.layers[mt][i].Deletions, "layer %d slot %d still holds a served clone", mt, i)
		}
		for i := served; i < len(batch); i++ {
			require.NotNilf(t, r.layers[mt][i].Additions, "layer %d slot %d lost a row it has not served", mt, i)
		}
	}
}

// TestBatchReaderAtProductionWindow runs the differential over a batch several
// times the real window, which no other test here does: they either set a tiny
// window to reach the boundaries or carry too few keys for memtableWindowKeys to have a
// second window at all. A window boundary that only appears at the real size
// would have nothing else to fail.
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

	// Through the constructor, at the real window: every other test here builds
	// the reader by hand with a small one, so nothing else covers the size the
	// constructor actually picks.
	keys := sortedKeysOf(t, batch)
	reader, err := NewRoaringSetBatchReader(view, keys)
	require.NoError(t, err)

	requireMatchesPerKey(t, b, view, reader, keys, "")

	windows := (n + memtableWindowKeys - 1) / memtableWindowKeys
	require.Equal(t, windows, flushing.roaringSetGetWindowCalls, "one read per window")
	require.Equal(t, windows, active.roaringSetGetWindowCalls, "one read per window")
}

// TestBatchReaderRejectsItsBounds covers the two constructor arguments that fail
// differently when wrong: a non-positive window panics deep in the first fill,
// while a non-positive budget degrades silently to a fill per key — see the guards
// in newRoaringSetBatchReader. Both are refused there instead.
//
// The window read itself takes any budget, and is tested at zero and one for the
// first-key floor. This is the caller's arithmetic, not the walk's.
func TestBatchReaderRejectsItsBounds(t *testing.T) {
	t.Parallel()
	keys := sortedKeysOf(t, []string{"a", "b"})
	view := BucketConsistentView{Active: newTestMemtableRoaringSet(nil)}

	for _, window := range []int{0, -1, -256} {
		_, err := newRoaringSetBatchReader(view, keys, window, math.MaxInt)
		require.Errorf(t, err, "window %d must be rejected", window)
	}
	for _, budget := range []int{0, -1, -256} {
		_, err := newRoaringSetBatchReader(view, keys, memtableWindowKeys, budget)
		require.Errorf(t, err, "budget %d must be rejected", budget)
	}
	// Both good, so the cases above fail on the bound named and not on the view.
	_, err := newRoaringSetBatchReader(view, keys, memtableWindowKeys, memtableWindowBytes)
	require.NoError(t, err)
}

// TestBatchReaderReadsPastTheBatch pins the one way left to ask for a row that
// is not there, and the shape of the failure: an error and neither a row nor a
// release. A release handed back beside an error is one nobody calls, since the
// folds drop it. Len is how many times Next answers, so asking again is the
// caller's error rather than a row it could have found.
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
	r, err := newRoaringSetBatchReader(view, keys, memtableWindowKeys, math.MaxInt)
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

// requireRowsAre checks the reader against written-down rows rather than
// against the per-key path. Both sides of the differential now fold through
// roaringSetGetWithLayers, so a bug there — the layer order, say — agrees with
// itself and passes every comparison. This is the only assertion that does not.
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
// must read the same as it would on its own. The tests that vary how the window
// falls use it; those asserting a specific row, a fill count or a rejected
// argument assert that directly.
func requireMatchesPerKey(t *testing.T, b *Bucket, view BucketConsistentView,
	r *RoaringSetBatchReader, keys inverted.SortedKeys, msg string,
) {
	t.Helper()
	for i := 0; i < keys.Len(); i++ {
		want, wr, err := b.roaringSetGetFromConsistentView(view, keys.At(i), concurrency.SROAR_MERGE)
		require.NoError(t, err)
		got, gr, err := r.Next(concurrency.SROAR_MERGE)
		require.NoErrorf(t, err, "read %d", i)
		require.Equalf(t, docsOrNil(want), docsOrNil(got), "%s: key %q (index %d)", msg, keys.At(i), i)
		wr()
		gr()
	}
}

// TestBatchReaderKeepsNoClonesOnceTheBatchIsWalked pins that a reader holding
// two memtables ends a batch with none of their rows still copied into it.
//
// A slot is emptied when the fold takes the row, so the ones at risk are those
// the fold never reaches: a memtable read before the one whose budget settles
// the window fills past that settlement, and those slots are never served. Since
// window widths only shrink, the batch's last windows are narrower than the
// buffer, so a fill that cleared only its own width would leave them behind for
// as long as the reader lives.
func TestBatchReaderKeepsNoClonesOnceTheBatchIsWalked(t *testing.T) {
	const (
		nKeys  = 10
		window = 8
	)

	names := make([]string, nKeys)
	for i := range names {
		names[i] = fmt.Sprintf("k%03d", i)
	}
	keys := sortedKeysOf(t, names)

	// Read first, and holds a row for every key, so it fills whatever width it
	// is asked for.
	flushing := newTestMemtableRoaringSet(nil)
	for i, n := range names {
		require.NoError(t, flushing.roaringSetAddList([]byte(n), []uint64{uint64(i)}))
	}

	// Read second, and its rows are wide enough that the budget below stops it
	// after one — so it, not the width, decides where each window ends.
	active := newTestMemtableRoaringSet(nil)
	wide := make([]uint64, 4096)
	for i := range wide {
		wide[i] = uint64(i)
	}
	for _, n := range names {
		require.NoError(t, active.roaringSetAddList([]byte(n), wide))
	}

	oneRow := make([]roaringset.BitmapLayer, 1)
	first, err := active.roaringSetGetWindow(keys, 0, 1, oneRow, math.MaxInt)
	require.NoError(t, err)

	b := &Bucket{strategy: StrategyRoaringSet, disk: &SegmentGroup{}, logger: nullLogger()}
	view := BucketConsistentView{Active: active, Flushing: flushing, Bucket: b}

	r, err := newRoaringSetBatchReader(view, keys, window, first.Bytes+1)
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

	for mt := range r.mtCount {
		buf := r.layers[mt][:cap(r.layers[mt])]
		for at, layer := range buf {
			require.Nil(t, layer.Additions,
				"memtable %d still holds additions cloned into slot %d", mt, at)
			require.Nil(t, layer.Deletions,
				"memtable %d still holds deletions cloned into slot %d", mt, at)
		}
	}

	// And the rows are right, which is the direction the batched parity table does
	// not reach: there the memtable read first is the one whose budget settles the
	// window, and here it is the one read second. What each is asked for depends on
	// the other, so both orders have to answer as the per-key path does.
	fresh, err := newRoaringSetBatchReader(view, keys, window, first.Bytes+1)
	require.NoError(t, err)
	requireMatchesPerKey(t, b, view, fresh, keys, "the second memtable narrowing")
}

// TestBatchReaderEndsWindowsOnEitherLimit covers a batch whose windows do not all
// end the same way. A window ends at the key count or at the byte budget,
// whichever it reaches first, and which one that is follows the rows: thin ones
// run to the count, fat ones spend the budget first. Every other budget case here
// meets one limit throughout, so a reader that had quietly stopped honouring the
// other would still pass them.
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

	// Room for two of the fat rows, so a window of four keys reaches the count
	// among the thin ones and the budget among the fat ones.
	oneFat := func() int {
		single := sortedKeysOf(t, []string{fmt.Sprintf("k%02d", thinKeys)})
		dst := make([]roaringset.BitmapLayer, 1)
		fill, err := active.roaringSetGetWindow(single, 0, 1, dst, math.MaxInt)
		require.NoError(t, err)
		require.Positive(t, fill.Bytes)
		return fill.Bytes
	}()

	r, err := newRoaringSetBatchReader(view, keys, window, 2*oneFat+1)
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

	// And the stats say which limit did it, which is the whole reason an operator
	// can tell "this batch was large" from "raise the byte budget".
	st := r.Stats()
	require.Positive(t, st.NarrowedFills, "the budget ended some windows")
	require.Less(t, st.NarrowedFills, st.Fills, "and the key count ended others")
	require.Equal(t, keys.Len(), st.KeysRead)
	// The peak is one window; the total is every window. Fat rows read across
	// several windows separate the two, which a running total alone would hide.
	require.Positive(t, st.BytesPeak)
	require.Greater(t, st.BytesCopied, st.BytesPeak,
		"a batch spanning several windows must copy more than the widest of them held")

	// And the rows are right either way, which is the point of ending early.
	r2, err := newRoaringSetBatchReader(view, keys, window, 2*oneFat+1)
	require.NoError(t, err)
	requireMatchesPerKey(t, b, view, r2, keys, "")
}

// TestBatchReaderHoldsOneBudgetAcrossMemtables pins that the byte budget bounds
// what a reader holds rather than what each of its memtables holds. Handing both
// the whole budget would double the peak whenever a flush is in flight — a
// condition the caller neither chooses nor observes — and nothing else here would
// notice, since every other budget case builds a single-memtable view.
func TestBatchReaderHoldsOneBudgetAcrossMemtables(t *testing.T) {
	t.Parallel()

	const nKeys = 200

	names := make([]string, nKeys)
	for i := range names {
		names[i] = fmt.Sprintf("k%05d", i)
	}
	keys := sortedKeysOf(t, names)

	// One document per container, which is the shape the budget exists for: the
	// rows are wide enough that a window of them reaches the budget well before it
	// reaches the key count.
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
		r, err := newRoaringSetBatchReader(view, keys, memtableWindowKeys, budget)
		require.NoError(t, err)
		_, release, err := r.Next(concurrency.SROAR_MERGE)
		require.NoError(t, err)
		release()
		return r.Stats()
	}

	one := peakOf(t, BucketConsistentView{Active: build(1), Bucket: b})
	two := peakOf(t, BucketConsistentView{Active: build(1), Flushing: build(500), Bucket: b})

	require.Equal(t, 1, one.Memtables)
	require.Equal(t, 2, two.Memtables)
	require.Positive(t, one.BytesPeak, "the fixture must reach the budget, or this proves nothing")

	require.LessOrEqual(t, two.BytesPeak, budget+one.BytesPeak/one.KeysRead,
		"two memtables must hold one budget between them, not one each")
	require.LessOrEqual(t, two.BytesPeak, one.BytesPeak*3/2,
		"a flush in flight must not roughly double what a window holds")
}
