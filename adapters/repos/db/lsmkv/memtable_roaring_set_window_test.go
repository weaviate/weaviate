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
	"errors"
	"fmt"
	"math"
	"math/rand"
	"path"
	"slices"
	"sort"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/inverted"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
)

// TestRoaringSetGetWindowMatchesPerKey is the differential the batch read has to
// survive: for any memtable and any batch, reading the batch in one pass must
// answer exactly what reading each key on its own does. The two walk the tree
// completely differently — one descends per key, the other advances two cursors
// past each other — so agreement is the property worth pinning, and the shapes
// below are the ones where a cursor can overshoot: batches far sparser than the
// memtable, far denser, disjoint from it, and sharing only their ends.
func TestRoaringSetGetWindowMatchesPerKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		memtableKeys []string
		batchKeys    []string
	}{
		{"empty batch", []string{"b", "d"}, nil},
		{"empty memtable", nil, []string{"a", "b"}},
		{"single key, hit", []string{"b"}, []string{"b"}},
		{"single key, missed", []string{"b"}, []string{"a", "c"}},
		{"batch entirely before the memtable", []string{"y", "z"}, []string{"a", "b", "c"}},
		{"batch entirely after the memtable", []string{"a", "b"}, []string{"y", "z"}},
		{"disjoint, interleaved", []string{"b", "d", "f"}, []string{"a", "c", "e", "g"}},
		{"identical", []string{"a", "b", "c"}, []string{"a", "b", "c"}},
		{"memtable much sparser", []string{"m"}, singleLetterKeys()},
		{"batch much sparser", singleLetterKeys(), []string{"m"}},
		{"only the ends shared", []string{"a", "m", "z"}, []string{"a", "z"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := memtableWith(t, tt.memtableKeys)
			keys := sortedKeysOf(t, tt.batchKeys)

			got := make([]roaringset.BitmapLayer, keys.Len())
			fillWindowOK(t, m, keys, 0, keys.Len(), got)

			// what reading one key at a time says, in the same form
			want := make([]roaringset.BitmapLayer, keys.Len())
			for i := 0; i < keys.Len(); i++ {
				layer, err := m.roaringSetGet(keys.At(i))
				if errors.Is(err, entlsmkv.NotFound) {
					continue
				}
				require.NoError(t, err)
				want[i] = layer
			}

			assert.Equal(t, layerDocsOf(want), layerDocsOf(got))
		})
	}
}

// TestRoaringSetGetWindowRandomized runs the same differential over random
// shapes, which is where an off-by-one in either cursor's jump shows up: it
// would drop or duplicate a key that no hand-written case happens to place.
func TestRoaringSetGetWindowRandomized(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewSource(7))
	for round := 0; round < 200; round++ {
		universe := 40
		mem := sampleDistinct(rnd, universe, rnd.Intn(universe+1))
		batch := sampleDistinct(rnd, universe, rnd.Intn(universe+1))

		m := memtableWith(t, mem)
		keys := sortedKeysOf(t, batch)

		got := make([]roaringset.BitmapLayer, keys.Len())
		fillWindowOK(t, m, keys, 0, keys.Len(), got)

		want := make([]roaringset.BitmapLayer, keys.Len())
		for i := 0; i < keys.Len(); i++ {
			if layer, err := m.roaringSetGet(keys.At(i)); err == nil {
				want[i] = layer
			}
		}
		require.Equalf(t, layerDocsOf(want), layerDocsOf(got),
			"round %d: memtable %v batch %v", round, mem, batch)
	}
}

// TestRoaringSetGetWindowMatchesWholeBatch pins that reading a batch in windows answers
// exactly what reading it whole does.
//
// The caller windows a large batch so the lock is held briefly and only one
// window's bitmaps are held at a time, which only works if the boundaries are
// invisible: every window starts with its own descent rather than where the
// last one stopped, so a key sitting on a boundary is the one a mistake drops
// or double-counts. Window size 1 is the degenerate end of that — every key is
// its own boundary.
func TestRoaringSetGetWindowMatchesWholeBatch(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewSource(11))
	for round := 0; round < 100; round++ {
		universe := 30
		mem := sampleDistinct(rnd, universe, rnd.Intn(universe+1))
		batch := sampleDistinct(rnd, universe, rnd.Intn(universe+1))

		m := memtableWith(t, mem)
		keys := sortedKeysOf(t, batch)

		whole := make([]roaringset.BitmapLayer, keys.Len())
		fillWindowOK(t, m, keys, 0, keys.Len(), whole)

		for _, w := range []int{1, 2, 3, 7, 1000} {
			// One slot per key, handed over a window at a time, exactly as the
			// batch reader does it. Writing outside its window would index past
			// the subslice rather than land in a neighbour's slot.
			got := make([]roaringset.BitmapLayer, keys.Len())
			for from := 0; from < keys.Len(); from += w {
				to := min(from+w, keys.Len())
				fillWindowOK(t, m, keys, from, to, got[from:to])
			}
			require.Equalf(t, layerDocsOf(whole), layerDocsOf(got),
				"round %d window %d: memtable %v batch %v", round, w, mem, batch)
		}
	}
}

// TestRoaringSetGetWindowRangeGuards separates the ranges that can be answered
// by reading nothing from the ones nobody asked for. A range outside the batch
// cannot be answered with "this memtable holds none of those keys", and a slice
// too short for its range has nowhere to put the last rows.
//
// A slice longer than the range is legal, and pinned here as such: fillWindow
// holds one buffer at the widest a window gets and asks for the narrower range
// its budget settled on.
//
// None of the erroring shapes is reachable from fillWindow, which always
// produces a range inside the batch and a slice at least its width. They are the
// arithmetic a second caller would get wrong.
func TestRoaringSetGetWindowRangeGuards(t *testing.T) {
	t.Parallel()

	m := memtableWith(t, []string{"a", "b", "c"})
	keys := sortedKeysOf(t, []string{"a", "b", "c"})

	tests := []struct {
		name     string
		from, to int
		slots    int
		wantErr  bool
	}{
		{name: "empty at the start", from: 0, to: 0, slots: 0},
		{name: "empty at the end", from: 3, to: 3, slots: 0},
		{name: "empty in the middle", from: 1, to: 1, slots: 0},
		{name: "slice longer than the window", from: 0, to: 2, slots: 3},
		{name: "whole batch passed for a late window", from: 1, to: 3, slots: 3},
		{name: "slots for an empty window", from: 1, to: 1, slots: 3},
		{name: "inverted", from: 2, to: 1, slots: 0, wantErr: true},
		{name: "before the first key", from: -1, to: 2, slots: 3, wantErr: true},
		{name: "one past the last", from: 0, to: 4, slots: 4, wantErr: true},
		{name: "far past the last", from: 1, to: 99, slots: 99, wantErr: true},
		{name: "slice shorter than the window", from: 0, to: 3, slots: 2, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			into := make([]roaringset.BitmapLayer, tc.slots)
			_, err := m.roaringSetGetWindow(keys, tc.from, tc.to, into, math.MaxInt)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestRoaringSetGetWindowDropsEmptySides pins which clone the read uses. A row
// written to but never deleted from still has both bitmaps allocated in the
// tree, and the window must hand back a nil deletion side rather than an empty
// one — that nil is what the reader's presence test reads. Plain Clone would
// return an allocated empty bitmap and go unnoticed everywhere else.
func TestRoaringSetGetWindowDropsEmptySides(t *testing.T) {
	t.Parallel()

	m := memtableWith(t, []string{"a", "b", "c"})
	require.NoError(t, m.roaringSetAddOne([]byte("adds-only"), 1))
	require.NoError(t, m.roaringSetRemoveOne([]byte("dels-only"), 2))

	keys := sortedKeysOf(t, []string{"adds-only", "dels-only"})
	into := make([]roaringset.BitmapLayer, keys.Len())
	fillWindowOK(t, m, keys, 0, keys.Len(), into)

	for i := 0; i < keys.Len(); i++ {
		switch string(keys.At(i)) {
		case "adds-only":
			require.NotNil(t, into[i].Additions)
			require.Nil(t, into[i].Deletions, "a row never deleted from must carry no deletion side")
		case "dels-only":
			require.Nil(t, into[i].Additions, "a row never added to must carry no addition side")
			require.NotNil(t, into[i].Deletions)
		}
	}
}

// TestRoaringSetGetWindowClearsWhatItIsGiven pins that absence is answered as
// absence even when the caller hands over a buffer it has used before. The read
// only writes the slots whose keys it holds, so anything left in the others
// would be read as a row this memtable has for that key.
func TestRoaringSetGetWindowClearsWhatItIsGiven(t *testing.T) {
	t.Parallel()

	m := memtableWith(t, []string{"a", "b", "c"})

	tests := []struct {
		name  string
		batch []string
	}{
		{name: "memtable holds none of them", batch: []string{"x", "y", "z"}},
		{name: "memtable holds some of them", batch: []string{"a", "y", "c"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			keys := sortedKeysOf(t, tc.batch)

			dirty := make([]roaringset.BitmapLayer, keys.Len())
			for i := range dirty {
				dirty[i] = roaringset.BitmapLayer{Additions: bitmapFromSlice([]uint64{uint64(900 + i)})}
			}
			fillWindowOK(t, m, keys, 0, keys.Len(), dirty)

			fresh := make([]roaringset.BitmapLayer, keys.Len())
			fillWindowOK(t, m, keys, 0, keys.Len(), fresh)

			require.Equal(t, layerDocsOf(fresh), layerDocsOf(dirty),
				"a reused buffer must answer exactly what a clean one does")
		})
	}
}

// TestRoaringSetGetWindowCopiesWhatItYields pins that the rows leave the read
// owning nothing of the memtable. The walk hands out the tree's own nodes, so
// only the copy keeps a caller from editing the memtable through the row it was
// given. The race detector catches the concurrent half of that; this catches the
// half that is wrong even single-threaded.
func TestRoaringSetGetWindowCopiesWhatItYields(t *testing.T) {
	t.Parallel()

	m := memtableWith(t, []string{"a", "b", "c"})
	keys := sortedKeysOf(t, []string{"a", "b", "c"})

	got := make([]roaringset.BitmapLayer, keys.Len())
	fillWindowOK(t, m, keys, 0, keys.Len(), got)
	before := layerDocsOf(got)

	for _, l := range got {
		if l.Additions != nil {
			l.Additions.Set(777)
		}
		if l.Deletions != nil {
			l.Deletions.Set(778)
		}
	}

	again := make([]roaringset.BitmapLayer, keys.Len())
	fillWindowOK(t, m, keys, 0, keys.Len(), again)
	require.Equal(t, before, layerDocsOf(again),
		"editing a row the read handed out must not reach the memtable")
}

// TestRoaringSetGetWindowRejectsOtherStrategies covers the guard that keeps the
// roaringset walk off a memtable holding something else, whose tree it would
// read as one.
func TestRoaringSetGetWindowRejectsOtherStrategies(t *testing.T) {
	t.Parallel()

	m := newTestMemtableReplace(map[string][]byte{"a": []byte("x")})
	keys := sortedKeysOf(t, []string{"a"})
	into := make([]roaringset.BitmapLayer, 1)
	_, err := m.roaringSetGetWindow(keys, 0, 1, into, math.MaxInt)
	require.Error(t, err)
}

func memtableWith(t *testing.T, keys []string) *Memtable {
	t.Helper()
	logger, _ := test.NewNullLogger()
	dir := path.Join(t.TempDir(), "fake")

	cl, err := newCommitLogger(dir, StrategyRoaringSet, 0)
	require.NoError(t, err)
	m, err := newMemtable(cl, nil, logger, nil, memtableConfig{
		path:     dir,
		strategy: StrategyRoaringSet,
	})
	require.NoError(t, err)

	// Every third key also carries a deletion, and every fifth carries only
	// one. Additions alone would make the deletion half of every comparison
	// vacuous, and a deletion-only row is the shape whose layer has a nil
	// Additions — the one a fold that assumes otherwise drops.
	for i, k := range keys {
		if i%5 != 0 {
			require.NoError(t, m.roaringSetAddOne([]byte(k), uint64(i)))
		}
		if i%3 == 0 || i%5 == 0 {
			require.NoError(t, m.roaringSetRemoveOne([]byte(k), uint64(1000+i)))
		}
	}
	return m
}

// layerDocsOf renders layers as their doc IDs, which is the only way to compare
// two reads of the same rows: each read clones, so the bitmaps are never the
// same objects.
func layerDocsOf(layers []roaringset.BitmapLayer) []layerDocs {
	out := make([]layerDocs, len(layers))
	for i, l := range layers {
		out[i] = layerDocs{additions: docsOrNil(l.Additions), deletions: docsOrNil(l.Deletions)}
	}
	return out
}

type layerDocs struct{ additions, deletions []uint64 }

func sortedKeysOf(tb testing.TB, keys []string) inverted.SortedKeys {
	tb.Helper()
	sorted := slices.Clone(keys)
	slices.Sort(sorted)
	total := 0
	for _, k := range sorted {
		total += len(k)
	}
	b := inverted.NewVarKeyBuilder(len(sorted), total)
	for _, k := range sorted {
		b.AppendString(k)
	}
	built, err := b.Build()
	require.NoError(tb, err)
	return built
}

func sampleDistinct(rnd *rand.Rand, universe, n int) []string {
	perm := rnd.Perm(universe)[:n]
	out := make([]string, 0, n)
	for _, v := range perm {
		out = append(out, fmt.Sprintf("key_%03d", v))
	}
	slices.Sort(out)
	return out
}

func singleLetterKeys() []string {
	out := make([]string, 0, 26)
	for c := byte('a'); c <= 'z'; c++ {
		out = append(out, string([]byte{c}))
	}
	return out
}

// TestMemtableLayersAreNeverBothNil pins what makes a window slot readable at
// all. The slot is a plain BitmapLayer whose zero value says "no row for this
// key", which only tells absent from present because every node a memtable
// builds allocates both of its bitmaps. That invariant belongs to roaringset, not
// to whatever reads a window, so it is checked rather than assumed: a
// node-creation path leaving both nil would make keys the memtable holds read as
// absent.
func TestMemtableLayersAreNeverBothNil(t *testing.T) {
	t.Parallel()
	rnd := rand.New(rand.NewSource(5))

	for round := 0; round < 30; round++ {
		universe := 20
		contents := map[string][]uint64{}
		for _, k := range sampleDistinct(rnd, universe, rnd.Intn(universe)+1) {
			contents[k] = []uint64{uint64(rnd.Intn(20))}
		}
		m := newTestMemtableRoaringSet(contents)

		// deletions too: a key whose row is only a delete still has to come back
		// as present, not as a zero layer
		for _, k := range sampleDistinct(rnd, universe, 3) {
			require.NoError(t, m.roaringSetRemoveOne([]byte(k), uint64(rnd.Intn(20))))
			contents[k] = nil
		}
		held := make([]string, 0, len(contents))
		for k := range contents {
			held = append(held, k)
		}
		sort.Strings(held)

		batch := sampleDistinct(rnd, universe, universe)
		sort.Strings(batch)
		keys := sortedKeysOf(t, batch)

		into := make([]roaringset.BitmapLayer, keys.Len())
		fillWindowOK(t, m, keys, 0, keys.Len(), into)

		// A node with both bitmaps nil leaves its slot untouched, which is what
		// absence looks like, so it goes missing from this list rather than
		// arriving as a zero layer. Comparing against what the memtable was
		// given is what catches it.
		reported := make([]string, 0, len(into))
		for i, layer := range into {
			if layer.Additions != nil || layer.Deletions != nil {
				reported = append(reported, string(keys.At(i)))
			}
		}
		require.NotEmpty(t, reported, "round %d matched nothing, so it pins nothing", round)
		require.Equalf(t, held, reported,
			"round %d: the memtable holds keys it did not report, so the dense "+
				"window reads them as absent", round)
	}
}

// TestRoaringSetGetWindowStopsAtTheBudget pins what bounds a window's memory.
// The key count caps how many rows a fill clones and nothing caps how big a row
// is, so without this a property with few values and many documents each turns
// one window into a multiple of what the constant suggests.
func TestRoaringSetGetWindowStopsAtTheBudget(t *testing.T) {
	t.Parallel()

	// Rows wide enough that a couple of them exhaust any budget worth setting.
	// Documents are spread so each lands in its own container, which is what
	// makes a row cost bytes rather than compress to nothing.
	const rows, docsPerRow = 8, 4096
	batch := make([]string, rows)
	for i := range batch {
		batch[i] = fmt.Sprintf("k%02d", i)
	}
	m := memtableWith(t, nil)
	for i, k := range batch {
		docs := make([]uint64, docsPerRow)
		for j := range docs {
			docs[j] = uint64(i + j*rows*8)
		}
		require.NoError(t, m.roaringSetAddList([]byte(k), docs))
	}
	keys := sortedKeysOf(t, batch)

	// what one row costs, so the budget can be expressed in rows
	one := make([]roaringset.BitmapLayer, 1)
	first, err := m.roaringSetGetWindow(keys, 0, 1, one, math.MaxInt)
	require.NoError(t, err)
	require.Positive(t, first.Bytes)

	tests := []struct {
		name   string
		budget int
		wantTo int
	}{
		{name: "unbudgeted reads the whole range", budget: math.MaxInt, wantTo: rows},
		{name: "stops before the row that would cross the budget", budget: 3 * first.Bytes, wantTo: 3},
		{name: "a budget under one row still takes one", budget: 1, wantTo: 1},
		{name: "a zero budget still takes one", budget: 0, wantTo: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dst := make([]roaringset.BitmapLayer, keys.Len())
			fill, err := m.roaringSetGetWindow(keys, 0, keys.Len(), dst, tc.budget)
			require.NoError(t, err)
			require.Equal(t, tc.wantTo, fill.To)
			require.LessOrEqual(t, fill.Bytes, max(tc.budget, first.Bytes),
				"only the always-taken first key may exceed the budget")

			// Past To the walk wrote nothing, and the caller must not read those
			// slots as absence — which is why fillWindow narrows winEnd to To.
			for i := fill.To; i < keys.Len(); i++ {
				require.Nilf(t, dst[i].Additions, "slot %d past To must be untouched", i)
			}
			for i := 0; i < fill.To; i++ {
				require.NotNilf(t, dst[i].Additions, "slot %d inside To must hold its row", i)
			}
		})
	}
}

// fillWindowOK reads a window and fails the test if it errors, so the callers
// that only care about what landed in dst stay one line. The bytes it reports
// are asserted where that is the subject.
func fillWindowOK(t *testing.T, m memtable, keys inverted.SortedKeys, from, to int, dst []roaringset.BitmapLayer) {
	t.Helper()
	// A budget nothing here can reach, so these callers read the whole range
	// they asked for. The budget's own behaviour is covered separately.
	fill, err := m.roaringSetGetWindow(keys, from, to, dst, math.MaxInt)
	require.NoError(t, err)
	require.Equal(t, to, fill.To, "an unbudgeted read must fill the whole range")
}

// TestRoaringSetGetWindowHoldsAtMostTheBudget pins the ceiling the budget
// enforces, which a fixture of equally sized rows cannot reach: spending there
// lands on the budget and never past it, so the same numbers come out whether a
// row is priced before it is copied or after. One fat row among thin ones is what
// tells the two apart, and it is the shape the budget exists for — a property
// whose values carry wildly different document counts.
func TestRoaringSetGetWindowHoldsAtMostTheBudget(t *testing.T) {
	t.Parallel()

	const keyCount, thinDocs, fatDocs = 8, 8, 4096

	// Sixty-four apart, and each key's documents are its own, so no two rows share a
	// container. What costs bytes here is the document count: the fat row's four
	// thousand fill a handful of containers densely, where the thin rows' eight share
	// one.
	docsFor := func(key, n int) []uint64 {
		docs := make([]uint64, n)
		for j := range docs {
			docs[j] = uint64(key*1_000_000 + j*64)
		}
		return docs
	}

	build := func(t *testing.T, fatAt int) (*Memtable, inverted.SortedKeys) {
		batch := make([]string, keyCount)
		for i := range batch {
			batch[i] = fmt.Sprintf("k%02d", i)
		}
		m := memtableWith(t, nil)
		for i, k := range batch {
			n := thinDocs
			if i == fatAt {
				n = fatDocs
			}
			require.NoError(t, m.roaringSetAddList([]byte(k), docsFor(i, n)))
		}
		return m, sortedKeysOf(t, batch)
	}

	costOf := func(t *testing.T, m *Memtable, keys inverted.SortedKeys, from, to int) int {
		dst := make([]roaringset.BitmapLayer, to-from)
		fill, err := m.roaringSetGetWindow(keys, from, to, dst, math.MaxInt)
		require.NoError(t, err)
		return fill.Bytes
	}

	t.Run("a fat row past the first ends the window before it", func(t *testing.T) {
		m, keys := build(t, keyCount-1)

		// Just past what the thin rows cost, so they all fit and the fat one cannot.
		// Not exactly their total: a budget landing on it stops a walk that prices
		// rows after copying them too, since its running total reaches the budget at
		// the same boundary, and the case would pass either way.
		thin := costOf(t, m, keys, 0, keyCount-1)
		budget := thin + 1
		require.Less(t, budget, thin+costOf(t, m, keys, keyCount-1, keyCount),
			"the fat row must not fit in a budget the thin ones leave room under")

		dst := make([]roaringset.BitmapLayer, keys.Len())
		fill, err := m.roaringSetGetWindow(keys, 0, keys.Len(), dst, budget)
		require.NoError(t, err)

		require.Equal(t, keyCount-1, fill.To, "the window must end before the fat row")
		require.LessOrEqual(t, fill.Bytes, budget, "a window must not hold more than the budget")
		require.Nil(t, dst[keyCount-1].Additions, "the fat row must not have been copied")
	})

	t.Run("a fat row at the first is taken whatever it costs", func(t *testing.T) {
		m, keys := build(t, 0)

		dst := make([]roaringset.BitmapLayer, keys.Len())
		fill, err := m.roaringSetGetWindow(keys, 0, keys.Len(), dst, 1)
		require.NoError(t, err)

		require.Equal(t, 1, fill.To, "the window holds only the row that overran it")
		require.Greater(t, fill.Bytes, 1, "the first row is taken past the budget")
		require.NotNil(t, dst[0].Additions, "the first row must be readable at any size")
	})
}
