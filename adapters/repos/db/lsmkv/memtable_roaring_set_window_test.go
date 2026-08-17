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
// Windowing a large batch is what holds the lock briefly and keeps one window's
// bitmaps in hand at a time, and it only works if the boundaries are invisible:
// every window starts with its own descent rather than where the last one stopped,
// so a key sitting on a boundary is the one a mistake drops or double-counts.
// Window size 1 is the degenerate end of that — every key is its own boundary.
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
			// One slot per key, handed over a window at a time, which is how a
			// caller reading a batch this way has to do it. Writing outside its
			// window would index past the subslice rather than land in a
			// neighbour's slot.
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

// TestRoaringSetGetWindowRangeGuards separates an empty window from a range or
// a slice the caller got wrong. Only the empty one can be answered by reading
// nothing; the rest cannot be answered with "this memtable holds none of those
// keys", because nobody asked about those keys.
//
// A caller that derives the range from its own batch and sizes the slice to it
// reaches none of the erroring shapes. They are the arithmetic to get wrong by
// hand — most of all the too-long slice, where the rows would land at the offsets
// of keys outside the window.
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
		// caught by the width check: to-from is negative and no slice matches it
		{name: "inverted", from: 2, to: 1, slots: 0, wantErr: true},
		{name: "before the first key", from: -1, to: 2, slots: 3, wantErr: true},
		{name: "one past the last", from: 0, to: 4, slots: 4, wantErr: true},
		{name: "far past the last", from: 1, to: 99, slots: 99, wantErr: true},
		{name: "slice shorter than the window", from: 0, to: 3, slots: 2, wantErr: true},
		{name: "slice longer than the window", from: 0, to: 2, slots: 3, wantErr: true},
		{name: "whole batch passed for a late window", from: 1, to: 3, slots: 3, wantErr: true},
		{name: "slots for an empty window", from: 1, to: 1, slots: 3, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			into := make([]roaringset.BitmapLayer, tc.slots)
			_, err := m.roaringSetGetWindow(keys, tc.from, tc.to, into)
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
// one, since a caller distinguishes "no row here" by that nil. Plain Clone would
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
	_, err := m.roaringSetGetWindow(keys, 0, 1, into)
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

// fillWindowOK reads a window and fails the test if it errors, so the callers that
// only care about what landed in dst stay one line. What it reports is discarded:
// no caller here reads it.
func fillWindowOK(t *testing.T, m memtable, keys inverted.SortedKeys, from, to int, dst []roaringset.BitmapLayer) {
	t.Helper()
	_, err := m.roaringSetGetWindow(keys, from, to, dst)
	require.NoError(t, err)
}
