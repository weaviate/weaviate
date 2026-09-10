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

// TestRoaringSetGetWindowMatchesPerKey is the differential the batch read has
// to survive: reading a batch in one pass must answer exactly what reading
// each key on its own does. The two walk the tree completely differently —
// one descends per key, the other advances two cursors past each other — so
// the shapes below target where a cursor can overshoot: batches far sparser
// or denser than the memtable, disjoint from it, or sharing only their ends.
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

// TestRoaringSetGetWindowMatchesWholeBatch pins that reading a batch in
// windows answers exactly what reading it whole does. Each window starts with
// its own descent rather than where the last one stopped, so a key on a
// window boundary is the one a mistake would drop or double-count; window
// size 1 is the degenerate case where every key is a boundary.
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
			// One slot per key, handed over a window at a time, as the batch
			// reader does it.
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

// TestRoaringSetGetWindowRangeGuards covers the rejected shapes: a range
// outside the batch, and a slice that is not exactly as wide as its range. A
// longer slice is rejected rather than tolerated, because the read clears
// everything it is given, so only an exact length makes what it erases the
// slots it was asked for. None of the erroring shapes is reachable from
// fillWindow, which slices exactly the range it asks for — they are the
// arithmetic a second caller could get wrong.
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
		{name: "slice longer than the window", from: 0, to: 2, slots: 3, wantErr: true},
		{name: "whole batch passed for a late window", from: 1, to: 3, slots: 3, wantErr: true},
		{name: "slots for an empty window", from: 1, to: 1, slots: 3, wantErr: true},
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

// TestRoaringSetGetWindowDropsEmptySides pins that a row's unused side comes
// back nil, not an allocated-but-empty bitmap: that nil is what the reader's
// presence test relies on, even though the tree itself allocates both sides.
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

// TestRoaringSetGetWindowClearsWhatItIsGiven pins that a reused, dirty buffer
// answers the same as a fresh one: the read only writes slots for keys it
// holds, so a stale slot would otherwise read as a row present.
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

// TestRoaringSetGetWindowCopiesWhatItYields pins that mutating a returned row
// never reaches the memtable — the walk hands out the tree's own nodes, so
// only the copy protects it. This is the single-threaded half of that
// contract; the race detector covers the concurrent half.
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

	// Every third key also carries a deletion, every fifth carries only a
	// deletion (a layer with a nil Additions, which a careless fold drops).
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

// layerDocsOf renders layers as their doc IDs, since each read clones and the
// bitmaps are never the same objects to compare directly.
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

// TestMemtableLayersAreNeverBothNil pins the invariant a window slot's zero
// value relies on to mean "absent": every node a memtable builds must
// allocate at least one of its two bitmaps. A node-creation path that left
// both nil would make a held key read as absent.
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

		// A node with both bitmaps nil would leave its slot untouched — missing
		// from this list rather than arriving as a zero layer.
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

// TestRoaringSetGetWindowStopsAtTheBudget pins the byte budget as a second
// bound on window memory alongside the key count: nothing else caps how big a
// single row is.
func TestRoaringSetGetWindowStopsAtTheBudget(t *testing.T) {
	t.Parallel()

	// Documents spread one per container, so a row costs real bytes.
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

// fillWindowOK reads a window and fails the test if it errors, so callers that
// only care about what landed in dst stay one line.
func fillWindowOK(t *testing.T, m memtable, keys inverted.SortedKeys, from, to int, dst []roaringset.BitmapLayer) {
	t.Helper()
	// Unbudgeted, so these callers always get the whole range they asked for.
	fill, err := m.roaringSetGetWindow(keys, from, to, dst, math.MaxInt)
	require.NoError(t, err)
	require.Equal(t, to, fill.To, "an unbudgeted read must fill the whole range")
}

// TestRoaringSetGetWindowHoldsAtMostTheBudget pins that pricing a row before
// copying it (not after) matters: only a fat row among thin ones can tell the
// two apart, since equally sized rows always land on the budget the same way
// either way.
func TestRoaringSetGetWindowHoldsAtMostTheBudget(t *testing.T) {
	t.Parallel()

	const keyCount, thinDocs, fatDocs = 8, 8, 4096

	// Each key's documents are 64 apart and its own, so no two rows share a
	// container.
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

		// One byte past the thin rows' total: exactly their total would also
		// pass under price-after-copy pricing, so it wouldn't distinguish the two.
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
