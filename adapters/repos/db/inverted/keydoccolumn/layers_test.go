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

package keydoccolumn

import (
	"fmt"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// segFromPairs builds a segment from sorted (key, docID) pairs.
func segFromPairs(keys [][]byte, docs []uint64) *segment {
	offsets := []uint32{0}
	var blob []byte
	dc := &docIDColumn{w: 8}
	uniformWidth := -1
	for i, k := range keys {
		blob = append(blob, k...)
		offsets = append(offsets, uint32(len(blob)))
		dc.append(docs[i])
		if uniformWidth == -1 {
			uniformWidth = len(k)
		} else if uniformWidth != len(k) {
			uniformWidth = -2
		}
	}
	return &segment{keys: buildKeyColumn(blob, offsets, uniformWidth), docs: dc}
}

// mockCursor is a roaringset.InnerCursor over fixed sorted entries, for testing
// MergeMemtableByCursor without a real memtable.
type mockCursor struct {
	keys   [][]byte
	layers []roaringset.BitmapLayer
	pos    int
}

func newMockCursor() *mockCursor { return &mockCursor{pos: -1} }

func (c *mockCursor) add(key []byte, adds, dels []uint64) *mockCursor {
	var a, d *sroar.Bitmap
	if len(adds) > 0 {
		a = sroar.FromSortedList(adds)
	}
	if len(dels) > 0 {
		d = sroar.FromSortedList(dels)
	}
	c.keys = append(c.keys, key)
	c.layers = append(c.layers, roaringset.BitmapLayer{Additions: a, Deletions: d})
	return c
}

func (c *mockCursor) cur() ([]byte, roaringset.BitmapLayer, error) {
	if c.pos < 0 || c.pos >= len(c.keys) {
		return nil, roaringset.BitmapLayer{}, nil
	}
	return c.keys[c.pos], c.layers[c.pos], nil
}

func (c *mockCursor) First() ([]byte, roaringset.BitmapLayer, error) { c.pos = 0; return c.cur() }
func (c *mockCursor) Next() ([]byte, roaringset.BitmapLayer, error)  { c.pos++; return c.cur() }

func (c *mockCursor) Seek([]byte) ([]byte, roaringset.BitmapLayer, error) {
	return nil, roaringset.BitmapLayer{}, nil
}

// newTestIndex builds an index over base with a logger attached, which the
// background flattening needs.
func newTestIndex(base *segment) *Index {
	logger, _ := test.NewNullLogger()
	idx := &Index{logger: logger}
	idx.state.Store(&indexState{base: base})
	return idx
}

// waitForFlatten blocks until no background flattening is in flight.
func waitForFlatten(t *testing.T, idx *Index) {
	t.Helper()
	require.Eventually(t, func() bool {
		return !idx.flattening.Load()
	}, 5*time.Second, time.Millisecond, "background flattening must finish")
}

func resolveSorted(idx *Index, keys ...string) []uint64 {
	sorted := slices.Clone(keys)
	slices.Sort(sorted)
	arr := sroar.FromSortedList(idx.Resolve(testKeys(sorted...)).SortedDocs()).ToArray()
	sort.Slice(arr, func(i, j int) bool { return arr[i] < arr[j] })
	return arr
}

func TestLayersFlatten(t *testing.T) {
	// base: a→1, b→2, c→3
	base := segFromPairs([][]byte{[]byte("a"), []byte("b"), []byte("c")}, []uint64{1, 2, 3})
	idx := newTestIndex(base)

	require.Equal(t, []uint64{1, 2, 3}, resolveSorted(idx, "a", "b", "c"), "base only")

	// flush 1: add d→4 (new key), delete docID 2 (b's doc removed)
	require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().
		add([]byte("d"), []uint64{4}, nil)))
	// deletion of b's doc lives under key b in the same flush
	require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().
		add([]byte("b"), nil, []uint64{2})))

	require.Equal(t, []uint64{1, 3, 4}, resolveSorted(idx, "a", "b", "c", "d"),
		"b's doc deleted, d added")
	require.Equal(t, []uint64{}, resolveSorted(idx, "b"), "b resolves to nothing after delete")

	// flush 3: b re-added with a new doc (5) — newest-wins over the earlier delete
	require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().
		add([]byte("b"), []uint64{5}, nil)))

	require.Equal(t, []uint64{5}, resolveSorted(idx, "b"), "b re-added wins over delete")
	require.Equal(t, []uint64{1, 3, 4, 5}, resolveSorted(idx, "a", "b", "c", "d"))
}

func TestLayersUpdateInSingleFlush(t *testing.T) {
	// base: x→10
	idx := newTestIndex(segFromPairs([][]byte{[]byte("x")}, []uint64{10}))

	// one flush both deletes x's old doc (10) and adds the new one (20)
	require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().
		add([]byte("x"), []uint64{20}, []uint64{10})))

	require.Equal(t, []uint64{20}, resolveSorted(idx, "x"), "update: new doc replaces old")
}

func TestFlattenIntoBase(t *testing.T) {
	old := flattenLayersThreshold
	flattenLayersThreshold = 3
	defer func() { flattenLayersThreshold = old }()

	// base: a→1, b→2, c→3
	idx := newTestIndex(segFromPairs(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")}, []uint64{1, 2, 3}))

	require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().add([]byte("d"), []uint64{4}, nil))) // add d
	require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().add([]byte("b"), nil, []uint64{2}))) // delete b
	require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().add([]byte("e"), []uint64{5}, nil))) // add e → flattens

	waitForFlatten(t, idx)

	state := idx.state.Load()
	require.Empty(t, state.layers, "layers must be flattened into the base at the threshold")
	require.Equal(t, 4, state.base.keys.len(), "base holds the net state (a,c,d,e; b deleted)")

	require.Equal(t, []uint64{1, 3, 4, 5}, resolveSorted(idx, "a", "b", "c", "d", "e"),
		"net state after flattening: b deleted, d/e added")
	require.Equal(t, []uint64{}, resolveSorted(idx, "b"))

	// keeps working after the flattening: re-add b over the flattened base
	require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().add([]byte("b"), []uint64{6}, nil)))
	require.Equal(t, []uint64{6}, resolveSorted(idx, "b"))
	require.Equal(t, []uint64{1, 3, 4, 5, 6}, resolveSorted(idx, "a", "b", "c", "d", "e"))
}

// TestFlattenDoesNotDropConcurrentFlushes pins the single-flight guard: flattenings
// run in the background while flushes keep arriving, and each flattening drops exactly
// the layers it consumed. Without the guard two overlapping flattenings each drop the
// same count, discarding whatever arrived in between.
func TestFlattenDoesNotDropConcurrentFlushes(t *testing.T) {
	old := flattenLayersThreshold
	flattenLayersThreshold = 2
	defer func() { flattenLayersThreshold = old }()

	const numFlushes = 200
	idx := newTestIndex(segFromPairs([][]byte{[]byte("base")}, []uint64{1}))

	keys := make([]string, 0, numFlushes+1)
	want := make([]uint64, 0, numFlushes+1)
	keys = append(keys, "base")
	want = append(want, 1)
	for i := 0; i < numFlushes; i++ {
		key := fmt.Sprintf("key_%04d", i)
		require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().
			add([]byte(key), []uint64{uint64(100 + i)}, nil)))
		keys = append(keys, key)
		want = append(want, uint64(100+i))
	}

	waitForFlatten(t, idx)
	require.Equal(t, want, resolveSorted(idx, keys...),
		"every flushed key must survive the flattenings that ran alongside them")
}

// TestConcurrentReadsDuringFlatten resolves from many goroutines while flushes
// are merged in and flattenings publish new bases underneath them. Readers work
// from a state they loaded once, so a state swapped mid-resolve must not change
// what they see: a key present in the base stays resolvable throughout, and a
// key merged in before a resolve started never disappears. Run with -race, this
// is what pins the lock-free publication.
func TestConcurrentReadsDuringFlatten(t *testing.T) {
	old := flattenLayersThreshold
	flattenLayersThreshold = 2
	defer func() { flattenLayersThreshold = old }()

	const (
		numFlushes = 300
		numReaders = 8
	)
	idx := newTestIndex(segFromPairs([][]byte{[]byte("base")}, []uint64{1}))

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				require.Equal(t, []uint64{1}, resolveSorted(idx, "base"),
					"the base key must resolve across every published state")
			}
		}, idx.logger)
	}

	keys := []string{"base"}
	want := []uint64{1}
	for i := 0; i < numFlushes; i++ {
		key := fmt.Sprintf("key_%04d", i)
		require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().
			add([]byte(key), []uint64{uint64(100 + i)}, nil)))
		keys = append(keys, key)
		want = append(want, uint64(100+i))
	}
	close(stop)
	wg.Wait()

	waitForFlatten(t, idx)
	require.Equal(t, want, resolveSorted(idx, keys...),
		"every flushed key must survive flattenings that ran under concurrent readers")
}

// TestMergeMemtableByCursorKeepsDeletionsKeyed pins that a flush records which key each
// deleted docID belonged to, not just the set of docIDs. Resolution still
// applies them result-wide, but a flattening — and per-key deletion after it —
// needs the association.
func TestMergeMemtableByCursorKeepsDeletionsKeyed(t *testing.T) {
	idx := newTestIndex(segFromPairs([][]byte{[]byte("a")}, []uint64{1}))

	require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().
		add([]byte("a"), nil, []uint64{3, 7}). // one key retiring two docIDs
		add([]byte("b"), nil, []uint64{5}).
		add([]byte("c"), []uint64{9}, nil)))

	layers := idx.state.Load().layers
	require.Len(t, layers, 1)
	r := layers[0]

	gotDels := map[string][]uint64{}
	for i := 0; i < r.dels.keys.len(); i++ {
		k := string(r.dels.keys.appendKey(i, nil))
		gotDels[k] = segmentDocs(r.dels, i)
	}
	require.Equal(t, map[string][]uint64{"a": {3, 7}, "b": {5}}, gotDels,
		"every deletion keeps the key it was issued under")

	require.Equal(t, 1, r.adds.keys.len(), "only c was added")
	require.Equal(t, "c", string(r.adds.keys.appendKey(0, nil)))
	require.Equal(t, uint64(9), r.adds.docs.at(0))
}

// TestFlushedKeyWithSeveralDocumentsKeepsBoth pins that a key arriving with more
// than one document keeps all of them rather than being refused or truncated:
// the property was configured on the understanding that its values are unique,
// and a violation must cost the operator a warning, not a document.
func TestFlushedKeyWithSeveralDocumentsKeepsBoth(t *testing.T) {
	idx := newTestIndex(segFromPairs([][]byte{[]byte("a")}, []uint64{1}))
	require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().add([]byte("b"), []uint64{5, 9}, nil)))

	require.Equal(t, []uint64{5, 9}, resolveSorted(idx, "b"),
		"both documents under the key must resolve")
	require.Equal(t, []uint64{1, 5, 9}, resolveSorted(idx, "a", "b"))

	// and they survive a flattening
	idx.flattenIntoBase()
	require.Equal(t, []uint64{1, 5, 9}, resolveSorted(idx, "a", "b"),
		"flattening must not lose the extra document")

	// deleting one leaves the other
	require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().add([]byte("b"), nil, []uint64{5})))
	require.Equal(t, []uint64{9}, resolveSorted(idx, "b"),
		"a deletion retires one document, not the key")
}

// TestOverflowBoundCountsKeysNotDocuments pins what the bound measures. Overflow
// is a map, so its cost follows the number of keys holding more than one
// document, not the documents inside them: one value shared by many documents is
// cheap and stays, many values each duplicated is what gets refused.
func TestOverflowBoundCountsKeysNotDocuments(t *testing.T) {
	oldFloor, oldRate := overflowFloor, overflowRowsPerKeyLimit
	overflowFloor, overflowRowsPerKeyLimit = 1, 10
	defer func() { overflowFloor, overflowRowsPerKeyLimit = oldFloor, oldRate }()

	t.Run("one key with many documents is allowed", func(t *testing.T) {
		idx := newTestIndex(segFromPairs([][]byte{[]byte("a")}, []uint64{1}))
		require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().
			add([]byte("b"), []uint64{5, 6, 7, 8, 9, 10, 11, 12}, nil)))
		require.Equal(t, []uint64{5, 6, 7, 8, 9, 10, 11, 12}, resolveSorted(idx, "b"))
	})

	t.Run("many keys each with a duplicate is refused", func(t *testing.T) {
		idx := newTestIndex(segFromPairs([][]byte{[]byte("a")}, []uint64{1}))
		cursor := newMockCursor()
		for _, k := range []string{"b", "c", "d"} { // past the floor of 1
			cursor.add([]byte(k), []uint64{5, 6}, nil)
		}
		err := idx.MergeMemtableByCursor(cursor)
		require.Error(t, err, "past the bound the structure is the wrong one for this data")
		require.Contains(t, err.Error(), "unique values")
	})
}

// TestResolutionCarriesSeveralDocumentsPerKey pins the working set's two ways of
// holding a document — the slot and the extras list — against each other:
// deleting either must leave the other, and the bitmap must carry both.
func TestResolutionCarriesSeveralDocumentsPerKey(t *testing.T) {
	base := segFromPairs([][]byte{[]byte("a"), []byte("b")}, []uint64{1, 2})
	idx := newTestIndex(base)
	// b gains two more documents, so its key holds three
	require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().add([]byte("b"), []uint64{7, 9}, nil)))

	require.Equal(t, []uint64{2, 7, 9}, resolveSorted(idx, "b"), "all three resolve")
	require.Equal(t, []uint64{1, 2, 7, 9}, resolveSorted(idx, "a", "b"))

	// retire the one in the slot: the extras survive
	require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().add([]byte("b"), nil, []uint64{2})))
	require.Equal(t, []uint64{7, 9}, resolveSorted(idx, "b"))

	// retire one of the extras: the other survives
	require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().add([]byte("b"), nil, []uint64{7})))
	require.Equal(t, []uint64{9}, resolveSorted(idx, "b"))

	// document 0 is ordinary; nothing about it reads as an empty slot
	idx0 := newTestIndex(segFromPairs([][]byte{[]byte("z")}, []uint64{0}))
	require.Equal(t, []uint64{0}, resolveSorted(idx0, "z"))
}

func TestLayersDeleteOnlyFlush(t *testing.T) {
	idx := newTestIndex(segFromPairs([][]byte{[]byte("k")}, []uint64{7}))
	require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().add([]byte("k"), nil, []uint64{7})))
	require.Equal(t, []uint64{}, resolveSorted(idx, "k"), "delete-only flush removes the doc")
}
