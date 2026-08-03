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

package columnar

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// segFromPairs builds a columnarSegment from sorted (key, docID) pairs.
func segFromPairs(keys [][]byte, docs []uint64) *columnarSegment {
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
	return &columnarSegment{keys: buildKeyColumn(blob, offsets, uniformWidth), docs: dc}
}

// mockCursor is a roaringset.InnerCursor over fixed sorted entries, for testing
// AbsorbFlush without a real memtable.
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

func resolveSorted(idx *ColumnarIndex, keys ...string) []uint64 {
	q := make([][]byte, len(keys))
	for i, k := range keys {
		q[i] = []byte(k)
	}
	sort.Slice(q, func(i, j int) bool { return string(q[i]) < string(q[j]) })
	arr := idx.ResolveContainsAny(q).ToArray()
	sort.Slice(arr, func(i, j int) bool { return arr[i] < arr[j] })
	return arr
}

func TestRunsFold(t *testing.T) {
	// base: a→1, b→2, c→3
	base := segFromPairs([][]byte{[]byte("a"), []byte("b"), []byte("c")}, []uint64{1, 2, 3})
	idx := &ColumnarIndex{base: base}

	require.Equal(t, []uint64{1, 2, 3}, resolveSorted(idx, "a", "b", "c"), "base only")

	// flush 1: add d→4 (new key), delete docID 2 (b's doc removed)
	require.NoError(t, idx.AbsorbFlush(newMockCursor().
		add([]byte("d"), []uint64{4}, nil)))
	// deletion of b's doc lives under key b in the same flush
	require.NoError(t, idx.AbsorbFlush(newMockCursor().
		add([]byte("b"), nil, []uint64{2})))

	require.Equal(t, []uint64{1, 3, 4}, resolveSorted(idx, "a", "b", "c", "d"),
		"b's doc deleted, d added")
	require.Equal(t, []uint64{}, resolveSorted(idx, "b"), "b resolves to nothing after delete")

	// flush 3: b re-added with a new doc (5) — newest-wins over the earlier delete
	require.NoError(t, idx.AbsorbFlush(newMockCursor().
		add([]byte("b"), []uint64{5}, nil)))

	require.Equal(t, []uint64{5}, resolveSorted(idx, "b"), "b re-added wins over delete")
	require.Equal(t, []uint64{1, 3, 4, 5}, resolveSorted(idx, "a", "b", "c", "d"))
}

func TestRunsUpdateInSingleFlush(t *testing.T) {
	// base: x→10
	idx := &ColumnarIndex{base: segFromPairs([][]byte{[]byte("x")}, []uint64{10})}

	// one flush both deletes x's old doc (10) and adds the new one (20)
	require.NoError(t, idx.AbsorbFlush(newMockCursor().
		add([]byte("x"), []uint64{20}, []uint64{10})))

	require.Equal(t, []uint64{20}, resolveSorted(idx, "x"), "update: new doc replaces old")
}

func TestRunsDeleteOnlyFlush(t *testing.T) {
	idx := &ColumnarIndex{base: segFromPairs([][]byte{[]byte("k")}, []uint64{7})}
	require.NoError(t, idx.AbsorbFlush(newMockCursor().add([]byte("k"), nil, []uint64{7})))
	require.Equal(t, []uint64{}, resolveSorted(idx, "k"), "delete-only flush removes the doc")
}
