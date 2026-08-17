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
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// TestResolutionWideDocuments pins documents either side of the width bound, MaxUint32 included, reading back as themselves.
func TestResolutionWideDocuments(t *testing.T) {
	const (
		wide = uint64(math.MaxUint32) + 5
		// the docID count from which a query takes 64-bit slots
		bound = uint64(1<<32 - narrowHeadroom)
	)

	tests := []struct {
		name string
		// docs go in at successive query positions; the resolution is sized for them unless slots is set
		docs []uint64
		// slots and docIDCount size the resolution explicitly.
		slots      int
		docIDCount uint64
		// then is what happens after those inserts.
		then func(t *testing.T, r *Resolution)
		// narrow is whether the result can still be read 32 bits wide
		narrow bool
		// want defaults to docs, sorted
		want []uint64
	}{
		{name: "a count just below the bound stays narrow", docs: []uint64{0, 1, bound - 2}, narrow: true},
		{name: "a count at the bound takes 64 bits", docs: []uint64{0, bound - 1}},
		{name: "at the sentinel", docs: []uint64{math.MaxUint32}},
		{name: "one wide among narrow", docs: []uint64{7, math.MaxUint32 + 1, 9}},
		{name: "far past the sentinel", docs: []uint64{math.MaxUint64 - 1}},
		{
			name: "a memtable document within the headroom stays narrow",
			docs: []uint64{3}, slots: 2, docIDCount: 1_000,
			then: func(t *testing.T, r *Resolution) {
				// key 1 of a window at 0; the zero layer is key 0, which the memtable did not touch
				require.NoError(t, r.ApplyMemtableWindow(0, [][]roaringset.BitmapLayer{{
					{},
					{Additions: bitmapOf(5, math.MaxUint32-1), Deletions: bitmapOf()},
				}}))
			},
			narrow: true,
			want:   []uint64{3, 5, math.MaxUint32 - 1},
		},
		{
			name: "the same wide document reached twice is held once",
			docs: []uint64{wide},
			then: func(t *testing.T, r *Resolution) {
				r.insert(0, wide)
			},
			want: []uint64{wide},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slots, docIDCount := tt.slots, tt.docIDCount
			if slots == 0 {
				slots, docIDCount = len(tt.docs), docIDCountFor(tt.docs)
			}
			res := newResolution(slots, docIDCount)
			for qi, doc := range tt.docs {
				res.insert(qi, doc)
			}
			if tt.then != nil {
				tt.then(t, res)
			}

			narrow, ok := res.sortedDocs32()
			require.Equal(t, tt.narrow, ok, "which width the result can be read at")

			var got []uint64
			if ok {
				for _, d := range narrow {
					got = append(got, uint64(d))
				}
			} else {
				got = res.sortedDocs()
			}

			want := tt.want
			if want == nil {
				want = append([]uint64(nil), tt.docs...)
				slices.Sort(want)
			}
			assert.Equal(t, want, got, "every document must come back as itself")
		})
	}
}

// heldDocs reads r at 64 bits whatever its slot width, consuming it.
func heldDocs(r *Resolution) []uint64 {
	if narrow, ok := r.sortedDocs32(); ok {
		out := make([]uint64, len(narrow))
		for i, d := range narrow {
			out[i] = uint64(d)
		}
		return out
	}
	return r.sortedDocs()
}

// TestResolutionRefusesMemtableDocumentPastItsWidth pins a 32-bit resolution failing on, not truncating, a document it cannot hold.
func TestResolutionRefusesMemtableDocumentPastItsWidth(t *testing.T) {
	for _, doc := range []uint64{math.MaxUint32, 1 << 32, 1<<32 + 7} {
		t.Run(fmt.Sprint(doc), func(t *testing.T) {
			res := newResolution(1, 1_000)
			require.NotNil(t, res.docs32)
			err := res.ApplyMemtableWindow(0, [][]roaringset.BitmapLayer{{
				{Additions: bitmapOf(3, doc), Deletions: bitmapOf()},
			}})
			require.Error(t, err)
			assert.Contains(t, err.Error(), fmt.Sprintf("docID %d", doc))
			assert.Contains(t, err.Error(), fmt.Sprint(uint64(math.MaxUint32)))
		})
	}

	t.Run("a deletion that cannot be held is no error", func(t *testing.T) {
		res := newResolution(1, 1_000)
		res.insert(0, 3)
		require.NoError(t, res.ApplyMemtableWindow(0, [][]roaringset.BitmapLayer{{
			{Additions: bitmapOf(), Deletions: bitmapOf(1<<32 + 3)},
		}}))
		assert.Equal(t, []uint64{3}, heldDocs(res), "a truncated deletion must not retire document 3")
	})
}

// TestResolutionMaterializesWideDocuments pins the 64-bit form materializing documents above 2^32 as themselves.
func TestResolutionMaterializesWideDocuments(t *testing.T) {
	for _, tc := range []struct {
		name string
		pool roaringset.BitmapBufPool
	}{
		{name: "noop", pool: roaringset.NewBitmapBufPoolNoop()},
		{name: "ranged", pool: roaringset.NewBitmapBufPoolRanged(nil, 1<<20, nil, 1<<10, 1<<20)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := newResolution(3, 1<<33)
			require.NotNil(t, res.docs, "a count past the bound takes 64-bit slots")
			res.insert(0, 5)
			res.insert(1, 1<<32+1)
			res.insert(1, 1<<33-1) // an extra
			res.insert(2, 1<<32)

			bm, put := res.Materialize(tc.pool)
			defer put()
			assert.Equal(t, []uint64{5, 1 << 32, 1<<32 + 1, 1<<33 - 1}, bm.ToArray())
		})
	}
}

// bitmapOf builds a bitmap holding docs, as a memtable layer would.
func bitmapOf(docs ...uint64) *sroar.Bitmap {
	bm := sroar.NewBitmap()
	for _, d := range docs {
		bm.Set(d)
	}
	return bm
}

// docIDCountFor is the count a shard counter would report for docs, which sets the slot width.
func docIDCountFor(docs []uint64) uint64 {
	var max uint64
	for _, d := range docs {
		if d > max {
			max = d
		}
	}
	return max + 1
}

// TestResolutionDeletesUnderOneKey pins many deletions under one key leaving a document another key shares.
func TestResolutionDeletesUnderOneKey(t *testing.T) {
	// key 1 shares document 1_500 with key 0, so a deletion under 0 must leave it
	res := newResolution(2, 10_000)
	res.insert(1, 1)
	res.insert(1, 1_500)
	var want []uint64
	for d := uint64(1); d <= 2_000; d++ {
		res.insert(0, d)
		if d%2 == 1 {
			want = append(want, d)
		}
	}
	for d := uint64(2); d <= 2_000; d += 2 {
		res.delete(0, d)
	}
	want = append(want, 1, 1_500)
	slices.Sort(want)
	assert.Equal(t, want, heldDocs(res))
}
