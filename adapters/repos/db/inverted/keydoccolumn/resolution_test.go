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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// TestResolutionWideDocuments pins what happens to documents a 32-bit slot
// cannot hold, when the resolution was sized for them up front — which is what a
// shard whose counter has passed 2^32 gets. Truncating one would answer a
// different document, plausibly and silently, and the boundary matters twice
// over: MaxUint32 is the narrow slots' own sentinel, so a document at that value
// must not read back as an empty slot.
func TestResolutionWideDocuments(t *testing.T) {
	tests := []struct {
		name string
		docs []uint64
		// narrow is whether the result can still be read 32 bits wide
		narrow bool
	}{
		{"all narrow", []uint64{0, 1, math.MaxUint32 - 1}, true},
		{"at the sentinel", []uint64{math.MaxUint32}, false},
		{"past the sentinel", []uint64{math.MaxUint32 + 1}, false},
		{"one wide among narrow", []uint64{7, math.MaxUint32 + 1, 9}, false},
		{"far past the sentinel", []uint64{math.MaxUint64 - 1}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := newResolution(len(tt.docs), maxIDFor(tt.docs))
			for qi, doc := range tt.docs {
				res.insert(qi, doc)
			}

			narrow, ok := res.SortedDocs32()
			require.Equal(t, tt.narrow, ok, "which width the result can be read at")

			var got []uint64
			if ok {
				for _, d := range narrow {
					got = append(got, uint64(d))
				}
			} else {
				got = res.SortedDocs()
			}

			want := append([]uint64(nil), tt.docs...)
			slicesSortUint64(want)
			assert.Equal(t, want, got, "every document must come back as itself")
		})
	}
}

// TestResolutionWidensForMemtableDocument pins the one case up-front sizing
// cannot cover. The slots are sized before the memtables are read, so a write
// that lands in between can be wider than they are — and unlike the index's own
// segments, an unflushed layer cannot be asked its maximum before the resolution
// exists. Applying it has to widen, or the document is lost or truncated.
func TestResolutionWidensForMemtableDocument(t *testing.T) {
	const wide = uint64(math.MaxUint32) + 5

	res := newResolution(2, 1_000) // sized narrow: the counter had not seen the write
	res.insert(0, 3)

	res.ApplyMemtableMatches([]roaringset.LayerMatches{{
		At:     []uint32{1},
		Layers: []roaringset.BitmapLayer{{Additions: bitmapOf(wide), Deletions: bitmapOf()}},
	}})

	_, ok := res.SortedDocs32()
	require.False(t, ok, "the memtable's document forces the wider result")
	assert.Equal(t, []uint64{3, wide}, res.SortedDocs(),
		"what the slots held before the widening must survive it")
}

// TestResolutionWideDuplicate pins that the same wide document reached through
// one key twice is held once, as a narrow one is.
func TestResolutionWideDuplicate(t *testing.T) {
	const wide = uint64(math.MaxUint32) + 5

	res := newResolution(1, 1_000)
	res.ensureFits(wide)
	res.insert(0, wide)
	res.insert(0, wide)

	assert.Equal(t, []uint64{wide}, res.SortedDocs())
}

// TestResolutionUnwidenedWideDocumentIsNotTruncated pins what happens when a
// caller inserts a document without widening for it first, which the callers in
// this package do not do but a future one might. The document cannot fit a slot,
// so the answer is either the truncated document — a different one, silently —
// or not the narrow form at all. It must be the latter.
func TestResolutionUnwidenedWideDocumentIsNotTruncated(t *testing.T) {
	const wide = uint64(math.MaxUint32) + 5

	res := newResolution(2, 1_000)
	res.insert(0, 3)
	res.insert(1, wide) // no ensureFits first

	_, ok := res.SortedDocs32()
	require.False(t, ok, "the narrow result must be refused rather than truncated")
	assert.Equal(t, []uint64{3, wide}, res.SortedDocs(),
		"the document must still be answered, as itself")
}

// bitmapOf builds a bitmap holding docs, as a memtable layer would.
func bitmapOf(docs ...uint64) *sroar.Bitmap {
	bm := sroar.NewBitmap()
	for _, d := range docs {
		bm.Set(d)
	}
	return bm
}

// maxIDFor is the bound a shard counter would report for these documents, which
// is what decides how wide the slots start.
func maxIDFor(docs []uint64) uint64 {
	var max uint64
	for _, d := range docs {
		if d > max {
			max = d
		}
	}
	return max
}

func slicesSortUint64(s []uint64) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
