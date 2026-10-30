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
	"slices"
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
	const wide = uint64(math.MaxUint32) + 5

	tests := []struct {
		name string
		// docs are inserted at successive query positions. Unless slots says
		// otherwise the resolution is sized exactly for them, which is the
		// ordinary case: the shard counter has already seen every document a
		// segment holds.
		docs []uint64
		// slots and docIDCount size the resolution explicitly, for the cases
		// where the sizing turns out not to be enough.
		slots      int
		docIDCount uint64
		// then is what happens after those inserts.
		then func(t *testing.T, r *Resolution)
		// narrow is whether the result can still be read 32 bits wide
		narrow bool
		// want defaults to docs, sorted
		want []uint64
	}{
		{name: "all narrow", docs: []uint64{0, 1, math.MaxUint32 - 1}, narrow: true},
		{name: "at the sentinel", docs: []uint64{math.MaxUint32}},
		{name: "past the sentinel", docs: []uint64{math.MaxUint32 + 1}},
		{name: "one wide among narrow", docs: []uint64{7, math.MaxUint32 + 1, 9}},
		{name: "far past the sentinel", docs: []uint64{math.MaxUint64 - 1}},
		{
			// The one case up-front sizing cannot cover: the slots are sized
			// before the memtables are read, so a write landing in between can be
			// wider than they are. An unflushed layer cannot be asked its maximum
			// before the resolution exists — the index's own segments can — so
			// applying it has to widen or the document is lost or truncated.
			name: "a memtable document wider than the slots widens them",
			docs: []uint64{3}, slots: 2, docIDCount: 1_000,
			then: func(t *testing.T, r *Resolution) {
				// key 1 of a window starting at 0, so a zero layer stands for
				// the key the memtable said nothing about
				r.ApplyMemtableWindow(0, [][]roaringset.BitmapLayer{{
					{},
					{Additions: bitmapOf(wide), Deletions: bitmapOf()},
				}})
			},
			want: []uint64{3, wide},
		},
		{
			name:  "the same wide document reached twice is held once",
			slots: 1, docIDCount: 1_000,
			then: func(t *testing.T, r *Resolution) {
				r.ensureFits(wide)
				r.insert(0, wide)
				r.insert(0, wide)
			},
			want: []uint64{wide},
		},
		{
			// what happens when a caller inserts without widening first, which
			// the callers in this package do not do but a future one might: the
			// document cannot fit a slot, so the answer is either the truncated
			// document — a different one, silently — or not the narrow form at
			// all. It must be the latter.
			name: "a wide document inserted without widening is not truncated",
			docs: []uint64{3}, slots: 2, docIDCount: 1_000,
			then: func(t *testing.T, r *Resolution) {
				r.insert(1, wide) // no ensureFits first
			},
			want: []uint64{3, wide},
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

// bitmapOf builds a bitmap holding docs, as a memtable layer would.
func bitmapOf(docs ...uint64) *sroar.Bitmap {
	bm := sroar.NewBitmap()
	for _, d := range docs {
		bm.Set(d)
	}
	return bm
}

// docIDCountFor is what a shard counter would report having issued for these
// documents, which is what decides how wide the slots start.
func docIDCountFor(docs []uint64) uint64 {
	var max uint64
	for _, d := range docs {
		if d > max {
			max = d
		}
	}
	return max + 1
}

// TestResolutionRetiresEveryCopy pins that retiring a document retires it,
// however many places the resolution happens to be holding it in.
//
// A key holds its first document in a slot and the rest in the extras, so which
// of the two any document sits in depends only on how many others share its key
// — and a document can end up in both, because slots empty. Retiring only the
// copy found first leaves the other behind, and answers a document the caller
// deleted.
func TestResolutionRetiresEveryCopy(t *testing.T) {
	const (
		add = true
		del = false
	)
	type step struct {
		adds bool
		doc  uint64
	}
	tests := []struct {
		name  string
		steps []step
		want  []uint64
	}{
		{
			name: "listed twice in the extras",
			steps: []step{
				{add, 5}, // takes the slot
				{add, 9}, // goes to the extras
				{add, 9}, // the same document again
				{del, 9},
			},
			want: []uint64{5},
		},
		{
			name: "held in the slot and in the extras",
			steps: []step{
				{add, 1}, // takes the slot
				{add, 2}, // goes to the extras
				{del, 1}, // the slot empties
				{add, 2}, // so the same document takes it, and is now in both
				{del, 2},
			},
			want: []uint64{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := newResolution(1, 1_000)
			for _, s := range tt.steps {
				if s.adds {
					res.insert(0, s.doc)
				} else {
					res.delete(0, s.doc)
				}
			}
			assert.Equal(t, tt.want, res.sortedDocs())
		})
	}
}
