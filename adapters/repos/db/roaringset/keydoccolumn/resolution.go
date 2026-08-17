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

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// Resolution is one slot per query key holding its document, plus extras for a
// key holding several. The slots are 32 bits wide while the shard's docID count is
// below 2^32 minus [narrowHeadroom], a width chosen once per query and never widened.
type Resolution struct {
	// exactly one of docs and docs32 is non-nil
	docs   []uint64
	docs32 []uint32
	// extras holds, per query position, the documents beyond its slot's: a
	// bitmap, so a deletion under a key holding many is not a scan of them.
	extras map[int32]*sroar.Bitmap
}

// noDoc and noDoc32 mark an empty slot. No 32-bit resolution holds noDoc32, as
// ApplyMemtableWindow refuses it and column runs stay below the count; a docID
// counter starting at zero never reaches noDoc.
const (
	noDoc   = math.MaxUint64
	noDoc32 = math.MaxUint32
)

// narrowHeadroom is the fewest docIDs a 32-bit resolution leaves above the count
// it was sized from, for documents written during the query. No benchmark sets it,
// and a memtable document past 32 bits fails the query.
const narrowHeadroom = 1 << 24

// newResolution sizes a working set for numKeys query keys, holding documents
// drawn from the docIDCount the shard has issued.
func newResolution(numKeys int, docIDCount uint64) *Resolution {
	if docIDCount >= 1<<32-narrowHeadroom {
		docs := make([]uint64, numKeys)
		for i := range docs {
			docs[i] = noDoc
		}
		return &Resolution{docs: docs}
	}
	docs32 := make([]uint32, numKeys)
	for i := range docs32 {
		docs32[i] = noDoc32
	}
	return &Resolution{docs32: docs32}
}

// insert adds doc to the key at qi, to the extras if the slot holds another. doc
// must fit the slots or it is truncated; column runs hold none wider, and
// [Resolution.ApplyMemtableWindow] refuses wider memtable documents.
func (r *Resolution) insert(qi int, doc uint64) {
	if r.docs32 != nil {
		doc32 := uint32(doc)
		switch r.docs32[qi] {
		case noDoc32:
			r.docs32[qi] = doc32
			return
		case doc32: // already held
			return
		}
	}
	if r.docs != nil {
		switch r.docs[qi] {
		case noDoc:
			r.docs[qi] = doc
			return
		case doc: // already held
			return
		}
	}
	r.addExtra(qi, doc)
}

func (r *Resolution) addExtra(qi int, doc uint64) {
	if r.extras == nil {
		r.extras = map[int32]*sroar.Bitmap{}
	}
	bm := r.extras[int32(qi)]
	if bm == nil {
		bm = sroar.NewBitmap()
		r.extras[int32(qi)] = bm
	}
	bm.Set(doc)
}

// delete removes every copy of doc from the key at qi, in slot and extras; a doc
// the key does not hold is a no-op, so a stale deletion is harmless.
func (r *Resolution) delete(qi int, doc uint64) {
	if r.docs32 != nil {
		if doc < noDoc32 && r.docs32[qi] == uint32(doc) {
			r.docs32[qi] = noDoc32
		}
	} else if r.docs != nil && r.docs[qi] == doc {
		r.docs[qi] = noDoc
	}
	if bm := r.extras[int32(qi)]; bm != nil {
		bm.Remove(doc)
	}
}

// ApplyMemtableWindow applies one window of unflushed memtables over a resolution
// of the flushed data, with windows oldest first and windows[mt][i] holding key
// from+i. Calls covering different keys may come in any order. A 32-bit
// resolution fails on an addition at or past MaxUint32, left part-applied, so the
// caller must discard it.
func (r *Resolution) ApplyMemtableWindow(from int, windows [][]roaringset.BitmapLayer) error {
	for _, w := range windows {
		for i := range w {
			// a zero layer is a key this memtable did not touch
			if w[i].Deletions == nil && w[i].Additions == nil {
				continue
			}
			if err := r.fits(w[i].Additions); err != nil {
				return err
			}
			r.applyLayerBitmap(w[i].Deletions, from+i, false)
			r.applyLayerBitmap(w[i].Additions, from+i, true)
		}
	}
	return nil
}

func (r *Resolution) fits(additions *sroar.Bitmap) error {
	if r.docs32 == nil || additions.IsEmpty() {
		return nil
	}
	if last := additions.Maximum(); last >= noDoc32 {
		return fmt.Errorf("docID %d does not fit the 32-bit resolution chosen at query start: "+
			"documents must be below %d", last, uint64(noDoc32))
	}
	return nil
}

func (r *Resolution) applyLayerBitmap(bm *sroar.Bitmap, qi int, adds bool) {
	if bm.IsEmpty() {
		return
	}
	if first, last := bm.Minimum(), bm.Maximum(); last == first {
		r.apply(qi, first, adds)
		return
	}
	for _, doc := range bm.ToArray() {
		r.apply(qi, doc, adds)
	}
}

func (r *Resolution) apply(qi int, doc uint64, adds bool) {
	if adds {
		r.insert(qi, doc)
	} else {
		r.delete(qi, doc)
	}
}

// Materialize builds the bitmap of every held document into a buffer from pool
// and returns its release. It consumes the resolution and picks the width itself.
func (r *Resolution) Materialize(pool roaringset.BitmapBufPool) (*sroar.Bitmap, func()) {
	if docs, ok := r.sortedDocs32(); ok {
		return pool.SortedList32ToBuf(docs)
	}
	return pool.SortedListToBuf(r.sortedDocs())
}

// sortedDocs32 returns the held documents ascending, compacted into the slot
// array, with duplicates left for sroar to collapse, and consumes the resolution.
// It returns false, consuming nothing, when the slots are 64-bit.
func (r *Resolution) sortedDocs32() ([]uint32, bool) {
	if r.docs32 == nil {
		return nil, false
	}
	extras := r.extrasLen()
	// extras outgrow the slot array, so allocate once at the size needed
	out := r.docs32[:0]
	if need := len(r.docs32) + extras; cap(out) < need {
		out = make([]uint32, 0, need)
	}
	for _, held := range r.docs32 {
		if held != noDoc32 {
			out = append(out, held)
		}
	}
	for _, bm := range r.extras {
		for _, d := range bm.ToArray() {
			out = append(out, uint32(d))
		}
	}
	slices.Sort(out)
	return out, true
}

func (r *Resolution) extrasLen() int {
	n := 0
	for _, bm := range r.extras {
		n += bm.GetCardinality()
	}
	return n
}

// sortedDocs is [Resolution.sortedDocs32] for 64-bit slots, compacted in place
// unless extras outgrow them, and consumes the resolution.
func (r *Resolution) sortedDocs() []uint64 {
	extras := r.extrasLen()
	out := r.docs[:0]
	if need := len(r.docs) + extras; cap(out) < need {
		out = make([]uint64, 0, need)
	}
	for _, held := range r.docs {
		if held != noDoc {
			out = append(out, held)
		}
	}
	for _, bm := range r.extras {
		out = append(out, bm.ToArray()...)
	}
	slices.Sort(out)
	return out
}
