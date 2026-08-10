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

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// Resolution is the per-query-key working set a ContainsAny resolution is built
// up in: for each position in the query, the documents that key is currently
// known to hold.
//
// Almost every key holds exactly one document, so the common case is a single
// slot and nothing else. Extras carries the remainder for the rare key that
// holds several — a property whose values were meant to be unique but are not —
// so those documents are still answered rather than lost.
//
// An empty slot holds noDoc, so a slot and an extra express absence the same
// way: a slot empties by returning to noDoc, an extra by being removed from the
// list.
type Resolution struct {
	docs   []uint64
	extras []extraDoc
}

// extraDoc is one document beyond the first held by the query key at qi.
type extraDoc struct {
	qi  int32
	doc uint64
}

// noDoc marks a slot holding nothing. Out of the range of real documents, which
// come from a counter starting at zero.
const noDoc = math.MaxUint64

// newResolution sizes a working set for numKeys query keys.
func newResolution(numKeys int) *Resolution {
	docs := make([]uint64, numKeys)
	for i := range docs {
		docs[i] = noDoc
	}
	return &Resolution{docs: docs}
}

// insert records that the key at qi holds doc. Adding, not replacing: a layer
// that supersedes an earlier document does so by deleting it, which is how
// roaringset layers express an update.
func (r *Resolution) insert(qi int, doc uint64) {
	switch r.docs[qi] {
	case noDoc:
		r.docs[qi] = doc
	case doc: // already held
	default:
		r.extras = append(r.extras, extraDoc{qi: int32(qi), doc: doc})
	}
}

// delete retires doc from the key at qi, wherever that key holds it. Naming a
// document the key does not hold is a no-op, which is what makes a deletion
// left behind by an already-superseded document harmless.
func (r *Resolution) delete(qi int, doc uint64) {
	if r.docs[qi] == doc {
		r.docs[qi] = noDoc
		return
	}
	for i := range r.extras {
		if int(r.extras[i].qi) == qi && r.extras[i].doc == doc {
			r.extras[i] = r.extras[len(r.extras)-1]
			r.extras = r.extras[:len(r.extras)-1]
			return
		}
	}
}

// ApplyMemtableMatches applies the bucket's unflushed layers over a resolution of
// the flushed ones, so a query sees writes that have not reached a segment yet.
//
// Each key is carried independently: a layer's deletions retire documents from
// that key alone, then its additions are added to it. Deletions therefore reach
// only the key they were issued under, which is what stops a document that also
// sits under another key from vanishing from both. The layers arrive oldest
// first, so replaying them in order settles a document added by one and deleted
// by a later one as deleted.
//
// Only the keys a layer actually holds arrive, each carrying the position it
// answers — a memtable holding a hundred of a query's hundred thousand keys is
// applied in a hundred steps, not a hundred thousand.
func (r *Resolution) ApplyMemtableMatches(matches []roaringset.LayerMatches) {
	for _, layer := range matches {
		for j, qi := range layer.At {
			r.applyLayerBitmap(layer.Layers[j].Deletions, int(qi), false)
			r.applyLayerBitmap(layer.Layers[j].Additions, int(qi), true)
		}
	}
}

// applyLayerBitmap applies one layer's documents for a key into the working set.
//
// A layer almost always names a single document per key, so that case is read
// without materializing the bitmap. Walking it with an iterator would read
// better but costs an allocation per call — here one per key per query.
func (r *Resolution) applyLayerBitmap(bm *sroar.Bitmap, qi int, adds bool) {
	if bm.IsEmpty() {
		return
	}
	if first := bm.Minimum(); bm.Maximum() == first {
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

// SortedDocs returns every document still held, ascending, with duplicates
// left in — one document can be reached through several query keys, and both
// sroar constructors collapse them while building. It consumes the resolution:
// nothing may read or amend it afterwards, and the returned slice aliases its
// storage.
//
// The survivors are compacted into the slot array itself rather than gathered
// into a second one of the same size — which is what consumes it, and is safe
// because both cursors walk in order and the write never overtakes the read.
func (r *Resolution) SortedDocs() []uint64 {
	out := r.docs[:0]
	for _, held := range r.docs {
		if held != noDoc {
			out = append(out, held)
		}
	}
	for _, e := range r.extras {
		out = append(out, e.doc)
	}
	slices.Sort(out)
	return out
}
