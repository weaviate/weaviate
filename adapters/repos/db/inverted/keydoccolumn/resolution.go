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
// An empty slot holds no document, so a slot and an extra express absence the
// same way: a slot empties by returning to its sentinel, an extra by being
// removed from the list.
//
// Documents are held as they are everywhere else, 64 bits wide, except on shards
// whose documents all fit in 32 — where the slots take that form instead and
// halve the largest allocation a query makes. There is one slot per query key,
// so a 100,000-key filter carries 400KB rather than 800KB, on every shard it
// touches at once.
//
// Which form a resolution takes is settled before anything is inserted, by every
// source naming the largest document it is about to contribute: the shard
// counter up front, an unflushed layer as it is applied, since it cannot be
// asked any earlier. Inserting therefore never changes the form, and never has
// to ask whether it should — which is what keeps it cheap enough for the
// compiler to fold into the scan that calls it once per matched row.
type Resolution struct {
	// exactly one of docs and docs32 is non-nil
	docs   []uint64
	docs32 []uint32
	extras []extraDoc
}

// extraDoc is one document beyond the first held by the query key at qi. Always
// 64 bits, whichever form the slots take, so a document that outgrew the slots
// still has somewhere to go.
type extraDoc struct {
	qi  int32
	doc uint64
}

// noDoc and noDoc32 mark a slot holding nothing, each out of the range of
// documents its form can hold: a resolution whose documents can reach noDoc32
// takes the 64-bit form, so no slot ever carries its sentinel as a document.
const (
	noDoc   = math.MaxUint64
	noDoc32 = math.MaxUint32
)

// newResolution sizes a working set for numKeys query keys, holding documents up
// to maxID.
func newResolution(numKeys int, maxID uint64) *Resolution {
	if maxID >= noDoc32 {
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

// insert records that the key at qi holds doc. Adding, not replacing: a layer
// that supersedes an earlier document does so by deleting it, which is how
// roaringset layers express an update.
//
// A key already holding another document sends this one to the extras, as does —
// defensively — a document too large for the slots, which cannot arrive: the
// callers settle the form with [Resolution.ensureFits] before inserting
// anything. Truncating it into a slot would answer a different document,
// silently, which is not a way to discover that a caller stopped doing that.
//
// Deliberately one function, out of line from nothing. It is small enough for
// the compiler to fold into the scan that calls it per matched row, and every
// case it could delegate is one the compiler would fold back in anyway.
func (r *Resolution) insert(qi int, doc uint64) {
	if r.docs32 != nil && doc < noDoc32 {
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
	r.extras = append(r.extras, extraDoc{qi: int32(qi), doc: doc})
}

// ensureFits moves the slots to their 64-bit form if the 32-bit one could not
// hold maxDoc, so that inserting it — and anything below it — never has to.
// Callers name the largest document they are about to insert, before they insert
// any of it.
//
// The alternative, converting on demand from the insert itself, costs more than
// it looks: the conversion loop then sits in the budget the compiler weighs when
// deciding whether to inline the insert into the scan, so a path taken on shards
// past four billion documents is paid for once per matched row on every other
// shard.
func (r *Resolution) ensureFits(maxDoc uint64) {
	if r.docs32 != nil && maxDoc >= noDoc32 {
		r.normalize()
	}
}

// normalize re-homes the slots from their 32-bit form into the ordinary 64-bit
// one, once, when a document turns up that the smaller form cannot express.
// Every slot keeps what it held, and the empty ones change sentinel.
func (r *Resolution) normalize() {
	docs := make([]uint64, len(r.docs32))
	for i, held := range r.docs32 {
		if held == noDoc32 {
			docs[i] = noDoc
			continue
		}
		docs[i] = uint64(held)
	}
	r.docs = docs
	r.docs32 = nil
}

// delete retires doc from the key at qi, wherever that key holds it. Naming a
// document the key does not hold is a no-op, which is what makes a deletion
// left behind by an already-superseded document harmless.
//
// Every copy goes, not the first one found. A key holds a document once however
// many times it was added, and a slot enforces that by construction — it holds
// one document, so adding the same one twice changes nothing. The extras are a
// list and cannot, so a document added twice is listed twice, and retiring one
// entry would leave the other behind: a document the caller retired, still
// answered. Which of the two a document lands in depends only on how many others
// share its key, so they have to agree.
func (r *Resolution) delete(qi int, doc uint64) {
	if r.docs32 != nil {
		if doc < noDoc32 && r.docs32[qi] == uint32(doc) {
			r.docs32[qi] = noDoc32
			return
		}
	} else if r.docs[qi] == doc {
		r.docs[qi] = noDoc
		return
	}
	for i := 0; i < len(r.extras); {
		if int(r.extras[i].qi) == qi && r.extras[i].doc == doc {
			r.extras[i] = r.extras[len(r.extras)-1]
			r.extras = r.extras[:len(r.extras)-1]
			continue // the entry swapped in has not been looked at yet
		}
		i++
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
	last := bm.Maximum()
	if adds {
		// A memtable is the one source that can hold a document the resolution
		// was not sized for: it was sized from a counter read before this layer
		// was, and a write can land in between. Settling the form here costs the
		// maximum already at hand, and leaves the insert with nothing to check.
		r.ensureFits(last)
	}
	if first := bm.Minimum(); last == first {
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

// SortedDocs32 returns every document still held, ascending, with duplicates
// left in — one document can be reached through several query keys, and the
// sroar constructors collapse them while building. The second return is false
// when the resolution holds its documents in the ordinary 64-bit form, in which
// case the caller must use [Resolution.SortedDocs]; nothing is consumed then.
//
// It consumes the resolution otherwise: nothing may read or amend it
// afterwards, and the returned slice aliases its storage. The survivors are
// compacted into the slot array itself rather than gathered into a second one
// of the same size — which is what consumes it, and is safe because both
// cursors walk in order and the write never overtakes the read. Keeping the
// result in the slots is also what makes the smaller form worth having: copying
// it out at this point would give back what it saved.
func (r *Resolution) SortedDocs32() ([]uint32, bool) {
	if r.docs32 == nil {
		return nil, false
	}
	for _, e := range r.extras {
		if e.doc >= noDoc32 {
			// Only reachable if a caller inserted without settling the form
			// first, which [Resolution.insert] parks in the extras rather than
			// truncating. Refusing here is the other half of not truncating.
			return nil, false
		}
	}
	out := r.docs32[:0]
	for _, held := range r.docs32 {
		if held != noDoc32 {
			out = append(out, held)
		}
	}
	for _, e := range r.extras {
		out = append(out, uint32(e.doc))
	}
	slices.Sort(out)
	return out, true
}

// SortedDocs is [Resolution.SortedDocs32] at the ordinary width, and answers
// whichever form the slots take — a caller that does not want the smaller one
// can always use it. The 64-bit slots compact in place as the 32-bit ones do;
// the 32-bit slots cannot, since the result is wider than they are, so that case
// allocates. It consumes the resolution either way.
func (r *Resolution) SortedDocs() []uint64 {
	if r.docs32 != nil {
		out := make([]uint64, 0, len(r.docs32)+len(r.extras))
		for _, held := range r.docs32 {
			if held != noDoc32 {
				out = append(out, uint64(held))
			}
		}
		for _, e := range r.extras {
			out = append(out, e.doc)
		}
		slices.Sort(out)
		return out
	}

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
