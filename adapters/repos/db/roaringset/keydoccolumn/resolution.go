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
// Documents are held 64 bits wide, except on shards whose documents all fit in
// 32, where the slots take that form and halve the largest allocation a query
// makes: one slot per query key means a 100,000-key filter carries 400KB rather
// than 800KB, on every shard it touches at once.
//
// Whether the slots are 32- or 64-bit is chosen before the first insert, from
// the largest document each source says it will contribute. Inserting therefore
// never has to ask.
//
// The zero value is not usable: a resolution comes from an index resolving a
// batch of keys, which is what sizes its slots to that batch.
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

// noDoc and noDoc32 mark a slot holding nothing.
//
// noDoc32 is out of range by construction: a resolution whose documents can
// reach it takes the 64-bit form instead, so no narrow slot carries its own
// sentinel as a document.
//
// noDoc is the one docID this structure cannot represent — there is no wider
// form to move to — and a slot given it reads back as empty. Nothing bounds that
// except the source of docIDs: they come from a counter that starts at zero and
// hands them out one at a time, so reaching MaxUint64 means having issued every
// other docID first.
const (
	noDoc   = math.MaxUint64
	noDoc32 = math.MaxUint32
)

// newResolution sizes a working set for numKeys query keys, holding documents
// drawn from the docIDCount the shard has issued.
func newResolution(numKeys int, docIDCount uint64) *Resolution {
	if docIDCount > noDoc32 {
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
// callers choose the width with [Resolution.ensureFits] before inserting
// anything. Truncating it into a slot would answer a different document,
// silently.
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

// ensureFits widens the slots to 64 bits if the 32-bit form could not hold
// maxDoc, so that inserting it — and anything below it — never has to. Callers
// name the largest document they are about to insert, before they insert any of
// it.
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

// normalize widens the slots from 32 to 64 bits, once, when a document turns up
// that the smaller form cannot express. Every slot keeps what it held, and the
// empty ones change sentinel.
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
// Every copy goes, not just the first: the extras are a list and can hold the
// same document twice, and a key can hold one in a slot and in the extras at
// once, because slots empty and are taken again.
func (r *Resolution) delete(qi int, doc uint64) {
	if r.docs32 != nil {
		if doc < noDoc32 && r.docs32[qi] == uint32(doc) {
			r.docs32[qi] = noDoc32
		}
	} else if r.docs != nil && r.docs[qi] == doc {
		r.docs[qi] = noDoc
	}
	if len(r.extras) == 0 {
		// The shape this index is for: one document per key, nothing beyond the
		// slots, so the scan below is skipped entirely rather than walked empty.
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

// ApplyMemtableWindow applies one window of the bucket's unflushed layers over
// a resolution of the flushed ones, so a query sees writes that have not reached
// a segment yet. windows holds one dense window per memtable, oldest first, and
// covers the query keys from..from+len.
//
// Each key is applied on its own: a layer's deletions retire documents from that
// key alone, then its additions are added to it. Deletions therefore reach only
// the key they were issued under, which is what stops a document that also sits
// under another key from vanishing from both. The memtables arrive oldest first,
// so replaying them in order settles a document added by one and deleted by a
// later one as deleted.
//
// Windows may be applied in any order, and the result is what applying every
// memtable over the whole batch would give: two query keys never touch the same
// state, so only the order within one key matters, and that is fixed here.
func (r *Resolution) ApplyMemtableWindow(from int, windows [][]roaringset.BitmapLayer) {
	for _, w := range windows {
		for i := range w {
			// A zero layer is a key this memtable said nothing about, which is
			// most of a window: a memtable is a delta.
			if w[i].Deletions == nil && w[i].Additions == nil {
				continue
			}
			r.applyLayerBitmap(w[i].Deletions, from+i, false)
			r.applyLayerBitmap(w[i].Additions, from+i, true)
		}
	}
}

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

// Materialize builds the bitmap of every document this resolution holds, into a
// buffer from pool, and hands back the release that returns it. It consumes the
// resolution: nothing may read or amend it afterwards.
//
// One method rather than a narrow one and a wide one for the caller to choose
// between, because the choice is not the caller's to make and getting it wrong
// is quiet. A resolution holding its documents 32 bits wide answers a narrow
// request and a wide one alike, so a caller taking the narrow path and ignoring
// its second return gets an empty answer where the documents are; and taking the
// wide path first leaves the narrow one reading storage already consumed. Both
// compile, both read naturally.
func (r *Resolution) Materialize(pool roaringset.BitmapBufPool) (*sroar.Bitmap, func()) {
	if docs, ok := r.sortedDocs32(); ok {
		return pool.SortedList32ToBuf(docs)
	}
	return pool.SortedListToBuf(r.sortedDocs())
}

// sortedDocs32 returns every document still held, ascending, with duplicates
// left in — one document can be reached through several query keys, and the
// sroar constructors collapse them while building. The second return is false
// when the resolution holds its documents in the ordinary 64-bit form, in which
// case the caller must use [Resolution.sortedDocs]; nothing is consumed then.
//
// It consumes the resolution otherwise: nothing may read or amend it afterwards,
// and the returned slice aliases its storage. The survivors are compacted into
// the slot array itself — what makes the smaller form worth having, since
// copying them out would give back what it saved. A key holding several
// documents outgrows that array, which holds one slot per query key, so a batch
// with extras grows the output instead of reusing it.
func (r *Resolution) sortedDocs32() ([]uint32, bool) {
	if r.docs32 == nil {
		return nil, false
	}
	for _, e := range r.extras {
		if e.doc >= noDoc32 {
			// Only reachable if a caller inserted without widening first, which
			// [Resolution.insert] parks in the extras rather than truncating.
			// Refusing here is the other half of not truncating.
			return nil, false
		}
	}
	// One slot per query key, so a batch with extras outgrows it. Both counts
	// are known here, so the growth is one allocation of the right size rather
	// than a doubling walk from the slot array's capacity.
	out := r.docs32[:0]
	if need := len(r.docs32) + len(r.extras); cap(out) < need {
		out = make([]uint32, 0, need)
	}
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

// sortedDocs is [Resolution.sortedDocs32] at the ordinary width, and answers
// whichever form the slots take — a caller that does not want the smaller one
// can always use it. The 64-bit slots compact in place as the 32-bit ones do;
// the 32-bit slots cannot, since the result is wider than they are, so that case
// allocates. It consumes the resolution either way.
//
// A key holding several documents outgrows the slot array, which holds one slot
// per query key, so a batch with extras allocates once at the size it needs.
func (r *Resolution) sortedDocs() []uint64 {
	// Exactly one slot array is ever set, so only one of the two loops below
	// runs. Written as one body rather than a branch per width because the only
	// difference between them is which slots are read.
	out := r.docs[:0]
	if need := len(r.docs) + len(r.docs32) + len(r.extras); cap(out) < need {
		out = make([]uint64, 0, need)
	}
	for _, held := range r.docs32 {
		if held != noDoc32 {
			out = append(out, uint64(held))
		}
	}
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
