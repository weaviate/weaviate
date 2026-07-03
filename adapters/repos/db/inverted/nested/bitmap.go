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

package nested

import (
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// Position encoding layout (64 bits total):
//
//	| elemIdx (16 bits) | docID (48 bits) |
//	| bits 63-48        | bits 47-0       |
//
// elemIdx: 1-based counter assigned depth-first across the entire document.
// Never resets per top-level element; each object node or scalar-array slot
// gets exactly one index globally within the document.
//
// docID: 48-bit internal document identifier.
const (
	elemBits = 16
	docBits  = 48

	elemShift = docBits             // 48
	elemMask  = (1 << elemBits) - 1 // 0xFFFF
	docMask   = (1 << docBits) - 1  // 0x0000_FFFF_FFFF_FFFF

	// zeroElemBits zeroes elem bits (63-48), keeping only docID.
	// It is also used as the bucket mask in LiftToAncestor: sroar requires
	// mask & 0xFFFF == 0xFFFF, and zeroElemBits = 0x0000_FFFF_FFFF_FFFF
	// satisfies that constraint.
	zeroElemBits = ^(uint64(elemMask) << elemShift)

	MaxElems = 1 << elemBits      // 65536; indices 1..65535 are valid
	MaxDocID = (1 << docBits) - 1 // 281474976710655
)

// Encode packs an element index and document ID into a single uint64 position
// value. elemIdx is 1-based (0 is reserved/invalid). Both fields are masked to
// their declared widths before packing, so out-of-range inputs are silently
// clipped.
//
// elemIdx is uint32 to leave headroom for future widening without an API
// break. Only the low elemBits bits are used; upper bits are masked away.
func Encode(elemIdx uint32, docID uint64) uint64 {
	return (uint64(elemIdx)&elemMask)<<elemShift | (docID & docMask)
}

// DecodeElemIdx extracts the element index from an encoded position.
func DecodeElemIdx(pos uint64) uint32 {
	return uint32((pos >> elemShift) & elemMask)
}

// DecodeDocID extracts the document ID from an encoded position.
func DecodeDocID(pos uint64) uint64 {
	return pos & docMask
}

// Compile-time guard: posArena and walk-phase scratch hold ElemIdx (uint32).
// Safe as long as elemBits ≤ 32. A negative shift count in a constant expression
// is a compile error in Go, so this fires if elemBits is widened past 32.
const _ = ElemIdx(0) + ElemIdx(1<<(32-elemBits)-1)

// PositionsWithDocID encodes raw element indices into full uint64 positions by
// merging each index with docID via Encode. This is the sole site where ElemIdx
// crosses the boundary into encoded uint64 for bitmap operations. Returns a new
// slice; does not retain or modify the input. Variadic so callers can pass an
// existing posArena subslice with a spread (`idxs...`) or a single scalar marker
// (e.g. an anchor's self-marker) without materialising a one-element slice.
func PositionsWithDocID(docID uint64, idxs ...ElemIdx) []uint64 {
	out := make([]uint64, len(idxs))
	for i, idx := range idxs {
		out[i] = Encode(uint32(idx), docID)
	}
	return out
}

// BitmapOps provides pool-backed versions of every bitmap merge operation used
// by the nested filter executor. Each method returns the result bitmap and a
// release function; callers must invoke release() when the bitmap is no longer
// needed so the underlying buffer is returned to the pool.
//
// Using pool-backed allocations on the hot resolution path reduces GC pressure
// because intermediate bitmaps do not escape to the heap.
type BitmapOps struct {
	pool roaringset.BitmapBufPool
}

// NewBitmapOps constructs a BitmapOps that allocates result bitmaps from pool.
// Pass roaringset.NewBitmapBufPoolNoop() in tests or the real pool in production.
func NewBitmapOps(pool roaringset.BitmapBufPool) *BitmapOps {
	return &BitmapOps{pool: pool}
}

// NewEmpty returns an empty bitmap backed by a pool buffer sized to minCap
// bytes. As values are added the bitmap may outgrow the initial buffer and
// allocate internally, but for typical use (result ⊆ some known upper bound)
// the hint avoids that reallocation.
func (o *BitmapOps) NewEmpty(minCap int) (result *sroar.Bitmap, release func()) {
	buf, release := o.pool.Get(minCap)
	return sroar.NewBitmapToBuf(buf), release
}

// MaskElem zeroes the elem bits of raw and returns the doc bitmap in a pool
// buffer. The caller must invoke release() when the result is no longer needed.
//
// Deduplicates: multiple positions with different elemIdx values but the same
// docID collapse to a single value with elemIdx=0.
func (o *BitmapOps) MaskElem(raw *sroar.Bitmap) (doc *sroar.Bitmap, release func()) {
	buf, release := o.pool.Get(raw.LenInBytes())
	return raw.MaskedToBuf(zeroElemBits, buf), release
}

// LiftToAncestor projects child markers to their nearest ancestor markers in
// parentAnchor by predecessor scan within the same docID bucket.
//
// parentAnchor does not have to be the immediately enclosing collection — any
// ancestor anchor works. With _anchor(garages), child cars lift to their owning
// garage. With _anchor(countries), the same cars lift directly to their owning
// country, skipping the intermediate garage level. The result is equivalent to
// a chain of single-level lifts; the direct call is cheaper.
//
// children must contain positions that sit strictly below the target ancestor
// scope — the intended use is exact child self markers such as
// m_pinned_match = value ∩ _idx ∩ _anchor(childPath).
//
// For each child position c the method delegates to sroar.FloorMaskedToBuf,
// which emits the greatest parent marker p ≤ c sharing the same docID bucket
// (same key under zeroElemBits masking). The ≤ predicate is behaviorally
// identical to strict < here: DFS pre-order assignment in assign.go guarantees
// a parent's self-marker always has a strictly lower elemIdx than any
// descendant, so a parent and child position never share the same 64-bit value.
//
// Multiple children may lift to the same ancestor; the bitmap naturally
// deduplicates.
//
// Load-bearing assumption (see assign.go): the walker emits elemIdx in strict
// DFS order via nextElem() across all top-level elements without resetting per
// root. The DFS guarantee ensures a parent's self-marker always has a strictly
// lower elemIdx than any descendant, making the predecessor scan correct for
// both single-level and multi-level lifts.
func (o *BitmapOps) LiftToAncestor(children, parentAnchor *sroar.Bitmap) (parent *sroar.Bitmap, release func()) {
	if children.IsEmpty() || parentAnchor.IsEmpty() {
		return sroar.NewBitmap(), func() {}
	}
	buf, release := o.pool.Get(parentAnchor.LenInBytes())
	return sroar.FloorMaskedToBuf(children, parentAnchor, zeroElemBits, buf), release
}

// AndAll returns the intersection of all raw position bitmaps in a pool
// buffer. Returns an empty (non-pooled) bitmap when raws is empty. The loop
// exits early when the running intersection becomes empty — further ANDs
// cannot change the result.
func (o *BitmapOps) AndAll(raws []*sroar.Bitmap, maxConcurrency int) (raw *sroar.Bitmap, release func()) {
	if len(raws) == 0 {
		return sroar.NewBitmap(), func() {}
	}
	raw, release = o.pool.CloneToBuf(raws[0])
	for _, bm := range raws[1:] {
		raw.AndConc(bm, maxConcurrency)
		if raw.IsEmpty() {
			return raw, release
		}
	}
	return raw, release
}

// AndNot clones base into a pool buffer and subtracts subtract in place,
// returning the resulting bitmap and a release callback. Used to materialize
// the positive bitmap for NotEqual (universe AND-NOT denylist) without
// mutating the source universe bitmap.
func (o *BitmapOps) AndNot(base, subtract *sroar.Bitmap, maxConcurrency int) (raw *sroar.Bitmap, release func()) {
	raw, release = o.pool.CloneToBuf(base)
	raw.AndNotConc(subtract, maxConcurrency)
	return raw, release
}

// OrAll returns the union of all raw position bitmaps in a pool buffer.
// The largest input is cloned as the accumulator and the rest are folded
// in — avoiding one OrConc pass and starting from a pre-populated
// container structure (typically faster than growing an empty buffer
// through repeated ORs). Returns an empty (non-pooled) bitmap when raws
// is empty.
//
// TODO aliszka:nested_filtering: revisit buffer sizing. Cloning the
// largest input still under-sizes the accumulator when the remaining
// inputs contain disjoint values — internal growth fires. Sum of input
// sizes is the safe upper bound but wastes pool capacity in the common
// overlapping case. Profile the nested-filter workload to pick the
// sweet spot.
//
// TODO aliszka:nested_filtering: consider sorting the remaining inputs
// in descending order by LenInBytes before folding them in. OR is
// commutative so correctness is unaffected; the question is whether
// accumulator-growth cost dominates enough for sort order to matter.
// Profile before adding the sort cost.
func (o *BitmapOps) OrAll(raws []*sroar.Bitmap, maxConcurrency int) (raw *sroar.Bitmap, release func()) {
	if len(raws) == 0 {
		return sroar.NewBitmap(), func() {}
	}
	largestIdx := 0
	for i := 1; i < len(raws); i++ {
		if raws[i].LenInBytes() > raws[largestIdx].LenInBytes() {
			largestIdx = i
		}
	}
	raw, release = o.pool.CloneToBuf(raws[largestIdx])
	for i, bm := range raws {
		if i == largestIdx {
			continue
		}
		raw.OrConc(bm, maxConcurrency)
	}
	return raw, release
}
