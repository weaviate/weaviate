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

package nestedlegacy

import (
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// Position encoding layout (64 bits total):
//
//	| root_idx (14 bits) | leaf_idx (14 bits) | docID (36 bits) |
//	| bits 63-50         | bits 49-36         | bits 35-0       |
//
// root_idx: 1-based index into top-level object array. Always 1 for
// standalone objects (treated as implicit 1-element array).
//
// leaf_idx: 1-based counter assigned depth-first within a root element.
// Resets per root. Scalar array elements each get their own leaf_idx.
// Object elements with descendant leaves are intermediate (inherit
// descendants' positions); those without get their own leaf_idx.
//
// docID: 36-bit internal document identifier.
const (
	rootBits = 14
	leafBits = 14
	docBits  = 36

	rootShift = leafBits + docBits // 50
	leafShift = docBits            // 36

	rootMask = (1 << rootBits) - 1 // 0x3FFF
	leafMask = (1 << leafBits) - 1 // 0x3FFF
	docMask  = (1 << docBits) - 1  // 0x0000000FFFFFFFFF

	// Bitmasks for Masked() operations on sroar bitmaps.
	// zeroRootBits zeroes root bits (63-50), keeping leaf+docID.
	zeroRootBits = ^(uint64(rootMask) << rootShift)
	// zeroLeafBits zeroes leaf bits (49-36), keeping root+docID.
	zeroLeafBits = ^(uint64(leafMask) << leafShift)

	MaxRoots         = 1 << rootBits      // 16384
	MaxLeavesPerRoot = 1 << leafBits      // 16384
	MaxDocID         = (1 << docBits) - 1 // 68719476735; 68.7B
)

// Encode packs root index, leaf index, and document ID into a single uint64
// position value. Root and leaf indices are 1-based (0 is reserved/invalid).
// All three fields are masked to their declared widths (rootMask, leafMask,
// docMask) before packing, so out-of-range inputs are silently clipped.
func Encode(rootIdx, leafIdx uint16, docID uint64) uint64 {
	return (uint64(rootIdx)&rootMask)<<rootShift |
		(uint64(leafIdx)&leafMask)<<leafShift |
		(docID & docMask)
}

// DecodeRootIdx extracts the root index from an encoded position.
func DecodeRootIdx(pos uint64) uint16 {
	return uint16((pos >> rootShift) & rootMask)
}

// DecodeLeafIdx extracts the leaf index from an encoded position.
func DecodeLeafIdx(pos uint64) uint16 {
	return uint16((pos >> leafShift) & leafMask)
}

// DecodeDocID extracts the document ID from an encoded position.
func DecodeDocID(pos uint64) uint64 {
	return pos & docMask
}

// OrDocID ORs a real docID into position templates that have docID=0.
// Returns a new slice; does not modify the input.
func OrDocID(positions []uint64, docID uint64) []uint64 {
	masked := docID & docMask
	out := make([]uint64, len(positions))
	for i, p := range positions {
		out[i] = p | masked
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

// MaskLeaf zeroes the leaf bits of raw and returns the rootDoc bitmap in a
// pool buffer. The caller must invoke release() when the result is no longer
// needed.
func (o *BitmapOps) MaskLeaf(raw *sroar.Bitmap) (rootDoc *sroar.Bitmap, release func()) {
	buf, release := o.pool.Get(raw.LenInBytes())
	return raw.MaskedToBuf(zeroLeafBits, buf), release
}

// MaskRootLeaf zeroes both root and leaf bits of positions, returning only
// docIDs in a pool buffer. Use as the final step to extract plain document
// IDs. positions may be raw or rootDoc.
func (o *BitmapOps) MaskRootLeaf(positions *sroar.Bitmap) (doc *sroar.Bitmap, release func()) {
	buf, release := o.pool.Get(positions.LenInBytes())
	return positions.MaskedToBuf(zeroRootBits&zeroLeafBits, buf), release
}

// LiftToAncestor projects child markers to their nearest ancestor markers in
// parentAnchor by predecessor scan within the same (root_idx, docID) bucket.
//
// parentAnchor does not have to be the immediately enclosing collection — any
// ancestor anchor works. With `_anchor(garages)`, child cars lift to their
// owning garage. With `_anchor(countries)`, the same cars lift directly to
// their owning country, skipping the intermediate garage level. The result is
// equivalent to a chain of single-level lifts; the direct call is cheaper.
//
// children must contain positions that sit strictly below the target ancestor
// scope — the intended use is exact child self markers such as
// `m_pinned_match = value ∩ _idx ∩ _anchor(childPath)`.
//
// For each child position c the method delegates to sroar.FloorMaskedToBuf,
// which emits the greatest parent marker p ≤ c sharing the same
// (root_idx, docID) bucket (same key under zeroLeafBits masking). The ≤
// predicate is behaviorally identical to strict < here: DFS pre-order
// assignment in assign.go guarantees a parent's self-marker always has a
// strictly lower leaf_idx than any descendant, so a parent and child position
// never share the same 64-bit value.
//
// Multiple children may lift to the same ancestor; the bitmap naturally
// deduplicates.
//
// Load-bearing assumptions (see assign.go):
//   - the walker emits leaf_idx in strict DFS order via nextLeaf();
//   - distinct top-level array elements get distinct root_idx values, so the
//     (root_idx, docID) bucket key separates per-root subtrees.
//
// If either contract changes, multi-level lifts can silently return wrong
// ancestors. Single-level lifts remain correct as long as parentAnchor is the
// immediate parent.
func (o *BitmapOps) LiftToAncestor(children, parentAnchor *sroar.Bitmap) (parent *sroar.Bitmap, release func()) {
	if children.IsEmpty() || parentAnchor.IsEmpty() {
		return sroar.NewBitmap(), func() {}
	}
	buf, release := o.pool.Get(parentAnchor.LenInBytes())
	return sroar.FloorMaskedToBuf(children, parentAnchor, zeroLeafBits, buf), release
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

// AndAllMaskLeaf zeroes the leaf bits of each raw bitmap, ANDs them all, and
// returns the rootDoc bitmap in a pool buffer. Returns an empty (non-pooled)
// bitmap when raws is empty. The loop exits early when the running
// intersection becomes empty — further ANDs cannot change the result.
func (o *BitmapOps) AndAllMaskLeaf(raws []*sroar.Bitmap, maxConcurrency int) (rootDoc *sroar.Bitmap, release func()) {
	if len(raws) == 0 {
		return sroar.NewBitmap(), func() {}
	}
	buf, release := o.pool.Get(raws[0].LenInBytes())
	rootDoc = raws[0].MaskedToBuf(zeroLeafBits, buf)
	for _, bm := range raws[1:] {
		rootDoc.AndMaskedConc(bm, zeroLeafBits, maxConcurrency)
		if rootDoc.IsEmpty() {
			return rootDoc, release
		}
	}
	return rootDoc, release
}

// MaskLeafAnd intersects rawA and rawB on raw positions, zeroes the leaf bits
// of the result, and returns the rootDoc bitmap in a pool buffer. Equivalent
// to MaskLeaf(sroar.And(rawA, rawB)) but uses a single fused operation.
//
// Both inputs must be raw position bitmaps (non-zero leaf bits).
func (o *BitmapOps) MaskLeafAnd(rawA, rawB *sroar.Bitmap) (rootDoc *sroar.Bitmap, release func()) {
	buf, release := o.pool.Get(min(rawA.LenInBytes(), rawB.LenInBytes()))
	return sroar.MaskedAndToBuf(rawA, rawB, zeroLeafBits, buf), release
}

// IntersectsMaskedLeaf reports whether rootDoc and raw share at least one
// position after zeroing raw's leaf bits. rootDoc must already be leaf-masked.
// No allocation is performed — use this for cheap element pre-checks before
// running the full per-element intersection.
func (o *BitmapOps) IntersectsMaskedLeaf(rootDoc, raw *sroar.Bitmap) bool {
	return rootDoc.IntersectsMasked(raw, zeroLeafBits)
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

// CrossLeafCopresenceAll returns the union of values from all input
// bitmaps whose (root, docID) projection appears in every input — leaf
// differences across inputs are tolerated. Equivalent to
// sroar.CopresenceByMask with mask=zeroLeafBits. Result is allocated in
// a pool buffer sized to the largest input. Returns an empty
// (non-pooled) bitmap when raws is empty.
//
// Use case: cross-disjoint-leaf AND under position-level evaluation.
// When operands sit at different leaves of the same element (sibling
// sub-arrays without a Phase 3 scalar bridge), raw AndAll gives ∅ even
// when the same physical element satisfies every operand. This op keeps
// the contributing leaf positions while filtering by (root, doc)
// co-presence, so the result composes raw with further within-root
// operations.
//
// TODO aliszka:nested_filtering: revisit buffer sizing. max(LenInBytes)
// is usually adequate since the AND step at the heart of copresence can
// only shrink contributions, but heavy overlap with disjoint groupings
// could still grow the result above max. Sum of input sizes is the safe
// upper bound. Profile to pick the sweet spot.
func (o *BitmapOps) CrossLeafCopresenceAll(raws []*sroar.Bitmap) (raw *sroar.Bitmap, release func()) {
	if len(raws) == 0 {
		return sroar.NewBitmap(), func() {}
	}
	maxLen := 0
	for _, bm := range raws {
		if l := bm.LenInBytes(); l > maxLen {
			maxLen = l
		}
	}
	buf, release := o.pool.Get(maxLen)
	return sroar.CopresenceByMaskToBuf(raws, zeroLeafBits, buf), release
}
