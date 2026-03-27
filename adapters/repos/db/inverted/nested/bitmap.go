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
	"fmt"

	"github.com/weaviate/sroar"
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

	MaxRoots         = 1 << rootBits
	MaxLeavesPerRoot = 1 << leafBits
	MaxDocID         = (1 << docBits) - 1
)

// Encode packs root index, leaf index, and document ID into a single uint64
// position value. Root and leaf indices are 1-based (0 is reserved/invalid).
func Encode(rootIdx, leafIdx uint16, docID uint64) uint64 {
	return (uint64(rootIdx) << rootShift) |
		(uint64(leafIdx) << leafShift) |
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

// EncodePositions creates position values for a given root/leaf pair across
// multiple documents. Useful for building bitmaps from position templates
// (where docID=0) by ORing in the real docID.
func EncodePositions(rootIdx, leafIdx uint16, docIDs []uint64) []uint64 {
	base := (uint64(rootIdx) << rootShift) | (uint64(leafIdx) << leafShift)
	out := make([]uint64, len(docIDs))
	for i, d := range docIDs {
		out[i] = base | (d & docMask)
	}
	return out
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

// ValidateRootIdx checks that a root index fits within the 14-bit field.
func ValidateRootIdx(rootIdx int) error {
	if rootIdx < 1 || rootIdx >= MaxRoots {
		return fmt.Errorf("root index %d out of range [1, %d)", rootIdx, MaxRoots)
	}
	return nil
}

// ValidateLeafIdx checks that a leaf index fits within the 14-bit field.
func ValidateLeafIdx(leafIdx int) error {
	if leafIdx < 1 || leafIdx >= MaxLeavesPerRoot {
		return fmt.Errorf("leaf index %d out of range [1, %d)", leafIdx, MaxLeavesPerRoot)
	}
	return nil
}

// MaskLeaf zeroes the leaf bits of every value in bm, collapsing positions
// to root+docID granularity. Values that differed only in leaf_idx become
// identical after masking, allowing element-level alignment across sub-trees.
func MaskLeaf(bm *sroar.Bitmap) *sroar.Bitmap {
	return bm.Masked(zeroLeafBits)
}

// MaskRootLeaf zeroes both root and leaf bits of every value in bm, keeping
// only the docID. Use this as the final step to extract plain document IDs
// from a position bitmap.
func MaskRootLeaf(bm *sroar.Bitmap) *sroar.Bitmap {
	return bm.Masked(zeroRootBits & zeroLeafBits)
}

// AndAll returns the intersection of all bitmaps on raw positions. Inputs are
// not modified. Returns an empty bitmap if bitmaps is empty.
func AndAll(bitmaps []*sroar.Bitmap) *sroar.Bitmap {
	if len(bitmaps) == 0 {
		return sroar.NewBitmap()
	}
	result := bitmaps[0].Clone()
	for _, bm := range bitmaps[1:] {
		result.And(bm)
	}
	return result
}

// AndAllMaskLeaf zeroes the leaf bits of each bitmap and ANDs them all,
// returning root+docID values present across every input. Use this to find
// which document-element pairs satisfy all conditions simultaneously.
// Returns an empty bitmap if bitmaps is empty.
func AndAllMaskLeaf(bitmaps []*sroar.Bitmap) *sroar.Bitmap {
	if len(bitmaps) == 0 {
		return sroar.NewBitmap()
	}
	result := MaskLeaf(bitmaps[0])
	for _, bm := range bitmaps[1:] {
		result.AndMasked(bm, zeroLeafBits)
	}
	return result
}

// MaskLeafAnd intersects a and b on raw positions and zeroes the leaf bits of
// the result, returning root+docID values. Both a and b must be raw position
// bitmaps. Equivalent to MaskLeaf(sroar.And(a, b)) but avoids allocating the
// intermediate AND bitmap.
func MaskLeafAnd(a, b *sroar.Bitmap) *sroar.Bitmap {
	return sroar.MaskedAnd(a, b, zeroLeafBits)
}

// AndWithMaskLeaf intersects a with b after zeroing b's leaf bits, returning
// root+docID values. a must already be leaf-masked (e.g. from MaskLeaf or
// AndAllMaskLeaf); only b is leaf-masked before the AND. Avoids allocating an
// intermediate masked copy of b.
func AndWithMaskLeaf(a, b *sroar.Bitmap) *sroar.Bitmap {
	return a.Clone().AndMasked(b, zeroLeafBits)
}
