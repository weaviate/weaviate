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

package lsmkv

import (
	"fmt"

	"github.com/weaviate/weaviate/usecases/byteops"
)

// ColumnarGetVectorFloatsWithView reads docID's vector payload decoded as
// float32s (token-concatenated for multi-vector columns) against an
// already-acquired consistent view. No locks are taken per call — the view
// must have been obtained via GetConsistentView and must not have been
// released yet. This is the zero-copy twin of ColumnarGetVectorFloats, built
// for the rescore hot path where one view serves hundreds of per-candidate
// reads.
//
// Zero-copy contract: when the row is found in one of the view's pinned disk
// segments — and the platform allows reinterpreting the little-endian payload
// bytes as float32s (see byteops.Float32sFromBytesZeroCopy) — the returned
// slice ALIASES the segment's mmap'd contents and aliased is true. The alias
// is safe exactly as long as the view is held: GetConsistentView increments
// every segment's refcount, and a compaction that replaces segments parks
// them in segmentsAwaitingDrop until their refcount reaches zero — only then
// are they munmap'd and deleted (see SegmentGroup.getConsistentViewOfSegments
// and the compaction drop path). The caller must (a) finish all reads of the
// slice before calling ReleaseView and (b) never write through it: segment
// mmaps are read-only mappings, a write faults.
//
// Memtable rows are deliberately excluded from aliasing: memtable row storage
// is heap memory mutated in place under the memtable's own locks (same-docID
// re-puts overwrite the payload bytes, deletes flip the tombstone), and a
// consistent view pins memtable *references*, not their contents. An alias
// into a memtable could therefore observe torn or changed data even while the
// view is held. Rows found in the active or flushing memtable — like
// big-endian platforms and misaligned payloads — are decoded into dst by copy
// and returned with aliased == false.
//
// Returns (floats, aliased, found, err); found is false for missing docIDs
// and tombstones. An error means the segment list contained a non-columnar
// segment — an invariant violation, not a "not found".
func (b *Bucket) ColumnarGetVectorFloatsWithView(view BucketConsistentView,
	docID uint64, dst []float32,
) (floats []float32, aliased bool, found bool, err error) {
	// direct comparison instead of CheckExpectedStrategy: the variadic
	// expected-strategies slice is a measurable allocation on this hot path
	// (one fetch per rescore candidate)
	if b.strategy != StrategyColumnar {
		return dst, false, false, fmt.Errorf("strategy %q expected, got %q",
			StrategyColumnar, b.strategy)
	}

	if out, found, tomb := view.Active.columnarLookupFloats(docID, dst); found {
		return out, false, !tomb, nil
	}
	if view.Flushing != nil {
		if out, found, tomb := view.Flushing.columnarLookupFloats(docID, dst); found {
			return out, false, !tomb, nil
		}
	}

	segments := view.Disk
	for i := len(segments) - 1; i >= 0; i-- {
		seg, err := asColumnarSegment(segments[i], i)
		if err != nil {
			return dst, false, false, fmt.Errorf("columnar floats view lookup: %w", err)
		}
		ref, found, tomb := seg.columnarData.lookupPayloadRef(seg.contents, docID)
		if !found {
			continue
		}
		if tomb {
			return dst, false, false, nil
		}
		if aliasedFloats, ok := byteops.Float32sFromBytesZeroCopy(ref); ok {
			return aliasedFloats, true, true, nil
		}
		// big-endian platform or misaligned payload: decode by copy
		return BytesToFloat32s(ref, dst), false, true, nil
	}
	return dst, false, false, nil
}
