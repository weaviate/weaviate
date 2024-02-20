//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hashtree

import "math"

var _ AggregatedHashTree = (*SegmentedHashTree)(nil)

type SegmentedHashTree struct {
	segmentSize uint64
	segments    []uint64
	hashtree    AggregatedHashTree
}

func NewSegmentedHashTree(segmentSize uint64, segments []uint64, maxHeight int) *SegmentedHashTree {
	capacity := uint64(len(segments)) * segmentSize
	return newSegmentedHashTree(segmentSize, segments, NewCompactHashTree(capacity, maxHeight))
}

func newSegmentedHashTree(segmentSize uint64, segments []uint64, underlyingHashtree AggregatedHashTree) *SegmentedHashTree {
	if len(segments) == 0 {
		panic("illegal segments")
	}

	if segmentSize < 1 {
		panic("illegal segment size")
	}

	// validate segments are in increasing order and that there is enough space in between
	var prevSegmentEnd uint64

	for _, segmentStart := range segments {
		if segmentStart < prevSegmentEnd {
			panic("illegal segments")
		}

		if math.MaxUint64-segmentSize < segmentStart {
			panic("illegal segments")
		}

		prevSegmentEnd = segmentStart + segmentSize - 1
	}

	// prevent undesired effects if segment list is externally manipulated
	ownSegments := make([]uint64, len(segments))
	copy(ownSegments, segments)

	return &SegmentedHashTree{
		segmentSize: segmentSize,
		segments:    ownSegments,
		hashtree:    underlyingHashtree,
	}
}

func (ht *SegmentedHashTree) SegmentSize() uint64 {
	return ht.segmentSize
}

func (ht *SegmentedHashTree) Segments() []uint64 {
	// prevent undesired effects if segment list is externally manipulated
	segments := make([]uint64, len(ht.segments))
	copy(segments, ht.segments)
	return segments
}

func (ht *SegmentedHashTree) Height() int {
	return ht.hashtree.Height()
}

func (ht *SegmentedHashTree) AggregateLeafWith(i uint64, val []byte) AggregatedHashTree {
	ht.hashtree.AggregateLeafWith(ht.mapLeaf(i), val)

	return ht
}

func (ht *SegmentedHashTree) mapLeaf(i uint64) uint64 {
	// find the segment the leaf belong to
	for segment, segmentStart := range ht.segments {
		if i >= segmentStart && i < segmentStart+ht.segmentSize {
			return uint64(segment)*ht.segmentSize + (i - ht.segments[segment])
		}
	}

	panic("out of segment")
}

func (ht *SegmentedHashTree) unmapLeaf(mappedLeaf uint64) uint64 {
	segment := mappedLeaf / ht.segmentSize

	return ht.segments[segment] + (mappedLeaf - segment*ht.segmentSize)
}

func (ht *SegmentedHashTree) Sync() AggregatedHashTree {
	ht.hashtree.Sync()
	return ht
}

func (ht *SegmentedHashTree) Root() Digest {
	return ht.hashtree.Root()
}

func (ht *SegmentedHashTree) Level(level int, discriminant *Bitset, digests []Digest) (n int, err error) {
	return ht.hashtree.Level(level, discriminant, digests)
}

func (ht *SegmentedHashTree) Reset() AggregatedHashTree {
	ht.hashtree.Reset()
	return ht
}

func (ht *SegmentedHashTree) Clone() AggregatedHashTree {
	clone := &SegmentedHashTree{
		segmentSize: ht.segmentSize,
		segments:    ht.segments,
		hashtree:    (ht.hashtree.Clone()).(*CompactHashTree),
	}

	return clone
}
