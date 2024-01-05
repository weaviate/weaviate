//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hashtree

import "math"

var _ AggregatedHashTree = (*MultiSegmentHashTree)(nil)

type Segment [2]uint64

func NewSegment(start, size uint64) Segment {
	return [2]uint64{start, size}
}

func (s Segment) Start() uint64 {
	return s[0]
}

func (s Segment) Size() uint64 {
	return s[1]
}

type MultiSegmentHashTree struct {
	segments []Segment
	hashtree AggregatedHashTree
}

func NewMultiSegmentHashTree(segments []Segment, maxHeight int) *MultiSegmentHashTree {
	if len(segments) == 0 {
		panic("illegal segments")
	}

	// validate segments are in increasing order and that there is enough space in between
	var prevSegmentEnd uint64
	var capacity uint64

	for _, s := range segments {
		segmentStart := s.Start()
		segmentSize := s.Size()

		if segmentStart < prevSegmentEnd {
			panic("illegal segments")
		}

		if segmentSize < 1 {
			panic("illegal segment size")
		}

		if math.MaxUint64-segmentSize < segmentStart {
			panic("illegal segments")
		}

		prevSegmentEnd = segmentStart + segmentSize - 1
		capacity += segmentSize
	}

	// prevent undesired effects if segment list is externally manipulated
	ownSegments := make([]Segment, len(segments))
	copy(ownSegments, segments)

	return &MultiSegmentHashTree{
		segments: ownSegments,
		hashtree: NewCompactHashTree(capacity, maxHeight),
	}
}

func (ht *MultiSegmentHashTree) Height() int {
	return ht.hashtree.Height()
}

func (ht *MultiSegmentHashTree) AggregateLeafWith(i uint64, val []byte) AggregatedHashTree {
	ht.hashtree.AggregateLeafWith(ht.mapLeaf(i), val)

	return ht
}

func (ht *MultiSegmentHashTree) mapLeaf(i uint64) uint64 {
	var offset uint64

	// find the leaf
	for _, segment := range ht.segments {
		segmentStart := segment.Start()
		segmentSize := segment.Size()

		if i >= segmentStart && i < segmentStart+segmentSize {
			return offset + (i - segmentStart)
		}

		offset += segmentSize
	}

	panic("out of segment")
}

func (ht *MultiSegmentHashTree) unmapLeaf(mappedLeaf uint64) uint64 {
	var offset uint64

	// find the segment
	for _, segment := range ht.segments {
		segmentSize := segment.Size()

		if mappedLeaf < offset+segmentSize {
			return segment.Start() + (mappedLeaf - offset)
		}

		offset += segmentSize
	}

	panic("out of segment")
}

func (ht *MultiSegmentHashTree) Sync() AggregatedHashTree {
	ht.hashtree.Sync()
	return ht
}

func (ht *MultiSegmentHashTree) Level(level int, discriminant *Bitset, digests []Digest) (n int, err error) {
	return ht.hashtree.Level(level, discriminant, digests)
}

func (ht *MultiSegmentHashTree) Reset() AggregatedHashTree {
	ht.hashtree.Reset()
	return ht
}

func (ht *MultiSegmentHashTree) Clone() AggregatedHashTree {
	clone := &MultiSegmentHashTree{
		segments: ht.segments,
		hashtree: (ht.hashtree.Clone()).(*CompactHashTree),
	}

	return clone
}
