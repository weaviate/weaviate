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

type SegmentedHashTree struct {
	segmentSize uint64
	segments    []uint64
	hashtree    *CompactHashTree
}

func NewSegmentedHashTree(segmentSize uint64, segments []uint64, maxHeight int) *SegmentedHashTree {
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

	capacity := uint64(len(segments)) * segmentSize

	return &SegmentedHashTree{
		segmentSize: segmentSize,
		segments:    ownSegments,
		hashtree:    NewCompactHashTree(capacity, maxHeight),
	}
}

func (ht *SegmentedHashTree) Height() int {
	return ht.hashtree.Height()
}

func (ht *SegmentedHashTree) AggregateLeafWith(i uint64, val []byte) *SegmentedHashTree {
	// validate leaf belong to one of the segments
	segmentFound := -1

	for segment, segmentStart := range ht.segments {
		if i >= segmentStart && i < segmentStart+ht.segmentSize {
			segmentFound = segment
			break
		}
	}

	if segmentFound < 0 {
		panic("out of segment")
	}

	ht.hashtree.AggregateLeafWith(uint64(segmentFound)*ht.segmentSize+i%ht.segmentSize, val)

	return ht
}

func (ht *SegmentedHashTree) Level(level int, discriminant *Bitset, digests []Digest) (n int, err error) {
	return ht.hashtree.Level(level, discriminant, digests)
}

func (ht *SegmentedHashTree) Reset() *SegmentedHashTree {
	ht.hashtree.Reset()
	return ht
}
