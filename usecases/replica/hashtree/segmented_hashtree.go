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

type SegmentedHashTree struct {
	segments    []uint64
	segmentSize uint64
	hashtree    *CompactHashTree
}

func NewSegmentedHashTree(segments []uint64, segmentSize uint64, maxHeight int) *SegmentedHashTree {
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

		prevSegmentEnd = segmentStart + segmentSize - 1
	}

	// prevent undesired effects if segment list is externally manipulated
	ownSegments := make([]uint64, len(segments))
	copy(ownSegments, segments)

	capacity := uint64(len(segments)) * segmentSize

	return &SegmentedHashTree{
		segments:    ownSegments,
		segmentSize: segmentSize,
		hashtree:    NewCompactHashTree(capacity, maxHeight),
	}
}

func (ht *SegmentedHashTree) Height() int {
	return ht.hashtree.Height()
}

func (ht *SegmentedHashTree) AggregateLeafWith(i uint64, val []byte) *SegmentedHashTree {
	// validate leaf belong to one of the segments
	var segmentFound bool

	for _, segmentStart := range ht.segments {
		if i >= segmentStart && i < segmentStart+uint64(ht.segmentSize) {
			segmentFound = true
		}
	}

	if !segmentFound {
		panic("out of segment")
	}

	ht.hashtree.AggregateLeafWith(i, val)

	return ht
}

func (ht *SegmentedHashTree) Level(level int, discriminant *Bitset, digests []Digest) (n int, err error) {
	return ht.hashtree.Level(level, discriminant, digests)
}

func (ht *SegmentedHashTree) Reset() *SegmentedHashTree {
	ht.hashtree.Reset()
	return ht
}
