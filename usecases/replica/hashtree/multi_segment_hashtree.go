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

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
)

var _ AggregatedHashTree = (*MultiSegmentHashTree)(nil)

const SegmentLength = 16

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

func (s *Segment) MarshalBinary() ([]byte, error) {
	var bs [SegmentLength]byte

	binary.BigEndian.PutUint64(bs[:], s[0])
	binary.BigEndian.PutUint64(bs[8:], s[1])

	return bs[:], nil
}

func (s *Segment) UnmarshalBinary(bs []byte) error {
	if len(bs) != SegmentLength {
		return fmt.Errorf("invalid Segment serialization")
	}

	s[0] = binary.BigEndian.Uint64(bs[:])
	s[1] = binary.BigEndian.Uint64(bs[8:])

	return nil
}

type MultiSegmentHashTree struct {
	segments []Segment
	hashtree AggregatedHashTree
}

func NewMultiSegmentHashTree(segments []Segment, maxHeight int) *MultiSegmentHashTree {
	var capacity uint64

	for _, s := range segments {
		capacity += s.Size()
	}

	return newMultiSegmentHashTree(segments, NewCompactHashTree(capacity, maxHeight))
}

func newMultiSegmentHashTree(segments []Segment, underlyingHashTree AggregatedHashTree) *MultiSegmentHashTree {
	if len(segments) == 0 {
		panic("illegal segments")
	}

	// prevent undesired effects if segment list is externally manipulated
	ownSegments := make([]Segment, len(segments))
	copy(ownSegments, segments)

	sort.SliceStable(ownSegments, func(i, j int) bool {
		return ownSegments[i].Start() < ownSegments[j].Start()
	})

	// validate segments are in increasing order and that there is enough space in between
	var minNextSegmentStart uint64

	for _, s := range ownSegments {
		segmentStart := s.Start()
		segmentSize := s.Size()

		if segmentStart < minNextSegmentStart {
			panic("illegal segments")
		}

		if segmentSize < 1 {
			panic("illegal segment size")
		}

		if math.MaxUint64-segmentSize < segmentStart {
			// invariant: i == len(ownSegments)-1
			minNextSegmentStart = segmentSize - (math.MaxUint64 - segmentStart)

			if len(ownSegments) > 1 && minNextSegmentStart > ownSegments[0].Start() {
				panic("illegal segments")
			}
		} else {
			minNextSegmentStart = segmentStart + segmentSize
		}
	}

	return &MultiSegmentHashTree{
		segments: ownSegments,
		hashtree: underlyingHashTree,
	}
}

func (ht *MultiSegmentHashTree) Segments() []Segment {
	// prevent undesired effects if segment list is externally manipulated
	segments := make([]Segment, len(ht.segments))
	copy(segments, ht.segments)
	return segments
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

		if math.MaxUint64-segmentSize < segmentStart {
			// note there is at most there one segment satisfying this condition

			if i >= segmentStart {
				return offset + (i - segmentStart)
			} else if i < segmentSize-(math.MaxUint64-segmentStart) {
				return offset + (math.MaxUint64 - segmentStart) + i
			}
		} else {
			if i >= segmentStart && i < segmentStart+segmentSize {
				return offset + (i - segmentStart)
			}
		}

		offset += segmentSize
	}

	panic("out of segment")
}

func (ht *MultiSegmentHashTree) unmapLeaf(mappedLeaf uint64) uint64 {
	var offset uint64

	// find the segment
	for _, segment := range ht.segments {
		segmentStart := segment.Start()
		segmentSize := segment.Size()

		if mappedLeaf < offset+segmentSize {
			// segment was found

			if math.MaxUint64-segmentSize < segmentStart {
				// note there is at most there one segment satisfying this condition

				if mappedLeaf >= offset+(math.MaxUint64-segmentStart) {
					return segment.Start() + (mappedLeaf - offset)
				} else {
					return (math.MaxUint64 - segmentStart) + (mappedLeaf - offset)
				}
			} else {
				return segment.Start() + (mappedLeaf - offset)
			}
		}

		offset += segmentSize
	}

	panic("out of segment")
}

func (ht *MultiSegmentHashTree) Sync() AggregatedHashTree {
	ht.hashtree.Sync()
	return ht
}

func (ht *MultiSegmentHashTree) Root() Digest {
	return ht.hashtree.Root()
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
