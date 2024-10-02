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

package visited

import "sync"

type segment struct {
	bitSet []byte
}

func newSegment(size int) *segment {
	return &segment{
		bitSet: make([]byte, size),
	}
}

func (s *segment) setMasked(byteInSegment uint64, mask byte) {
	s.bitSet[byteInSegment] = s.bitSet[byteInSegment] | mask
}

func (s *segment) getMasked(byteInSegment uint64, mask byte) bool {
	return s.bitSet[byteInSegment]&mask != 0
}

type segmentedBitSet struct {
	segments []*segment
	pool     *sync.Pool
}

func newSegmentedBitSet(size, collisionRate int) *segmentedBitSet {
	return &segmentedBitSet{
		segments: make([]*segment, size/collisionRate+1),
		pool: &sync.Pool{
			New: func() interface{} {
				return newSegment(collisionRate/8 + 1)
			},
		},
	}
}

func (s *segmentedBitSet) set(segmentedInded, byteInSegment uint64, mask byte) {
	s.segments[segmentedInded].setMasked(byteInSegment, mask)
}

func (s *segmentedBitSet) get(segmentedInded, byteInSegment uint64, mask byte) bool {
	return s.segments[segmentedInded].getMasked(byteInSegment, mask)
}

func (s *segmentedBitSet) grow(size uint64) {
	s.segments = append(s.segments, make([]*segment, size)...)
}

type SparseSet struct {
	collisionRate    uint64
	collidingBitSet  []byte
	segmentedBitSets *segmentedBitSet
	masks            []byte
}

func NewSparseSet(size, collisionRate int) *SparseSet {
	masks := make([]byte, 8)
	for i := range masks {
		masks[i] = 1 << i
	}
	return &SparseSet{
		collisionRate:    uint64(collisionRate),
		collidingBitSet:  make([]byte, (size/collisionRate)/8+1),
		segmentedBitSets: newSegmentedBitSet(size, collisionRate),
		masks:            masks,
	}
}

func (s SparseSet) Len() int { return len(s.collidingBitSet) * int(s.collisionRate*8) }

func (s *SparseSet) Visit(node uint64) {
	if node >= uint64(s.Len()) {
		s.grow(node)
	}
	segment := node / s.collisionRate / 8
	segmentIndex := node / s.collisionRate % 8
	segmentedInded := node / s.collisionRate
	if s.collidingBitSet[segment]&s.masks[segmentIndex] == 0 {
		s.resetSegment(segmentedInded)
		s.collidingBitSet[segment] = s.collidingBitSet[segment] | s.masks[segmentIndex]
	}
	byteInSegment := (node % s.collisionRate) / 8
	s.segmentedBitSets.set(segmentedInded, byteInSegment, s.masks[node%8])
}

func (s *SparseSet) Visited(node uint64) bool {
	if node >= uint64(s.Len()) {
		return false
	}
	segment := node / s.collisionRate / 8
	segmentIndex := node / s.collisionRate % 8
	if s.collidingBitSet[segment]&s.masks[segmentIndex] == 0 {
		return false
	}
	segmentedInded := node / s.collisionRate
	byteInSegment := (node % s.collisionRate) / 8
	return s.segmentedBitSets.get(segmentedInded, byteInSegment, s.masks[node%8])
}

func (s *SparseSet) Reset() {
	for i := range s.collidingBitSet {
		s.collidingBitSet[i] = 0
	}
	for i, segment := range s.segmentedBitSets.segments {
		if segment != nil {
			s.segmentedBitSets.pool.Put(segment)
			s.segmentedBitSets.segments[i] = nil
		}
	}
}

func (s *SparseSet) resetSegment(segmentId uint64) {
	s.segmentedBitSets.segments[segmentId] = s.segmentedBitSets.pool.Get().(*segment)
	segment := s.segmentedBitSets.segments[segmentId]
	for i := range segment.bitSet {
		segment.bitSet[i] = 0
	}
}

func (s *SparseSet) grow(size uint64) {
	growSize := (size-uint64(s.Len()))/s.collisionRate + 8
	s.collidingBitSet = append(s.collidingBitSet, make([]byte, growSize/8)...)
	s.segmentedBitSets.grow(growSize)
}
