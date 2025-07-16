//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package visited

import (
	"math/bits"
	"sync"
)

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
	segmentedBitSets *segmentedBitSet // Most frequently accessed
	collidingBitSet  []byte           // Second most frequent
	collisionRate    uint64           // Used in calculations
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
	}
}

func (s SparseSet) Len() int { return len(s.collidingBitSet) * int(s.collisionRate*8) }

func (s *SparseSet) Visit(node uint64) {
	if node >= uint64(s.Len()) {
		s.grow(node)
	}

	// Pre-compute bit shift values for collisionRate (assuming it's a power of 2)
	collisionShift := uint64(bits.TrailingZeros64(s.collisionRate))

	// Optimized address calculations using bit operations
	segment := node >> (collisionShift + 3)      // node / (collisionRate * 8)
	segmentIndex := (node >> collisionShift) & 7 // (node / collisionRate) % 8
	segmentedIndex := node >> collisionShift     // node / collisionRate

	// Check if segment is already marked as having visited nodes
	segmentMask := byte(1 << segmentIndex)
	if s.collidingBitSet[segment]&segmentMask == 0 {
		s.resetSegment(segmentedIndex)
		s.collidingBitSet[segment] |= segmentMask
	}

	// Set the bit in the segment's bitset
	byteInSegment := (node & (s.collisionRate - 1)) >> 3 // (node % collisionRate) / 8
	bitMask := byte(1 << (node & 7))                     // 1 << (node % 8)

	// Inline the segment access to avoid function call overhead
	s.segmentedBitSets.segments[segmentedIndex].bitSet[byteInSegment] |= bitMask
}

func (s *SparseSet) Visited(node uint64) bool {
	if node >= uint64(s.Len()) {
		return false
	}

	// Assuming collisionRate is a power of 2, use bit shifts
	collisionShift := uint64(bits.TrailingZeros64(s.collisionRate))

	segment := node >> (collisionShift + 3)      // divide by collisionRate * 8
	segmentIndex := (node >> collisionShift) & 7 // mod 8

	if s.collidingBitSet[segment]&(1<<segmentIndex) == 0 {
		return false
	}

	segmentedIndex := node >> collisionShift
	byteInSegment := (node & (s.collisionRate - 1)) >> 3 // mod collisionRate, then div 8
	bitMask := byte(1 << (node & 7))                     // mod 8

	return s.segmentedBitSets.segments[segmentedIndex].bitSet[byteInSegment]&bitMask != 0
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
	if segmentId >= uint64(len(s.segmentedBitSets.segments)) {
		s.segmentedBitSets.grow(segmentId - uint64(len(s.segmentedBitSets.segments)) + 1)
	}
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
