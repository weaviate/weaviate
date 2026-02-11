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
	"math"
	"math/bits"
	"sync"
)

type segment struct {
	words []uint64
}

type segmentedBitSet struct {
	segments    []segment
	pool        *sync.Pool
	wordsPerSeg int
}

func newSegmentedBitSet(size, collisionRate int) *segmentedBitSet {
	cr := uint64(collisionRate)
	wordsPerSeg := int((cr + 63) >> 6)

	return &segmentedBitSet{
		segments:    make([]segment, size/collisionRate+1),
		wordsPerSeg: wordsPerSeg,
		pool: &sync.Pool{
			New: func() interface{} {
				return make([]uint64, wordsPerSeg)
			},
		},
	}
}

type SparseSet struct {
	segmentedBitSets *segmentedBitSet
	collidingBitSet  []uint64
	collisionRate    uint64
	collisionShift   uint8
	maxNodeExclusive uint64
}

func NewSparseSet(size, collisionRate int) *SparseSet {
	cr := uint64(collisionRate)

	cb := make([]uint64, (size/int(cr))/64+1)

	s := &SparseSet{
		collisionRate:    cr,
		collisionShift:   uint8(bits.TrailingZeros64(cr)),
		collidingBitSet:  cb,
		segmentedBitSets: newSegmentedBitSet(size, collisionRate),
	}
	s.maxNodeExclusive = uint64(len(cb)) * 64 * cr
	return s
}

func growToUint64SliceLen(slc []uint64, need uint64) []uint64 {
	if uint64(len(slc)) >= need {
		return slc
	}
	newLen := uint64(len(slc))
	if newLen == 0 {
		newLen = 1
	}
	for newLen < need {
		newLen *= 2
	}
	if newLen > uint64(math.MaxInt) {
		panic("growToUint64SliceLen: requested length over MaxInt")
	}
	extra := int(newLen - uint64(len(slc)))
	return append(slc, make([]uint64, extra)...)
}

func growToSegmentSliceLen(slc []segment, need uint64) []segment {
	if uint64(len(slc)) >= need {
		return slc
	}
	newLen := uint64(len(slc))
	if newLen == 0 {
		newLen = 1
	}
	for newLen < need {
		newLen *= 2
	}
	if newLen > uint64(math.MaxInt) {
		panic("growToSegmentSliceLen: requested length over MaxInt")
	}
	extra := int(newLen - uint64(len(slc)))
	return append(slc, make([]segment, extra)...)
}

func (s *SparseSet) grow(node uint64) {
	segmentedIndex := node >> s.collisionShift
	needCBWords := (segmentedIndex >> 6) + 1
	needSegs := segmentedIndex + 1

	s.collidingBitSet = growToUint64SliceLen(s.collidingBitSet, needCBWords)
	s.segmentedBitSets.segments = growToSegmentSliceLen(s.segmentedBitSets.segments, needSegs)

	s.maxNodeExclusive = uint64(len(s.collidingBitSet)) * 64 * s.collisionRate
}

func (s *SparseSet) resetSegment(segmentId uint64) {
	if segmentId >= uint64(len(s.segmentedBitSets.segments)) {
		s.grow(segmentId << s.collisionShift)
	}

	seg := &s.segmentedBitSets.segments[segmentId]
	if seg.words == nil {
		seg.words = s.segmentedBitSets.pool.Get().([]uint64)
	}
	clear(seg.words)
}

func (s *SparseSet) Reset() {
	clear(s.collidingBitSet)
	for i := range s.segmentedBitSets.segments {
		seg := &s.segmentedBitSets.segments[i]
		if seg.words != nil {
			s.segmentedBitSets.pool.Put(seg.words)
			seg.words = nil
		}
	}
}

func (s *SparseSet) Visit(node uint64) {
	if node >= s.maxNodeExclusive {
		s.grow(node)
	}

	cb := s.collidingBitSet
	segs := s.segmentedBitSets.segments
	shift := s.collisionShift
	cr := s.collisionRate

	segmentedIndex := node >> shift

	cbWord := segmentedIndex >> 6
	cbBit := segmentedIndex & 63
	cbMask := uint64(1) << cbBit

	if cbWord >= uint64(len(cb)) || segmentedIndex >= uint64(len(segs)) {
		s.grow(node)
		cb = s.collidingBitSet
		segs = s.segmentedBitSets.segments
		if cbWord >= uint64(len(cb)) || segmentedIndex >= uint64(len(segs)) {
			return
		}
	}

	if cb[cbWord]&cbMask == 0 {
		s.resetSegment(segmentedIndex)
		cb = s.collidingBitSet
		segs = s.segmentedBitSets.segments
		cb[cbWord] |= cbMask
	}

	seg := &segs[segmentedIndex]
	if seg.words == nil {
		return
	}

	off := node & (cr - 1)
	wordInSeg := off >> 6
	seg.words[wordInSeg] |= uint64(1) << (off & 63)
}

func (s *SparseSet) Visited(node uint64) bool {
	max := s.maxNodeExclusive
	if node >= max {
		return false
	}

	cb := s.collidingBitSet
	segs := s.segmentedBitSets.segments

	segmentedIndex := node >> s.collisionShift
	cbWord := segmentedIndex >> 6
	if cbWord >= uint64(len(cb)) {
		return false
	}

	if cb[cbWord]&(uint64(1)<<(segmentedIndex&63)) == 0 {
		return false
	}

	if segmentedIndex >= uint64(len(segs)) {
		return false
	}

	seg := &segs[segmentedIndex]

	off := node & (s.collisionRate - 1)
	return seg.words[off>>6]&(uint64(1)<<(off&63)) != 0
}
