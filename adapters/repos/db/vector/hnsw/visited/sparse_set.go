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

package visited

import (
	"math"
	"math/bits"
	"slices"
)

type segment struct {
	words []uint64
}

// slabSegments is how many segments' worth of words each slab allocation
// holds: 64KiB at the production collisionRate of 4096.
const slabSegments = 128

type segmentedBitSet struct {
	segments    []segment
	slab        []uint64
	wordsPerSeg int
}

func newSegmentedBitSet(segCount, collisionRate int) *segmentedBitSet {
	return &segmentedBitSet{
		segments:    make([]segment, segCount),
		wordsPerSeg: (collisionRate + 63) >> 6,
	}
}

// allocSegment hands out one segment's worth of words, carved from a shared
// slab so that activating thousands of segments costs one allocation per
// slabSegments instead of one (plus interface boxing) each, and keeps the
// segments of a query contiguous in memory. The returned words are always
// zeroed: fresh slabs come zeroed from make, and previously handed-out
// segments are cleared by Reset before their cb bit can be observed unset
// again. The three-index slice caps each segment at wordsPerSeg so an
// accidental append cannot bleed into the neighboring segment.
func (b *segmentedBitSet) allocSegment() []uint64 {
	if len(b.slab) < b.wordsPerSeg {
		// Small sets never need more than len(segments) segments in total,
		// so cap the slab to avoid over-allocating for tiny indexes.
		b.slab = make([]uint64, min(slabSegments, len(b.segments))*b.wordsPerSeg)
	}
	words := b.slab[:b.wordsPerSeg:b.wordsPerSeg]
	b.slab = b.slab[b.wordsPerSeg:]
	return words
}

type SparseSet struct {
	segmentedBitSets *segmentedBitSet
	collidingBitSet  []uint64
	collisionRate    uint64
	collisionShift   uint8
	maxNodeExclusive uint64

	// Track what was activated in this query so Reset() can clear only those.
	touchedSegs []uint32
	touchedCB   []uint32
}

func NewSparseSet(size, collisionRate int) *SparseSet {
	cr := uint64(collisionRate)

	cb := make([]uint64, (size/int(cr))/64+1)
	// Align segments length to collidingBitSet domain (cb words * 64 bits).
	segCount := len(cb) * 64

	s := &SparseSet{
		collisionRate:    cr,
		collisionShift:   uint8(bits.TrailingZeros64(cr)),
		collidingBitSet:  cb,
		segmentedBitSets: newSegmentedBitSet(segCount, collisionRate),
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
	slc = slices.Grow(slc, extra)
	return slc[:newLen]
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
	slc = slices.Grow(slc, extra)
	return slc[:newLen]
}

func (s *SparseSet) grow(node uint64) {
	segmentedIndex := node >> s.collisionShift
	needCBWords := (segmentedIndex >> 6) + 1
	needSegs := needCBWords << 6 // aligned: cbWords*64

	s.collidingBitSet = growToUint64SliceLen(s.collidingBitSet, needCBWords)
	s.segmentedBitSets.segments = growToSegmentSliceLen(s.segmentedBitSets.segments, needSegs)

	s.maxNodeExclusive = uint64(len(s.collidingBitSet)) * 64 * s.collisionRate
}

// Reset clears only what was touched in the previous query.
// This keeps allocated segment words around once created (lazy, monotonic).
func (s *SparseSet) Reset() {
	for _, sid := range s.touchedSegs {
		seg := &s.segmentedBitSets.segments[uint64(sid)]
		if seg.words != nil {
			clear(seg.words)
		}
	}
	for _, w := range s.touchedCB {
		s.collidingBitSet[uint64(w)] = 0
	}
	s.touchedSegs = s.touchedSegs[:0]
	s.touchedCB = s.touchedCB[:0]
}

func (s *SparseSet) Visit(node uint64) {
	_ = s.CheckAndVisit(node) // ignores return
}

// CheckAndVisit returns true if node was already visited; otherwise it marks it visited and returns false.
// This fuses Visited()+Visit() into a single pass and removes duplicate cb/segment accesses.
func (s *SparseSet) CheckAndVisit(node uint64) bool {
	if node >= s.maxNodeExclusive {
		s.grow(node)
		if node >= s.maxNodeExclusive {
			return false
		}
	}

	cb := s.collidingBitSet
	segs := s.segmentedBitSets.segments
	shift := s.collisionShift
	cr := s.collisionRate

	segmentedIndex := node >> shift
	cbWord := segmentedIndex >> 6
	cbBit := segmentedIndex & 63
	cbMask := uint64(1) << cbBit

	if cbWord >= uint64(len(cb)) {
		// should be rare given maxNodeExclusive guard, but keep safe
		return false
	}

	segActive := cb[cbWord]&cbMask != 0
	seg := &segs[segmentedIndex]

	// Lazily allocate segment on first activation this query. No clear
	// needed: allocSegment guarantees zeroed words, and retained segments
	// were cleared by Reset.
	if !segActive {
		if seg.words == nil {
			seg.words = s.segmentedBitSets.allocSegment()
		}
		cb[cbWord] |= cbMask
		s.touchedSegs = append(s.touchedSegs, uint32(segmentedIndex))
		s.touchedCB = append(s.touchedCB, uint32(cbWord))
	}

	off := node & (cr - 1)
	wordInSeg := off >> 6
	bitMask := uint64(1) << (off & 63)

	// Check+set in one go
	prev := seg.words[wordInSeg]
	if prev&bitMask != 0 {
		return true
	}
	seg.words[wordInSeg] = prev | bitMask
	return false
}

func (s *SparseSet) Visited(node uint64) bool {
	if node >= s.maxNodeExclusive {
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

	seg := &segs[segmentedIndex]
	if seg.words == nil {
		return false
	}

	off := node & (s.collisionRate - 1)
	return seg.words[off>>6]&(uint64(1)<<(off&63)) != 0
}
