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

	// Track what was activated in this query so Reset() can clear only those.
	touchedSegs []uint32
	touchedCB   []uint32
}

func NewSparseSet(size, collisionRate int) *SparseSet {
	cr := uint64(collisionRate)

	cb := make([]uint64, (size/int(cr))/64+1)
	segCount := len(cb) * 64

	s := &SparseSet{
		collisionRate:    cr,
		collisionShift:   uint8(bits.TrailingZeros64(cr)),
		collidingBitSet:  cb,
		segmentedBitSets: newSegmentedBitSet(size, collisionRate),
	}
	// Align segments length to collidingBitSet domain.
	if len(s.segmentedBitSets.segments) < segCount {
		s.segmentedBitSets.segments = make([]segment, segCount)
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
	_ = s.VisitIfNotVisited(node) // ignores return
}

// VisitIfNotVisited returns true if node was already visited; otherwise it marks it visited and returns false.
// This fuses Visited()+Visit() into a single pass and removes duplicate cb/segment accesses.
func (s *SparseSet) VisitIfNotVisited(node uint64) bool {
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

	// Lazily allocate segment on first activation this query.
	if !segActive {
		if seg.words == nil {
			seg.words = s.segmentedBitSets.pool.Get().([]uint64)
			clear(seg.words)
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
