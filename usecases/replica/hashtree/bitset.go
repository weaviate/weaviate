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

package hashtree

import (
	"encoding/binary"
	"fmt"
	"math/bits"
)

type Bitset struct {
	size     int
	bits     []int64
	setCount int
}

func NewBitset(size int) *Bitset {
	return &Bitset{
		size: size,
		bits: make([]int64, (size+63)/64),
	}
}

func (bset *Bitset) Size() int {
	return bset.size
}

func (bset *Bitset) Set(i int) *Bitset {
	if bset.IsSet(i) {
		return bset
	}

	bset.bits[i/64] |= 1 << (i % 64)
	bset.setCount++

	return bset
}

func (bset *Bitset) Unset(i int) *Bitset {
	if !bset.IsSet(i) {
		return bset
	}

	bset.bits[i/64] &= ^(1 << (i % 64))
	bset.setCount--

	return bset
}

func (bset *Bitset) IsSet(i int) bool {
	if bset.size <= i {
		panic("index out of range")
	}

	return bset.bits[i/64]&(1<<(i%64)) != 0
}

func (bset *Bitset) AllSet() bool {
	return bset.SetCount() == bset.size
}

func (bset *Bitset) SetCount() int {
	return bset.setCount
}

func (bset *Bitset) SetAll() *Bitset {
	for i := 0; i < len(bset.bits); i++ {
		bset.bits[i] = -1
	}

	bset.setCount = bset.size

	return bset
}

func (bset *Bitset) Reset() *Bitset {
	for i := 0; i < len(bset.bits); i++ {
		bset.bits[i] = 0
	}

	bset.setCount = 0

	return bset
}

func (bset *Bitset) Marshal() ([]byte, error) {
	b := make([]byte, 8+8*len(bset.bits))

	binary.BigEndian.PutUint32(b, uint32(bset.size))
	binary.BigEndian.PutUint32(b[4:], uint32(bset.setCount))

	off := 8
	for _, n := range bset.bits {
		binary.BigEndian.PutUint64(b[off:], uint64(n))
		off += 8
	}

	return b, nil
}

func (bset *Bitset) Unmarshal(b []byte) error {
	if len(b) < 8 {
		return fmt.Errorf("invalid bset serialization")
	}

	bset.size = int(binary.BigEndian.Uint32(b))
	bset.setCount = int(binary.BigEndian.Uint32(b[4:]))

	n := (bset.size + 63) / 64

	if len(b) != 8+n*8 {
		return fmt.Errorf("invalid bset serialization")
	}

	bset.bits = make([]int64, n)

	off := 8
	for i := 0; i < n; i++ {
		bset.bits[i] = int64(binary.BigEndian.Uint64(b[off:]))
		off += 8
	}

	return nil
}

// ExtractSlice returns a new Bitset of size count containing the bits at
// positions [offset, offset+count) from this bitset. Used to produce a
// level-local discriminant (size = nodesAtLevel(level)) from the full-tree
// discriminant (size = NodesCount(height)) before sending it over the wire,
// reducing per-call payload by a factor of (height+1) across a full traversal.
// It panics if offset or count is negative, or if offset+count exceeds the bitset size.
func (bset *Bitset) ExtractSlice(offset, count int) *Bitset {
	if offset < 0 || count < 0 || offset+count > bset.size {
		panic(fmt.Sprintf("ExtractSlice out of range [%d, %d) for bitset of size %d", offset, offset+count, bset.size))
	}
	result := NewBitset(count)
	if count == 0 {
		return result
	}

	srcWordStart := offset / 64
	srcBitOff := uint(offset % 64)
	dstWords := len(result.bits)

	if srcBitOff == 0 {
		copy(result.bits, bset.bits[srcWordStart:srcWordStart+dstWords])
	} else {
		for i := 0; i < dstWords; i++ {
			result.bits[i] = int64(uint64(bset.bits[srcWordStart+i]) >> srcBitOff)
			if srcWordStart+i+1 < len(bset.bits) {
				result.bits[i] |= bset.bits[srcWordStart+i+1] << (64 - srcBitOff)
			}
		}
	}

	// Clear any bits beyond count in the last word.
	if tail := uint(count % 64); tail != 0 {
		result.bits[dstWords-1] &= int64((uint64(1) << tail) - 1)
	}

	for _, w := range result.bits {
		result.setCount += bits.OnesCount64(uint64(w))
	}

	return result
}

func (bset *Bitset) Clone() *Bitset {
	clone := &Bitset{
		size:     bset.size,
		bits:     make([]int64, len(bset.bits)),
		setCount: bset.setCount,
	}

	copy(clone.bits, bset.bits)

	return clone
}
