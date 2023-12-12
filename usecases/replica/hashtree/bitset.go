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
	if bset.size < i {
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
