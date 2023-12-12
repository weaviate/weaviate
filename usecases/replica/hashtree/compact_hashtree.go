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

type CompactHashTree struct {
	hashtree    *HashTree
	leavesCount int
}

func NewCompactHashTree(maxNumberOfElements int, maxHeight int) *CompactHashTree {
	if maxNumberOfElements < 1 {
		panic("illegal max number of elements")
	}

	if maxHeight < 1 {
		panic("illegal max height")
	}

	height := requiredHeight(maxNumberOfElements)

	if height > maxHeight {
		height = maxHeight
	}

	return &CompactHashTree{
		hashtree:    NewHashTree(height),
		leavesCount: LeavesCount(height),
	}
}

func (ht *CompactHashTree) Height() int {
	return ht.hashtree.Height()
}

func (ht *CompactHashTree) AggregateLeafWith(i int, val []byte) *CompactHashTree {
	ht.hashtree.AggregateLeafWith(i%ht.leavesCount, val)
	return ht
}

func (ht *CompactHashTree) Level(level int, discriminant *Bitset, digests []Digest) (n int, err error) {
	return ht.hashtree.Level(level, discriminant, digests)
}

func (ht *CompactHashTree) Reset() *CompactHashTree {
	ht.hashtree.Reset()
	return ht
}

func requiredHeight(n int) int {
	h := 0

	for ; n > 0; h++ {
		n = n >> 1
	}

	return h
}
