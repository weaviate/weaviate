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
	capacity uint64
	hashtree *HashTree

	// derived values from capacity and hashtree height
	// kept here just to avoid recalculation
	leavesCount                 int
	groupSize                   uint64
	extendedGroupSize           uint64
	extendedGroupsCount         int
	leavesCountInExtendedGroups uint64
}

func NewCompactHashTree(capacity uint64, maxHeight int) *CompactHashTree {
	if capacity < 1 {
		panic("illegal capacity")
	}

	if maxHeight < 1 {
		panic("illegal max height")
	}

	height := requiredHeight(capacity)

	if height > maxHeight {
		height = maxHeight
	}

	leavesCount := LeavesCount(height)
	groupSize := capacity / uint64(leavesCount)
	extendedGroupSize := groupSize + 1
	extendedGroupsCount := int(capacity % uint64(leavesCount))
	leavesCountInExtendedGroups := uint64(extendedGroupsCount) * extendedGroupSize

	return &CompactHashTree{
		capacity: capacity,
		hashtree: NewHashTree(height),

		leavesCount:                 leavesCount,
		groupSize:                   groupSize,
		extendedGroupSize:           extendedGroupSize,
		extendedGroupsCount:         extendedGroupsCount,
		leavesCountInExtendedGroups: leavesCountInExtendedGroups,
	}
}

func (ht *CompactHashTree) Height() int {
	return ht.hashtree.Height()
}

// AggregateLeafWith aggregates a new value into a shared leaf
// Each compacted leaf is shared by a number of consecutive leaves
func (ht *CompactHashTree) AggregateLeafWith(i uint64, val []byte) *CompactHashTree {
	if i >= ht.capacity {
		panic("out of capacity")
	}

	ht.hashtree.AggregateLeafWith(ht.mapLeaf(i), val)

	return ht
}

func (ht *CompactHashTree) mapLeaf(i uint64) int {
	if i < ht.leavesCountInExtendedGroups {
		return int(i / ht.extendedGroupSize)
	} else {
		return int((i - uint64(ht.extendedGroupsCount)) / ht.groupSize)
	}
}

func (ht *CompactHashTree) unmapLeaf(mappedLeaf int) uint64 {
	if mappedLeaf < ht.extendedGroupsCount {
		return uint64(mappedLeaf) * ht.extendedGroupSize
	}

	return uint64(mappedLeaf)*ht.groupSize + uint64(ht.extendedGroupsCount)
}

func (ht *CompactHashTree) Level(level int, discriminant *Bitset, digests []Digest) (n int, err error) {
	return ht.hashtree.Level(level, discriminant, digests)
}

func (ht *CompactHashTree) Reset() *CompactHashTree {
	ht.hashtree.Reset()
	return ht
}

func requiredHeight(n uint64) int {
	h := 0

	for ; n > 0; h++ {
		n = n >> 1
	}

	return h
}
