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
	"errors"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/require"
)

func TestCompactHashTree(t *testing.T) {
	capacity := uint64(1)
	maxHeight := 2
	expectedHeight := 1

	ht := NewCompactHashTree(capacity, maxHeight)

	require.Equal(t, expectedHeight, ht.Height())

	someValue := []byte("somevalue")

	ht.AggregateLeafWith(0, someValue)

	var rootLevel [1]Digest

	n, err := ht.Level(0, NewBitset(ht.Height()).Set(0), rootLevel[:])
	require.NoError(t, err)
	require.Equal(t, 1, n)

	h := murmur3.New128()
	h.Write(someValue)
	expectedRootH1, expectedRootH2 := h.Sum128()
	expectedRoot := Digest{expectedRootH1, expectedRootH2}

	require.Equal(t, expectedRoot, rootLevel[0])
}

func TestCompactedHashTree(t *testing.T) {
	capacity := 1024
	maxHeight := 6
	expectedHeight := 6

	ht := NewCompactHashTree(uint64(capacity), maxHeight)

	require.Equal(t, expectedHeight, ht.Height())

	valuePrefix := "somevalue"

	for i := 0; i < capacity; i++ {
		ht.AggregateLeafWith(uint64(i), []byte(fmt.Sprintf("%s%d", valuePrefix, i)))
	}
}

func TestCompactBigHashTree(t *testing.T) {
	capacity := uint64(math.MaxUint64 / 128)
	maxHeight := 16
	expectedHeight := 16

	ht := NewCompactHashTree(capacity, maxHeight)

	require.Equal(t, expectedHeight, ht.Height())

	actualNumberOfElements := 1_000_000

	valuePrefix := "somevalue"

	for i := 0; i < actualNumberOfElements; i++ {
		ht.AggregateLeafWith(uint64(i), []byte(fmt.Sprintf("%s%d", valuePrefix, i)))
	}

	var rootLevel [1]Digest

	n, err := ht.Level(0, NewBitset(ht.Height()).Set(0), rootLevel[:])
	require.NoError(t, err)
	require.Equal(t, 1, n)
}

func TestCompactHashTreeComparisonHeight1(t *testing.T) {
	capacity := uint64(math.MaxUint64 / 128)
	maxHeight := 16

	ht1 := NewCompactHashTree(capacity, maxHeight)
	ht2 := NewCompactHashTree(capacity, maxHeight)

	diffReader, err := CompactHashTreeDiff(ht1, ht2)
	require.NoError(t, err)
	require.NotNil(t, diffReader)

	_, _, err = diffReader.Next()
	require.ErrorIs(t, err, ErrNoMoreDifferences)

	ht1.AggregateLeafWith(16, []byte("val1"))

	diffReader, err = CompactHashTreeDiff(ht1, ht2)
	require.NoError(t, err)

	diff1, diff2, err := diffReader.Next()
	require.NoError(t, err)
	require.EqualValues(t, 0, diff1)
	require.EqualValues(t, capacity/uint64(LeavesCount(ht1.Height())), diff2)

	_, _, err = diffReader.Next()
	require.ErrorIs(t, err, ErrNoMoreDifferences)

	ht2.AggregateLeafWith(0, []byte("val1"))

	diffReader, err = CompactHashTreeDiff(ht1, ht2)
	require.NoError(t, err)

	_, _, err = diffReader.Next()
	require.ErrorIs(t, err, ErrNoMoreDifferences)
}

func TestCompactHashTreeLeafMapping(t *testing.T) {
	capacity := uint64(100_000)
	maxHeight := 13

	ht := NewCompactHashTree(capacity, maxHeight)

	require.True(t, ht.Height() <= maxHeight)
	require.Equal(t, LeavesCount(ht.Height()), ht.leavesCount)
	require.True(t, uint64(ht.leavesCount) <= capacity)

	var prevMappedLeaf int

	for l := uint64(0); l < capacity; l++ {
		ml := ht.mapLeaf(l)
		require.True(t, uint64(ml) <= l)
		require.True(t, ml <= ht.leavesCount)

		if l > 0 {
			require.True(t, prevMappedLeaf <= ml)
		}

		uml := ht.unmapLeaf(ml)
		require.True(t, uml <= capacity)
		require.True(t, uml <= l)

		var groupSize uint64

		if l < ht.leavesCountInExtendedGroups {
			groupSize = ht.extendedGroupSize
		} else {
			groupSize = ht.groupSize
		}

		require.True(t, l <= uml+groupSize)

		for i := uml; i < l; i++ {
			// i should be mapped to the same leaf
			require.Equal(t, ml, ht.mapLeaf(i))
		}

		for i := l; i < uml+groupSize; i++ {
			// i should be mapped to the same leaf
			require.Equal(t, ml, ht.mapLeaf(i))
		}

		if uml+groupSize < capacity {
			// uml+groupSize should be assigned to the next leaf
			nextLeaf := ht.mapLeaf(uml + groupSize)
			require.Equal(t, ml+1, nextLeaf)
		}

		prevMappedLeaf = ml
	}
}

func TestCompactHashTreeComparisonIncrementalConciliation(t *testing.T) {
	capacity := 10_000
	maxHeight := 7

	ht1 := NewCompactHashTree(uint64(capacity), maxHeight)
	ht2 := NewCompactHashTree(uint64(capacity), maxHeight)

	diffReader, err := CompactHashTreeDiff(ht1, ht2)
	require.NoError(t, err)
	require.NotNil(t, diffReader)

	_, _, err = diffReader.Next()
	require.ErrorIs(t, err, ErrNoMoreDifferences) // no differences should be found

	for i := 0; i < capacity; i++ {
		ht1.AggregateLeafWith(uint64(i), []byte(fmt.Sprintf("val1_%d", i)))
		ht2.AggregateLeafWith(uint64(i), []byte(fmt.Sprintf("val2_%d", i)))
	}

	conciliated := make(map[int]struct{})
	concilliationOrder := rand.Perm(capacity)

	var prevDiffCount int
	var diffCount int

	for _, i := range concilliationOrder {
		_, ok := conciliated[i]
		require.False(t, ok)

		ht1.AggregateLeafWith(uint64(i), []byte(fmt.Sprintf("val2_%d", i)))
		ht2.AggregateLeafWith(uint64(i), []byte(fmt.Sprintf("val1_%d", i)))

		conciliated[i] = struct{}{}

		diffReader, err := CompactHashTreeDiff(ht1, ht2)
		require.NoError(t, err)

		diffCount = 0

		var prevDiff uint64

		for {
			diff0, diff1, err := diffReader.Next()
			if errors.Is(err, ErrNoMoreDifferences) {
				break
			}
			require.NoError(t, err)
			require.LessOrEqual(t, diff0, diff1)
			require.LessOrEqual(t, prevDiff, diff1)

			if prevDiff > 0 {
				require.Less(t, prevDiff, diff0)

				for d := prevDiff + 1; d < diff0; d++ {
					_, ok := conciliated[int(d)]
					require.True(t, ok)
				}
			}

			var diffFound bool

			for d := diff0; d <= diff1; d++ {
				_, ok := conciliated[int(d)]
				if !ok {
					diffCount++
					diffFound = true
				}
			}

			require.True(t, diffFound)

			prevDiff = diff1
		}

		// pending differences
		if prevDiffCount > 0 {
			require.Less(t, diffCount, prevDiffCount)
		}

		prevDiffCount = diffCount
	}

	require.Zero(t, diffCount)
}
