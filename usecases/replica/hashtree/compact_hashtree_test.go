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
	expectedHeight := 0

	ht, err := NewCompactHashTree(capacity, maxHeight)
	require.NoError(t, err)

	require.Equal(t, expectedHeight, ht.Height())

	someValue := []byte("somevalue")

	err = ht.AggregateLeafWith(0, someValue)
	require.NoError(t, err)

	var rootLevel [1]Digest

	n, err := ht.Level(0, NewBitset(NodesCount(ht.Height())).Set(0), rootLevel[:])
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

	ht, err := NewCompactHashTree(uint64(capacity), maxHeight)
	require.NoError(t, err)

	require.Equal(t, expectedHeight, ht.Height())

	valuePrefix := "somevalue"

	for i := 0; i < capacity; i++ {
		err = ht.AggregateLeafWith(uint64(i), []byte(fmt.Sprintf("%s%d", valuePrefix, i)))
		require.NoError(t, err)
	}
}

func TestCompactBigHashTree(t *testing.T) {
	capacity := uint64(math.MaxUint64 / 128)
	maxHeight := 10
	expectedHeight := 10

	ht, err := NewCompactHashTree(capacity, maxHeight)
	require.NoError(t, err)

	require.Equal(t, expectedHeight, ht.Height())

	actualNumberOfElements := 10_000

	valuePrefix := "somevalue"

	for i := 0; i < actualNumberOfElements; i++ {
		err = ht.AggregateLeafWith(uint64(i), []byte(fmt.Sprintf("%s%d", valuePrefix, i)))
		require.NoError(t, err)
	}

	var rootLevel [1]Digest

	n, err := ht.Level(0, NewBitset(ht.Height()).Set(0), rootLevel[:])
	require.NoError(t, err)
	require.Equal(t, 1, n)
}

func TestCompactHashTreeComparisonHeight1(t *testing.T) {
	capacity := uint64(math.MaxUint64 / 128)
	maxHeight := 12

	ht1, err := NewCompactHashTree(capacity, maxHeight)
	require.NoError(t, err)

	ht2, err := NewCompactHashTree(capacity, maxHeight)
	require.NoError(t, err)

	diff, err := ht1.Diff(ht2)
	require.NoError(t, err)
	require.NotNil(t, diff)

	rangeReader := ht1.NewRangeReader(diff)

	_, _, err = rangeReader.Next()
	require.ErrorIs(t, err, ErrNoMoreRanges)

	err = ht1.AggregateLeafWith(16, []byte("val1"))
	require.NoError(t, err)

	diff, err = ht1.Diff(ht2)
	require.NoError(t, err)

	rangeReader = ht1.NewRangeReader(diff)

	diff1, diff2, err := rangeReader.Next()
	require.NoError(t, err)
	require.EqualValues(t, 0, diff1)
	require.EqualValues(t, capacity/uint64(LeavesCount(ht1.Height())), diff2)

	_, _, err = rangeReader.Next()
	require.ErrorIs(t, err, ErrNoMoreRanges)

	err = ht2.AggregateLeafWith(0, []byte("val1"))
	require.NoError(t, err)

	diff, err = ht1.Diff(ht2)
	require.NoError(t, err)

	rangeReader = ht1.NewRangeReader(diff)

	_, _, err = rangeReader.Next()
	require.ErrorIs(t, err, ErrNoMoreRanges)
}

func TestCompactHashTreeLeafMapping(t *testing.T) {
	capacity := uint64(10_000)
	maxHeight := 11

	ht, err := NewCompactHashTree(capacity, maxHeight)
	require.NoError(t, err)

	require.True(t, ht.Height() <= maxHeight)
	require.Equal(t, LeavesCount(ht.Height()), ht.leavesCount)
	require.True(t, uint64(ht.leavesCount) <= capacity)

	var prevMappedLeaf uint64

	for l := uint64(0); l < capacity; l++ {
		ml := ht.mapLeaf(l)
		require.True(t, uint64(ml) <= l)
		require.True(t, ml <= uint64(ht.leavesCount))

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
	capacity := 1_000
	maxHeight := 7

	ht1, err := NewCompactHashTree(uint64(capacity), maxHeight)
	require.NoError(t, err)

	ht2, err := NewCompactHashTree(uint64(capacity), maxHeight)
	require.NoError(t, err)

	diff, err := ht1.Diff(ht2)
	require.NoError(t, err)
	require.NotNil(t, diff)

	rangeReader := ht1.NewRangeReader(diff)

	_, _, err = rangeReader.Next()
	require.ErrorIs(t, err, ErrNoMoreRanges) // no differences should be found

	for i := 0; i < capacity; i++ {
		err = ht1.AggregateLeafWith(uint64(i), []byte(fmt.Sprintf("val1_%d", i)))
		require.NoError(t, err)

		err = ht2.AggregateLeafWith(uint64(i), []byte(fmt.Sprintf("val2_%d", i)))
		require.NoError(t, err)
	}

	conciliated := make(map[int]struct{})
	concilliationOrder := rand.Perm(capacity)

	var prevDiffCount int
	var diffCount int

	for _, i := range concilliationOrder {
		_, ok := conciliated[i]
		require.False(t, ok)

		err = ht1.AggregateLeafWith(uint64(i), []byte(fmt.Sprintf("val2_%d", i)))
		require.NoError(t, err)

		err = ht2.AggregateLeafWith(uint64(i), []byte(fmt.Sprintf("val1_%d", i)))
		require.NoError(t, err)

		conciliated[i] = struct{}{}

		diff, err := ht1.Diff(ht2)
		require.NoError(t, err)

		rangeReader := ht1.NewRangeReader(diff)

		diffCount = 0

		var prevDiff uint64

		for {
			diff0, diff1, err := rangeReader.Next()
			if errors.Is(err, ErrNoMoreRanges) {
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
