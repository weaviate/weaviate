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
	"fmt"
	"math"
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
	capacity := uint64(1_000_000)
	maxHeight := 16

	ht := NewCompactHashTree(capacity, maxHeight)

	require.LessOrEqual(t, ht.Height(), maxHeight)
	require.Equal(t, LeavesCount(ht.Height()), ht.leavesCount)
	require.LessOrEqual(t, uint64(ht.leavesCount), capacity)

	for l := uint64(0); l < capacity; l++ {
		ml := ht.mapLeaf(l)
		require.LessOrEqual(t, ml, ht.leavesCount)
		require.LessOrEqual(t, uint64(ml), l)

		uml := ht.unmapLeaf(ml)
		require.LessOrEqual(t, uml, capacity)
		require.LessOrEqual(t, uml, l)

		var groupSize uint64

		if l < ht.leavesCountInExtendedGroups {
			groupSize = ht.extendedGroupSize
		} else {
			groupSize = ht.groupSize
		}

		require.LessOrEqual(t, l, uml+groupSize)
	}
}
