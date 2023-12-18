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

func TestSegmentedHashTree(t *testing.T) {
	segmentSize := uint64(1_000)
	segments := []uint64{0}
	maxHeight := 1

	expectedHeight := 1

	ht := NewSegmentedHashTree(segmentSize, segments, maxHeight)

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

func TestSegmentHashTree(t *testing.T) {
	segmentSize := uint64(100)
	segments := []uint64{100, 300, 900}
	maxHeight := 4

	ht := NewSegmentedHashTree(segmentSize, segments, maxHeight)

	valuePrefix := "somevalue"

	for _, s := range segments {
		for i := 0; i < int(segmentSize); i++ {
			ht.AggregateLeafWith(s+uint64(i), []byte(fmt.Sprintf("%s%d", valuePrefix, i)))
		}
	}
}

func TestSegmentBigHashTree(t *testing.T) {
	segmentSize := uint64(math.MaxUint64 / 128)
	segments := []uint64{1_000, segmentSize + 3_000, 2*segmentSize + 9_000}
	maxHeight := 16
	expectedHeight := 16

	ht := NewSegmentedHashTree(segmentSize, segments, maxHeight)

	require.Equal(t, expectedHeight, ht.Height())

	actualNumberOfElementsPerSegment := 1_000_000

	valuePrefix := "somevalue"

	for _, s := range segments {
		for i := 0; i < actualNumberOfElementsPerSegment; i++ {
			ht.AggregateLeafWith(s+uint64(i), []byte(fmt.Sprintf("%s%d", valuePrefix, i)))
		}
	}

	var rootLevel [1]Digest

	n, err := ht.Level(0, NewBitset(ht.Height()).Set(0), rootLevel[:])
	require.NoError(t, err)
	require.Equal(t, 1, n)
}

func TestSegmentHashTreeComparisonHeight1(t *testing.T) {
	segmentSize := uint64(math.MaxUint64 / 128)
	segments := []uint64{1_000, segmentSize + 3_000, 2*segmentSize + 9_000}
	maxHeight := 16

	ht1 := NewSegmentedHashTree(segmentSize, segments, maxHeight)
	ht2 := NewSegmentedHashTree(segmentSize, segments, maxHeight)

	diffReader, err := SegmentedHashTreeDiff(ht1, ht2)
	require.NoError(t, err)
	require.NotNil(t, diffReader)

	_, _, err = diffReader.Next()
	require.ErrorIs(t, err, ErrNoMoreDifferences)

	ht1.AggregateLeafWith(1_000, []byte("val1"))

	diffReader, err = SegmentedHashTreeDiff(ht1, ht2)
	require.NoError(t, err)

	diff0, diff1, err := diffReader.Next()
	require.NoError(t, err)
	require.EqualValues(t, 1_000, diff0)
	require.Less(t, diff1, 1_000+segmentSize)

	_, _, err = diffReader.Next()
	require.ErrorIs(t, err, ErrNoMoreDifferences)

	ht2.AggregateLeafWith(1_000, []byte("val1"))

	diffReader, err = SegmentedHashTreeDiff(ht1, ht2)
	require.NoError(t, err)

	_, _, err = diffReader.Next()
	require.ErrorIs(t, err, ErrNoMoreDifferences)
}
