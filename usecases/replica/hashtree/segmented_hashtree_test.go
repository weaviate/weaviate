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
	segments := []uint64{0}
	segmentSize := uint64(1_000)
	maxHeight := 1

	expectedHeight := 1

	ht := NewSegmentedHashTree(segments, segmentSize, maxHeight)

	require.Equal(t, expectedHeight, ht.Height())

	someValue := []byte("somevalue")

	ht.AggregateLeafWith(0, someValue)

	var rootLevel [1]Digest

	n, err := ht.Level(0, NewBitset(1).SetAll(), rootLevel[:])
	require.NoError(t, err)
	require.Equal(t, 1, n)

	h := murmur3.New128()
	h.Write(someValue)
	expectedRootH1, expectedRootH2 := h.Sum128()
	expectedRoot := Digest{expectedRootH1, expectedRootH2}

	require.Equal(t, expectedRoot, rootLevel[0])
}

func TestSegmentHashTree(t *testing.T) {
	segments := []uint64{100, 300, 900}
	segmentSize := uint64(100)
	maxHeight := 4

	ht := NewSegmentedHashTree(segments, segmentSize, maxHeight)

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

	ht := NewSegmentedHashTree(segments, segmentSize, maxHeight)

	require.Equal(t, expectedHeight, ht.Height())

	actualNumberOfElementsPerSegment := 1_000_000

	valuePrefix := "somevalue"

	for _, s := range segments {
		for i := 0; i < actualNumberOfElementsPerSegment; i++ {
			ht.AggregateLeafWith(s+uint64(i), []byte(fmt.Sprintf("%s%d", valuePrefix, i)))
		}
	}

	var rootLevel [1]Digest

	n, err := ht.Level(0, NewBitset(1).SetAll(), rootLevel[:])
	require.NoError(t, err)
	require.Equal(t, 1, n)
}

func TestSegmentHashTreeComparisonHeight1(t *testing.T) {
	capacity := uint64(math.MaxUint64 / 128)
	maxHeight := 16

	ht1 := NewCompactHashTree(capacity, maxHeight)
	ht2 := NewCompactHashTree(capacity, maxHeight)

	diff, err := CompactHashTreeDiff(ht1, ht2) // diff is set to one for all differing paths
	require.NoError(t, err)
	require.Zero(t, diff.SetCount())

	ht1.AggregateLeafWith(0, []byte("val1"))

	diff, err = CompactHashTreeDiff(ht1, ht2)
	require.NoError(t, err)
	require.True(t, diff.IsSet(0)) // root should differ
	require.Equal(t, ht1.Height(), diff.SetCount())

	ht2.AggregateLeafWith(0, []byte("val1"))

	diff, err = CompactHashTreeDiff(ht1, ht2)
	require.NoError(t, err)
	require.Zero(t, diff.SetCount())
}
