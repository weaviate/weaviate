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
	"sort"
	"testing"

	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/require"
)

func TestSingleSegmentedHashTree(t *testing.T) {
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

func TestMultiSegmentedHashTree(t *testing.T) {
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

func TestSegmentedBigHashTree(t *testing.T) {
	totalSegmentsCount := 128

	segmentSize := uint64(math.MaxUint64 / uint64(totalSegmentsCount))

	segments := make([]uint64, 30)

	for i, s := range rand.Perm(totalSegmentsCount)[:len(segments)] {
		segments[i] = uint64(s) * segmentSize
	}

	sort.Slice(segments, func(i, j int) bool { return segments[i] < segments[j] })

	maxHeight := 16
	expectedHeight := 16

	ht := NewSegmentedHashTree(segmentSize, segments, maxHeight)

	require.Equal(t, expectedHeight, ht.Height())
	require.Equal(t, uint64(len(segments))*segmentSize, ht.hashtree.(*CompactHashTree).capacity)

	actualNumberOfElementsPerSegment := 1_000_000

	valuePrefix := "somevalue"

	for _, s := range segments {
		for i := 0; i < actualNumberOfElementsPerSegment; i++ {
			l := s + uint64(rand.Int()%int(segmentSize))
			ht.AggregateLeafWith(l, []byte(fmt.Sprintf("%s%d", valuePrefix, l)))
		}
	}

	var rootLevel [1]Digest

	n, err := ht.Level(0, NewBitset(ht.Height()).Set(0), rootLevel[:])
	require.NoError(t, err)
	require.Equal(t, 1, n)
}

func TestSegmentedHashTreeComparisonHeight1(t *testing.T) {
	segmentSize := uint64(math.MaxUint64 / 128)
	segments := []uint64{1_000, segmentSize + 3_000, 2*segmentSize + 9_000}
	maxHeight := 16

	ht1 := NewSegmentedHashTree(segmentSize, segments, maxHeight)
	ht2 := NewSegmentedHashTree(segmentSize, segments, maxHeight)

	diff, err := ht1.Diff(ht2)
	require.NoError(t, err)

	diffReader := ht1.NewDiffReader(diff)

	_, _, err = diffReader.Next()
	require.ErrorIs(t, err, ErrNoMoreDifferences)

	ht1.AggregateLeafWith(1_000, []byte("val1"))

	diff, err = ht1.Diff(ht2)
	require.NoError(t, err)

	diffReader = ht1.NewDiffReader(diff)

	diff0, diff1, err := diffReader.Next()
	require.NoError(t, err)
	require.EqualValues(t, 1_000, diff0)
	require.Less(t, diff1, 1_000+segmentSize)

	_, _, err = diffReader.Next()
	require.ErrorIs(t, err, ErrNoMoreDifferences)

	ht2.AggregateLeafWith(1_000, []byte("val1"))

	diff, err = ht1.Diff(ht2)
	require.NoError(t, err)

	diffReader = ht1.NewDiffReader(diff)

	_, _, err = diffReader.Next()
	require.ErrorIs(t, err, ErrNoMoreDifferences)
}

func TestSegmentedHashTreeComparisonIncrementalConciliation(t *testing.T) {
	leavesSpace := 1_000_000
	totalSegmentsCount := 128
	segmentSize := leavesSpace / totalSegmentsCount
	actualNumberOfElementsPerSegment := segmentSize / 100
	maxHeight := 11

	segments := make([]uint64, 9)

	for i, s := range rand.Perm(totalSegmentsCount)[:len(segments)] {
		segments[i] = uint64(s * segmentSize)
	}

	sort.Slice(segments, func(i, j int) bool { return segments[i] < segments[j] })

	ht1 := NewSegmentedHashTree(uint64(segmentSize), segments, maxHeight)
	ht2 := NewSegmentedHashTree(uint64(segmentSize), segments, maxHeight)

	diff, err := ht1.Diff(ht2)
	require.NoError(t, err)

	diffReader := ht1.NewDiffReader(diff)

	_, _, err = diffReader.Next()
	require.ErrorIs(t, err, ErrNoMoreDifferences) // no differences should be found

	toConciliate := make(map[uint64]struct{}, actualNumberOfElementsPerSegment*len(segments))

	for _, s := range segments {
		for _, i := range rand.Perm(segmentSize)[:actualNumberOfElementsPerSegment] {
			l := s + uint64(i)

			ht1.AggregateLeafWith(l, []byte(fmt.Sprintf("val1_%d", l)))
			ht1.AggregateLeafWith(l, []byte(fmt.Sprintf("val2_%d", l)))

			toConciliate[l] = struct{}{}
		}
	}

	conciliated := make(map[uint64]struct{})

	var prevDiffCount int
	var diffCount int

	for l := range toConciliate {
		_, ok := conciliated[l]
		require.False(t, ok)

		ht1.AggregateLeafWith(l, []byte(fmt.Sprintf("val2_%d", l)))
		ht2.AggregateLeafWith(l, []byte(fmt.Sprintf("val1_%d", l)))

		conciliated[l] = struct{}{}

		diff, err := ht1.Diff(ht2)
		require.NoError(t, err)

		diffReader := ht1.NewDiffReader(diff)

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
					_, ok := toConciliate[d]
					if !ok {
						continue
					}

					_, ok = conciliated[d]
					require.True(t, ok)
				}
			}

			var diffFound bool

			for d := diff0; d <= diff1; d++ {
				_, ok := toConciliate[d]
				if !ok {
					continue
				}

				_, ok = conciliated[d]
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
	require.EqualValues(t, toConciliate, conciliated)
}
