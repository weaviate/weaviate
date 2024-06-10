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
	"sort"
	"testing"

	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/require"
)

func TestMultiSegmentHashTree(t *testing.T) {
	segments := []Segment{NewSegment(0, 1_000)}
	maxHeight := 1

	expectedHeight := 1

	ht, err := NewMultiSegmentHashTree(segments, maxHeight)
	require.NoError(t, err)

	require.Equal(t, expectedHeight, ht.Height())

	someValue := []byte("somevalue")

	err = ht.AggregateLeafWith(0, someValue)
	require.NoError(t, err)

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

func TestMultiSegmentHashTree1(t *testing.T) {
	segments := []Segment{NewSegment(0, 100), NewSegment(300, 100), NewSegment(900, 100)}
	maxHeight := 4

	ht, err := NewMultiSegmentHashTree(segments, maxHeight)
	require.NoError(t, err)

	valuePrefix := "somevalue"

	for _, s := range segments {
		for i := 0; i < int(s.Size()); i++ {
			err = ht.AggregateLeafWith(s.Start()+uint64(i), []byte(fmt.Sprintf("%s%d", valuePrefix, i)))
			require.NoError(t, err)
		}
	}
}

func TestMultiSegmentBigHashTree(t *testing.T) {
	totalSegmentsCount := 128

	segmentSize := uint64(math.MaxUint64 / uint64(totalSegmentsCount))

	segments := make([]Segment, 30)

	for i, s := range rand.Perm(totalSegmentsCount)[:len(segments)] {
		segments[i] = NewSegment(uint64(s)*segmentSize, segmentSize)
	}

	sort.Slice(segments, func(i, j int) bool { return segments[i].Start() < segments[j].Start() })

	maxHeight := 16
	expectedHeight := 16

	ht, err := NewMultiSegmentHashTree(segments, maxHeight)
	require.NoError(t, err)

	require.Equal(t, expectedHeight, ht.Height())
	require.Equal(t, uint64(len(segments))*segmentSize, ht.hashtree.(*CompactHashTree).capacity)

	actualNumberOfElementsPerSegment := 100_000

	valuePrefix := "somevalue"

	for _, s := range segments {
		for i := 0; i < actualNumberOfElementsPerSegment; i++ {
			l := s.Start() + uint64(rand.Int()%int(s.Size()))
			err = ht.AggregateLeafWith(l, []byte(fmt.Sprintf("%s%d", valuePrefix, l)))
			require.NoError(t, err)
		}
	}

	var rootLevel [1]Digest

	n, err := ht.Level(0, NewBitset(ht.Height()).Set(0), rootLevel[:])
	require.NoError(t, err)
	require.Equal(t, 1, n)
}

func TestMultiSegmentHashTreeComparisonHeight1(t *testing.T) {
	segmentSize := uint64(math.MaxUint64 / 128)
	segments := []Segment{
		NewSegment(1_000, segmentSize),
		NewSegment(segmentSize+3_000, segmentSize),
		NewSegment(2*segmentSize+9_000, segmentSize),
	}
	maxHeight := 16

	ht1, err := NewMultiSegmentHashTree(segments, maxHeight)
	require.NoError(t, err)

	ht2, err := NewMultiSegmentHashTree(segments, maxHeight)
	require.NoError(t, err)

	diff, err := ht1.Diff(ht2)
	require.NoError(t, err)
	require.NotNil(t, diff)

	rangeReader := ht1.NewRangeReader(diff)

	_, _, err = rangeReader.Next()
	require.ErrorIs(t, err, ErrNoMoreRanges)

	err = ht1.AggregateLeafWith(1_000, []byte("val1"))
	require.NoError(t, err)

	diff, err = ht1.Diff(ht2)
	require.NoError(t, err)

	rangeReader = ht1.NewRangeReader(diff)

	diff0, diff1, err := rangeReader.Next()
	require.NoError(t, err)
	require.EqualValues(t, 1_000, diff0)
	require.Less(t, diff1, 1_000+segmentSize)

	_, _, err = rangeReader.Next()
	require.ErrorIs(t, err, ErrNoMoreRanges)

	err = ht2.AggregateLeafWith(1_000, []byte("val1"))
	require.NoError(t, err)

	diff, err = ht1.Diff(ht2)
	require.NoError(t, err)

	rangeReader = ht1.NewRangeReader(diff)

	_, _, err = rangeReader.Next()
	require.ErrorIs(t, err, ErrNoMoreRanges)
}

func TestMultiSegmentHashTreeComparisonIncrementalConciliation(t *testing.T) {
	leavesSpace := 1_000_000
	totalSegmentsCount := 128
	segmentSize := leavesSpace / totalSegmentsCount
	actualNumberOfElementsPerSegment := segmentSize / 100
	maxHeight := 11

	segments := make([]Segment, 9)

	for i, s := range rand.Perm(totalSegmentsCount)[:len(segments)] {
		segments[i] = NewSegment(uint64(s*segmentSize), uint64(segmentSize))
	}

	sort.Slice(segments, func(i, j int) bool { return segments[i].Start() < segments[j].Start() })

	ht1, err := NewMultiSegmentHashTree(segments, maxHeight)
	require.NoError(t, err)

	ht2, err := NewMultiSegmentHashTree(segments, maxHeight)
	require.NoError(t, err)

	diff, err := ht1.Diff(ht2)
	require.NoError(t, err)

	rangeReader := ht1.NewRangeReader(diff)

	_, _, err = rangeReader.Next()
	require.ErrorIs(t, err, ErrNoMoreRanges) // no differences should be found

	toConciliate := make(map[uint64]struct{}, actualNumberOfElementsPerSegment*len(segments))

	for _, s := range segments {
		for _, i := range rand.Perm(segmentSize)[:actualNumberOfElementsPerSegment] {
			l := s.Start() + uint64(i)

			err = ht1.AggregateLeafWith(l, []byte(fmt.Sprintf("val1_%d", l)))
			require.NoError(t, err)

			err = ht1.AggregateLeafWith(l, []byte(fmt.Sprintf("val2_%d", l)))
			require.NoError(t, err)

			toConciliate[l] = struct{}{}
		}
	}

	conciliated := make(map[uint64]struct{})

	var prevDiffCount int
	var diffCount int

	for l := range toConciliate {
		_, ok := conciliated[l]
		require.False(t, ok)

		err = ht1.AggregateLeafWith(l, []byte(fmt.Sprintf("val2_%d", l)))
		require.NoError(t, err)

		err = ht2.AggregateLeafWith(l, []byte(fmt.Sprintf("val1_%d", l)))
		require.NoError(t, err)

		conciliated[l] = struct{}{}

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

func TestNonUniformMultiSegmentHashTreeComparisonIncrementalConciliation(t *testing.T) {
	segments := make([]Segment, 7)
	maxSegmentSize := 1_000

	for i := range segments {
		var segmentStart uint64

		if i > 0 {
			segmentStart = segments[i-1].Start() + segments[i-1].Size()
		}

		segmentStart += rand.Uint64() % 10_000

		segmentSize := 1 + rand.Intn(maxSegmentSize)

		segments[i] = NewSegment(uint64(segmentStart), uint64(segmentSize))
	}

	maxHeight := 11

	ht1, err := NewMultiSegmentHashTree(segments, maxHeight)
	require.NoError(t, err)

	ht2, err := NewMultiSegmentHashTree(segments, maxHeight)
	require.NoError(t, err)

	diff, err := ht1.Diff(ht2)
	require.NoError(t, err)

	rangeReader := ht1.NewRangeReader(diff)

	_, _, err = rangeReader.Next()
	require.ErrorIs(t, err, ErrNoMoreRanges) // no differences should be found

	toConciliate := make(map[uint64]struct{})

	for _, s := range segments {
		segmentSize := int(s.Size())

		elementsInSegment := 1 + segmentSize/10

		for _, i := range rand.Perm(segmentSize)[:elementsInSegment] {
			l := s.Start() + uint64(i)

			err = ht1.AggregateLeafWith(l, []byte(fmt.Sprintf("val1_%d", l)))
			require.NoError(t, err)

			err = ht1.AggregateLeafWith(l, []byte(fmt.Sprintf("val2_%d", l)))
			require.NoError(t, err)

			toConciliate[l] = struct{}{}
		}
	}

	conciliated := make(map[uint64]struct{})

	var prevDiffCount int
	var diffCount int

	for l := range toConciliate {
		_, ok := conciliated[l]
		require.False(t, ok)

		err = ht1.AggregateLeafWith(l, []byte(fmt.Sprintf("val2_%d", l)))
		require.NoError(t, err)

		err = ht2.AggregateLeafWith(l, []byte(fmt.Sprintf("val1_%d", l)))
		require.NoError(t, err)

		conciliated[l] = struct{}{}

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
