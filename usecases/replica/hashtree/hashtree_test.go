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
	"math/rand"
	"sync"
	"testing"

	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/require"
)

func TestIllegalHashTree(t *testing.T) {
	_, err := NewHashTree(-1)
	require.ErrorIs(t, err, ErrIllegalArguments)
}

func TestSmallestHashTree(t *testing.T) {
	height := 0

	ht, err := NewHashTree(height)
	require.NoError(t, err)

	require.Equal(t, height, ht.Height())

	someValue := []byte("somevalue")

	err = ht.AggregateLeafWith(0, someValue)
	require.NoError(t, err)

	var rootLevel [1]Digest

	n, err := ht.Level(0, NewBitset(NodesCount(height)).SetAll(), rootLevel[:])
	require.NoError(t, err)
	require.Equal(t, 1, n)

	h := murmur3.New128()
	h.Write(someValue)
	expectedRootH1, expectedRootH2 := h.Sum128()
	expectedRoot := Digest{expectedRootH1, expectedRootH2}

	require.Equal(t, expectedRoot, rootLevel[0])
}

func TestHashTree(t *testing.T) {
	height := 1

	ht, err := NewHashTree(height)
	require.NoError(t, err)

	require.Equal(t, height, ht.Height())

	leavesCount := LeavesCount(ht.Height())

	someValues := [][]byte{
		[]byte("somevalue1"),
		[]byte("somevalue2"),
	}

	err = ht.AggregateLeafWith(0, someValues[0])
	require.NoError(t, err)

	err = ht.AggregateLeafWith(1, someValues[1])
	require.NoError(t, err)

	discriminant := NewBitset(NodesCount(height)).SetAll()

	digests := make([]Digest, leavesCount)

	n, err := ht.Level(1, discriminant, digests)
	require.NoError(t, err)
	require.Equal(t, 2, n)

	for i := 0; i < leavesCount; i++ {
		h := murmur3.New128()
		h.Write(someValues[i])
		expectedDigestH1, expectedDigestH2 := h.Sum128()
		expectedDigest := Digest{expectedDigestH1, expectedDigestH2}
		require.Equal(t, expectedDigest, digests[i])
	}

	h := murmur3.New128()
	for i := 0; i < leavesCount; i++ {
		writeDigest(h, digests[i])
	}
	expectedRootH1, expectedRootH2 := h.Sum128()
	expectedRoot := Digest{expectedRootH1, expectedRootH2}

	discriminant.Reset().Set(0)

	n, err = ht.Level(0, discriminant, digests)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, expectedRoot, digests[0])
}

func TestBigHashTree(t *testing.T) {
	height := 10

	ht, err := NewHashTree(height)
	require.NoError(t, err)

	require.Equal(t, height, ht.Height())

	leavesCount := LeavesCount(ht.Height())
	valuePrefix := "somevalue"

	for i := 0; i < leavesCount; i++ {
		err := ht.AggregateLeafWith(uint64(i), []byte(fmt.Sprintf("%s%d", valuePrefix, i)))
		require.NoError(t, err)
	}

	var rootDigests [1]Digest

	n, err := ht.Level(0, NewBitset(NodesCount(height)).SetAll(), rootDigests[:])
	require.NoError(t, err)
	require.Equal(t, 1, n)
}

func TestHashTreeComparisonOneLeafAtATime(t *testing.T) {
	height := 0

	ht1, err := NewHashTree(height)
	require.NoError(t, err)

	ht2, err := NewHashTree(height)
	require.NoError(t, err)

	diff, err := ht1.Diff(ht2)
	require.NoError(t, err)
	require.NotNil(t, diff)

	rangeReader := ht1.NewRangeReader(diff)

	_, _, err = rangeReader.Next()
	require.ErrorIs(t, err, ErrNoMoreRanges) // no differences should be found

	for l := 0; l < LeavesCount(height); l++ {
		err = ht1.AggregateLeafWith(uint64(l), []byte("val1"))
		require.NoError(t, err)

		err = ht2.AggregateLeafWith(uint64(l), []byte("val2"))
		require.NoError(t, err)

		diff, err = ht1.Diff(ht2)
		require.NoError(t, err)

		rangeReader := ht1.NewRangeReader(diff)

		diff0, diff1, err := rangeReader.Next()
		require.NoError(t, err)
		require.EqualValues(t, l, diff0) // leaf should differ
		require.EqualValues(t, l, diff1) // leaf should differ

		_, _, err = rangeReader.Next()
		require.ErrorIs(t, err, ErrNoMoreRanges) // no more differences should be found

		err = ht1.AggregateLeafWith(uint64(l), []byte("val2"))
		require.NoError(t, err)

		err = ht2.AggregateLeafWith(uint64(l), []byte("val1"))
		require.NoError(t, err)

		diff, err = ht1.Diff(ht2)
		require.NoError(t, err)

		rangeReader = ht1.NewRangeReader(diff)

		_, _, err = rangeReader.Next()
		require.ErrorIs(t, err, ErrNoMoreRanges) // no differences should be found
	}
}

func TestHashTreeAllDiffInSingleIteration(t *testing.T) {
	height := 8

	ht1, err := NewHashTree(height)
	require.NoError(t, err)

	ht2, err := NewHashTree(height)
	require.NoError(t, err)

	diff, err := ht1.Diff(ht2)
	require.NoError(t, err)
	require.NotNil(t, diff)

	rangeReader := ht1.NewRangeReader(diff)

	_, _, err = rangeReader.Next()
	require.ErrorIs(t, err, ErrNoMoreRanges)

	leavesCount := LeavesCount(height)

	for l := range leavesCount {
		if l%2 == 0 {
			err = ht1.AggregateLeafWith(uint64(l), fmt.Appendf(nil, "val_%d", l))
			require.NoError(t, err)
		} else {
			err = ht2.AggregateLeafWith(uint64(l), fmt.Appendf(nil, "val_%d", l))
			require.NoError(t, err)
		}
	}

	diff, err = ht1.Diff(ht2)
	require.NoError(t, err)
	require.NotNil(t, diff)

	rangeReader = ht1.NewRangeReader(diff)

	i, j, err := rangeReader.Next()
	require.NoError(t, err)

	require.Zero(t, i, 0)
	require.EqualValues(t, leavesCount-1, j)

	_, _, err = rangeReader.Next()
	require.ErrorIs(t, err, ErrNoMoreRanges)
}

func TestHashTreeComparisonIncrementalConciliation(t *testing.T) {
	height := 7

	ht1, err := NewHashTree(height)
	require.NoError(t, err)

	ht2, err := NewHashTree(height)
	require.NoError(t, err)

	diff, err := ht1.Diff(ht2)
	require.NoError(t, err)
	require.NotNil(t, diff)

	rangeReader := ht1.NewRangeReader(diff)

	_, _, err = rangeReader.Next()
	require.ErrorIs(t, err, ErrNoMoreRanges) // no differences should be found

	leavesCount := LeavesCount(height)

	for l := 0; l < leavesCount; l++ {
		err = ht1.AggregateLeafWith(uint64(l), []byte("val1"))
		require.NoError(t, err)

		err = ht2.AggregateLeafWith(uint64(l), []byte("val2"))
		require.NoError(t, err)
	}

	conciliated := make(map[int]struct{})
	concilliationOrder := rand.Perm(leavesCount)

	for _, l := range concilliationOrder {
		_, ok := conciliated[l]
		require.False(t, ok)

		err = ht1.AggregateLeafWith(uint64(l), []byte("val2"))
		require.NoError(t, err)

		err = ht2.AggregateLeafWith(uint64(l), []byte("val1"))
		require.NoError(t, err)

		conciliated[l] = struct{}{}

		diff, err = ht1.Diff(ht2)
		require.NoError(t, err)

		rangeReader := ht1.NewRangeReader(diff)

		diffCount := 0

		prevDiff := uint64(0)

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

			for d := diff0; d <= diff1; d++ {
				_, ok := conciliated[int(d)]
				require.False(t, ok)

				diffCount++
			}

			prevDiff = diff1
		}

		// pending differences
		require.Equal(t, leavesCount-len(conciliated), diffCount)
	}
}

func TestHashTreeComparisonHeight2(t *testing.T) {
	height := 2

	ht1, err := NewHashTree(height)
	require.NoError(t, err)

	ht2, err := NewHashTree(height)
	require.NoError(t, err)

	diff, err := ht1.Diff(ht2)
	require.NoError(t, err)
	require.NotNil(t, diff)

	rangeReader := ht1.NewRangeReader(diff)

	_, _, err = rangeReader.Next()
	require.ErrorIs(t, err, ErrNoMoreRanges) // no differences should be found

	err = ht1.AggregateLeafWith(0, []byte("val11"))
	require.NoError(t, err)

	err = ht1.AggregateLeafWith(1, []byte("val12"))
	require.NoError(t, err)

	err = ht2.AggregateLeafWith(0, []byte("val21"))
	require.NoError(t, err)

	err = ht2.AggregateLeafWith(1, []byte("val22"))
	require.NoError(t, err)

	diff, err = ht1.Diff(ht2)
	require.NoError(t, err)

	rangeReader = ht1.NewRangeReader(diff)

	diff0, diff1, err := rangeReader.Next()
	require.NoError(t, err)
	require.EqualValues(t, 0, diff0) // leaf0 should differ
	require.EqualValues(t, 1, diff1) // leaf1 should differ

	_, _, err = rangeReader.Next()
	require.ErrorIs(t, err, ErrNoMoreRanges) // no more differences should be found

	err = ht1.AggregateLeafWith(0, []byte("val21"))
	require.NoError(t, err)

	err = ht2.AggregateLeafWith(0, []byte("val11"))
	require.NoError(t, err)

	diff, err = ht1.Diff(ht2)
	require.NoError(t, err)

	rangeReader = ht1.NewRangeReader(diff)

	diff0, diff1, err = rangeReader.Next()
	require.NoError(t, err)
	require.EqualValues(t, 1, diff0) // leaf1 should differ
	require.EqualValues(t, 1, diff1) // leaf1 should differ

	_, _, err = rangeReader.Next()
	require.ErrorIs(t, err, ErrNoMoreRanges) // no more differences should be found

	err = ht1.AggregateLeafWith(1, []byte("val22"))
	require.NoError(t, err)

	err = ht2.AggregateLeafWith(1, []byte("val12"))
	require.NoError(t, err)

	diff, err = ht1.Diff(ht2)
	require.NoError(t, err)

	rangeReader = ht1.NewRangeReader(diff)

	_, _, err = rangeReader.Next()
	require.ErrorIs(t, err, ErrNoMoreRanges) // no differences should be found
}

func TestHashTreeRandomAggregationOrder(t *testing.T) {
	maxHeight := 10

	for h := 1; h < maxHeight; h++ {
		var prevRoot Digest

		leavesCount := LeavesCount(h)

		insertionCount := 7 * leavesCount

		insertionAtLeaves := make([]int, insertionCount)
		for i := 0; i < len(insertionAtLeaves); i++ {
			insertionAtLeaves[i] = rand.Int() % leavesCount
		}

		for it := 0; it < 3; it++ {
			ht, err := NewHashTree(h)
			require.NoError(t, err)

			valuePrefix := "somevalue"

			leavesUpdates := make([]int, leavesCount)

			for _, l := range rand.Perm(insertionCount) {
				leaf := insertionAtLeaves[l]

				err := ht.AggregateLeafWith(uint64(leaf), []byte(fmt.Sprintf("%s%d", valuePrefix, leavesUpdates[leaf])))
				require.NoError(t, err)

				leavesUpdates[leaf]++
			}

			var rootDigests [1]Digest

			n, err := ht.Level(0, NewBitset(NodesCount(h)).SetAll(), rootDigests[:])
			require.NoError(t, err)
			require.Equal(t, 1, n)

			if it == 0 {
				prevRoot = rootDigests[0]
			}

			require.Equal(t, prevRoot, rootDigests[0])
		}

	}
}

func TestHashTreeConcurrentInsertions(t *testing.T) {
	height := 9

	leavesCount := LeavesCount(height)

	insertionAtLeaves := rand.Perm(leavesCount)
	var prevRoot Digest

	ht, err := NewHashTree(height)
	require.NoError(t, err)

	for it := 0; it < 3; it++ {
		valuePrefix := "somevalue"

		insertionCh := make(chan int)

		var workersDone sync.WaitGroup
		workersCount := 10
		workersDone.Add(workersCount)

		for w := 0; w < workersCount; w++ {
			go func() {
				for leaf := range insertionCh {
					err := ht.AggregateLeafWith(uint64(leaf), []byte(fmt.Sprintf("%s%d", valuePrefix, leaf)))
					require.NoError(t, err)
				}
				workersDone.Done()
			}()
		}

		for _, l := range insertionAtLeaves {
			insertionCh <- insertionAtLeaves[l]
		}

		close(insertionCh)
		workersDone.Wait()

		var rootDigests [1]Digest

		n, err := ht.Level(0, NewBitset(height).SetAll(), rootDigests[:])
		require.NoError(t, err)
		require.Equal(t, 1, n)

		if it == 0 {
			prevRoot = rootDigests[0]
		}

		require.Equal(t, prevRoot, rootDigests[0])

		ht.Reset()
	}
}
