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
	"math/rand"
	"sync"
	"testing"

	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/require"
)

func TestIllegalHashTree(t *testing.T) {
	require.Panics(t, func() { NewHashTree(-1) })
}

func TestSmallestHashTree(t *testing.T) {
	height := 1

	ht := NewHashTree(height)

	require.Equal(t, height, ht.Height())

	someValue := []byte("somevalue")

	ht.AggregateLeafWith(0, someValue)

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
	height := 2

	ht := NewHashTree(height)

	require.Equal(t, height, ht.Height())

	leavesCount := LeavesCount(ht.Height())

	someValues := [][]byte{
		[]byte("somevalue1"),
		[]byte("somevalue2"),
	}

	ht.
		AggregateLeafWith(0, someValues[0]).
		AggregateLeafWith(1, someValues[1])

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
	height := 15

	ht := NewHashTree(height)

	require.Equal(t, height, ht.Height())

	leavesCount := LeavesCount(ht.Height())
	valuePrefix := "somevalue"

	for i := 0; i < leavesCount; i++ {
		ht.AggregateLeafWith(i, []byte(fmt.Sprintf("%s%d", valuePrefix, i)))
	}

	var rootDigests [1]Digest

	n, err := ht.Level(0, NewBitset(NodesCount(height)).SetAll(), rootDigests[:])
	require.NoError(t, err)
	require.Equal(t, 1, n)
}

func TestHashTreeComparisonOneLeafAtATime(t *testing.T) {
	height := 13

	ht1 := NewHashTree(height)
	ht2 := NewHashTree(height)

	diffReader, err := HashTreeDiff(ht1, ht2)
	require.NoError(t, err)
	require.NotNil(t, diffReader)

	_, _, err = diffReader.Next()
	require.ErrorIs(t, err, ErrNoMoreDifferences) // no differences should be found

	for l := 0; l < LeavesCount(height); l++ {
		ht1.AggregateLeafWith(l, []byte("val1"))
		ht2.AggregateLeafWith(l, []byte("val2"))

		diffReader, err = HashTreeDiff(ht1, ht2)
		require.NoError(t, err)

		diff0, diff1, err := diffReader.Next()
		require.NoError(t, err)
		require.Equal(t, l, diff0) // leaf should differ
		require.Equal(t, l, diff1) // leaf should differ

		_, _, err = diffReader.Next()
		require.ErrorIs(t, err, ErrNoMoreDifferences) // no more differences should be found

		ht1.AggregateLeafWith(l, []byte("val2"))
		ht2.AggregateLeafWith(l, []byte("val1"))

		diffReader, err = HashTreeDiff(ht1, ht2)
		require.NoError(t, err)

		_, _, err = diffReader.Next()
		require.ErrorIs(t, err, ErrNoMoreDifferences) // no differences should be found
	}
}

func TestHashTreeComparisonIncrementalConciliation(t *testing.T) {
	height := 11

	ht1 := NewHashTree(height)
	ht2 := NewHashTree(height)

	diffReader, err := HashTreeDiff(ht1, ht2)
	require.NoError(t, err)
	require.NotNil(t, diffReader)

	_, _, err = diffReader.Next()
	require.ErrorIs(t, err, ErrNoMoreDifferences) // no differences should be found

	leavesCount := LeavesCount(height)

	for l := 0; l < leavesCount; l++ {
		ht1.AggregateLeafWith(l, []byte("val1"))
		ht2.AggregateLeafWith(l, []byte("val2"))
	}

	conciliated := make(map[int]struct{})
	concilliationOrder := rand.Perm(leavesCount)

	for _, l := range concilliationOrder {
		_, ok := conciliated[l]
		require.False(t, ok)

		ht1.AggregateLeafWith(l, []byte("val2"))
		ht2.AggregateLeafWith(l, []byte("val1"))

		conciliated[l] = struct{}{}

		diffReader, err = HashTreeDiff(ht1, ht2)
		require.NoError(t, err)

		diffCount := 0

		for {
			diff0, diff1, err := diffReader.Next()
			if errors.Is(err, ErrNoMoreDifferences) {
				break
			}
			require.NoError(t, err)
			require.LessOrEqual(t, diff0, diff1)

			for d := diff0; d <= diff1; d++ {
				_, ok := conciliated[d]
				require.False(t, ok)

				diffCount++
			}
		}

		// pending differences
		require.Equal(t, leavesCount-len(conciliated), diffCount)
	}
}

func TestHashTreeComparisonHeight2(t *testing.T) {
	height := 2

	ht1 := NewHashTree(height)
	ht2 := NewHashTree(height)

	diffReader, err := HashTreeDiff(ht1, ht2)
	require.NoError(t, err)
	require.NotNil(t, diffReader)

	_, _, err = diffReader.Next()
	require.ErrorIs(t, err, ErrNoMoreDifferences) // no differences should be found

	ht1.
		AggregateLeafWith(0, []byte("val11")).
		AggregateLeafWith(1, []byte("val12"))
	ht2.
		AggregateLeafWith(0, []byte("val21")).
		AggregateLeafWith(1, []byte("val22"))

	diffReader, err = HashTreeDiff(ht1, ht2)
	require.NoError(t, err)

	diff0, diff1, err := diffReader.Next()
	require.NoError(t, err)
	require.Equal(t, 0, diff0) // leaf0 should differ
	require.Equal(t, 1, diff1) // leaf1 should differ

	_, _, err = diffReader.Next()
	require.ErrorIs(t, err, ErrNoMoreDifferences) // no more differences should be found

	ht1.AggregateLeafWith(0, []byte("val21"))
	ht2.AggregateLeafWith(0, []byte("val11"))

	diffReader, err = HashTreeDiff(ht1, ht2)
	require.NoError(t, err)

	diff0, diff1, err = diffReader.Next()
	require.NoError(t, err)
	require.Equal(t, 1, diff0) // leaf1 should differ
	require.Equal(t, 1, diff1) // leaf1 should differ

	_, _, err = diffReader.Next()
	require.ErrorIs(t, err, ErrNoMoreDifferences) // no more differences should be found

	ht1.AggregateLeafWith(1, []byte("val22"))
	ht2.AggregateLeafWith(1, []byte("val12"))

	diffReader, err = HashTreeDiff(ht1, ht2)
	require.NoError(t, err)

	_, _, err = diffReader.Next()
	require.ErrorIs(t, err, ErrNoMoreDifferences) // no differences should be found
}

func TestHashTreeRandomAggregationOrder(t *testing.T) {
	maxHeight := 16

	for h := 1; h < maxHeight; h++ {
		var prevRoot Digest

		leavesCount := LeavesCount(h)

		insertionCount := 7 * leavesCount

		insertionAtLeaves := make([]int, insertionCount)
		for i := 0; i < len(insertionAtLeaves); i++ {
			insertionAtLeaves[i] = rand.Int() % leavesCount
		}

		for it := 0; it < 3; it++ {
			ht := NewHashTree(h)

			valuePrefix := "somevalue"

			leavesUpdates := make([]int, leavesCount)

			for _, l := range rand.Perm(insertionCount) {
				leaf := insertionAtLeaves[l]

				ht.AggregateLeafWith(leaf, []byte(fmt.Sprintf("%s%d", valuePrefix, leavesUpdates[leaf])))

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
	height := 16

	leavesCount := LeavesCount(height)

	insertionAtLeaves := rand.Perm(leavesCount)
	var prevRoot Digest

	ht := NewHashTree(height)

	for it := 0; it < 3; it++ {
		valuePrefix := "somevalue"

		insertionCh := make(chan int)

		var workersDone sync.WaitGroup
		workersCount := 10
		workersDone.Add(workersCount)

		for w := 0; w < workersCount; w++ {
			go func() {
				for leaf := range insertionCh {
					ht.AggregateLeafWith(leaf, []byte(fmt.Sprintf("%s%d", valuePrefix, leaf)))
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
