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
	"testing"

	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/require"
)

func TestSmallestHashTree(t *testing.T) {
	height := 1

	ht := NewHashTree(height)

	require.Equal(t, height, ht.Height())

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

	discriminant := NewBitset(leavesCount).SetAll()

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

	n, err := ht.Level(0, NewBitset(1).SetAll(), rootDigests[:])
	require.NoError(t, err)
	require.Equal(t, 1, n)
}

func TestHashTreeComparisonHeight1(t *testing.T) {
	height := 1

	ht1 := NewHashTree(height)
	ht2 := NewHashTree(height)

	diffReader, err := HashTreeDiff(ht1, ht2) // diff is set to one for all differing paths
	require.NoError(t, err)
	require.NotNil(t, diffReader)

	_, err = diffReader.Next()
	require.ErrorIs(t, err, ErrNoMoreDifferences) // no differences should be found

	ht1.AggregateLeafWith(0, []byte("val1"))
	ht2.AggregateLeafWith(0, []byte("val2"))

	diffReader, err = HashTreeDiff(ht1, ht2)
	require.NoError(t, err)

	diff, err := diffReader.Next()
	require.NoError(t, err)
	require.Equal(t, 0, diff) // leaf should differ

	_, err = diffReader.Next()
	require.ErrorIs(t, err, ErrNoMoreDifferences) // no more differences should be found

	ht1.AggregateLeafWith(0, []byte("val2"))
	ht2.AggregateLeafWith(0, []byte("val1"))

	diffReader, err = HashTreeDiff(ht1, ht2)
	require.NoError(t, err)

	_, err = diffReader.Next()
	require.ErrorIs(t, err, ErrNoMoreDifferences) // no differences should be found
}

func TestHashTreeComparisonHeight2(t *testing.T) {
	height := 2

	ht1 := NewHashTree(height)
	ht2 := NewHashTree(height)

	diffReader, err := HashTreeDiff(ht1, ht2) // diff is set to one for all differing paths
	require.NoError(t, err)
	require.NotNil(t, diffReader)

	_, err = diffReader.Next()
	require.ErrorIs(t, err, ErrNoMoreDifferences) // no differences should be found

	ht1.
		AggregateLeafWith(0, []byte("val11")).
		AggregateLeafWith(1, []byte("val12"))
	ht2.
		AggregateLeafWith(0, []byte("val21")).
		AggregateLeafWith(1, []byte("val22"))

	diffReader, err = HashTreeDiff(ht1, ht2) // diff is set to one for all differing paths
	require.NoError(t, err)

	diff, err := diffReader.Next()
	require.NoError(t, err)
	require.Equal(t, 0, diff) // leaf0 should differ

	diff, err = diffReader.Next()
	require.NoError(t, err)
	require.Equal(t, 1, diff) // leaf1 should differ

	_, err = diffReader.Next()
	require.ErrorIs(t, err, ErrNoMoreDifferences) // no more differences should be found

	ht1.AggregateLeafWith(0, []byte("val21"))
	ht2.AggregateLeafWith(0, []byte("val11"))

	diffReader, err = HashTreeDiff(ht1, ht2)
	require.NoError(t, err)

	diff, err = diffReader.Next()
	require.NoError(t, err)
	require.Equal(t, 0, diffReader) // leaf0 should differ

	diff, err = diffReader.Next()
	require.NoError(t, err)
	require.Equal(t, 1, diffReader) // leaf1 should differ

	_, err = diffReader.Next()
	require.ErrorIs(t, err, ErrNoMoreDifferences) // no more differences should be found

	ht1.AggregateLeafWith(1, []byte("val22"))
	ht2.AggregateLeafWith(1, []byte("val12"))

	diffReader, err = HashTreeDiff(ht1, ht2)
	require.NoError(t, err)

	_, err = diffReader.Next()
	require.ErrorIs(t, err, ErrNoMoreDifferences) // no differences should be found
}
