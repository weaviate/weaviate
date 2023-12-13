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
	capacity := 1
	maxHeight := 2
	expectedHeight := 1

	ht := NewCompactHashTree(capacity, maxHeight)

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

func TestCompactedHashTree(t *testing.T) {
	capacity := 1024
	maxHeight := 6
	expectedHeight := 6

	ht := NewCompactHashTree(capacity, maxHeight)

	require.Equal(t, expectedHeight, ht.Height())

	valuePrefix := "somevalue"

	for i := 0; i < capacity; i++ {
		ht.AggregateLeafWith(i, []byte(fmt.Sprintf("%s%d", valuePrefix, i)))
	}
}

func TestCompactBigHashTree(t *testing.T) {
	capacity := math.MaxUint64 / 128
	maxHeight := 16
	expectedHeight := 16

	ht := NewCompactHashTree(capacity, maxHeight)

	require.Equal(t, expectedHeight, ht.Height())

	actualNumberOfElements := 1_000_000

	valuePrefix := "somevalue"

	for i := 0; i < actualNumberOfElements; i++ {
		ht.AggregateLeafWith(i, []byte(fmt.Sprintf("%s%d", valuePrefix, i)))
	}

	var rootLevel [1]Digest

	n, err := ht.Level(0, NewBitset(1).SetAll(), rootLevel[:])
	require.NoError(t, err)
	require.Equal(t, 1, n)
}

func TestCompactHashTreeComparisonHeight1(t *testing.T) {
	capacity := math.MaxUint64 / 128
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
