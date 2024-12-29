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

package lsmkv

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestQuantileKeysSingleSegment(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(
		ctx, dir, "", logger, nil, cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)

	importConsecutiveKeys(t, b, 0, 1000)

	// all cyclemmanagers are noops, so we need to explicitly flush if we want a
	// segment to be built
	require.Nil(t, b.FlushAndSwitch())

	quantiles := b.QuantileKeys(10)

	asNumbers := make([]uint64, len(quantiles))
	for i, q := range quantiles {
		asNumbers[i] = binary.BigEndian.Uint64(q)
	}

	// validate there are no duplicates, and each key is strictly greater than
	// the last
	for i, n := range asNumbers {
		if i == 0 {
			continue
		}

		prev := asNumbers[i-1]
		assert.Greater(t, n, prev)
	}

	// assert on distribution
	idealStepSize := float64(1000) / float64(len(asNumbers)+1)
	for i, n := range asNumbers {
		actualStepSize := float64(n) / float64(i+1)
		assert.InEpsilon(t, idealStepSize, actualStepSize, 0.1)
	}
}

func TestQuantileKeysMultipleSegmentsUniqueEntries(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(
		ctx, dir, "", logger, nil, cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)

	importConsecutiveKeys(t, b, 0, 1000)

	// all cyclemmanagers are noops, so we need to explicitly flush if we want a
	// segment to be built
	require.Nil(t, b.FlushAndSwitch())

	importConsecutiveKeys(t, b, 1000, 2000)

	// all cyclemmanagers are noops, so we need to explicitly flush if we want a
	// segment to be built
	require.Nil(t, b.FlushAndSwitch())

	quantiles := b.QuantileKeys(10)

	asNumbers := make([]uint64, len(quantiles))
	for i, q := range quantiles {
		asNumbers[i] = binary.BigEndian.Uint64(q)
	}

	// validate there are no duplicates, and each key is strictly greater than
	// the last
	for i, n := range asNumbers {
		if i == 0 {
			continue
		}

		prev := asNumbers[i-1]
		assert.Greater(t, n, prev)
	}
}

func importConsecutiveKeys(t *testing.T, b *Bucket, start, end uint64) {
	for i := start; i < end; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		err := b.Put(key, key)
		require.Nil(t, err)
	}
}

func TestKeyDistributionExample(t *testing.T) {
	inputKeyStrings := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}
	inputKeys := make([][]byte, len(inputKeyStrings))
	for i, s := range inputKeyStrings {
		inputKeys[i] = []byte(s)
	}
	q := 3

	picked := pickEvenlyDistributedKeys(inputKeys, q)
	expectKeyStrings := []string{"C", "F", "I"}
	expectKeys := make([][]byte, len(expectKeyStrings))
	for i, s := range expectKeyStrings {
		expectKeys[i] = []byte(s)
	}

	assert.Equal(t, expectKeys, picked)
}

func TestPickEvenlyDistributedKeys(t *testing.T) {
	for input := 0; input < 100; input++ {
		for q := 1; q < 100; q++ {
			t.Run(fmt.Sprintf("input=%d, q=%d", input, q), func(t *testing.T) {
				keys := make([][]byte, input)
				for i := 0; i < input; i++ {
					key := make([]byte, 8)
					binary.BigEndian.PutUint64(key, uint64(i))
					keys[i] = key
				}

				picked := pickEvenlyDistributedKeys(keys, q)

				// make sure there are never more results than q
				require.LessOrEqual(t, len(picked), q)

				// make sure that we get q results if there are at least q keys
				if input >= q {
					require.Equal(t, q, len(picked))
				} else {
					// if there are fewer keys than q, we should get all of them
					require.Equal(t, input, len(picked))
				}

				// make sure there are no duplicates
				for i, key := range picked {
					if i == 0 {
						continue
					}

					prev := binary.BigEndian.Uint64(picked[i-1])
					curr := binary.BigEndian.Uint64(key)

					require.Greater(t, curr, prev, "found duplicate picks")
				}
			})
		}
	}
}
