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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInvertedEncoderDecoderJourney(t *testing.T) {
	// this test first encodes the map pairs, then decodes them and replace
	// duplicates, remove tombstones, etc.
	type test struct {
		name string
		in   []InvertedPair
		out  []InvertedPair
	}

	tests := []test{
		{
			name: "single pair",
			in: []InvertedPair{
				NewInvertedPairFromDocIdAndTf(1, 1, 1, false),
			},
			out: []InvertedPair{
				NewInvertedPairFromDocIdAndTf(1, 1, 1, false),
			},
		},
		{
			name: "single pair, updated value",
			in: []InvertedPair{
				NewInvertedPairFromDocIdAndTf(1, 1, 1, false),
				NewInvertedPairFromDocIdAndTf(1, 2, 2, false),
			},
			out: []InvertedPair{
				NewInvertedPairFromDocIdAndTf(1, 2, 2, false),
			},
		},
		{
			name: "single pair, tombstone added",
			in: []InvertedPair{
				NewInvertedPairFromDocIdAndTf(1, 1, 2, true),
			},
			out: []InvertedPair{},
		},
		{
			name: "single pair, tombstone added, same value added again",
			in: []InvertedPair{
				NewInvertedPairFromDocIdAndTf(1, 1, 1, false),
				NewInvertedPairFromDocIdAndTf(1, 1, 1, true),
				NewInvertedPairFromDocIdAndTf(1, 1, 2, false),
			},
			out: []InvertedPair{
				NewInvertedPairFromDocIdAndTf(1, 1, 2, false),
			},
		},
		{
			name: "multiple values, combination of updates and tombstones",
			in: []InvertedPair{
				NewInvertedPairFromDocIdAndTf(1, 1, 1, false),
				NewInvertedPairFromDocIdAndTf(2, 2, 2, false),
				NewInvertedPairFromDocIdAndTf(3, 3, 3, false),
				NewInvertedPairFromDocIdAndTf(3, 4, 4, false),
				NewInvertedPairFromDocIdAndTf(2, 2, 2, true),
				NewInvertedPairFromDocIdAndTf(3, 5, 5, false),
				NewInvertedPairFromDocIdAndTf(2, 3, 3, false),
			},
			out: []InvertedPair{
				NewInvertedPairFromDocIdAndTf(1, 1, 1, false),
				NewInvertedPairFromDocIdAndTf(2, 3, 3, false),
				NewInvertedPairFromDocIdAndTf(3, 5, 5, false),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded := make([]value, len(test.in))
			for i, kv := range test.in {
				enc, err := newInvertedEncoder().Do(kv)
				require.Nil(t, err)
				encoded[i] = enc[0]
			}
			res, err := newInvertedDecoder().Do(encoded, false)
			require.Nil(t, err)
			// NOTE: we are accepting that the order can be lost on updates
			assert.ElementsMatch(t, test.out, res)
		})
	}
}

func TestInvertedDecoderRemoveTombstones(t *testing.T) {
	t.Run("single entry, no tombstones", func(t *testing.T) {
		m := newInvertedDecoder()
		input := mustEncodeInverted([]InvertedPair{
			NewInvertedPairFromDocIdAndTf(1, 1, 1, false),
		})

		actual, err := m.doSimplified(input)
		require.Nil(t, err)

		expected := []InvertedPair{
			NewInvertedPairFromDocIdAndTf(1, 1, 1, false),
		}
		assert.Equal(t, expected, actual)
	})

	t.Run("single entry, single tombstone", func(t *testing.T) {
		m := newInvertedDecoder()
		input := mustEncodeInverted([]InvertedPair{
			NewInvertedPairFromDocIdAndTf(1, 1, 1, false),
			NewInvertedPairFromDocIdAndTf(1, 1, 1, true),
		})

		actual, err := m.doSimplified(input)
		require.Nil(t, err)

		expected := []InvertedPair{}
		assert.Equal(t, expected, actual)
	})

	t.Run("single entry, single tombstone, then read", func(t *testing.T) {
		m := newInvertedDecoder()
		input := mustEncodeInverted([]InvertedPair{
			NewInvertedPairFromDocIdAndTf(1, 1, 1, false),
			NewInvertedPairFromDocIdAndTf(1, 1, 1, true),
			NewInvertedPairFromDocIdAndTf(1, 1, 1, false),
		})

		actual, err := m.doSimplified(input)
		require.Nil(t, err)

		expected := []InvertedPair{
			NewInvertedPairFromDocIdAndTf(1, 1, 1, false),
		}
		assert.Equal(t, expected, actual)
	})

	t.Run("three entries, two tombstones at the end", func(t *testing.T) {
		m := newInvertedDecoder()
		input := mustEncodeInverted([]InvertedPair{
			NewInvertedPairFromDocIdAndTf(1, 1, 1, false),
			NewInvertedPairFromDocIdAndTf(2, 2, 2, false),
			NewInvertedPairFromDocIdAndTf(3, 3, 3, false),
			NewInvertedPairFromDocIdAndTf(1, 1, 1, true),
			NewInvertedPairFromDocIdAndTf(2, 2, 2, true),
		})

		actual, err := m.doSimplified(input)
		require.Nil(t, err)

		expected := []InvertedPair{
			NewInvertedPairFromDocIdAndTf(3, 3, 3, false),
		}
		assert.Equal(t, expected, actual)
	})

	t.Run("three entries, two tombstones at the end, then recreate the first", func(t *testing.T) {
		m := newInvertedDecoder()
		input := mustEncodeInverted([]InvertedPair{
			NewInvertedPairFromDocIdAndTf(1, 1, 1, false),
			NewInvertedPairFromDocIdAndTf(2, 2, 2, false),
			NewInvertedPairFromDocIdAndTf(3, 3, 3, false),
			NewInvertedPairFromDocIdAndTf(1, 1, 1, true),
			NewInvertedPairFromDocIdAndTf(2, 2, 2, true),
			NewInvertedPairFromDocIdAndTf(1, 1, 1, false),
			NewInvertedPairFromDocIdAndTf(2, 2, 2, false),
		})

		actual, err := m.doSimplified(input)
		require.Nil(t, err)

		expected := []InvertedPair{
			NewInvertedPairFromDocIdAndTf(3, 3, 3, false),
			NewInvertedPairFromDocIdAndTf(1, 1, 1, false),
			NewInvertedPairFromDocIdAndTf(2, 2, 2, false),
		}
		assert.Equal(t, expected, actual)
	})
}

func mustEncodeInverted(kvs []InvertedPair) []value {
	res, err := newInvertedEncoder().DoMulti(kvs)
	if err != nil {
		panic(err)
	}

	return res
}

func Test_InvertedPair_EncodingBytes(t *testing.T) {
	kv := NewInvertedPairFromDocIdAndTf(1, 1, 1, false)

	control, err := kv.Bytes()
	assert.Nil(t, err)

	encoded := make([]byte, kv.Size())
	err = kv.EncodeBytes(encoded)
	assert.Nil(t, err)

	assert.Equal(t, control, encoded)
}
