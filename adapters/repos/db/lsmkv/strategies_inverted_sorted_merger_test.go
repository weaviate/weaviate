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

func Test_SortedInvertedMerger_RemoveTombstones(t *testing.T) {
	t.Run("single entry, no tombstones", func(t *testing.T) {
		m := newSortedInvertedMerger()
		input1 := []InvertedPair{
			NewInvertedPairFromDocIdAndTf(1, 1, 1, false),
		}

		input := [][]InvertedPair{input1}

		actual, err := m.do(input)
		require.Nil(t, err)

		expected := []InvertedPair{
			NewInvertedPairFromDocIdAndTf(1, 1, 1, false),
		}
		assert.Equal(t, expected, actual)
	})

	t.Run("single entry, single tombstone for unrelated key", func(t *testing.T) {
		m := newSortedInvertedMerger()
		input1 := []InvertedPair{
			NewInvertedPairFromDocIdAndTf(1, 1, 1, false),
			NewInvertedPairFromDocIdAndTf(2, 1, 1, true),
		}

		input := [][]InvertedPair{input1}

		actual, err := m.do(input)
		require.Nil(t, err)

		expected := []InvertedPair{
			NewInvertedPairFromDocIdAndTf(1, 1, 1, false),
		}
		assert.Equal(t, expected, actual)
	})

	t.Run("single entry with tombstone over two segments", func(t *testing.T) {
		m := newSortedInvertedMerger()
		input := [][]InvertedPair{
			{
				NewInvertedPairFromDocIdAndTf(1, 1, 1, false),
			},
			{
				NewInvertedPairFromDocIdAndTf(1, 1, 1, true),
			},
		}

		actual, err := m.do(input)
		require.Nil(t, err)

		expected := []InvertedPair{}
		assert.Equal(t, expected, actual)
	})

	t.Run("multiple segments including updates", func(t *testing.T) {
		m := newSortedInvertedMerger()
		input := [][]InvertedPair{
			{
				NewInvertedPairFromDocIdAndTf(1, 1, 1, false),
				NewInvertedPairFromDocIdAndTf(3, 1, 1, false),
				NewInvertedPairFromDocIdAndTf(5, 1, 1, false),
			},
			{
				NewInvertedPairFromDocIdAndTf(1, 2, 2, false),
				NewInvertedPairFromDocIdAndTf(3, 2, 2, false),
			},
			{
				NewInvertedPairFromDocIdAndTf(2, 3, 3, false),
			},
		}

		actual, err := m.do(input)
		require.Nil(t, err)

		expected := []InvertedPair{
			NewInvertedPairFromDocIdAndTf(1, 2, 2, false),
			NewInvertedPairFromDocIdAndTf(2, 3, 3, false),
			NewInvertedPairFromDocIdAndTf(3, 2, 2, false),
			NewInvertedPairFromDocIdAndTf(5, 1, 1, false),
		}
		assert.Equal(t, expected, actual)
	})

	t.Run("multiple segments including deletes and re-adds", func(t *testing.T) {
		m := newSortedInvertedMerger()
		input := [][]InvertedPair{
			{
				NewInvertedPairFromDocIdAndTf(1, 1, 1, false),
				NewInvertedPairFromDocIdAndTf(3, 1, 1, false),
				NewInvertedPairFromDocIdAndTf(5, 1, 1, false),
			},
			{
				NewInvertedPairFromDocIdAndTf(1, 2, 2, false),
				NewInvertedPairFromDocIdAndTf(2, 2, 2, true),
				NewInvertedPairFromDocIdAndTf(3, 2, 2, false),
			},
			{
				NewInvertedPairFromDocIdAndTf(2, 3, 3, false),
				NewInvertedPairFromDocIdAndTf(5, 3, 3, true),
			},
		}

		actual, err := m.do(input)
		require.Nil(t, err)

		expected := []InvertedPair{
			NewInvertedPairFromDocIdAndTf(1, 2, 2, false),
			NewInvertedPairFromDocIdAndTf(2, 3, 3, false),
			NewInvertedPairFromDocIdAndTf(3, 2, 2, false),
		}
		assert.Equal(t, expected, actual)
	})
}

func Test_SortedInvertedMerger_KeepTombstones(t *testing.T) {
	m := newSortedInvertedMerger()

	t.Run("multiple segments including updates, deletes in 2nd segment", func(t *testing.T) {
		input := [][]InvertedPair{
			{
				NewInvertedPairFromDocIdAndTf(1, 1, 1, false),
				NewInvertedPairFromDocIdAndTf(3, 1, 1, false),
				NewInvertedPairFromDocIdAndTf(5, 1, 1, false),
			},
			{
				NewInvertedPairFromDocIdAndTf(1, 2, 2, false),
				NewInvertedPairFromDocIdAndTf(2, 2, 2, false),
				NewInvertedPairFromDocIdAndTf(3, 2, 2, true),
			},
			{
				NewInvertedPairFromDocIdAndTf(2, 3, 3, false),
			},
		}

		expected := []InvertedPair{
			NewInvertedPairFromDocIdAndTf(1, 2, 2, false),
			NewInvertedPairFromDocIdAndTf(2, 3, 3, false),
			NewInvertedPairFromDocIdAndTf(3, 2, 2, true),
			NewInvertedPairFromDocIdAndTf(5, 1, 1, false),
		}

		t.Run("without reusable functionality - fresh state", func(t *testing.T) {
			actual, err := m.doKeepTombstones(input)
			require.Nil(t, err)

			assert.Equal(t, expected, actual)
		})

		t.Run("with reusable functionality - fresh state", func(t *testing.T) {
			m.reset(input)
			actual, err := m.doKeepTombstonesReusable()
			require.Nil(t, err)

			assert.Equal(t, expected, actual)
		})
	})

	t.Run("inverse order, deletes in 1st segment", func(t *testing.T) {
		input := [][]InvertedPair{
			{
				NewInvertedPairFromDocIdAndTf(2, 3, 3, false),
			},
			{
				NewInvertedPairFromDocIdAndTf(1, 2, 2, false),
				NewInvertedPairFromDocIdAndTf(2, 2, 2, false),
				NewInvertedPairFromDocIdAndTf(3, 2, 2, true),
			},
			{
				NewInvertedPairFromDocIdAndTf(1, 1, 1, false),
				NewInvertedPairFromDocIdAndTf(3, 1, 1, false),
				NewInvertedPairFromDocIdAndTf(5, 1, 1, false),
			},
		}

		expected := []InvertedPair{
			NewInvertedPairFromDocIdAndTf(1, 1, 1, false),
			NewInvertedPairFromDocIdAndTf(2, 2, 2, false),
			NewInvertedPairFromDocIdAndTf(3, 1, 1, false),
			NewInvertedPairFromDocIdAndTf(5, 1, 1, false),
		}

		t.Run("without reusable functionality - fresh state", func(t *testing.T) {
			actual, err := m.doKeepTombstones(input)
			require.Nil(t, err)

			assert.Equal(t, expected, actual)
		})

		t.Run("with reusable functionality - dirty state", func(t *testing.T) {
			m.reset(input)
			actual, err := m.doKeepTombstonesReusable()
			require.Nil(t, err)

			assert.Equal(t, expected, actual)
		})
	})
}
