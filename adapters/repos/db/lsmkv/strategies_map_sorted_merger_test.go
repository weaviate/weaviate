//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package lsmkv

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSortedDecoderRemoveTombstones(t *testing.T) {
	t.Run("single entry, no tombstones", func(t *testing.T) {
		m := newSortedMapMerger()
		input1 := []MapPair{
			{
				Key:   []byte("hello"),
				Value: []byte("world"),
			},
		}

		input := [][]MapPair{input1}

		actual, err := m.do(input)
		require.Nil(t, err)

		expected := []MapPair{
			{
				Key:   []byte("hello"),
				Value: []byte("world"),
			},
		}
		assert.Equal(t, expected, actual)
	})

	t.Run("single entry, single tombstone for unrelated key", func(t *testing.T) {
		m := newSortedMapMerger()
		input1 := []MapPair{
			{
				Key:   []byte("hello"),
				Value: []byte("world"),
			},
			{
				Key:       []byte("unrelated"),
				Tombstone: true,
			},
		}

		input := [][]MapPair{input1}

		actual, err := m.do(input)
		require.Nil(t, err)

		expected := []MapPair{
			{
				Key:   []byte("hello"),
				Value: []byte("world"),
			},
		}
		assert.Equal(t, expected, actual)
	})

	t.Run("single entry with tombstone over two segments", func(t *testing.T) {
		m := newSortedMapMerger()
		input := [][]MapPair{
			{
				{
					Key:   []byte("hello"),
					Value: []byte("world"),
				},
			},
			{
				{
					Key:       []byte("hello"),
					Tombstone: true,
				},
			},
		}

		actual, err := m.do(input)
		require.Nil(t, err)

		expected := []MapPair{}
		assert.Equal(t, expected, actual)
	})

	t.Run("multiple segments including updates", func(t *testing.T) {
		m := newSortedMapMerger()
		input := [][]MapPair{
			{
				{
					Key:   []byte("a"),
					Value: []byte("a1"),
				},
				{
					Key:   []byte("c"),
					Value: []byte("c1"),
				},
				{
					Key:   []byte("e"),
					Value: []byte("e1"),
				},
			},
			{
				{
					Key:   []byte("a"),
					Value: []byte("a2"),
				},
				{
					Key:   []byte("b"),
					Value: []byte("b2"),
				},
				{
					Key:   []byte("c"),
					Value: []byte("c2"),
				},
			},
			{
				{
					Key:   []byte("b"),
					Value: []byte("b3"),
				},
			},
		}

		actual, err := m.do(input)
		require.Nil(t, err)

		expected := []MapPair{
			{
				Key:   []byte("a"),
				Value: []byte("a2"),
			},
			{
				Key:   []byte("b"),
				Value: []byte("b3"),
			},
			{
				Key:   []byte("c"),
				Value: []byte("c2"),
			},
			{
				Key:   []byte("e"),
				Value: []byte("e1"),
			},
		}
		assert.Equal(t, expected, actual)
	})

	t.Run("multiple segments including deletes and re-adds", func(t *testing.T) {
		m := newSortedMapMerger()
		input := [][]MapPair{
			{
				{
					Key:   []byte("a"),
					Value: []byte("a1"),
				},
				{
					Key:   []byte("c"),
					Value: []byte("c1"),
				},
				{
					Key:   []byte("e"),
					Value: []byte("e1"),
				},
			},
			{
				{
					Key:   []byte("a"),
					Value: []byte("a2"),
				},
				{
					Key:       []byte("b"),
					Tombstone: true,
				},
				{
					Key:   []byte("c"),
					Value: []byte("c2"),
				},
			},
			{
				{
					Key:   []byte("b"),
					Value: []byte("b3"),
				},
				{
					Key:       []byte("e"),
					Tombstone: true,
				},
			},
		}

		actual, err := m.do(input)
		require.Nil(t, err)

		expected := []MapPair{
			{
				Key:   []byte("a"),
				Value: []byte("a2"),
			},
			{
				Key:   []byte("b"),
				Value: []byte("b3"),
			},
			{
				Key:   []byte("c"),
				Value: []byte("c2"),
			},
		}
		assert.Equal(t, expected, actual)
	})
}
