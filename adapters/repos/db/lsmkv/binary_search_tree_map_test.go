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

func Test_BinarySearchTreeMap(t *testing.T) {
	t.Run("single row key, single map key", func(t *testing.T) {
		tree := &binarySearchTreeMap{}
		rowKey := []byte("rowkey")

		pair1 := MapPair{
			Key:   []byte("map-key-1"),
			Value: []byte("map-value-1"),
		}

		tree.insert(rowKey, pair1)

		res, err := tree.get(rowKey)
		require.Nil(t, err)
		assert.Equal(t, []MapPair{
			{
				Key:   []byte("map-key-1"),
				Value: []byte("map-value-1"),
			},
		}, res)
	})

	t.Run("single row key, updated map value", func(t *testing.T) {
		tree := &binarySearchTreeMap{}
		rowKey := []byte("rowkey")

		tree.insert(rowKey, MapPair{
			Key:   []byte("c"),
			Value: []byte("c1"),
		})

		tree.insert(rowKey, MapPair{
			Key:   []byte("a"),
			Value: []byte("a1"),
		})

		tree.insert(rowKey, MapPair{
			Key:   []byte("b"),
			Value: []byte("b1"),
		})

		tree.insert(rowKey, MapPair{
			Key:   []byte("b"),
			Value: []byte("b2"),
		})

		tree.insert(rowKey, MapPair{
			Key:   []byte("a"),
			Value: []byte("a2"),
		})

		res, err := tree.get(rowKey)
		require.Nil(t, err)
		assert.Equal(t, []MapPair{
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
				Value: []byte("c1"),
			},
		}, res)
	})

	t.Run("two row keys, updated map value", func(t *testing.T) {
		tree := &binarySearchTreeMap{}
		rowKey1 := []byte("rowkey")
		rowKey2 := []byte("other-rowkey")

		tree.insert(rowKey1, MapPair{
			Key:   []byte("c"),
			Value: []byte("c1"),
		})

		tree.insert(rowKey1, MapPair{
			Key:   []byte("a"),
			Value: []byte("a1"),
		})

		tree.insert(rowKey2, MapPair{
			Key:   []byte("z"),
			Value: []byte("z1"),
		})

		tree.insert(rowKey1, MapPair{
			Key:   []byte("b"),
			Value: []byte("b1"),
		})

		tree.insert(rowKey2, MapPair{
			Key:   []byte("x"),
			Value: []byte("x1"),
		})

		tree.insert(rowKey1, MapPair{
			Key:   []byte("b"),
			Value: []byte("b2"),
		})

		tree.insert(rowKey1, MapPair{
			Key:   []byte("a"),
			Value: []byte("a2"),
		})

		tree.insert(rowKey2, MapPair{
			Key:   []byte("x"),
			Value: []byte("x2"),
		})

		res, err := tree.get(rowKey1)
		require.Nil(t, err)
		assert.Equal(t, []MapPair{
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
				Value: []byte("c1"),
			},
		}, res)

		res, err = tree.get(rowKey2)
		require.Nil(t, err)
		assert.Equal(t, []MapPair{
			{
				Key:   []byte("x"),
				Value: []byte("x2"),
			},
			{
				Key:   []byte("z"),
				Value: []byte("z1"),
			},
		}, res)
	})

	t.Run("single row key, deleted map values", func(t *testing.T) {
		tree := &binarySearchTreeMap{}
		rowKey := []byte("rowkey")

		tree.insert(rowKey, MapPair{
			Key:   []byte("c"),
			Value: []byte("c1"),
		})

		tree.insert(rowKey, MapPair{
			Key:   []byte("a"),
			Value: []byte("a1"),
		})

		tree.insert(rowKey, MapPair{
			Key:   []byte("b"),
			Value: []byte("b1"),
		})

		tree.insert(rowKey, MapPair{
			Key:       []byte("b"),
			Tombstone: true,
		})

		tree.insert(rowKey, MapPair{
			Key:       []byte("a"),
			Tombstone: true,
		})

		res, err := tree.get(rowKey)
		require.Nil(t, err)
		assert.Equal(t, []MapPair{
			{
				Key:       []byte("a"),
				Tombstone: true,
			},
			{
				Key:       []byte("b"),
				Tombstone: true,
			},
			{
				Key:   []byte("c"),
				Value: []byte("c1"),
			},
		}, res)
	})
}
