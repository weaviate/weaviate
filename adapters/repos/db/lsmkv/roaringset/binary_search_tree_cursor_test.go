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

package roaringset

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

func TestBSTCursor(t *testing.T) {
	bst := &BinarySearchTree{}

	in := []struct {
		key    string
		addVal uint64
		delVal uint64
	}{
		{"aaa", 1, 11},
		{"bbb", 2, 22},
		{"ccc", 3, 33},
		{"ddd", 4, 44},
	}

	for _, v := range in {
		bst.Insert([]byte(v.key), Insert{Additions: []uint64{v.addVal}, Deletions: []uint64{v.delVal}})
	}

	t.Run("start from beginning", func(t *testing.T) {
		cursor := NewBinarySearchTreeCursor(bst)

		key, layer, err := cursor.First()

		assert.Equal(t, []byte(in[0].key), key)
		assert.Equal(t, 1, layer.Additions.GetCardinality())
		assert.True(t, layer.Additions.Contains(in[0].addVal))
		assert.Equal(t, 1, layer.Deletions.GetCardinality())
		assert.True(t, layer.Deletions.Contains(in[0].delVal))
		assert.Nil(t, err)
	})

	t.Run("start from beginning and go through all", func(t *testing.T) {
		cursor := NewBinarySearchTreeCursor(bst)

		i := 0 // 1st match is "aaa"
		for key, layer, err := cursor.First(); key != nil; key, layer, err = cursor.Next() {
			assert.Equal(t, []byte(in[i].key), key)
			assert.Equal(t, 1, layer.Additions.GetCardinality())
			assert.True(t, layer.Additions.Contains(in[i].addVal))
			assert.Equal(t, 1, layer.Deletions.GetCardinality())
			assert.True(t, layer.Deletions.Contains(in[i].delVal))
			assert.Nil(t, err)
			i++
		}
		assert.Equal(t, i, len(in))
	})

	t.Run("seek matching element and go through rest", func(t *testing.T) {
		cursor := NewBinarySearchTreeCursor(bst)

		i := 1 // 1st match is "bbb"
		matching := []byte("bbb")
		for key, layer, err := cursor.Seek(matching); key != nil; key, layer, err = cursor.Next() {
			assert.Equal(t, []byte(in[i].key), key)
			assert.Equal(t, 1, layer.Additions.GetCardinality())
			assert.True(t, layer.Additions.Contains(in[i].addVal))
			assert.Equal(t, 1, layer.Deletions.GetCardinality())
			assert.True(t, layer.Deletions.Contains(in[i].delVal))
			assert.Nil(t, err)
			i++
		}
		assert.Equal(t, i, len(in))
	})

	t.Run("seek non-matching element and go through rest", func(t *testing.T) {
		cursor := NewBinarySearchTreeCursor(bst)

		i := 2 // 1st match is "ccc"
		nonMatching := []byte("bcde")
		for key, layer, err := cursor.Seek(nonMatching); key != nil; key, layer, err = cursor.Next() {
			assert.Equal(t, []byte(in[i].key), key)
			assert.Equal(t, 1, layer.Additions.GetCardinality())
			assert.True(t, layer.Additions.Contains(in[i].addVal))
			assert.Equal(t, 1, layer.Deletions.GetCardinality())
			assert.True(t, layer.Deletions.Contains(in[i].delVal))
			assert.Nil(t, err)
			i++
		}
		assert.Equal(t, i, len(in))
	})

	t.Run("seek missing element", func(t *testing.T) {
		cursor := NewBinarySearchTreeCursor(bst)

		missing := []byte("eee")
		key, layer, err := cursor.Seek(missing)

		assert.Nil(t, key)
		assert.True(t, layer.Additions.IsEmpty())
		assert.True(t, layer.Deletions.IsEmpty())
		assert.ErrorIs(t, err, lsmkv.NotFound)
	})

	t.Run("next after seek missing element does not change cursor's position", func(t *testing.T) {
		cursor := NewBinarySearchTreeCursor(bst)

		key1, _, err1 := cursor.First()

		missing := []byte("eee")
		cursor.Seek(missing)

		key2, _, err2 := cursor.Next()

		assert.Equal(t, []byte("aaa"), key1)
		assert.Nil(t, err1)
		assert.Equal(t, []byte("bbb"), key2)
		assert.Nil(t, err2)
	})

	t.Run("next after last is nil/empty", func(t *testing.T) {
		cursor := NewBinarySearchTreeCursor(bst)

		last := []byte("ddd")
		cursor.Seek(last)
		key, layer, err := cursor.Next()

		assert.Nil(t, key)
		assert.True(t, layer.Additions.IsEmpty())
		assert.True(t, layer.Deletions.IsEmpty())
		assert.Nil(t, err)
	})

	t.Run("first after final/empty next", func(t *testing.T) {
		cursor := NewBinarySearchTreeCursor(bst)

		last := []byte("ddd")
		cursor.Seek(last)
		cursor.Next()
		key, layer, err := cursor.First()

		assert.Equal(t, []byte(in[0].key), key)
		assert.Equal(t, 1, layer.Additions.GetCardinality())
		assert.True(t, layer.Additions.Contains(in[0].addVal))
		assert.Equal(t, 1, layer.Deletions.GetCardinality())
		assert.True(t, layer.Deletions.Contains(in[0].delVal))
		assert.Nil(t, err)
	})

	t.Run("seek after final/empty next", func(t *testing.T) {
		cursor := NewBinarySearchTreeCursor(bst)

		last := []byte("ddd")
		matching := []byte("bbb")
		cursor.Seek(last)
		cursor.Next()
		key, layer, err := cursor.Seek(matching)

		assert.Equal(t, []byte(in[1].key), key)
		assert.Equal(t, 1, layer.Additions.GetCardinality())
		assert.True(t, layer.Additions.Contains(in[1].addVal))
		assert.Equal(t, 1, layer.Deletions.GetCardinality())
		assert.True(t, layer.Deletions.Contains(in[1].delVal))
		assert.Nil(t, err)
	})
}
