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
)

func TestCombinedCursor(t *testing.T) {
	bst1 := createBst(t, []bstIn{
		{
			key:       "aaa",
			additions: []uint64{1},
			deletions: []uint64{},
		},
		{
			key:       "bbb",
			additions: []uint64{22},
			deletions: []uint64{},
		},
		{
			key:       "ccc",
			additions: []uint64{333},
			deletions: []uint64{},
		},
		{
			key:       "ddd",
			additions: []uint64{4444},
			deletions: []uint64{},
		},
	})

	bst2 := createBst(t, []bstIn{
		{
			key:       "aaa",
			additions: []uint64{2, 3},
			deletions: []uint64{1},
		},
		{
			key:       "bbb",
			additions: []uint64{33},
			deletions: []uint64{22},
		},
		{
			key:       "ggg",
			additions: []uint64{7777777, 8888888},
			deletions: []uint64{},
		},
	})

	bst3 := createBst(t, []bstIn{
		{
			key:       "bbb",
			additions: []uint64{22},
			deletions: []uint64{},
		},
		{
			key:       "ccc",
			additions: []uint64{},
			deletions: []uint64{333},
		},
		{
			key:       "eee",
			additions: []uint64{55555, 66666},
			deletions: []uint64{},
		},
		{
			key:       "fff",
			additions: []uint64{666666},
			deletions: []uint64{},
		},
		{
			key:       "hhh",
			additions: []uint64{999999999},
			deletions: []uint64{111111111, 222222222, 333333333},
		},
	})

	expected := []struct {
		key    string
		values []uint64
	}{
		{ // 0
			key:    "aaa",
			values: []uint64{2, 3},
		},
		{ // 1
			key:    "bbb",
			values: []uint64{22, 33},
		},
		{ // 2
			key:    "ddd",
			values: []uint64{4444},
		},
		{ // 3
			key:    "eee",
			values: []uint64{55555, 66666},
		},
		{ // 4
			key:    "fff",
			values: []uint64{666666},
		},
		{ // 5
			key:    "ggg",
			values: []uint64{7777777, 8888888},
		},
		{ // 6
			key:    "hhh",
			values: []uint64{999999999},
		},
	}

	t.Run("default cursor", func(t *testing.T) {
		t.Run("start from beginning", func(t *testing.T) {
			cursor := createCursor(t, bst1, bst2, bst3)

			key, bm := cursor.First()

			assert.Equal(t, []byte(expected[0].key), key)
			assert.Equal(t, len(expected[0].values), bm.GetCardinality())
			for _, v := range expected[0].values {
				assert.True(t, bm.Contains(v))
			}
		})

		t.Run("start from beginning and go through all", func(t *testing.T) {
			cursor := createCursor(t, bst1, bst2, bst3)

			i := 0 // 1st match is "aaa"
			for key, bm := cursor.First(); key != nil; key, bm = cursor.Next() {
				assert.Equal(t, []byte(expected[i].key), key)
				assert.Equal(t, len(expected[i].values), bm.GetCardinality())
				for _, v := range expected[i].values {
					assert.True(t, bm.Contains(v))
				}
				i++
			}
		})

		t.Run("start from beginning using Next and go through all", func(t *testing.T) {
			cursor := createCursor(t, bst1, bst2, bst3)

			i := 0 // 1st match is "aaa"
			for key, bm := cursor.Next(); key != nil; key, bm = cursor.Next() {
				assert.Equal(t, []byte(expected[i].key), key)
				assert.Equal(t, len(expected[i].values), bm.GetCardinality())
				for _, v := range expected[i].values {
					assert.True(t, bm.Contains(v))
				}
				i++
			}
		})

		t.Run("seek matching element and go through rest", func(t *testing.T) {
			cursor := createCursor(t, bst1, bst2, bst3)

			i := 2 // 1st match is "ddd"
			matching := []byte("ddd")
			for key, bm := cursor.Seek(matching); key != nil; key, bm = cursor.Next() {
				assert.Equal(t, []byte(expected[i].key), key)
				assert.Equal(t, len(expected[i].values), bm.GetCardinality())
				for _, v := range expected[i].values {
					assert.True(t, bm.Contains(v))
				}
				i++
			}
		})

		t.Run("seek non-matching element and go through rest", func(t *testing.T) {
			cursor := createCursor(t, bst1, bst2, bst3)

			i := 4 // 1st match is "fff"
			nonMatching := []byte("efg")
			for key, bm := cursor.Seek(nonMatching); key != nil; key, bm = cursor.Next() {
				assert.Equal(t, []byte(expected[i].key), key)
				assert.Equal(t, len(expected[i].values), bm.GetCardinality())
				for _, v := range expected[i].values {
					assert.True(t, bm.Contains(v))
				}
				i++
			}
		})

		t.Run("seek missing element", func(t *testing.T) {
			cursor := createCursor(t, bst1, bst2, bst3)

			missing := []byte("lll")
			key, bm := cursor.Seek(missing)

			assert.Nil(t, key)
			assert.NotNil(t, bm)
			assert.Equal(t, 0, bm.GetCardinality())
		})

		t.Run("next after seek missing element does not change cursor's position", func(t *testing.T) {
			cursor := createCursor(t, bst1, bst2, bst3)

			key1, _ := cursor.First()

			missing := []byte("lll")
			cursor.Seek(missing)

			key2, _ := cursor.Next()

			assert.Equal(t, []byte("aaa"), key1)
			assert.Equal(t, []byte("bbb"), key2)
		})

		t.Run("next after last is nil/empty", func(t *testing.T) {
			cursor := createCursor(t, bst1, bst2, bst3)

			last := []byte("hhh")
			cursor.Seek(last)
			key, bm := cursor.Next()

			assert.Nil(t, key)
			assert.NotNil(t, bm)
			assert.Equal(t, 0, bm.GetCardinality())
		})

		t.Run("first after final/empty next", func(t *testing.T) {
			cursor := createCursor(t, bst1, bst2, bst3)

			last := []byte("hhh")
			cursor.Seek(last)
			cursor.Next()
			key, bm := cursor.First()

			assert.Equal(t, []byte(expected[0].key), key)
			assert.Equal(t, len(expected[0].values), bm.GetCardinality())
			for _, v := range expected[0].values {
				assert.True(t, bm.Contains(v))
			}
		})

		t.Run("seek after final/empty next", func(t *testing.T) {
			cursor := createCursor(t, bst1, bst2, bst3)

			last := []byte("hhh")
			matching := []byte("eee")
			cursor.Seek(last)
			cursor.Next()
			key, bm := cursor.Seek(matching)

			assert.Equal(t, []byte(expected[3].key), key)
			assert.Equal(t, len(expected[3].values), bm.GetCardinality())
			for _, v := range expected[3].values {
				assert.True(t, bm.Contains(v))
			}
		})
	})

	t.Run("cursor key only", func(t *testing.T) {
		t.Run("start from beginning", func(t *testing.T) {
			cursor := createCursorKeyOnly(t, bst1, bst2, bst3)

			key, bm := cursor.First()

			assert.Equal(t, []byte(expected[0].key), key)
			assert.Nil(t, bm)
		})

		t.Run("start from beginning and go through all", func(t *testing.T) {
			cursor := createCursorKeyOnly(t, bst1, bst2, bst3)

			i := 0 // 1st match is "aaa"
			for key, bm := cursor.First(); key != nil; key, bm = cursor.Next() {
				assert.Equal(t, []byte(expected[i].key), key)
				assert.Nil(t, bm)
				i++
			}
		})

		t.Run("start from beginning using Next and go through all", func(t *testing.T) {
			cursor := createCursorKeyOnly(t, bst1, bst2, bst3)

			i := 0 // 1st match is "aaa"
			for key, bm := cursor.Next(); key != nil; key, bm = cursor.Next() {
				assert.Equal(t, []byte(expected[i].key), key)
				assert.Nil(t, bm)
				i++
			}
		})

		t.Run("seek matching element and go through rest", func(t *testing.T) {
			cursor := createCursorKeyOnly(t, bst1, bst2, bst3)

			i := 2 // 1st match is "ddd"
			matching := []byte("ddd")
			for key, bm := cursor.Seek(matching); key != nil; key, bm = cursor.Next() {
				assert.Equal(t, []byte(expected[i].key), key)
				assert.Nil(t, bm)
				i++
			}
		})

		t.Run("seek non-matching element and go through rest", func(t *testing.T) {
			cursor := createCursorKeyOnly(t, bst1, bst2, bst3)

			i := 4 // 1st match is "fff"
			nonMatching := []byte("efg")
			for key, bm := cursor.Seek(nonMatching); key != nil; key, bm = cursor.Next() {
				assert.Equal(t, []byte(expected[i].key), key)
				assert.Nil(t, bm)
				i++
			}
		})

		t.Run("seek missing element", func(t *testing.T) {
			cursor := createCursorKeyOnly(t, bst1, bst2, bst3)

			missing := []byte("lll")
			key, bm := cursor.Seek(missing)

			assert.Nil(t, key)
			assert.Nil(t, bm)
		})

		t.Run("next after seek missing element does not change cursor's position", func(t *testing.T) {
			cursor := createCursorKeyOnly(t, bst1, bst2, bst3)

			key1, _ := cursor.First()

			missing := []byte("lll")
			cursor.Seek(missing)

			key2, _ := cursor.Next()

			assert.Equal(t, []byte("aaa"), key1)
			assert.Equal(t, []byte("bbb"), key2)
		})

		t.Run("next after last is nil/empty", func(t *testing.T) {
			cursor := createCursorKeyOnly(t, bst1, bst2, bst3)

			last := []byte("hhh")
			cursor.Seek(last)
			key, bm := cursor.Next()

			assert.Nil(t, key)
			assert.Nil(t, bm)
		})

		t.Run("first after final/empty next", func(t *testing.T) {
			cursor := createCursorKeyOnly(t, bst1, bst2, bst3)

			last := []byte("hhh")
			cursor.Seek(last)
			cursor.Next()
			key, bm := cursor.First()

			assert.Equal(t, []byte(expected[0].key), key)
			assert.Nil(t, bm)
		})

		t.Run("seek after final/empty next", func(t *testing.T) {
			cursor := createCursorKeyOnly(t, bst1, bst2, bst3)

			last := []byte("hhh")
			matching := []byte("eee")
			cursor.Seek(last)
			cursor.Next()
			key, bm := cursor.Seek(matching)

			assert.Equal(t, []byte(expected[3].key), key)
			assert.Nil(t, bm)
		})
	})
}

type bstIn struct {
	key       string
	additions []uint64
	deletions []uint64
}

func createBst(t *testing.T, in []bstIn) *BinarySearchTree {
	bst := &BinarySearchTree{}
	for i := range in {
		bst.Insert([]byte(in[i].key), Insert{Additions: in[i].additions, Deletions: in[i].deletions})
	}
	return bst
}

func createCursor(t *testing.T, bsts ...*BinarySearchTree) *CombinedCursor {
	innerCursors := []InnerCursor{}
	for _, bst := range bsts {
		innerCursors = append(innerCursors, NewBinarySearchTreeCursor(bst))
	}
	return NewCombinedCursor(innerCursors, false)
}

func createCursorKeyOnly(t *testing.T, bsts ...*BinarySearchTree) *CombinedCursor {
	c := createCursor(t, bsts...)
	c.keyOnly = true
	return c
}
