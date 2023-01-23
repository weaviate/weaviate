package roaringset

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCombinedCursor(t *testing.T) {
	bst1 := createBst(t, []bstIn{
		{"aaa", []uint64{1}, []uint64{}},
		{"bbb", []uint64{22}, []uint64{}},
		{"ccc", []uint64{333}, []uint64{}},
		{"ddd", []uint64{4444}, []uint64{}},
	})

	bst2 := createBst(t, []bstIn{
		{"aaa", []uint64{2, 3}, []uint64{1}},
		{"bbb", []uint64{33}, []uint64{22}},
		{"ggg", []uint64{7777777, 8888888}, []uint64{}},
	})

	bst3 := createBst(t, []bstIn{
		{"bbb", []uint64{22}, []uint64{}},
		{"ccc", []uint64{}, []uint64{333}},
		{"eee", []uint64{55555, 66666}, []uint64{}},
		{"fff", []uint64{666666}, []uint64{}},
		{"hhh", []uint64{999999999}, []uint64{111111111, 222222222, 333333333}},
	})

	expected := []struct {
		key    string
		values []uint64
	}{
		{"aaa", []uint64{2, 3}},
		{"bbb", []uint64{22, 33}},
		{"ccc", []uint64{}},
		{"ddd", []uint64{4444}},
		{"eee", []uint64{55555, 66666}},
		{"fff", []uint64{666666}},
		{"ggg", []uint64{7777777, 8888888}},
		{"hhh", []uint64{999999999}},
	}

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

	t.Run("seek matching element and go through rest", func(t *testing.T) {
		cursor := createCursor(t, bst1, bst2, bst3)

		i := 3 // 1st match is "ddd"
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

		i := 5 // 1st match is "fff"
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

		assert.Equal(t, []byte(expected[4].key), key)
		assert.Equal(t, len(expected[4].values), bm.GetCardinality())
		for _, v := range expected[4].values {
			assert.True(t, bm.Contains(v))
		}
	})
}

type bstIn struct {
	key    string
	addVal []uint64
	delVal []uint64
}

func createBst(t *testing.T, in []bstIn) *BinarySearchTree {
	bst := &BinarySearchTree{}
	for i := range in {
		bst.Insert([]byte(in[i].key), Insert{Additions: in[i].addVal, Deletions: in[i].delVal})
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
