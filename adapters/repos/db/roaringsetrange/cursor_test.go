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

package roaringsetrange

import (
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

func TestCombinedCursor(t *testing.T) {
	logger, _ := test.NewNullLogger()

	t.Run("single inner cursor", func(t *testing.T) {
		createCursor := func() *CombinedCursor {
			innerCursor := newFixedInnerCursor().
				withEntry(0, []uint64{1, 2, 3, 11, 22, 33, 111, 222, 333}, []uint64{2, 3, 4, 22, 333}).
				withEntry(1, []uint64{11, 22, 33}, []uint64{44}).
				withEntry(2, []uint64{111, 222, 333}, []uint64{0})

			return NewCombinedCursor([]InnerCursor{innerCursor}, logger)
		}

		t.Run("first and nexts", func(t *testing.T) {
			cursor := createCursor()

			key0, bm0, ok0 := cursor.First()
			key1, bm1, ok1 := cursor.Next()
			key2, bm2, ok2 := cursor.Next()
			key3, bm3, ok3 := cursor.Next()
			key4, bm4, ok4 := cursor.Next()

			assert.Equal(t, uint8(0), key0)
			assert.ElementsMatch(t, []uint64{1, 2, 3, 11, 22, 33, 111, 222, 333}, bm0.ToArray())
			assert.True(t, ok0)

			assert.Equal(t, uint8(1), key1)
			assert.ElementsMatch(t, []uint64{11, 22, 33}, bm1.ToArray())
			assert.True(t, ok1)

			assert.Equal(t, uint8(2), key2)
			assert.ElementsMatch(t, []uint64{111, 222, 333}, bm2.ToArray())
			assert.True(t, ok2)

			assert.Equal(t, uint8(0), key3)
			assert.Empty(t, bm3.ToArray())
			assert.False(t, ok3)

			assert.Equal(t, uint8(0), key4)
			assert.Empty(t, bm4.ToArray())
			assert.False(t, ok4)
		})

		t.Run("only nexts", func(t *testing.T) {
			cursor := createCursor()

			key0, bm0, ok0 := cursor.Next()
			key1, bm1, ok1 := cursor.Next()
			key2, bm2, ok2 := cursor.Next()
			key3, bm3, ok3 := cursor.Next()

			assert.Equal(t, uint8(0), key0)
			assert.ElementsMatch(t, []uint64{1, 2, 3, 11, 22, 33, 111, 222, 333}, bm0.ToArray())
			assert.True(t, ok0)

			assert.Equal(t, uint8(1), key1)
			assert.ElementsMatch(t, []uint64{11, 22, 33}, bm1.ToArray())
			assert.True(t, ok1)

			assert.Equal(t, uint8(2), key2)
			assert.ElementsMatch(t, []uint64{111, 222, 333}, bm2.ToArray())
			assert.True(t, ok2)

			assert.Equal(t, uint8(0), key3)
			assert.Empty(t, bm3.ToArray())
			assert.False(t, ok3)
		})

		t.Run("first, nexts and first again", func(t *testing.T) {
			cursor := createCursor()

			key0, bm0, ok0 := cursor.First()
			key1, bm1, ok1 := cursor.Next()
			key2, bm2, ok2 := cursor.Next()
			key3, bm3, ok3 := cursor.Next()
			key4, bm4, ok4 := cursor.First()
			key5, bm5, ok5 := cursor.Next()
			key6, bm6, ok6 := cursor.First()

			assert.Equal(t, uint8(0), key0)
			assert.ElementsMatch(t, []uint64{1, 2, 3, 11, 22, 33, 111, 222, 333}, bm0.ToArray())
			assert.True(t, ok0)

			assert.Equal(t, uint8(1), key1)
			assert.ElementsMatch(t, []uint64{11, 22, 33}, bm1.ToArray())
			assert.True(t, ok1)

			assert.Equal(t, uint8(2), key2)
			assert.ElementsMatch(t, []uint64{111, 222, 333}, bm2.ToArray())
			assert.True(t, ok2)

			assert.Equal(t, uint8(0), key3)
			assert.Empty(t, bm3.ToArray())
			assert.False(t, ok3)

			assert.Equal(t, uint8(0), key4)
			assert.ElementsMatch(t, []uint64{1, 2, 3, 11, 22, 33, 111, 222, 333}, bm4.ToArray())
			assert.True(t, ok4)

			assert.Equal(t, uint8(1), key5)
			assert.ElementsMatch(t, []uint64{11, 22, 33}, bm5.ToArray())
			assert.True(t, ok5)

			assert.Equal(t, uint8(0), key6)
			assert.ElementsMatch(t, []uint64{1, 2, 3, 11, 22, 33, 111, 222, 333}, bm6.ToArray())
			assert.True(t, ok6)
		})
	})

	t.Run("multiple inner cursors", func(t *testing.T) {
		createCursor := func() *CombinedCursor {
			innerCursor1 := newFixedInnerCursor().
				withEntry(0, []uint64{1, 2, 3, 11, 22, 33, 111, 222, 333}, []uint64{}).
				withEntry(1, []uint64{11, 22, 33}, []uint64{}).
				withEntry(2, []uint64{111, 222, 333}, []uint64{})

			innerCursor2 := newFixedInnerCursor().
				withEntry(0, []uint64{4, 5, 6, 44, 55, 66, 444, 555, 666}, []uint64{2, 3, 4, 22, 33, 44, 222, 333, 444}).
				withEntry(1, []uint64{44, 55, 66}, []uint64{44}).
				withEntry(3, []uint64{444, 555, 666}, []uint64{0})

			innerCursor3 := newFixedInnerCursor().
				withEntry(0, []uint64{7, 8, 9, 77, 88, 99, 777, 888, 999}, []uint64{5, 6, 7, 55, 66, 77, 555, 666, 777}).
				withEntry(3, []uint64{77, 88, 99}, []uint64{44}).
				withEntry(4, []uint64{777, 888, 999}, []uint64{0})

			return NewCombinedCursor([]InnerCursor{innerCursor1, innerCursor2, innerCursor3}, logger)
		}

		t.Run("first and nexts", func(t *testing.T) {
			cursor := createCursor()

			key0, bm0, ok0 := cursor.First()
			key1, bm1, ok1 := cursor.Next()
			key2, bm2, ok2 := cursor.Next()
			key3, bm3, ok3 := cursor.Next()
			key4, bm4, ok4 := cursor.Next()
			key5, bm5, ok5 := cursor.Next()
			key6, bm6, ok6 := cursor.Next()

			assert.Equal(t, uint8(0), key0)
			assert.ElementsMatch(t, []uint64{1, 4, 7, 8, 9, 11, 44, 77, 88, 99, 111, 444, 777, 888, 999}, bm0.ToArray())
			assert.True(t, ok0)

			assert.Equal(t, uint8(1), key1)
			assert.ElementsMatch(t, []uint64{11, 44}, bm1.ToArray())
			assert.True(t, ok1)

			assert.Equal(t, uint8(2), key2)
			assert.ElementsMatch(t, []uint64{111}, bm2.ToArray())
			assert.True(t, ok2)

			assert.Equal(t, uint8(3), key3)
			assert.ElementsMatch(t, []uint64{444, 77, 88, 99}, bm3.ToArray())
			assert.True(t, ok3)

			assert.Equal(t, uint8(4), key4)
			assert.ElementsMatch(t, []uint64{777, 888, 999}, bm4.ToArray())
			assert.True(t, ok4)

			assert.Equal(t, uint8(0), key5)
			assert.Empty(t, bm5.ToArray())
			assert.False(t, ok5)

			assert.Equal(t, uint8(0), key6)
			assert.Empty(t, bm6.ToArray())
			assert.False(t, ok6)
		})

		t.Run("only nexts", func(t *testing.T) {
			cursor := createCursor()

			key0, bm0, ok0 := cursor.Next()
			key1, bm1, ok1 := cursor.Next()
			key2, bm2, ok2 := cursor.Next()
			key3, bm3, ok3 := cursor.Next()
			key4, bm4, ok4 := cursor.Next()
			key5, bm5, ok5 := cursor.Next()
			key6, bm6, ok6 := cursor.Next()

			assert.Equal(t, uint8(0), key0)
			assert.ElementsMatch(t, []uint64{1, 4, 7, 8, 9, 11, 44, 77, 88, 99, 111, 444, 777, 888, 999}, bm0.ToArray())
			assert.True(t, ok0)

			assert.Equal(t, uint8(1), key1)
			assert.ElementsMatch(t, []uint64{11, 44}, bm1.ToArray())
			assert.True(t, ok1)

			assert.Equal(t, uint8(2), key2)
			assert.ElementsMatch(t, []uint64{111}, bm2.ToArray())
			assert.True(t, ok2)

			assert.Equal(t, uint8(3), key3)
			assert.ElementsMatch(t, []uint64{444, 77, 88, 99}, bm3.ToArray())
			assert.True(t, ok3)

			assert.Equal(t, uint8(4), key4)
			assert.ElementsMatch(t, []uint64{777, 888, 999}, bm4.ToArray())
			assert.True(t, ok4)

			assert.Equal(t, uint8(0), key5)
			assert.Empty(t, bm5.ToArray())
			assert.False(t, ok5)

			assert.Equal(t, uint8(0), key6)
			assert.Empty(t, bm6.ToArray())
			assert.False(t, ok6)
		})

		t.Run("first, nexts and first again", func(t *testing.T) {
			cursor := createCursor()

			key0, bm0, ok0 := cursor.First()
			key1, bm1, ok1 := cursor.Next()
			key2, bm2, ok2 := cursor.Next()
			key3, bm3, ok3 := cursor.Next()
			key4, bm4, ok4 := cursor.Next()
			key5, bm5, ok5 := cursor.Next()
			key6, bm6, ok6 := cursor.First()
			key7, bm7, ok7 := cursor.Next()
			key8, bm8, ok8 := cursor.First()

			assert.Equal(t, uint8(0), key0)
			assert.ElementsMatch(t, []uint64{1, 4, 7, 8, 9, 11, 44, 77, 88, 99, 111, 444, 777, 888, 999}, bm0.ToArray())
			assert.True(t, ok0)

			assert.Equal(t, uint8(1), key1)
			assert.ElementsMatch(t, []uint64{11, 44}, bm1.ToArray())
			assert.True(t, ok1)

			assert.Equal(t, uint8(2), key2)
			assert.ElementsMatch(t, []uint64{111}, bm2.ToArray())
			assert.True(t, ok2)

			assert.Equal(t, uint8(3), key3)
			assert.ElementsMatch(t, []uint64{444, 77, 88, 99}, bm3.ToArray())
			assert.True(t, ok3)

			assert.Equal(t, uint8(4), key4)
			assert.ElementsMatch(t, []uint64{777, 888, 999}, bm4.ToArray())
			assert.True(t, ok4)

			assert.Equal(t, uint8(0), key5)
			assert.Empty(t, bm5.ToArray())
			assert.False(t, ok5)

			assert.Equal(t, uint8(0), key6)
			assert.ElementsMatch(t, []uint64{1, 4, 7, 8, 9, 11, 44, 77, 88, 99, 111, 444, 777, 888, 999}, bm6.ToArray())
			assert.True(t, ok6)

			assert.Equal(t, uint8(1), key7)
			assert.ElementsMatch(t, []uint64{11, 44}, bm7.ToArray())
			assert.True(t, ok7)

			assert.Equal(t, uint8(0), key8)
			assert.ElementsMatch(t, []uint64{1, 4, 7, 8, 9, 11, 44, 77, 88, 99, 111, 444, 777, 888, 999}, bm8.ToArray())
			assert.True(t, ok8)
		})
	})
}

type fixedInnerCursor struct {
	entries []fixedInnerCursorEntry
	pos     int
}

func newFixedInnerCursor() *fixedInnerCursor {
	return &fixedInnerCursor{
		entries: make([]fixedInnerCursorEntry, 0),
		pos:     0,
	}
}

func (c *fixedInnerCursor) withEntry(key uint8, additions []uint64, deletions []uint64) *fixedInnerCursor {
	c.entries = append(c.entries, fixedInnerCursorEntry{
		key:       key,
		additions: additions,
		deletions: deletions,
	})
	return c
}

func (c *fixedInnerCursor) First() (uint8, roaringset.BitmapLayer, bool) {
	c.pos = 0
	return c.Next()
}

func (c *fixedInnerCursor) Next() (uint8, roaringset.BitmapLayer, bool) {
	if c.pos >= len(c.entries) {
		return 0, roaringset.BitmapLayer{}, false
	}

	defer func() { c.pos++ }()
	return c.entries[c.pos].key,
		roaringset.BitmapLayer{
			Additions: roaringset.NewBitmap(c.entries[c.pos].additions...),
			Deletions: roaringset.NewBitmap(c.entries[c.pos].deletions...),
		},
		true
}

type fixedInnerCursorEntry struct {
	key       uint8
	additions []uint64
	deletions []uint64
}
