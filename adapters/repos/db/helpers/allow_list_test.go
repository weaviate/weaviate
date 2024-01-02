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

package helpers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
)

func TestAllowList(t *testing.T) {
	t.Run("allowlist created with no values", func(t *testing.T) {
		al := NewAllowList()

		assert.Equal(t, 0, al.Len())
		assert.True(t, al.IsEmpty())

		assert.Equal(t, uint64(0), al.Min())
		assert.Equal(t, uint64(0), al.Max())
	})

	t.Run("allowlist created with initial values", func(t *testing.T) {
		al := NewAllowList(1, 2, 3)

		assert.Equal(t, 3, al.Len())
		assert.False(t, al.IsEmpty())

		assert.True(t, al.Contains(1))
		assert.True(t, al.Contains(2))
		assert.True(t, al.Contains(3))

		assert.Equal(t, uint64(1), al.Min())
		assert.Equal(t, uint64(3), al.Max())
	})

	t.Run("allowlist with inserted values", func(t *testing.T) {
		al := NewAllowList(1, 2, 3)
		al.Insert(4, 5)

		assert.Equal(t, 5, al.Len())
		assert.False(t, al.IsEmpty())

		assert.True(t, al.Contains(1))
		assert.True(t, al.Contains(2))
		assert.True(t, al.Contains(3))
		assert.True(t, al.Contains(4))
		assert.True(t, al.Contains(5))

		assert.Equal(t, uint64(1), al.Min())
		assert.Equal(t, uint64(5), al.Max())
	})

	t.Run("allowlist exported to slice", func(t *testing.T) {
		al := NewAllowList(1, 2, 3)
		al.Insert(4, 5)

		assert.ElementsMatch(t, []uint64{1, 2, 3, 4, 5}, al.Slice())
	})

	t.Run("allowlist deepcopy", func(t *testing.T) {
		al := NewAllowList(1, 2, 3)
		copy := al.DeepCopy()
		al.Insert(4, 5)

		assert.Equal(t, 5, al.Len())
		assert.False(t, al.IsEmpty())

		assert.True(t, al.Contains(1))
		assert.True(t, al.Contains(2))
		assert.True(t, al.Contains(3))
		assert.True(t, al.Contains(4))
		assert.True(t, al.Contains(5))

		assert.Equal(t, uint64(1), al.Min())
		assert.Equal(t, uint64(5), al.Max())

		assert.Equal(t, 3, copy.Len())
		assert.False(t, copy.IsEmpty())

		assert.True(t, copy.Contains(1))
		assert.True(t, copy.Contains(2))
		assert.True(t, copy.Contains(3))

		assert.Equal(t, uint64(1), copy.Min())
		assert.Equal(t, uint64(3), copy.Max())
	})

	t.Run("allowlist created from bitmap", func(t *testing.T) {
		bm := roaringset.NewBitmap(1, 2, 3)

		al := NewAllowListFromBitmap(bm)
		bm.SetMany([]uint64{4, 5})

		assert.Equal(t, 5, al.Len())
		assert.False(t, al.IsEmpty())

		assert.True(t, al.Contains(1))
		assert.True(t, al.Contains(2))
		assert.True(t, al.Contains(3))
		assert.True(t, al.Contains(4))
		assert.True(t, al.Contains(5))

		assert.Equal(t, uint64(1), al.Min())
		assert.Equal(t, uint64(5), al.Max())
	})

	t.Run("allowlist created from bitmap deepcopy", func(t *testing.T) {
		bm := roaringset.NewBitmap(1, 2, 3)

		al := NewAllowListFromBitmapDeepCopy(bm)
		bm.SetMany([]uint64{4, 5})

		assert.Equal(t, 3, al.Len())
		assert.False(t, al.IsEmpty())

		assert.True(t, al.Contains(1))
		assert.True(t, al.Contains(2))
		assert.True(t, al.Contains(3))

		assert.Equal(t, uint64(1), al.Min())
		assert.Equal(t, uint64(3), al.Max())
	})
}

func TestAllowList_Iterator(t *testing.T) {
	t.Run("empty bitmap iterator", func(t *testing.T) {
		it := NewAllowList().Iterator()

		id1, ok1 := it.Next()
		id2, ok2 := it.Next()

		assert.Equal(t, 0, it.Len())
		assert.False(t, ok1)
		assert.Equal(t, uint64(0), id1)
		assert.False(t, ok2)
		assert.Equal(t, uint64(0), id2)
	})

	t.Run("iterating step by step", func(t *testing.T) {
		it := NewAllowList(3, 2, 1).Iterator()

		id1, ok1 := it.Next()
		id2, ok2 := it.Next()
		id3, ok3 := it.Next()
		id4, ok4 := it.Next()

		assert.Equal(t, 3, it.Len())
		assert.True(t, ok1)
		assert.Equal(t, uint64(1), id1)
		assert.True(t, ok2)
		assert.Equal(t, uint64(2), id2)
		assert.True(t, ok3)
		assert.Equal(t, uint64(3), id3)
		assert.False(t, ok4)
		assert.Equal(t, uint64(0), id4)
	})

	t.Run("iterating in loop", func(t *testing.T) {
		it := NewAllowList(3, 2, 1).Iterator()
		ids := []uint64{}

		for id, ok := it.Next(); ok; id, ok = it.Next() {
			ids = append(ids, id)
		}

		assert.Equal(t, 3, it.Len())
		assert.Equal(t, []uint64{1, 2, 3}, ids)
	})
}

func TestAllowList_LimitedIterator(t *testing.T) {
	t.Run("empty bitmap iterator", func(t *testing.T) {
		it := NewAllowList().LimitedIterator(2)

		id1, ok1 := it.Next()
		id2, ok2 := it.Next()

		assert.Equal(t, 0, it.Len())
		assert.False(t, ok1)
		assert.Equal(t, uint64(0), id1)
		assert.False(t, ok2)
		assert.Equal(t, uint64(0), id2)
	})

	t.Run("iterating step by step (higher limit)", func(t *testing.T) {
		it := NewAllowList(3, 2, 1).LimitedIterator(4)

		id1, ok1 := it.Next()
		id2, ok2 := it.Next()
		id3, ok3 := it.Next()
		id4, ok4 := it.Next()

		assert.Equal(t, 3, it.Len())
		assert.True(t, ok1)
		assert.Equal(t, uint64(1), id1)
		assert.True(t, ok2)
		assert.Equal(t, uint64(2), id2)
		assert.True(t, ok3)
		assert.Equal(t, uint64(3), id3)
		assert.False(t, ok4)
		assert.Equal(t, uint64(0), id4)
	})

	t.Run("iterating step by step (equal limit)", func(t *testing.T) {
		it := NewAllowList(3, 2, 1).LimitedIterator(3)

		id1, ok1 := it.Next()
		id2, ok2 := it.Next()
		id3, ok3 := it.Next()
		id4, ok4 := it.Next()

		assert.Equal(t, 3, it.Len())
		assert.True(t, ok1)
		assert.Equal(t, uint64(1), id1)
		assert.True(t, ok2)
		assert.Equal(t, uint64(2), id2)
		assert.True(t, ok3)
		assert.Equal(t, uint64(3), id3)
		assert.False(t, ok4)
		assert.Equal(t, uint64(0), id4)
	})

	t.Run("iterating step by step (lower limit)", func(t *testing.T) {
		it := NewAllowList(3, 2, 1).LimitedIterator(2)

		id1, ok1 := it.Next()
		id2, ok2 := it.Next()
		id3, ok3 := it.Next()

		assert.Equal(t, 2, it.Len())
		assert.True(t, ok1)
		assert.Equal(t, uint64(1), id1)
		assert.True(t, ok2)
		assert.Equal(t, uint64(2), id2)
		assert.False(t, ok3)
		assert.Equal(t, uint64(0), id3)
	})

	t.Run("iterating in loop (higher limit)", func(t *testing.T) {
		it := NewAllowList(3, 2, 1).LimitedIterator(4)
		ids := []uint64{}

		for id, ok := it.Next(); ok; id, ok = it.Next() {
			ids = append(ids, id)
		}

		assert.Equal(t, 3, it.Len())
		assert.Equal(t, []uint64{1, 2, 3}, ids)
	})

	t.Run("iterating in loop (equal limit)", func(t *testing.T) {
		it := NewAllowList(3, 2, 1).LimitedIterator(3)
		ids := []uint64{}

		for id, ok := it.Next(); ok; id, ok = it.Next() {
			ids = append(ids, id)
		}

		assert.Equal(t, 3, it.Len())
		assert.Equal(t, []uint64{1, 2, 3}, ids)
	})

	t.Run("iterating in loop (lower limit)", func(t *testing.T) {
		it := NewAllowList(3, 2, 1).LimitedIterator(2)
		ids := []uint64{}

		for id, ok := it.Next(); ok; id, ok = it.Next() {
			ids = append(ids, id)
		}

		assert.Equal(t, 2, it.Len())
		assert.Equal(t, []uint64{1, 2}, ids)
	})
}
