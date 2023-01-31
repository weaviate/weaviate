//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
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
	})

	t.Run("allowlist created with initial values", func(t *testing.T) {
		al := NewAllowList(1, 2, 3)

		assert.Equal(t, 3, al.Len())
		assert.True(t, al.Contains(1))
		assert.True(t, al.Contains(2))
		assert.True(t, al.Contains(3))
		assert.False(t, al.IsEmpty())
	})

	t.Run("allowlist with inserted values", func(t *testing.T) {
		al := NewAllowList(1, 2, 3)
		al.Insert(4, 5)

		assert.Equal(t, 5, al.Len())
		assert.True(t, al.Contains(1))
		assert.True(t, al.Contains(2))
		assert.True(t, al.Contains(3))
		assert.True(t, al.Contains(4))
		assert.True(t, al.Contains(5))
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
		assert.Equal(t, 3, copy.Len())
		assert.True(t, copy.Contains(1))
		assert.True(t, copy.Contains(2))
		assert.True(t, copy.Contains(3))
	})

	t.Run("allowlist created from bitmap", func(t *testing.T) {
		bm := roaringset.NewBitmap(1, 2, 3)

		al := NewAllowListFromBitmap(bm)
		bm.SetMany([]uint64{4, 5})

		assert.Equal(t, 5, al.Len())
	})

	t.Run("allowlist created from bitmap deepcopy", func(t *testing.T) {
		bm := roaringset.NewBitmap(1, 2, 3)

		al := NewAllowListFromBitmapDeepCopy(bm)
		bm.SetMany([]uint64{4, 5})

		assert.Equal(t, 3, al.Len())
	})
}

func TestAllowList_Iterator(t *testing.T) {
	t.Run("empty bitmap iterator", func(t *testing.T) {
		it := NewAllowList().Iterator()

		id1, ok1 := it.Next()
		id2, ok2 := it.Next()

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

		assert.True(t, ok1)
		assert.Equal(t, uint64(1), id1)
		assert.True(t, ok2)
		assert.Equal(t, uint64(2), id2)
		assert.True(t, ok3)
		assert.Equal(t, uint64(3), id3)
		assert.False(t, ok4)
		assert.Equal(t, uint64(0), id4)

		itWith0 := NewAllowList(2, 1, 0).Iterator()

		id1, ok1 = itWith0.Next()
		id2, ok2 = itWith0.Next()
		id3, ok3 = itWith0.Next()
		id4, ok4 = itWith0.Next()

		assert.True(t, ok1)
		assert.Equal(t, uint64(0), id1)
		assert.True(t, ok2)
		assert.Equal(t, uint64(1), id2)
		assert.True(t, ok3)
		assert.Equal(t, uint64(2), id3)
		assert.False(t, ok4)
		assert.Equal(t, uint64(0), id4)
	})

	t.Run("iterating with for loop", func(t *testing.T) {
		it := NewAllowList(3, 2, 1).Iterator()
		ids := []uint64{}

		for id, ok := it.Next(); ok; id, ok = it.Next() {
			ids = append(ids, id)
		}

		assert.Equal(t, []uint64{1, 2, 3}, ids)

		itWith0 := NewAllowList(2, 1, 0).Iterator()
		ids = []uint64{}

		for id, ok := itWith0.Next(); ok; id, ok = itWith0.Next() {
			ids = append(ids, id)
		}

		assert.Equal(t, []uint64{0, 1, 2}, ids)
	})
}
