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

func TestCondense(t *testing.T) {
	t.Run("And with itself (internal array)", func(t *testing.T) {
		bm := NewBitmap(slice(0, 1000)...)
		for i := 0; i < 10; i++ {
			bm.And(bm)
		}
		bmLen := len(bm.ToBuffer())

		condensed := Condense(bm)
		condensedLen := len(condensed.ToBuffer())

		assert.Greater(t, bmLen, condensedLen)
		assert.ElementsMatch(t, bm.ToArray(), condensed.ToArray())
	})

	t.Run("And with itself (internal bitmap)", func(t *testing.T) {
		bm := NewBitmap(slice(0, 3000)...)
		for i := 0; i < 10; i++ {
			bm.And(bm)
		}
		bmLen := len(bm.ToBuffer())

		condensed := Condense(bm)
		condensedLen := len(condensed.ToBuffer())

		assert.Greater(t, bmLen, condensedLen)
		assert.ElementsMatch(t, bm.ToArray(), condensed.ToArray())
	})

	t.Run("And (internal arrays)", func(t *testing.T) {
		bm1 := NewBitmap(slice(0, 1000)...)
		bm2 := NewBitmap(slice(500, 1500)...)
		bm := bm1.Clone()
		bm.And(bm2)
		bmLen := len(bm.ToBuffer())

		condensed := Condense(bm)
		condensedLen := len(condensed.ToBuffer())

		assert.Greater(t, bmLen, condensedLen)
		assert.ElementsMatch(t, bm.ToArray(), condensed.ToArray())
	})

	t.Run("And (internal bitmaps)", func(t *testing.T) {
		bm1 := NewBitmap(slice(0, 4000)...)
		bm2 := NewBitmap(slice(1000, 5000)...)
		bm := bm1.Clone()
		bm.And(bm2)
		bmLen := len(bm.ToBuffer())

		condensed := Condense(bm)
		condensedLen := len(condensed.ToBuffer())

		assert.Greater(t, bmLen, condensedLen)
		assert.ElementsMatch(t, bm.ToArray(), condensed.ToArray())
	})

	t.Run("And (internal bitmaps to bitmap with few elements)", func(t *testing.T) {
		// this is not optimal. Internally elements will be stored in bitmap,
		// though they would easily fit into array
		bm1 := NewBitmap(slice(0, 4000)...)
		bm2 := NewBitmap(slice(1000, 5000)...)
		bm := bm1.Clone()
		bm.And(bm2)
		bmLen := len(bm.ToBuffer())

		condensed := Condense(bm)
		condensedLen := len(condensed.ToBuffer())

		assert.Greater(t, bmLen, condensedLen)
		assert.ElementsMatch(t, bm.ToArray(), condensed.ToArray())
	})

	t.Run("Remove (array)", func(t *testing.T) {
		bm := NewBitmap(slice(0, 1000)...)
		for i := uint64(2); i < 1000; i++ {
			bm.Remove(i)
		}
		bmLen := len(bm.ToBuffer())

		condensed := Condense(bm)
		condensedLen := len(condensed.ToBuffer())

		assert.Greater(t, bmLen, condensedLen)
		assert.ElementsMatch(t, bm.ToArray(), condensed.ToArray())
	})

	t.Run("Remove (bitmap)", func(t *testing.T) {
		bm := NewBitmap(slice(0, 100_000)...)
		for i := uint64(10_000); i < 100_000; i++ {
			bm.Remove(i)
		}
		bmLen := len(bm.ToBuffer())

		condensed := Condense(bm)
		condensedLen := len(condensed.ToBuffer())

		assert.Greater(t, bmLen, condensedLen)
		assert.ElementsMatch(t, bm.ToArray(), condensed.ToArray())
	})
}

func slice(from, to uint64) []uint64 {
	len := to - from
	s := make([]uint64, len)
	for i := uint64(0); i < len; i++ {
		s[i] = from + i
	}
	return s
}
