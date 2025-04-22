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
	"github.com/stretchr/testify/require"
)

func TestBitmap_Condense(t *testing.T) {
	t.Run("And with itself (internal array)", func(t *testing.T) {
		bm := NewBitmap(slice(0, 1000)...)
		for i := 0; i < 10; i++ {
			bm.And(bm)
		}
		bmLen := len(bm.ToBuffer())

		condensed := Condense(bm)
		condensedLen := len(condensed.ToBuffer())

		// As of sroar 0.0.5 "And" merge is optimized not to expand
		// existing bitmap when not needed. Therefore calling Condense
		// does not guarantee decreasing bitmap size
		assert.GreaterOrEqual(t, bmLen, condensedLen)
		assert.ElementsMatch(t, bm.ToArray(), condensed.ToArray())
	})

	t.Run("And with itself (internal bitmap)", func(t *testing.T) {
		bm := NewBitmap(slice(0, 5000)...)
		for i := 0; i < 10; i++ {
			bm.And(bm)
		}
		bmLen := len(bm.ToBuffer())

		condensed := Condense(bm)
		condensedLen := len(condensed.ToBuffer())

		// As of sroar 0.0.5 "And" merge is optimized not to expand
		// existing bitmap when not needed. Therefore calling Condense
		// does not guarantee decreasing bitmap size
		assert.GreaterOrEqual(t, bmLen, condensedLen)
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
		bm1 := NewBitmap(slice(0, 5000)...)
		bm2 := NewBitmap(slice(1000, 6000)...)
		bm := bm1.Clone()
		bm.And(bm2)
		bmLen := len(bm.ToBuffer())

		condensed := Condense(bm)
		condensedLen := len(condensed.ToBuffer())

		// As of sroar 0.0.5 "And" merge is optimized not to expand
		// existing bitmap when not needed. Therefore calling Condense
		// does not guarantee decreasing bitmap size
		assert.GreaterOrEqual(t, bmLen, condensedLen)
		assert.ElementsMatch(t, bm.ToArray(), condensed.ToArray())
	})

	t.Run("And (internal bitmaps to bitmap with few elements)", func(t *testing.T) {
		bm1 := NewBitmap(slice(0, 5000)...)
		bm2 := NewBitmap(slice(4000, 9000)...)
		bm := bm1.Clone()
		bm.And(bm2)
		bmLen := len(bm.ToBuffer())

		condensed := Condense(bm)
		condensedLen := len(condensed.ToBuffer())

		// As of sroar 0.0.5 "And" merge is optimized not to expand
		// existing bitmap when not needed. Therefore calling Condense
		// does not guarantee decreasing bitmap size
		assert.GreaterOrEqual(t, bmLen, condensedLen)
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

func TestBitmapFactory(t *testing.T) {
	maxId := uint64(10)
	maxIdGetter := func() uint64 { return maxId }
	bmf := NewBitmapFactory(NewBitmapBufPoolNoop(), maxIdGetter)

	t.Run("prefilled bitmap includes increment", func(t *testing.T) {
		expPrefilledMaxId := maxId + defaultIdIncrement
		expPrefilledCardinality := int(maxId + defaultIdIncrement + 1)

		bm, release := bmf.GetBitmap()
		defer release()

		require.NotNil(t, bm)
		assert.Equal(t, expPrefilledMaxId, bmf.prefilled.Maximum())
		assert.Equal(t, expPrefilledCardinality, bmf.prefilled.GetCardinality())
		assert.Equal(t, maxId, bm.Maximum())
		assert.Equal(t, int(maxId)+1, bm.GetCardinality())
	})

	t.Run("maxId increased up to increment threshold does not change internal bitmap", func(t *testing.T) {
		expPrefilledMaxId := bmf.prefilled.Maximum()

		maxId += 10
		bm1, release1 := bmf.GetBitmap()
		defer release1()

		require.NotNil(t, bm1)
		assert.Equal(t, expPrefilledMaxId, bmf.prefilled.Maximum())
		assert.Equal(t, int(expPrefilledMaxId)+1, bmf.prefilled.GetCardinality())
		assert.Equal(t, maxId, bm1.Maximum())
		assert.Equal(t, int(maxId)+1, bm1.GetCardinality())

		maxId += (defaultIdIncrement - 10)
		bm2, release2 := bmf.GetBitmap()
		defer release2()

		require.NotNil(t, bm2)
		assert.Equal(t, expPrefilledMaxId, bmf.prefilled.Maximum())
		assert.Equal(t, int(expPrefilledMaxId)+1, bmf.prefilled.GetCardinality())
		assert.Equal(t, maxId, bm2.Maximum())
		assert.Equal(t, int(maxId)+1, bm2.GetCardinality())
	})

	t.Run("maxId surpasses increment threshold changes internal bitmap", func(t *testing.T) {
		maxId += 1
		expPrefilledMaxId := maxId + defaultIdIncrement

		bm, release := bmf.GetBitmap()
		defer release()

		require.NotNil(t, bm)
		assert.Equal(t, expPrefilledMaxId, bmf.prefilled.Maximum())
		assert.Equal(t, int(expPrefilledMaxId)+1, bmf.prefilled.GetCardinality())
		assert.Equal(t, maxId, bm.Maximum())
		assert.Equal(t, int(maxId)+1, bm.GetCardinality())
	})
}
